#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'tim'
import uuid
from yeti.api import db
from sqlalchemy.orm import relation
from datetime import datetime
from yeti.common.utils import resource_id
import inspect
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy.sql.annotation import Annotated
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm.mapper import class_mapper


class BaseModel(db.Model):
    __abstract__ = True

    @classmethod
    def attrs(cls):
        ats = []
        for v1, v2 in inspect.getmembers(cls, lambda a: not inspect.isroutine(a)):
            if isinstance(v2, InstrumentedAttribute) and isinstance(v2.expression, Annotated):
                ats.append(v1)

        return ats

    def to_dict(self):
        column_name_list = [
            value[0] for value in self._sa_instance_state.attrs.items()
            ]

        d = {}
        for c in column_name_list:
            value = getattr(self, c, None)
            if isinstance(value, list):
                new_vals = []
                for v in value:
                    new_vals.append(v.to_dict())

                d[c] = new_vals
            else:
                if hasattr(value, 'to_dict'):
                    d[c] = value.to_dict()
                else:
                    d[c] = value

        return d

    @staticmethod
    def commit():
        db.session.commit()

    def add(self):
        db.session.add(self)

    @classmethod
    def bulk_save(cls, list):
        db.session.bulk_insert_mappings(cls, list)

    @classmethod
    def bulk_update(cls, list):
        db.session.bulk_update_mappings(cls, list)

    @staticmethod
    def rollback():
        db.session.rollback()

    @staticmethod
    def flush():
        db.session.flush()


class Resource(BaseModel):
    resource_type = None
    id = db.Column(db.String(100), default=resource_id(resource_type), primary_key=True)
    instance_id = db.Column(db.String(100))
    name = db.Column(db.String(100))
    created_at = db.Column(db.DateTime, default=datetime.now)
    deleted_at = db.Column(db.DateTime)
    deleted = db.Column(db.Integer)
    region_id = db.Column(db.String(100))
    pool_id = db.Column(db.String(100))
    tag_id = db.Column(db.String(100))


class Tag(BaseModel):
    __bind_key__ = 'psql'
    id = db.Column(db.String(100), default=resource_id('tag'), primary_key=True)
    resource_id = db.Column(db.String(100))
    resource_ins_id = db.Column(db.String(100))
    resource_type = db.Column(db.String(50))
    deleted = db.Column(db.Integer)  # 1: deleted
    deleted_at = db.Column(db.DateTime)
    tags = db.Column('tags', JSONB)

    @classmethod
    def list_tags(cls):
        return db.session.execute('select distinct(kv.*) from tag,jsonb_each(tag.tags) as kv',
                                  mapper=class_mapper(cls))

    @classmethod
    def list_tag_keys(cls):
        return db.session.execute('select distinct(kv.key) from tag,jsonb_each(tag.tags) as kv',
                                  mapper=class_mapper(cls))

    @classmethod
    def get_tag(cls, resource_id, resource_type):
        return db.session.execute("select t.* from tag, jsonb_each(tags) as t\
                                  where tag.resource_id = '%s' and tag.resource_type = '%s'" % (
            resource_id, resource_type),
                                  mapper=class_mapper(cls))


class BaseCost(BaseModel):
    __abstract__ = True
    consolidate_id = db.Column(db.String(100), primary_key=True)
    resource_type = db.Column(db.String(50))
    resource_ins_id = db.Column(db.String(1000))
    resource_id = db.Column(db.String(100))
    usage_type = db.Column(db.String(100))
    quantity = db.Column(db.Float)
    rate = db.Column(db.Float)
    cost = db.Column(db.Float)
    operation = db.Column(db.String(100))
    # detail = db.Column(JSONB)
    start = db.Column(db.DateTime)
    end = db.Column(db.DateTime)
    region_id = db.Column(db.String(50))
    pool_id = db.Column(db.String(50))
    record_id = db.Column(db.String(100))
    record_time = db.Column(db.DateTime)
    tag_id = db.Column(db.String(100))


class CostHourly(BaseCost):
    pass


class CostDaily(BaseCost):
    pass


class CostMonthly(BaseCost):
    pass


class CostYearly(BaseCost):
    pass


class BillPollerHistory(BaseModel):
    id = db.Column(db.String(100), default=resource_id('bph', 16), primary_key=True)
    region_id = db.Column(db.String(100))
    pool_id = db.Column(db.String(100))
    finish_at = db.Column(db.DateTime)
    bill_time = db.Column(db.DateTime)
