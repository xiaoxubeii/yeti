#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'tim'
from yeti.db.model import CostDaily, CostHourly, CostMonthly, BillPollerHistory
from sqlalchemy import and_, or_
from yeti.api import db
from yeti.common.utils import check_ip


class BaseDAO(object):
    @classmethod
    def gt_(cls, **kwargs):
        key = kwargs.keys()[0]
        return getattr(cls.model_class, key) > kwargs[key]

    @classmethod
    def ge_(cls, **kwargs):
        key = kwargs.keys()[0]
        return getattr(cls.model_class, key) >= kwargs[key]

    @classmethod
    def lt_(cls, **kwargs):
        key = kwargs.keys()[0]
        return getattr(cls.model_class, key) < kwargs[key]

    @classmethod
    def le_(cls, **kwargs):
        key = kwargs.keys()[0]
        return getattr(cls.model_class, key) <= kwargs[key]

    @classmethod
    def not_(cls, **kwargs):
        key = kwargs.keys()[0]
        return getattr(cls.model_class, key) != kwargs[key]

    @classmethod
    def and_(cls, *args, **kwargs):
        args = list(args)
        for k, v in kwargs.iteritems():
            args.append(getattr(cls.model_class, k) == v)

        return and_(*filter(lambda x: x is not None, args))

    @classmethod
    def or_(cls, *args, **kwargs):
        args = list(args)
        for k, v in kwargs.iteritems():
            args.append(getattr(cls.model_class, k) == v)
        return or_(*args)

    @classmethod
    def in_(cls, kv):
        key = getattr(cls.model_class, kv['key'])
        return key.in_(kv['value'])

    @classmethod
    def have_keys(cls, keys):
        return [k for k in keys if k in cls.model_class.attrs()]

    @classmethod
    def list(cls, clause=None, search=None, is_page=False, orders=None, **kwargs):
        try:
            kwargs.pop('_')
        except KeyError:
            pass

        if search:
            attrs = cls.model_class.attrs()
            search_clause = []
            for s in search:
                search_clause.append(cls.or_(*
                                             [getattr(getattr(cls.model_class, a), 'like')(
                                                 '%s%s%s' % ('%', s, '%')) for a
                                              in attrs]))

            search_clause = cls.or_(*search_clause)
            clause = cls.and_(search_clause, clause)

        order_args = []
        if orders:
            for k, v in orders.iteritems():
                if k == 'pdl':
                    k = 'pdl_id'
                order_args.append(getattr(getattr(cls.model_class, k), v)())

        if is_page:
            page = 1
            per_page = 20
            if 'page' in kwargs:
                page = int(kwargs.pop('page'))
            if 'per_page' in kwargs:
                per_page = int(kwargs.pop('per_page'))

            if order_by_multi_model_name:
                if order_by_multi_model_name == ElasticIP:
                    result = cls.model_class.query.join(ElasticIP, ElasticIP.server_id == cls.model_class.id). \
                        filter(clause, **kwargs).order_by(*order_args).paginate(page, per_page)
                elif order_by_multi_model_name == Pool:
                    result = cls.model_class.query.join(Pool, Pool.id == cls.model_class.pool_id). \
                        filter(clause, **kwargs).order_by(*order_args).paginate(page, per_page)
                elif order_by_multi_model_name == IP:
                    result = cls.model_class.query.join(IP, IP.associate_dev_id == cls.model_class.id). \
                        filter(clause, **kwargs).order_by(*order_args).paginate(page, per_page)

                if result.total == 0:
                    result = cls.filter(clause, **kwargs). \
                        order_by(getattr(getattr(cls.model_class, 'created_at'), 'asc')()).paginate(page, per_page)
            else:
                result = cls.filter(clause, **kwargs).order_by(*order_args).paginate(page, per_page)

            return {'page': result.page, 'pages': result.pages, 'per_page': result.per_page, 'total': result.total,
                    'items': [m.to_dict() for m in result.items]}
        else:
            return [m.to_dict() for m in cls.filter(clause, **kwargs).order_by(*order_args).all()]

    @classmethod
    def filter(cls, clause=None, **kwargs):
        if clause is not None:
            return cls.model_class.query.filter(cls.and_(clause, **kwargs))
        else:
            return cls.model_class.query.filter_by(**kwargs)

    @classmethod
    def like(cls, clause, **kwargs):
        if clause is None and kwargs.get('region') == 'ALL':
            kwargs.pop('region')
        arg = kwargs.pop('arg')
        search_by = kwargs.pop('search_by')
        model_field = getattr(cls.model_class, search_by)
        return cls.model_class.query.filter(model_field.like('%' + arg + '%'))

    @classmethod
    def count(cls, clause=None, **kwargs):
        return cls.filter(clause, **kwargs).count()

    @classmethod
    def get(cls, clause, id):
        result = cls.filter(clause, id=id).first()
        if result:
            return result.to_dict()

    @classmethod
    def delete(cls, clause, **kwargs):
        cls.filter(clause, **kwargs).delete()
        cls.model_class.flush()

    @classmethod
    def update(cls, clause, id, **kwargs):
        cls.filter(clause, id=id).update(kwargs)
        cls.model_class.flush()

    @classmethod
    def create(cls, **kwargs):
        model = cls.model_class(**kwargs)
        model.add()
        model.flush()
        return model.to_dict()

    @classmethod
    def bulk_create(cls, list):
        cls.model_class.bulk_save(list)

    @classmethod
    def bulk_update(cls, list):
        cls.model_class.bulk_update(list)

    @classmethod
    def rollback(cls):
        cls.model_class.rollback()

    @classmethod
    def commit(cls):
        cls.model_class.commit()

    @classmethod
    def list_ret_attrs(cls, *args):
        attrs = []
        for a in args:
            attrs.append(getattr(cls.model_class, a))
        return cls.model_class.query.with_entities(*attrs).all()


class BaseCostDAO(object):
    name = None

    @classmethod
    def get_data(cls, **kwargs):
        engine = db.get_engine(db.get_app(), bind='psql')
        groupby = kwargs.get('group_by', '')
        if groupby:
            if groupby.startswith('tag:'):
                groupby = ', tv.%s' % groupby.replace('tag:', '')
            else:
                f_gb = ', t.%s'
                groupby = f_gb % groupby

        filter = kwargs.get('filter', '')
        where = ''
        if filter:
            where = []
            if 'tag' in filter:
                tag = filter.pop('tag')
                where = map(lambda t: "tv.%s='%s'" % t, tag.iteritems())

            where.extend(map(lambda t: "t.%s='%s'" % t, filter.iteritems()))
            where = ' and '.join(where)
            where = ' where %s ' % where

        time_bt = kwargs.get('time_bt')
        if time_bt:
            time_where = "t.start>='%s' and t.start<='%s'"
            time_where = time_where % time_bt
            if where != '':
                where = '%s and %s' % (where, time_where)
            else:
                where = ' where %s ' % time_where

        sql = 'select t.start, sum(t.cost) as cost %s from %s t ' \
              'left join tag_view tv on t.tag_id = tv.id ' \
              ' %s ' \
              'group by t.start %s ' \
              'order by t.start asc' % (groupby, cls.name, where, groupby)
        result = engine.execute(sql)
        return result.fetchall()


class CostMonthlyDAO(BaseCostDAO, BaseDAO):
    model_class = CostMonthly
    name = 'cost_monthly'


class CostYearlyDAO(BaseCostDAO, BaseDAO):
    model_class = CostYearly
    name = 'cost_yearly'


class CostDailyDAO(BaseCostDAO, BaseDAO):
    model_class = CostDaily
    name = 'cost_daily'


class CostHourlyDAO(BaseCostDAO, BaseDAO):
    model_class = CostHourly
    name = 'cost_hourly'

    @classmethod
    def list_bt(cls, start, end, **kwargs):
        return cls.list(cls.and_(cls.ge_(start=start), cls.le_(start=end)), **kwargs)


class BillPollerHistoryDAO(BaseDAO):
    model_class = BillPollerHistory


class ResourceDAO(BaseDAO):
    model_class = Resource
