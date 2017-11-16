#!/usr/bin/env python
# -*- coding: utf-8 -*-
from yeti.api import BaseResource
from yeti.common.utils import copy_dict
from yeti.service.tag import Tag as TagServ
from flask import request
import json


class Tag(BaseResource):
    RESOURCE_NAME = 'tags'

    def __init__(self):
        self.tag_serv = TagServ()

    def post(self, context):
        tag = request.get_json()

        return self.tag_serv.create(context, tag, need_policy=True)

    def get(self, context, id=None):
        return_keys = ['id', 'resource_id', 'resource_ins_id', 'resource_type', 'deleted', 'deleted_at', 'tags']
        if id:
            result = self.tag_serv.get(id)
            return copy_dict(result, return_keys)
        else:
            q_args = dict(request.args)
            search = None
            order_by = None
            if 'search' in q_args:
                search = q_args.pop('search')

            if 'order_by' in q_args:
                order_by = q_args.pop('order_by')
                order_by = (o.rsplit('_', 1) for o in order_by)
                order_by = dict(order_by)

            is_page = True
            if 'is_page' in q_args:
                is_page = q_args.pop('is_page')
                if is_page[0] == "False":
                    is_page = False
            format_qargs = self._get_query_args(q_args)

            result = self.tag_serv.list(context, need_policy=True, clause=None, search=None,
                                        orders=order_by, is_page=is_page, **format_qargs)
            result['items'] = copy_dict(result['items'], return_keys)
            return result

    def put(self, context):
        data = request.get_json()
        self.tag_serv.update(context, data, need_policy=True)

    def delete(self, context, id):
        self.tag_serv.delete(context, id, need_policy=True)