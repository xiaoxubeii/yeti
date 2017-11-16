#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'tim'
from yeti.provider import BaseProvider
import json
from yeti import CONF
from yeti.service.policy_validator import PolicyValidator
from yeti.common.exception import PermissionDenied
from yeti.common.const import Const as const
import requests
from yeti.common.utils import import_class
from yeti.common.log import LOG


class BaseService(object):
    dao_class = None
    resource_type_name = None
    namespace = None

    def _get_context_id(self, context):
        def get_id(attrname):
            attr = getattr(context, attrname)
            if attr:
                return attr['id']

        new_did = {}
        new_did['user_id'] = get_id('current_user')
        new_did['account_id'] = get_id('current_account')
        new_did['pool_id'] = get_id('pool')
        new_did['region_id'] = get_id('region')
        return new_did

    def _inject_metadata(self, context, condition):
        if condition:
            con_str = json.dumps(condition)
            con_str %= self._get_context_id(context)
            return json.loads(con_str)
        else:
            con_str = self._get_context_id(context)
            return con_str

    def list(self, context=None, clause=None, search=None, is_page=False, orders=False, need_policy=False, **kwargs):
        if need_policy:
            # clause = self.dao.and_(clause, self._validate_policy(context, 'list', '*'))
            clause = self.dao.and_(clause, self.validate_iam(context, const.METHOD_LIST))

        return self.dao.list(clause=clause, search=search, is_page=is_page, orders=orders, **kwargs)

    def oper_map(self, field, oper, value):
        if oper:
            t = {'str': str,
                 'int': int}
            ks = oper.split('_')
            return getattr(field, '__%s__' % ks[1])(t[ks[0]](value))
        else:
            return getattr(field, '__eq__')(value)

    def _oper_convert(self, con_kv):
        oper = con_kv[0]
        values = con_kv[1]

        val_kv = values.items()[0]
        field = val_kv[0].rsplit(':', 1)[-1]
        if hasattr(self.dao.model_class, field):
            field = getattr(self.dao.model_class, field)
            val_v = val_kv[1]
            return self.oper_map(field, oper, val_v)

    def _oper_convert_new(self, dic):
        if hasattr(self.dao.model_class, dic.keys()[0]):
            field = getattr(self.dao.model_class, dic.keys()[0])
            val_v = dic[dic.keys()[0]]
            return self.oper_map(field, None, val_v)

    def validate_iam(self, context, method):
        # method_name = method + self.dao_class.__name__.replace("DAO", '')
        # if method == 'List':
        #     method_name = method_name + 's'
        #
        # headers = {
        #     'Content-type': 'application/json',
        #     'App-Id': CONF.iam_app_id,
        #     'Api-Key': CONF.iam_app_key
        # }
        # data = {"username": context.current_user['id'], "action": method_name}
        # resp = requests.get(CONF.iam_url+'/permission', params=data,
        #                     headers=headers)
        # if resp.status_code != 200:
        #     raise PermissionDenied(resp.status_code)
        # else:
        condition = {"region_id": "%(region_id)s"}
        conditions = self._inject_metadata(context, condition)
        con_clause = []
        mapper = self._oper_convert_new(conditions)
        con_clause.append(mapper)

        return self.dao.and_(*con_clause)

    def _validate_policy(self, context, method, resource):
        action = '%s:%s_%s' % (self.namespace, method, self.resource_type_name)

        condition = PolicyValidator.validate(context, action,
                                             '%s:%s:%s:%s' % (
                                                 CONF.platform_code, self.namespace, self.resource_type_name, resource))

        if condition:
            conditions = self._inject_metadata(context, condition)['condition']
            con_clause = []
            for con in conditions:
                con_clause.append(self._oper_convert(con.items()[0]))

            return self.dao.and_(*con_clause)

        return None

    def _fill_context(self, context, entity):
        keys = self.dao.have_keys(('account_id', 'create_user_id', 'pool_id', 'region_id'))
        if keys:
            if 'account_id' in keys and hasattr(context, 'current_account'):
                entity['account_id'] = context.current_account['id']
            if 'create_user_id' in keys and hasattr(context, 'current_user'):
                entity['create_user_id'] = context.current_user['id']
            if 'pool_id' in keys and hasattr(context, 'pool'):
                entity['pool_id'] = context.pool['id']
            if 'region_id' in keys and hasattr(context, 'region'):
                entity['region_id'] = context.region['id']

        return entity

    def create(self, context, entity, need_policy=False):
        # if need_policy:
        #     # self._validate_policy(context, 'create', '*')
        #     self.validate_iam(context, const.METHOD_CREATE)
        entity = self._fill_context(context, entity)
        return self.dao.create(**entity)

    def first(self, **kwargs):
        model = self.dao.filter(**kwargs).first()
        if model:
            return model.to_dict()

    def get(self, context, id, need_policy=False):
        if need_policy:
            # return self.dao.get(self._validate_policy(context, 'get', id), id)
            return self.dao.get(self.validate_iam(context, const.METHOD_GET), id)
        else:
            return self.dao.get(None, id)

    def update(self, context, id, need_policy=False, **kwargs):
        # if need_policy:
        #     # self._validate_policy(context, 'update', id)
        #     self.validate_iam(context, const.METHOD_UPDATE)

        return self.dao.update(None, id, **kwargs)

    def delete(self, context, id, need_policy=False):
        # if need_policy:
        #     # self._validate_policy(context, 'delete', id)
        #     self.validate_iam(context, const.METHOD_DELETE)
        self.dao.delete(None, id=id)

    def __init__(self):
        self.dao = self.dao_class

    @staticmethod
    def _get_provider(context):
        pool = context.pool
        if pool.get('provider'):
            Provider = BaseProvider.get_provider(pool['provider'])
            return Provider(**json.loads(pool['meta']))


# class IAMFilter(object):
#     @staticmethod
#     def filter(resource_type):
#         # TODO
#         # policy_str = 'arn:*:::owner_id:server/*'
#         # policy = self._resolv_policy(policy_str)
#
#         return BaseDAO.and_(account_id=request.current_account['id'], owner_id=request.current_user['id'])
#
#     def _resolv_policy(self, policy_str):
#         pass

class BaseResource(BaseService):
    RESOURCES = ['server', 'disk', 'loadbalance', 'nosql']
    PACK_DIR = 'yeti.service'

    @classmethod
    def update_tag_id(cls, resource_id, tag_id):
        cls.dao.update(None, resource_id, tag_id=tag_id)

    @classmethod
    def _get_res_ins(cls, resource_type):
        return import_class('%s.%s.%s' % (cls.PACK_DIR, resource_type, r.capitalize()))()

    @classmethod
    def list_all_resources(cls):
        res_result = []
        for r in cls.RESOURCES:
            try:
                cls = import_class('%s.%s.%s' % (cls.PACK_DIR, r, r.capitalize()))
                r_ins = cls()
                l = r_ins.list()
                if l:
                    res_result.extend(r_ins.list())
            except ImportError:
                LOG.info('No module name %s, ignore it.' % r)

        return res_result

    @classmethod
    def update_res_tag(cls, resource_id, resource_type, tag_id):
        res_ins = cls._get_res_ins(resource_type)
        res_ins.update_tag_id(resource_id, tag_id)
