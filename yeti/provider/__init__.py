#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'tim'
from yeti.common.utils import import_class
from yeti import CONF
from yeti.db.dao import CostHourlyDAO, CostDailyDAO, CostMonthlyDAO, CostYearlyDAO, BillPollerHistoryDAO
import pandas as pd
from hashlib import md5
from datetime import datetime


class BaseProvider(object):
    @staticmethod
    def get_provider(provider):
        return import_class(provider)

    def create_instance(self, res):
        raise NotImplementedError

    def get_instance(self, id):
        raise NotImplementedError

    def delete_instance(self, id):
        raise NotImplementedError

    def list_instances(self, **kwargs):
        raise NotImplementedError

    def list_images(self, **kwargs):
        raise NotImplementedError

    def list_networks(self, **kwargs):
        raise NotImplementedError

    def list_flavors(self, **kwargs):
        raise NotImplementedError

    def bind_eips(self, ins_ids):
        raise NotImplementedError

    def bind_secgs(self, servers):
        raise NotImplementedError

    # building, active, inactive, error, configuring
    def format_state(self, in_state):
        return in_state

    def list_instances(self):
        raise NotImplementedError

    def _convert_ins(self, ins):
        new_ins = {}
        for k in ('image_id', 'mac', 'state', 'size_id', 'fixed_ip', 'created_at', 'id', 'hostname'):
            new_ins[k] = None

        return new_ins

    def format_state(self, in_state):
        """
        :param in_state:
        :return: building, running, stopping, stopped, error, inactive, rebooting, booting
        """
        raise NotImplementedError


class BaseBillPoller(object):
    def __init__(self):
        self.bphdao = BillPollerHistoryDAO()

    def _need_do(self, context, day):
        results = self.bphdao.list(bill_time=day, region_id=context.region['id'], pool_id=context.pool['id'])
        return not results

    def etl(self, context, df):
        raise NotImplementedError

    def poll_monthly_data(self, context, month, use_cache=False):
        raise NotImplementedError

    def poll_data(self, dt, ignore_position=False):
        raise NotImplementedError

    def consolidate(self, df):
        if df is not None and not df.empty:
            self._consolidate(df, '1H', CostHourlyDAO)
            self._consolidate(df, '1D', CostDailyDAO)
            self._consolidate(df, '1MS', CostMonthlyDAO)
            self._consolidate(df, '1AS', CostYearlyDAO)

    def poll_daily_data(self, context, day):
        df = self.poll_data(day)
        df = self.etl(context, df)
        self.cosolidate(df)

    def _poll_hourly(self, df):
        chd = CostHourlyDAO()
        new_df = pd.DataFrame(df)
        data = new_df.T.to_dict().values()
        chd.bulk_create(data)

    def _consolidate(self, df, freq, dao):
        df = pd.DataFrame(df)
        df.set_index(pd.DatetimeIndex(df['start']), inplace=True)

        df = \
            df.groupby(
                [pd.TimeGrouper(freq=freq), 'resource_type', 'usage_type', 'operation', 'resource_ins_id',
                 'tag_id']).agg(
                {'cost': 'sum', 'quantity': 'sum', 'rate': 'mean', 'record_id': 'first',
                 'record_time': 'first', 'region_id': 'first', 'pool_id': 'first'}).reset_index()

        def consolidate_id(r):
            m = md5()
            m.update(r[['start', 'resource_type', 'usage_type', 'operation', 'resource_ins_id']].to_json())
            return m.hexdigest()

        df['consolidate_id'] = df.apply(consolidate_id, axis=1)

        result = dao.list(dao.in_({'value': df['consolidate_id'].tolist(), 'key': 'consolidate_id'}))
        df_result = pd.DataFrame(result)

        if not df_result.empty:
            df_update = pd.merge(left=df, right=df_result, left_on='consolidate_id', right_on='consolidate_id',
                                 how='inner',
                                 suffixes=('a', 'b'))
            df_update['cost'] = df_update['costa'] + df_update['costb']
            df_update['quantity'] = df_update['quantitya'] + df_update['quantityb']
            df_update['record_id'] = df_update['record_ida']
            df_update['record_time'] = df_update['record_timea']
            dao.bulk_update(
                df_update[['cost', 'quantity', 'record_id', 'record_time', 'consolidate_id']].to_dict(orient='records'))

        if df_result.empty:
            df_create = df
        else:
            df_create = df[-df['consolidate_id'].isin(df_result['consolidate_id'])]

        if not df_create.empty:
            dao.bulk_create(df_create.to_dict(orient='records'))
