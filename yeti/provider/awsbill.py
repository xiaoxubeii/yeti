#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'tim'
from yeti.db.dao import CostHourlyDAO, CostDailyDAO, CostMonthlyDAO, CostYearlyDAO
from yeti.service.common import Region, Pool
from yeti.service import BaseService, BaseResource
from yeti.common.utils import unix_time_millis
import pandas as pd
from datetime import datetime
import boto3
import os
from yeti import CONF
import zipfile
from yeti.common.utils import resource_id
from pandas.errors import EmptyDataError
from hashlib import md5
from yeti.provider import BaseBillPoller
from yeti.service.server import Server


class yetiPoller(BaseBillPoller):
    bucket_name = 'billing-td'
    s3 = boto3.resource('s3')

    @classmethod
    def poll_data(cls, dt, ignore_position=False, use_cache=False):
        month_str = dt.strftime('%Y-%m')

        def download_bill_file():
            file_name = '238370294713-aws-billing-detailed-line-items-with-resources-and-tags-ACTS-%s.csv' % month_str

            if not use_cache:
                cls.s3.Bucket(cls.bucket_name).download_file(
                    '%s.zip' % file_name,
                    '%s/%s.zip' % (CONF.yetiing, file_name))
                zip_ref = zipfile.ZipFile('%s/%s.zip' % (CONF.yetiing, file_name), 'r')
                zip_ref.extractall(path=CONF.yetiing)
                zip_ref.close()

            file_path = os.path.join(CONF.yetiing, file_name)
            return pd.read_csv(file_path)

        df = download_bill_file()
        if not ignore_position:
            position_path = '%s/%s_record_position' % (CONF.yetiing, id)
            if not os.path.exists(position_path):
                with open(position_path, 'w+') as f:
                    f.write(str(0))

            with open(position_path, 'r+') as f:
                try:
                    line = int(f.read())
                except:
                    line = 0

                try:
                    df = df[line:]
                    if not df.empty:
                        f.seek(0)
                        f.write(str(line + len(df)))
                    return df
                except EmptyDataError as e:
                    pass
        else:
            return df

    def etl(self, context, df):
        # def convert_row(row):
        #     return row.to_json()

        if not df.empty:
            reses = BaseResource().list_all_resources()
            r_df = pd.DataFrame(reses)
            r_df.rename(columns={'id': 'resource_id', 'instance_id': 'resource_ins_id'}, inplace=True)

            # df['detail'] = df.apply(convert_row, axis=1)
            df = df[
                ['ProductName', 'UsageType', 'UsageStartDate', 'UsageQuantity', 'Rate', 'Cost', 'Operation',
                 'ResourceId']]
            df = df.rename(
                columns={'ProductName': 'resource_type', 'UsageType': 'usage_type', 'UsageStartDate': 'start',
                         'UsageQuantity': 'quantity', 'Rate': 'rate', 'Cost': 'cost',
                         'Operation': 'operation', 'ResourceId': 'resource_ins_id'})

            df[['cost', 'quantity', 'rate']] = df[['cost', 'quantity', 'rate']].fillna(0)
            df['start'] = df['start'].fillna(datetime(year=1970, month=1, day=1))
            df[['resource_type', 'usage_type', 'operation', 'resource_ins_id']] = df[
                ['resource_type', 'usage_type', 'operation', 'resource_ins_id']].fillna('')

            df['record_id'] = resource_id('rec', 32)()
            df['record_time'] = datetime.now()
            df['region_id'] = context.region['id']
            df['pool_id'] = context.pool['id']

            result_df = pd.merge(df, r_df[['resource_id', 'resource_ins_id', 'tag_id']], how='left',
                                 on='resource_ins_id')
            result_df = result_df.fillna('')
            return result_df[
                ['resource_type', 'usage_type', 'start', 'quantity', 'rate', 'cost', 'operation', 'resource_ins_id',
                 'record_id', 'record_time', 'region_id', 'pool_id', 'tag_id']]

    def poll_monthly_data(self, context, month, use_cache=False):
        df = self.poll_data(month, True, use_cache)
        df = self.etl(context, df)
        self.consolidate(df)
