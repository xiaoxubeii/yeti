#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'tim'
from yeti.service import BaseService
from yeti.service.common import Pool, Region
from yeti.api import FakeRequestContext
from yeti.common.log import LOG
from yeti.api import db
from yeti.db.dao import CostHourlyDAO, CostDailyDAO, CostMonthlyDAO, CostYearlyDAO
import datetime
import pandas as pd
from yeti.common.utils import unix_time_millis, last_day_of_month
import sys
import traceback


class Bill(BaseService):
    dao_class = None

    def __init__(self):
        self.chd = CostHourlyDAO
        self.cdd = CostDailyDAO
        self.cmd = CostMonthlyDAO
        self.cyd = CostYearlyDAO

    '''
    query = {
            'aggregate': 'data',
            'consolidate': 'monthly',
            'start': start,
            'end': end,
            'group_by': 'resource_type',
            'filter':{}
        }
    '''

    def _get_data(self, **kwargs):
        dao = kwargs['dao']
        kwargs['time_bt'] = (self._dt_to_str(kwargs['start']), self._dt_to_str(kwargs['end']))
        result = dao.get_data(**kwargs)
        if result:
            groupby = kwargs['group_by']
            result_df = pd.DataFrame(result, columns=['start', 'cost', groupby])
            result_df = result_df.set_index('start')
            result_df[kwargs['group_by']].fillna('', inplace=True)
            result_df = result_df.groupby([pd.Grouper(freq=kwargs['groupby_freq']), kwargs['group_by']])[
                'cost'].sum().unstack()

            data_ranges = pd.date_range(kwargs['start'], kwargs['end'], freq=kwargs.pop('dt_freq')).to_pydatetime()
            cols = result_df.columns.tolist()
            last_df = pd.DataFrame(columns=cols, index=data_ranges)
            last_df.update(result_df)
            result_df = last_df
            result_df.fillna(0, inplace=True)

            if groupby in ('region_id', 'pool_id'):
                if groupby == 'region_id':
                    regions = Region().list()
                    ds = {d['id']: d['name'] for d in regions}
                else:
                    pools = Pool().list_all()
                    ds = {d['id']: '%s-%s' % (d['region']['name'], d['name']) for d in pools}

                result_df.rename(columns={c: ds.get(c, '') for c in result_df.columns}, inplace=True)

            data = {}
            data['data'] = result_df.to_dict(orient='list')
            data['time'] = result_df.index.tolist()
            data['time'] = map(unix_time_millis, data['time'])

            avg = result_df.apply(lambda t: t.mean()).to_dict()
            max = result_df.apply(lambda t: t.max()).to_dict()
            total = result_df.apply(lambda t: t.sum()).to_dict()
            stats_df = pd.DataFrame({'avg': avg, 'max': max, 'total': total})
            data['stats'] = stats_df.T.to_dict()

            aggregate = kwargs.get('aggregate')
            if aggregate == 'data':
                data['data']['aggregated'] = result_df.sum(axis=1).tolist()
            elif aggregate == 'stats':
                data['stats']['aggregated'] = stats_df.sum().to_dict()

            return data

    def _format_dt(self, dt_str):
        format_str = '%Y-%m-%d %I%p'
        return datetime.datetime.strptime(dt_str, format_str)

    def _dt_to_str(self, dt):
        format_str = '%Y-%m-%d %H:%M:%S'
        return dt.strftime(format_str)

    def _hourly(self, **kwargs):
        start = self._format_dt(kwargs['start'])
        start = start.replace(second=0, minute=0)
        kwargs['start'] = start

        end = self._format_dt(kwargs['end'])
        end = end.replace(second=0, minute=0)
        kwargs['end'] = end

        kwargs['groupby_freq'] = '1H'
        kwargs['dt_freq'] = '1H'
        kwargs['dao'] = self.chd
        return self._get_data(**kwargs)

    def _daily(self, **kwargs):
        start = self._format_dt(kwargs['start'])
        start = start.replace(hour=0, second=0, minute=0)
        kwargs['start'] = start

        end = self._format_dt(kwargs['end'])
        end = end.replace(hour=0, second=0, minute=0)
        kwargs['end'] = end

        kwargs['groupby_freq'] = '1D'
        kwargs['dt_freq'] = '1D'
        kwargs['dao'] = self.cdd
        return self._get_data(**kwargs)

    def _monthly(self, **kwargs):
        start = self._format_dt(kwargs['start'])
        start = start.replace(day=1, hour=0, second=0, minute=0)
        kwargs['start'] = start

        end = self._format_dt(kwargs['end'])
        end = end.replace(day=1, hour=0, second=0, minute=0)
        kwargs['end'] = end

        kwargs['groupby_freq'] = '1MS'
        kwargs['dt_freq'] = '1MS'
        kwargs['dao'] = self.cmd
        return self._get_data(**kwargs)

    def _yearly(self, **kwargs):
        start = self._format_dt(kwargs['start'])
        start = start.replace(month=1, day=1, hour=0, second=0, minute=0)
        kwargs['start'] = start

        end = self._format_dt(kwargs['end'])
        end = end.replace(month=1, day=1, hour=0, second=0, minute=0)
        kwargs['end'] = end

        kwargs['groupby_freq'] = '1AS'
        kwargs['dt_freq'] = '1AS'
        kwargs['dao'] = self.cyd
        return self._get_data(**kwargs)

    def get_data(self, **kwargs):
        return getattr(self, '_%s' % kwargs['consolidate'])(**kwargs)


def poll(day):
    pools = Pool().list_all()
    for p in pools:
        try:
            ctx = FakeRequestContext(region=p['region'], pool=p)
            provider = BaseService._get_provider(ctx)
            if hasattr(provider, 'poll'):
                LOG.info('Begin to poll data, day is %s, region_id is %s, pool_id is %s.' % (
                    day.strftime('%Y-%m-%d'),
                    p['region']['id'], p['id']))
                db.session()
                provider.poll(ctx, day)
                db.session.commit()
                db.session.remove()
        except Exception as e:
            LOG.error('Fail to poll data, reason is %s.' % e.message)
            db.session.rollback()


def do_poll():
    now = datetime.datetime.now()
    poll_day = datetime.datetime(year=now.year, month=now.month, day=now.day)
    # only support daily polling task
    poll(poll_day - datetime.timedelta(days=1))


def poll_monthly_data(pool_id, start, end, use_cache=False):
    p = Pool().get(None, pool_id)
    ctx = FakeRequestContext(region=p['region'], pool=p)
    provider = BaseService._get_provider(ctx)
    for m in pd.date_range(start, end, freq='1MS').to_pydatetime():
        db.session()
        try:
            LOG.info('Begin to poll data, month is %s, region_id is %s, pool_id is %s.' % (
                m.strftime('%Y-%m'), p['region']['id'], p['id']))
            provider.poll_monthly_data(ctx, m, use_cache)
            db.session.commit()
            LOG.info('Finish polling data, month is %s, region_id is %s, pool_id is %s.' % (
                m.strftime('%Y-%m'), p['region']['id'], p['id']))
        except NotImplementedError:
            pass
        except Exception as e:
            LOG.error(
                'Fail to poll data, month is %s, reason is %s.' % (m.strftime('%Y-%m'), traceback.format_exc()))
            db.session.rollback()
        db.session.remove()
