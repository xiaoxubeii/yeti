#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'tim'
from yeti.api import redis_conn
from rq_scheduler import Scheduler
from yeti import CONF
from yeti.service.bill import do_poll
import datetime
import pandas as pd
import sys
import os

scheduler = Scheduler(connection=redis_conn)


class BillPollerTask(object):
    @classmethod
    def start_daily(cls):
        cls.cancel()
        job = scheduler.cron(CONF.bill.poller_task_cron, do_poll, timeout=3600)
        with open(CONF.bill.poller_taskids, 'w+') as f:
            f.write(job.id)

    @classmethod
    def start_all(cls):
        dt_ranges = pd.date_range(sys.argv[1], sys.argv[2], freq='1MS').to_pydatetime()
        for dt in dt_ranges:
            poll(dt)

    @classmethod
    def cancel(cls):
        path = CONF.bill.poller_taskids
        if os.path.exists(path):
            with open(path, 'r') as f:
                scheduler.cancel(f.read())


BillPollerTask.start_daily()
