#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'tim'

import sys
from rq import Connection, Worker
import yeti
from redis import Redis
from yeti import CONF

redis_conn = Redis(CONF.redis_host, CONF.redis_port)
with Connection(redis_conn):
    qs = sys.argv[1:] or ['default']

    w = Worker(qs)
    w.work()
