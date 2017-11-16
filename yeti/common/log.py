#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'tim'
from logging.config import dictConfig
import logging
from flask import request
from yeti import CONF
import os


class RequestFilter(logging.Filter):
    def filter(self, record):
        try:
            record.requestId = request.request_id
        except Exception:
            record.requestId = None

        return True


log_path = os.getenv('yeti_LOG_PATH', CONF.log_path)
if not os.path.exists(log_path):
    os.makedirs(log_path)

log_path = os.path.join(log_path, 'yeti.log')

# TODO need move to conf
logging_config = dict(
    version=1,
    filters={
        'request_filter': {
            '()': RequestFilter
        }
    },
    formatters={
        'f': {
            'format': '%(asctime)s %(levelname)-6s %(pathname)s:%(funcName)s:%(lineno)s %(message)s',
            'datefmt': '%m-%d %H:%M:%S'},
        'request': {
            'format': '%(asctime)s %(levelname)-6s [%(requestId)s] %(pathname)s:%(funcName)s:%(lineno)s %(message)s',
            'datefmt': '%m-%d %H:%M:%S'}
    },
    handlers={
        'console_h': {'class': 'logging.StreamHandler',
                      'formatter': 'f',
                      'level': logging.DEBUG},
        'r_file_h': {
            'class': 'logging.FileHandler',
            'level': logging.DEBUG,
            'formatter': 'request',
            'filename': log_path,
            'filters': ['request_filter']
        },
        'r_console_h': {'class': 'logging.StreamHandler',
                        'formatter': 'request',
                        'level': logging.DEBUG, 'filters': ['request_filter']}

    },
    loggers={
        '': {'handlers': ['r_console_h', 'r_file_h'],
             'level': logging.DEBUG},
    }
)

dictConfig(logging_config)
LOG = logging.getLogger()
