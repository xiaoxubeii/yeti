#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'tim'
from flask import request
from functools import wraps
from yeti import CONF
from yeti.common.log import LOG
from yeti.common.exception import BaseException, PermissionDenied
from flask import make_response
import uuid
from yeti.api import cache
import requests
import json
import traceback


# request context decorator
def log_filter(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except BaseException as be:
            LOG.debug(be.message)
            return be.message, be.http_code
        except Exception as e:
            LOG.error(traceback.format_exc())
            return 'Internal Server Error.', 500

    return decorated_function
