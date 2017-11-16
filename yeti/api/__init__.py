#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'tim'

from flask import Flask
from flask.json import JSONEncoder
from flask_restful import Api
from flask_restful import Resource
import types
from datetime import datetime
from flask.ext.session import Session
from flask.ext.cache import Cache
from flask import session
from flask_sqlalchemy import SQLAlchemy
from yeti import CONF
import os, fnmatch, inspect, sys
from redis import Redis
from flask import ctx
import json


class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        try:
            if isinstance(obj, datetime):
                return obj.strftime('%Y-%m-%d %H:%M:%S')
            iterable = iter(obj)
        except TypeError:
            pass
        else:
            return list(iterable)
        return JSONEncoder.default(self, obj)


def api_route(self, *args, **kwargs):
    def wrapper(cls):
        self.add_resource(cls, *args, **kwargs)
        return cls

    return wrapper


class BaseResource(Resource):
    RESOURCE_NAME = ''

    def _get_query_args(self, q_args):
        query_args = {}
        for k, v in q_args.iteritems():
            if isinstance(v, list):
                v = v[0]
            query_args[k] = v

        return query_args

    def get(self, context, id=None):
        raise NotImplementedError()

    def delete(self, context, id):
        raise NotImplementedError()

    def post(self, context, model):
        raise NotImplementedError()

    def put(self, context, id):
        raise NotImplementedError()

    @classmethod
    def add_resource(cls):
        api.add_resource(cls, '/api/%s' % cls.RESOURCE_NAME, '/api/%s/<id>' % cls.RESOURCE_NAME)


app = Flask(__name__)
app.json_encoder = CustomJSONEncoder

SESSION_TYPE = 'redis'
CACHE_TYPE = 'redis'
SQLALCHEMY_DATABASE_URI = CONF.psql_db_url
app.config.from_object(__name__)

db = SQLAlchemy(app)

Session(app)
cache = Cache(app, config={'CACHE_TYPE': CACHE_TYPE, 'CACHE_REDIS_HOST': CONF.redis_host,
                           'CACHE_REDIS_PORT': CONF.redis_port})

from yeti.api.filter import auth_filter, log_filter, audit_filter, permission_filter

# api = Api(app, decorators=[audit_filter, permission_filter, auth_filter, log_filter])
api = Api(app, decorators=[audit_filter, auth_filter, log_filter])
api.route = types.MethodType(api_route, api)

redis_conn = Redis(CONF.redis_host, CONF.redis_port)


@app.before_request
def before_request():
    db.session()


@app.after_request
def after_request(response):
    db.session.commit()
    db.session.remove()

    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET,PUT,POST,DELETE'
    response.headers[
        'Access-Control-Allow-Headers'] = 'Origin, X-Requested-With, Content-Type, Accept, Authorization, X_POOL_ID, X_REGION_ID, X_TOKEN'
    return response


def import_api_module(package):
    curr_dir = os.path.dirname(os.path.realpath(__file__))
    modules = []
    for root, dirnames, filenames in os.walk(os.path.join(curr_dir, package)):
        for filename in fnmatch.filter(filenames, '*.py'):
            module = 'yeti.api.%s.%s' % (package, filename.replace('.py', ''))
            if module == 'yeti.api.core.static':
                pass
            __import__(module)
            modules.append(module)

    return modules


def import_api_class():
    modules = import_api_module('core')
    modules.extend(import_api_module('extension'))
    for m in modules:
        clsmems = inspect.getmembers(sys.modules[m],
                                     lambda member: inspect.isclass(member) and member.__module__ == m and issubclass(
                                         member, BaseResource))

        if clsmems and len(clsmems):
            for cls in clsmems:
                getattr(cls[1], 'add_resource')()
