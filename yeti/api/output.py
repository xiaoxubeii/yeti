#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'tim'
from yeti import CONF
import io
from flask import make_response, json
from yeti.common.exception import NotFound
from yeti.api import api


# output formatter
@api.representation('application/json')
def output_json(data, code, headers=None):
    resp = make_response(json.dumps(data), code)
    resp.headers.extend(headers or {})
    return resp


def output_html(data):
    try:
        with io.open('%s/pages/%s.html' % (CONF.static_path, data), mode='r', encoding='utf8') as f:
            resp = make_response(f.read())
            return resp
    except IOError:
        raise NotFound


def output_css(data):
    try:
        with io.open('%s/css/%s' % (CONF.static_path, data), mode='r', encoding='utf8') as f:
            resp = make_response(f.read())
            resp.headers['content-type'] = 'text/css'
            return resp
    except IOError:
        raise NotFound


def output_js(data):
    try:
        with io.open('%s/js/%s' % (CONF.static_path, data), 'r', encoding='utf8') as f:
            resp = make_response(f.read())
            resp.headers['content-type'] = 'application/x-javascript'
            return resp
    except IOError:
        raise NotFound


def output_font(data):
    try:
        with io.open('%s/font/%s' % (CONF.static_path, data), 'rb') as f:
            resp = make_response(f.read())
            resp.headers['content-type'] = 'application/octet-stream'
            return resp
    except IOError:
        raise NotFound


def output_img(data):
    try:
        with io.open('%s/images/%s' % (CONF.static_path, data), 'rb') as f:
            resp = make_response(f.read())
            resp.headers['content-type'] = 'application/x-png'
            return resp
    except IOError:
        raise NotFound
