#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'tim'
from yeti.api import app
from yeti.api import import_api_class
from yeti.api import build_cache
from yeti import CONF

import_api_class()

if __name__ == '__main__':
    app.run('0.0.0.0', port=CONF.port)
