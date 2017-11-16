#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'tim'
from yeti.common.utils import Config
import os

CONF = Config()

conf = os.environ.get('yeti_CONF', '/etc/yeti/yeti.conf')
CONF.load_config(conf)
