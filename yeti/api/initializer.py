#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'tim'
import sys
import inspect
import os
import fnmatch
from yeti.api import BaseResource
from yeti.api.main import cache
from yeti.service.common import Region
from yeti.db.model import *


def import_api_module(package):
    curr_dir = os.path.dirname(os.path.realpath(__file__))
    modules = []
    for root, dirnames, filenames in os.walk(os.path.join(curr_dir, package)):
        for filename in fnmatch.filter(filenames, '*.py'):
            module = 'yeti.api.%s.%s' % (package, filename.replace('.py', ''))
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


def build_cache():
    region = Region()
    cache.set('regions', region.list(), 0)


def init():
    import_api_class()
    build_cache()
