#!/usr/bin/env python
import setuptools
import json
import os

try:
    import multiprocessing
except ImportError:
    pass

ver = '1.0'
setuptools.setup(
    name='yeti',
    version=ver,
    packages=setuptools.find_packages(), include_package_data=True)
