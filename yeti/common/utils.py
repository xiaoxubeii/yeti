__author__ = 'tim'
from random import Random
import random
import ConfigParser
from ConfigParser import NoOptionError
import sys
import os
import re
import time
from multiprocessing import Process
import string
from flask import json
import datetime


def random_int(randomlength=8):
    str = ''
    chars = '0123456789'
    length = len(chars) - 1
    random = Random()
    for i in range(randomlength):
        str += chars[random.randint(0, length)]
    return int(str)


def random_list(l):
    return l[random.randint(0, len(l) - 1)]


class Config(object):
    def __init__(self, section='DEFAULT', config_path=None):
        self.section = section
        self.config_path = config_path
        self.config = ConfigParser.ConfigParser(allow_no_value=True)
        if config_path:
            self.load_config(config_path)

    def load_config(self, config_path):
        self.config_path = config_path
        self.config.read(config_path)

    def __getattr__(self, item):
        if self.config.has_option(self.section, item):
            return self.config.get(self.section, item)
        else:
            return Config(item, self.config_path)

    def get(self, item, section='DEFAULT'):
        try:
            return self.config.get(section, item)
        except NoOptionError:
            return

    def get_items(self, section='DEFAULT'):
        items = self.config.items(section)
        return {t[0]: t[1] for t in items}


def import_class(import_str):
    """Returns a class from a string including module and class."""
    mod_str, _sep, class_str = import_str.rpartition('.')
    __import__(mod_str)
    return getattr(sys.modules[mod_str], class_str)


def create_dir(dir):
    if not os.path.exists(dir):
        os.makedirs(dir)


def async_task(task_func, args, once=False, duration=60, interval=10):
    def _task_wrapper(*args):
        for i in xrange(duration / interval):
            try:
                task_func(*args)
            except TerminationException:
                break

            time.sleep(interval)

    if once:
        p = Process(target=task_func, args=args)
    else:
        p = Process(target=_task_wrapper, args=args)

    p.start()


class TerminationException(Exception):
    def __init__(self, result=None, *args, **kwargs):
        self.result = result
        super(TerminationException, self).__init__(*args, **kwargs)


def loop_check(func, args=(), kwargs={}, duration=60, interval=10):
    result = None
    for i in xrange(duration / interval):
        try:
            result = func(*args, **kwargs)
            time.sleep(interval)
        except TerminationException as e:
            result = e.result
            return result

    return result


def loop_check_state(func):
    fin_data = []

    def check():
        result = func()
        data = result[0]
        cond = result[1]

        for d in data:
            if cond(d):
                fin_data.append(d)
                if len(fin_data) == len(data):
                    raise TerminationException

    try:
        loop_check(check)
    except TerminationException:
        pass

    return fin_data


def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


def resource_id(res_prefix, units=8):
    return lambda: ('%s-%s' % (res_prefix, id_generator(units))).lower()


def copy_dict(data, keys):
    def to(dict, keys):
        return {k: dict[k] for k in keys}

    if isinstance(data, list):
        return [to(d, keys) for d in data]
    elif isinstance(data, dict):
        return to(data, keys)


# not utc
epoch = datetime.datetime.fromtimestamp(0)


def unix_time_millis(dt):
    return int((dt - epoch).total_seconds()) * 1000


def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)  # this will never fail
    return next_month - datetime.timedelta(days=next_month.day)


def find_last_index(str, sub_str):
    '''
    Return the index of sub_str last appear
    :param str:
    :param sub_str:
    :return: index
    '''
    last_position = -1
    while True:
        position = str.find(sub_str, last_position + 1)
        if position == -1:
            return last_position
        last_position = position

def get_name_by_hostname(hostname):
    return hostname[:find_last_index(hostname, '-')]


def check_ip(ip):
    p = re.compile('^((25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(25[0-5]|2[0-4]\d|[01]?\d\d?)$')
    if p.match(ip):
        return True
    else:
        return False
