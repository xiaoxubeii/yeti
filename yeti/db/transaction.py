#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'tim'
from yeti.api import db
from contextlib2 import contextmanager
from yeti.common.utils import TerminationException


@contextmanager
def transaction():
    db.session.begin_nested()
    try:
        yield
        db.session.commit()
    except TerminationException as te:
        db.session.commit()
        raise te
    except Exception as e:
        db.session.rollback()
        raise e
