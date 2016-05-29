# -*- coding: utf-8 -*-
"""
@author: Julien Wuthrich
"""
import math
import datetime


def to_sod(x):

    if isinstance(x, float) and math.isnan(x):
        return x

    if isinstance(x, basestring):
        hour, minute, second = map(int, str(x).split(":"))

    if isinstance(x, datetime.datetime):
        hour, minute, second = x.hour, x.minute, x.second

    sod = hour * 3600 + minute * 60 + second

    return sod
