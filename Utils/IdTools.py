# -*- coding: utf-8 -*-
"""
@author: Julien Wuthrich
"""
import numpy as np
import hashlib

import pandas as pd


def hash_frame(df):

    return hashlib.sha1(df.to_string()).hexdigest()


def sha1_for_named_columns(df, columns):

    intersection = [col for col in columns if col in df.columns]
    if len(intersection) == len(columns):
        sha1s = pd.Series(data="", index=range(len(df)))
        for i in columns:
            sha1s = sha1s.map(np.str) + df[i].map(np.str)
        sha1s = sha1s.apply(
            lambda s: hashlib.sha1(str(s).encode("utf8")).hexdigest()
        )
    else:
        sha1s = pd.Series(data=np.nan, index=range(len(df)))
    return sha1s
