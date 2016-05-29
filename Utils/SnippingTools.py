# -*- coding: utf-8 -*-
"""
@author: Julien Wuthrich
"""
import itertools
import zipfile
import hashlib
import unidecode

import pandas as pd

from Utils.DistanceTools import Distance


def dict_union(*args):

    return dict(itertools.chain.from_iterable(d.iteritems() for d in args))


def list_zip_files(afile):

    if not isinstance(afile, zipfile.ZipFile):
        afile = zipfile.ZipFile(afile)

    zfiles = [zf.filename for zf in afile.filelist]

    return zfiles


def hashfile(filepath):

    sha1 = hashlib.sha1()
    fichier = open(filepath, 'rb')
    try:
        sha1.update(fichier.read())
    finally:
        fichier.close()

    return sha1.hexdigest()


def fmt_str(bad_string):

    if type(bad_string) is str or unicode:
        better_string = bad_string.strip()
        better_string = unidecode.unidecode(better_string)

        return better_string.lower()
    else:

        return bad_string


def change_nan_value(df, new_value):

    return df.where((pd.notnull(df)), new_value)


def get_stops_close(df, dist=5):

    lis = []
    for j in df.index:
        distance = Distance.haversine(
            [df["Latitude_x"][j], df["Longitude_x"][j]],
            [df["Latitude_y"][j], df["Longitude_y"][j]])
        if distance <= dist:
            lis.append([df["Id_x"][j], df["Id_y"][j]])

    return pd.DataFrame(lis, columns=["Id", "NewId"])
