# -*- coding: utf-8 -*-
"""
@author: Julien Wuthrich
"""
import zipfile
import pandas as pd

import Utils.SnippingTools as Utils


_REQUIRED_FILES = {
    "agency.txt": {
        "id_field": "agency_id",
        "columns": [
            u'agency_id', u'agency_name', u'agency_url', u'agency_timezone']
    },
    "stops.txt": {
        "id_field": "stop_id",
        "columns": [
            u'stop_id', u'stop_code', u'stop_name', u'stop_lat', u'stop_lon'],
        "as_type": {"stop_id": str}
    },
    "calendar.txt": {
        "id_field": "service_id",
        "columns": [
            u'service_id', u'monday', u'tuesday', u'wednesday', u'thursday',
            u'friday', u'saturday', u'sunday', u'start_date', u'end_date'
        ],
        "as_type": {"service_id": str, 'end_date': int, 'start_date': int}
    },
    "routes.txt": {
        "id_field": "route_id",
        "columns": [
            u'route_id', u'route_short_name', u'route_long_name',
            u'route_type', u'route_color', u'route_text_color'
        ],
        "as_type": {"route_id": str}
    },
    "trips.txt": {
        "id_field": "trip_id",
        "columns": [u'route_id', u'service_id',
                    u'trip_id', u'trip_headsign', u'direction_id'],
        "as_type": {"trip_id": str, "service_id": str, 'route_id': str}
    },
    "stop_times.txt": {
        "id_field": ["trip_id", u'stop_sequence', "stop_id"],
        "columns": [u'trip_id', u'arrival_time', u'departure_time',
                    u'stop_id', u'stop_sequence', u'shape_dist_traveled'],
        "as_type": {"stop_id": str, "trip_id": str}
    }
}

_OPTIONAL_CALENDAR_DATES = {
    "calendar_dates.txt": {
        "id_field": ["service_id", "date"],
        "columns": [u"service_id", "date", "exception_type"],
        "as_type": {'service_id': str, 'date': int, "exception_type": int}
    }
}

_OPTIONAL_SHAPES = {
    "shapes.txt": {
        "id_field": "shape_id",
        "columns": [u'shape_id', u'shape_pt_lat',
                    u'shape_pt_lon', u'shape_pt_sequence'],
        "as_type": {"shape_id": str}
    }
}


def read_table(gtfs, reference, table, to_remap={}):

    if not isinstance(gtfs, zipfile.ZipFile):
        gtfs = zipfile.ZipFile(gtfs)

    df = pd.read_csv(
        gtfs.open(table),
        encoding='utf-8-sig',
        dtype=reference[table].get("as_type", {})
    )

    return df


def load_gtfs(gtfs, reference=_REQUIRED_FILES):

    z_gtfs = zipfile.ZipFile(gtfs)
    d_gtfs = {}

    if "shapes.txt" in Utils.list_zip_files(gtfs):
        reference = Utils.dict_union(reference, _OPTIONAL_SHAPES)

    if "calendar_dates.txt" in Utils.list_zip_files(gtfs):
        reference = Utils.dict_union(reference, _OPTIONAL_CALENDAR_DATES)

    tables = reference.keys()

    for table in tables:
        d_gtfs[table] = read_table(z_gtfs, reference, table)

    return d_gtfs
