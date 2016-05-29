# -*- coding: utf-8 -*-
"""
@author: Julien Wuthrich
"""
import os

import pandas as pd

from PreprocessGTFS.PreprocessStopTimes import EstimateStopTimes
from ScheduleReader.GtfsScheduleReader import load_gtfs
from Utils.SnippingTools import hashfile, fmt_str, change_nan_value
from Utils.SnippingTools import list_zip_files
from Utils.DateTools import to_sod
from PreprocessGTFS.GenerateShapes import Graph


class NormalizeGTFS(object):

    def __init__(self, gtfs_path):

        if not os.path.exists(gtfs_path):
            raise '%s is not a path' % gtfs_path
        self.gtfs_path = gtfs_path
        self.GTFS = load_gtfs(self.gtfs_path)
        self.start_date = min(self.GTFS["calendar.txt"]["start_date"])
        self.end_date = max(self.GTFS["calendar.txt"]["end_date"])

    @staticmethod
    def format_df(df, _REQUIRED_FIELD_NAMES):

        for cols in _REQUIRED_FIELD_NAMES:
            if cols not in df.columns:
                df[cols] = None
            else:
                try:
                    df[cols] = df[cols].map(fmt_str)
                except:
                    pass

        return df

    @staticmethod
    def route_direction(serie):

        lst_index = list(serie.loc[serie.shift(-1) == serie].index)
        for idx in lst_index:
            serie.loc[idx] += "_other_dire"

    def all_shapes(self, trips, gtfs):

        graph = Graph(self.gtfs_path)
        if 'shapes.txt' in list_zip_files(self.path_gtfs):
            if len(trips[trips["shape_id"].isnull()]) > 0:
                shapes = self.shapes()

                true_shapes = trips[~trips["shape_id"].isnull()]
                false_shapes = trips[trips["shape_id"].isnull()]

                shapes_df, generated_shapes = graph.generate_shapes(
                    self.gtfs_path, gtfs, false_shapes
                )
                shapes = pd.concat([shapes_df, shapes])
                trips = pd.concat([true_shapes, generated_shapes])
            else:
                shapes = self.shapes()
        else:
            shapes, trips = graph.generate_shapes(
                self.gtfs_path, gtfs, trips
            )

        return shapes, trips

    def is_calendar_dates(self):

        if 'calendar_dates.txt' in list_zip_files(self.gtfs_path):
            calendar_dates = self.calendar_dates()
        else:
            calendar_dates = pd.DataFrame()

        return calendar_dates

    def gtfs(self):

        return pd.DataFrame(
            [{
                'gtfs_id': hashfile(self.gtfs_path),
                'start_date': self.start_date,
                'end_date': self.end_date
            }]
        )

    def agency(self):

        _OPTIONAL_FIELD_NAMES = [
            'agency_id', 'agency_lang',
            'agency_phone', 'agency_fare_url'
        ]
        _REQUIRED_FIELD_NAMES = _OPTIONAL_FIELD_NAMES + [
            'agency_name', 'agency_url', 'agency_timezone'
        ]
        df = self.GTFS["agency.txt"]
        df = change_nan_value(df, None)

        return self.format_df(df, _REQUIRED_FIELD_NAMES)

    def calendar(self):

        _OPTIONAL_FIELD_NAMES = []
        _REQUIRED_FIELD_NAMES = _OPTIONAL_FIELD_NAMES + [
            'service_id', 'monday', 'tuesday',
            'wednesday', 'thursday', 'friday',
            'saturday', 'sunday',
            'start_date', 'end_date'
        ]
        df = self.GTFS["agency.txt"]
        df = change_nan_value(df, None)

        return self.format_df(df, _REQUIRED_FIELD_NAMES)

    def calendar_dates(self):

        _OPTIONAL_FIELD_NAMES = []
        _REQUIRED_FIELD_NAMES = _OPTIONAL_FIELD_NAMES + [
            'date', 'service_id', 'exception_type'
        ]
        df = self.GTFS["agency.txt"]
        df = change_nan_value(df, None)

        return self.format_df(df, _REQUIRED_FIELD_NAMES)

    def routes(self):

        _OPTIONAL_FIELD_NAMES = [
            'agency_id', 'route_desc',
            'route_color', 'route_text_color'
        ]
        _REQUIRED_FIELD_NAMES = _OPTIONAL_FIELD_NAMES + [
            'route_id', 'route_short_name',
            'route_long_name', 'route_type', 'route_url'
        ]
        df = self.GTFS["routes.txt"]
        df = change_nan_value(df, None)

        df["sha"] = (
            df["route_long_name"].map(str) +
            df["route_short_name"].map(str))
        df = df.sort("sha")
        self.route_direction(df["sha"])

        return self.format_df(df, _REQUIRED_FIELD_NAMES)

    def stops(self):

        _OPTIONAL_FIELD_NAMES = [
            'stop_code', 'stop_desc',
            'zone_id', 'stop_url',
            'location_type', 'parent_station',
            'stop_timezone', 'wheelchair_boarding'
        ]
        _REQUIRED_FIELD_NAMES = _OPTIONAL_FIELD_NAMES + [
            'stop_id', 'stop_name',
            'stop_lat', 'stop_lon'
        ]
        df = self.GTFS["stops.txt"]
        df = change_nan_value(df, None)

        return self.format_df(df, _REQUIRED_FIELD_NAMES)

    def stop_times(self):

        _OPTIONAL_FIELD_NAMES = [
            'stop_headsign', 'pickup_type', 'drop_off_type',
            'shape_dist_traveled', 'timepoint'
        ]
        _REQUIRED_FIELD_NAMES = _OPTIONAL_FIELD_NAMES + [
            'trip_id', 'arrival_time', 'departure_time',
            'stop_id', 'stop_sequence'
        ]
        df = self.GTFS["stop_times.txt"]
        df_trips = self.GTFS["trips.txt"]
        df = change_nan_value(df, None)

        try:
            df["arrival_time"] = df["arrival_time"].map(to_sod)
            df["departure_time"] = df["departure_time"].map(to_sod)
        except:
            df = EstimateStopTimes.prepare_stop_times(df)

        df = pd.merge(
            df,
            df_trips[["trip_id", "start_date", "end_date"]],
            on="trip_id"
        )

        return self.format_df(df, _REQUIRED_FIELD_NAMES)

    def trips(self):

        _OPTIONAL_FIELD_NAMES = [
            'trip_headsign', 'trip_short_name',
            'direction_id', 'block_id', 'shape_id',
            'wheelchair_accessible', 'bikes_allowed'
        ]
        _REQUIRED_FIELD_NAMES = _OPTIONAL_FIELD_NAMES + [
            'route_id', 'service_id',
            'trip_id'
        ]
        df = self.GTFS["stop_times.txt"]
        df_calendar = self.GTFS["calendar.txt"]
        df = change_nan_value(df, None)

        df = pd.merge(
            df,
            df_calendar[["service_id", "start_date", "end_date"]],
            on="service_id"
        )

        return self.format_df(df, _REQUIRED_FIELD_NAMES)

    def shapes(self):

        _OPTIONAL_FIELD_NAMES = [
            'shape_dist_traveled'
        ]

        _REQUIRED_FIELD_NAMES = _OPTIONAL_FIELD_NAMES + [
            'shape_id', 'shape_pt_lat',
            'shape_pt_lon', 'shape_pt_sequence',
        ]
        df = self.GTFS["shapes.txt"]
        df = change_nan_value(df, None)

        return self.format_df(df, _REQUIRED_FIELD_NAMES)

    def main(self):

        gtfs = self.gtfs()
        stops = self.stops()
        agency = self.agency()
        routes = self.routes()
        calendar = self.calendar()
        calendar_dates = self.is_calendar_dates()
        stop_times = self.stop_times()
        trips = self.trips()
        shapes, trips = self.all_shapes(trips, gtfs)

        return (
            agency, calendar, calendar_dates,
            gtfs, routes, shapes, stops,
            stop_times, trips
        )
