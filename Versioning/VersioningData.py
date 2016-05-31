# -*- coding: utf-8 -*-
"""
@author: Julien Wuthrich
"""
import datetime

import pandas as pd

import Utils.IdTools as idtools


class VersioningData(object):

    def __init__(self, GTFS):

        self.agency = GTFS[0]
        self.calendar = GTFS[1]
        self.calendar_dates = GTFS[2]
        self.gtfs = GTFS[3]
        self.routes = GTFS[4]
        self.shapes = GTFS[5]
        self.stops = GTFS[6]
        self.stop_times = GTFS[7]
        self.trips = GTFS[8]

    @staticmethod
    def gen_sdl_id_for_sequence(df, on_cols, id_col, table, seq_col):

        lst = []
        for sdl_id, uid in df.groupby(id_col):
            df_uid = idtools.hash_frame(
                uid.sort(seq_col)[
                    on_cols
                ]
            )

            lst.append((sdl_id, df_uid))

        df_sdl2db = pd.DataFrame(
            lst,
            columns=[str(table) + "ScheduleId", "Id"]
        )

        return df_sdl2db

    @staticmethod
    def gen_sdl_id(df, cols, _id, table):

        map_df = pd.DataFrame()
        sdl_id = table + "ScheduleId"
        map_df[sdl_id] = df[_id]
        map_df[_id] = idtools.sha1_for_named_columns(
            df,
            cols
        )

        return map_df

    @staticmethod
    def get_services_trips(df):

        service_trips = dict(
            [
                (serv, set(group["Id"]))
                for serv, group in df.groupby("ServiceId")
            ]
        )

        return service_trips

    @staticmethod
    def get_calendars_date_services(cal, calendar_dates):

        day_services = {}
        first_day = min(cal.StartDate)

        for day_idx in xrange((max(cal.EndDate) - first_day).days):
            day = first_day + datetime.timedelta(days=day_idx)

            weekday = day.strftime("%A").title()
            serv_selection = cal[
                (cal[weekday] == 1) &
                (cal.StartDate <= day) &
                (cal.EndDate >= day)
            ]
            services = set(serv_selection["ServiceId"])

            day_eserv = calendar_dates[calendar_dates["Date"] == day]

            add_services = set(
                day_eserv[day_eserv.ExceptionType == 1].ServiceId)
            del_services = set(
                day_eserv[day_eserv.ExceptionType == 2].ServiceId)

            services.update(add_services)
            services.discard(del_services)

            day_services[day] = services

        return day_services

    @staticmethod
    def replace_id_with_generate_id(df, map_id, table):

        table_id = table + "Id"
        table_sdl_id = table + "ScheduleId"

        df = df.rename(columns={"Id": table_id})
        df = pd.merge(
            df, map_id,
            left_on=table_id, right_on=table_sdl_id
        )

        df = df.drop(table_id, 1)

        return df

    def trip_to_date(self):

        date_trips_cols = ["TripId", "ServiceId", "Date", "GtfsId"]

        day_services = VersioningData.get_calendars_date_services(
            self.calendar, self.calendar_dates)
        service_trips = VersioningData.get_services_trips(self.trips)

        day_trips = []
        for day, services in day_services.iteritems():
            for service in services:
                for trip in service_trips.get(service, []):
                    day_trips.append(
                        (trip, service, day, self.gtfs["Id"][0])
                    )

        return pd.DataFrame(day_trips, columns=date_trips_cols)

    def routes(self):

        return VersioningData.gen_sdl_id(
            self.routes,
            ["ShortName", "LongName"],
            "Id",
            "Route"
        )

    def stops(self):

        return VersioningData.gen_sdl_id(
            self.stops,
            ["Latitude", "Longitude"],
            "Id",
            "Stop"
        )

    def shapes(self):

        return VersioningData.gen_sdl_id_for_sequence(
            self.shapes,
            ["Latitude", "Longitude"],
            "Id",
            "Shape",
            "ShapeSequence",
        )

    def stop_times(self, map_stops):

        df = pd.merge(
            self.stop_times, map_stops, left_on="StopId",
            right_on="StopScheduleId"
        )

        df["StopId"] = df["Id"]

        return VersioningData.gen_sdl_id_for_sequence(
            df,
            ["ArrivalTimeSeconds", "StopId"],
            "TripId",
            "Trip",
            "StopSequence"
        )

    def map_id(self):

        map_routes = self.routes(self.routes)
        map_stops = self.stops(self.stops)
        map_shapes = self.shapes(self.shapes)
        map_trips = self.stop_times(self.stop_times, map_stops)

        return (
            map_routes,
            map_stops,
            map_shapes,
            map_trips
        )

    def main(self):

        (
            map_routes, map_stops,
            map_shapes, map_trips
        ) = self.map_id()

        stops = VersioningData.replace_id_with_generate_id(
            self.stops, map_stops, "Stop")

        routes = VersioningData.replace_id_with_generate_id(
            self.routes, map_routes, "Route")

        shapes = VersioningData.replace_id_with_generate_id(
            self.shapes, map_shapes, "Shape")

        trips = self.trips.rename(columns={"Id": "TripdId"})
        trips["ShapeId"] = trips["ShapeId"].astype(unicode)
        trips["RouteId"] = trips["RouteId"].astype(unicode)

        trips = pd.merge(
            trips, map_shapes, left_on="ShapeId", right_on="ShapeScheduleId")
        trips["ShapeId"] = trips["Id"]
        trips = trips.drop("Id", 1)

        trips = pd.merge(
            trips, map_routes, left_on="RouteId", right_on="RouteScheduleId")
        trips["RouteId"] = trips["Id"]
        trips = trips.drop("Id", 1)

        trips = trips.rename(columns={"TripdId": "Id"})

        trips = VersioningData.replace_id_with_generate_id(
            trips, map_trips, "Trip")

        stop_times = pd.merge(
            self.stop_times, map_stops,
            left_on="StopId", right_on="StopScheduleId")
        stop_times["StopId"] = stop_times["Id"]
        stop_times = stop_times.drop("Id", 1)

        stop_times = pd.merge(
            stop_times, map_trips, left_on="TripId", right_on="TripScheduleId")
        stop_times["TripId"] = stop_times["Id"]
        stop_times = stop_times.drop("Id", 1)

        stops.drop_duplicates(subset="Id", inplace=True)
        routes.drop_duplicates(subset="Id", inplace=True)
        stop_times.drop_duplicates(
            subset=["TripId", "StopId"], inplace=True
        )
        trips.drop_duplicates(subset="Id", inplace=True)

        trip_to_date = self.trip_to_date()

        return (
            self.agency, self.calendar, self.calendar_dates,
            self.gtfs, routes, shapes, stops,
            stop_times, trips, trip_to_date
        )
