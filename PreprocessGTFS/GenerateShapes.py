# -*- coding: utf-8 -*-
"""
@author: Julien Wuthrich & Jos Rozen
"""
import hashlib
import networkx as nx
import multiprocessing as mp

import pandas as pd

from ScheduleReader.GtfsScheduleReader import load_gtfs


class Graph(object):

    def __init__(self, gtfs_path):

        self.gtfs_path = gtfs_path
        self.GTFS = load_gtfs(self.gtfs_path)

    @staticmethod
    def explore_digraph_from(route, start, path):

        if len(path) > 1:
            for np in path[:-1]:
                if start in route.successors(np):
                    route.remove_edge(np, start)
        path.append(start)
        for ns in route.successors(start):
            if ns not in path:
                Graph.explore_digraph_from(route, ns, path[:])

    @staticmethod
    def simplify_digraph(route):

        starts = []
        for node in route.nodes():
            if not route.predecessors(node):
                starts.append(node)
        if not starts:
            for node in route.nodes():
                if 1 == len(route.predecessors(node)):
                    starts.append(node)
        for s in starts:
            Graph.explore_digraph_from(route, s, [])

    @staticmethod
    def build_digraph(name, direction, trips, stoptimes):

        route = nx.DiGraph()
        for t, trip in trips[
            (trips["route_id"] == name) & (trips["direction_id"] == direction)
        ].iterrows():
            previous = None
            for s, stoptime in stoptimes[
                stoptimes["trip_id"] == trip["trip_id"]
            ].sort("stop_sequence").iterrows():
                current = stoptime["stop_id"]
                if current not in route.nodes():
                    route.add_node(current)
                if previous:
                    if (previous, current) not in route.edges():
                        route.add_edge(previous, current)
                previous = current
        return route

    def prepare_shapes(self, dict_shapes, dict_trip_to_shape, stops):

        shapes_df = pd.DataFrame()
        for key, val in dict_shapes.iteritems():
            shapes = sum(val, [])
            shapes = pd.DataFrame({"stop_id": shapes})
            shapes = pd.merge(
                shapes, stops, on="stop_id"
            )[["stop_lat", "stop_lon"]]
            dict_shapes[key] = shapes
            shapes["shape_id"] = key
            shapes_df = pd.concat([shapes_df, shapes])
            shapes_df["shape_pt_sequence"] = shapes_df.index

        trip_to_shape = pd.DataFrame(
            {
                "trip_id": dict_trip_to_shape.keys(),
                "shape_id": dict_trip_to_shape.values()
            }
        )
        return dict_shapes, trip_to_shape, shapes_df

    def graph_of_route_direction(self, route_id, direction):

        gtfs = load_gtfs(self.gtfs_path)
        stops = gtfs['stops.txt']
        stoptimes = gtfs["stop_times.txt"]
        trips = gtfs["trips.txt"]

        stoptimes = pd.merge(
            trips[["route_id", "trip_id", "direction_id"]],
            stoptimes, on="trip_id"
        )

        route = Graph.build_digraph(route_id, direction, trips, stoptimes)
        Graph.simplify_digraph(route)

        trip_of_direction_route = stoptimes[
            (stoptimes["route_id"] == route_id) &
            (stoptimes["direction_id"] == direction)
        ]

        list_trip_of_direction_route = trip_of_direction_route[
            "trip_id"].unique()
        dict_shapes = {}
        dict_trip_to_shape = {}

        for trip in list_trip_of_direction_route:
            trip_sequence = list(
                stoptimes[stoptimes["trip_id"] == trip]["stop_id"]
            )
            sha1 = hashlib.sha1(
                str(trip_sequence).encode("utf8")).hexdigest()
            dict_trip_to_shape[trip] = sha1
            if sha1 not in dict_shapes.keys():
                path = [trip_sequence[0]]
                for stop in trip_sequence[1:]:
                    try:
                        if len(path) == 1:
                            shortest_path = nx.shortest_path(
                                route, path[0], stop)
                        else:
                            shortest_path = nx.shortest_path(
                                route, path[-1][-1], stop
                            )
                    except:
                        shortest_path = [path[-1][-1], stop]

                    path.append(shortest_path)
                dict_shapes[sha1] = path[1:]

        return self.prepare_shapes(dict_shapes, dict_trip_to_shape, stops)

    def mp_graph_of_route_direction(self, args):

        return self.graph_of_route_direction(*args)

    def export_shapes(self, list_df, gtfs, trips):

        shapes_df = pd.DataFrame()
        trip_to_shape = pd.DataFrame()

        for df in list_df:
            shapes_df = pd.concat([shapes_df, df[2]])
            trip_to_shape = pd.concat([trip_to_shape, df[1]])

        trip_to_shape["trip_id"] = trip_to_shape["trip_id"].str.lower()

        shapes_df.rename(
            columns={
                "stop_lat": "shape_pt_lat", "stop_lon": "shape_pt_lon"
            }, inplace=True
        )

        shapes_df["shape_dist_traveled"] = None
        shapes_df["gtfs_id"] = gtfs['gtfs_id'][0]

        trip_to_shape = trip_to_shape[["trip_id", "shape_id"]]

        try:
            trips = trips.drop("shape_id", 1)
        except:
            pass

        trips = pd.merge(
            trips, trip_to_shape, on="trip_id"
        )

        return shapes_df, trips

    def generate_shapes(self, gtfs, trips):

        routes = self.GTFS["routes.txt"]
        list_route = routes["route_id"].unique()
        list_direction = [0, 1]

        l_args = [
            (
                route,
                direction
            )
            for route in list_route
            for direction in list_direction
        ]

        pool = mp.Pool()
        list_df = pool.map(self.mp_generate_seq_from_trips_for_a_route, l_args)
        pool.close()
        pool.join()

        return self.export_shapes(list_df, gtfs, trips)
