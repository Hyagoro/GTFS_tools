# -*- coding: utf-8 -*-
"""
@author: Julien Wuthrich
"""
import numpy as np

from scipy.interpolate import UnivariateSpline

from Utils.DateTools import to_sod


class EstimateStopTimes(object):

    def __init__(self):
        pass

    @staticmethod
    def get_interpolator(x, y):
        fxy = [(x, y)
               for x, y in zip(x, y) if not (np.isnan(y) or np.isnan(x))]

        fx, fy = zip(*fxy)
        ip = UnivariateSpline(fx, fy, k=1, s=0)

        def interpolator(x):
            return float(ip(x))

        return interpolator

    @staticmethod
    def fill_times(group, x_column, y_column):
        estimator = EstimateStopTimes.get_interpolator(
            group[x_column], group[y_column])
        estimate_time = lambda row: estimator(
            row[x_column]) if np.isnan(row[y_column]) else row[y_column]
        group[y_column] = group.apply(estimate_time, axis=1)

        return group

    @staticmethod
    def prepare_stop_times(table):
        x_column = "shape_dist_traveled"
        y_column = "arrival_time"

        propagate_times = (
            lambda x: x.arrival_time
            if x.arrival_time >= 0 else x.departure_time)
        table[y_column] = table.apply(propagate_times, axis=1)

        at_least_2 = (lambda g: g[y_column].dropna().size > 1)
        has_mintimes = table.groupby("trip_id").apply(at_least_2)
        bad_ones = list(has_mintimes[has_mintimes == False].index)

        if bad_ones:
            raise Exception("Unable to estimate trips")

        table[y_column] = table[y_column].apply(to_sod)

        estimate_arrival_times = (
            lambda x: EstimateStopTimes.fill_times(x, x_column, y_column))
        table = table.groupby("trip_id").apply(estimate_arrival_times)
        table["departure_time"] = table[y_column]
        table["arrival_time"] = table["departure_time"]

        return table
