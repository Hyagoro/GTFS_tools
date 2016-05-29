# -*- coding: utf-8 -*-
"""
@author: Julien Wuthrich
"""
import numpy as np


class Distance(object):

    def __init__(self):

        self.EARTH_RADIUS_IN_METERS = 6372797.560856

    def loc2array(self, loc):

        lat, lon = loc

        return np.array([lat, lon])

    def _haversine(self, o, d):

        o_rad = np.radians(o)
        d_rad = np.radians(d)

        lat_arc, lon_arc = abs(o_rad - d_rad)
        a = np.sin(lat_arc * 0.5)**2 + (
            np.cos(o_rad[0]) * np.cos(d_rad[0]) * np.sin(lon_arc * 0.5)**2
        )
        c = 2 * np.arcsin(np.sqrt(a))

        return self.EARTH_RADIUS_IN_METERS * c

    def haversine(self, origin, destination):

        o = self.loc2array(origin)
        d = self.loc2array(destination)

        return self._haversine(o, d)

    def euclidean(self, origin, destination):

        o = self.loc2array(origin)
        d = self.loc2array(destination)

        return np.linalg.norm(d - o)
