
import pyproj

from functools import partial, lru_cache
from shapely import ops
from shapely.wkt import loads


@lru_cache(maxsize=5)
def project(from_crs, to_crs):
    return partial(
        pyproj.transform, pyproj.Proj(init=from_crs), pyproj.Proj(init=to_crs)
    )


def transform(geometry, from_crs="epsg:4326", to_crs="epsg:2193"):
    return ops.transform(project(from_crs, to_crs), geometry)


# import geopandas as gp
# import pandas as pd
# import shapely as sp
# import numpy as np
# from numpy.linalg import norm
# from collections import deque
# from functools import partial, lru_cache
# from scipy.spatial import KDTree


# class Centreline(object):
#     def __init__(self, carrway):
#         # Used to find the displacement value of a point projected onto the
#         # nearest road feature. The length of the target line is specified and
#         # does not need to match the length of the geometry feature (this is
#         # usually the case with RAMM features).
#         #
#         # df can be a GeoDataFrame, or a standard DataFrame containing the RAMM
#         # carr_way table with the 'sh_direction' and 'sh_element_type' columns
#         # from the RAMM roadname table.

#         # The reference crs is used when projecting the point onto a line.
#         self.ref_crs = {"init": "epsg:2193"}

#         # Project to the reference crs:
#         self._df_features = _convert_to_geopandas(df, self.ref_crs)

#         geometry = []
#         idx = []
#         offset = 0.1  # Start/end offset in metres.
#         for i, row in self._df_features.iterrows():
#             # Add additional points over the first/last 20 metres to avoid
#             # incorrect snapping at gaps.
#             coords = deque(list(row.geometry.coords)[1:-2])
#             for rr in range(20, 0, -5):
#                 coords.appendleft(
#                     row.geometry.interpolate(
#                         rr / row.geometry.length, normalized=True
#                     ).coords[0]
#                 )
#                 coords.append(
#                     row.geometry.interpolate(
#                         1 - rr / row.geometry.length, normalized=True
#                     ).coords[0]
#                 )

#             # Modify the lines so that consecutive lines don't start/end right
#             # on top of one another:
#             coords.appendleft(
#                 row.geometry.interpolate(
#                     offset / row.geometry.length, normalized=True
#                 ).coords[0]
#             )
#             coords.append(
#                 row.geometry.interpolate(
#                     1 - offset / row.geometry.length, normalized=True
#                 ).coords[0]
#             )

#             # Extract the points and id for each feature:
#             n_points = len(coords)
#             geometry += [sp.geometry.Point(xy) for xy in coords]
#             idx += [row["carr_way_no"]] * n_points

#         # Store individual points with corresponding id in a GeoDataFrame:
#         self._df_points = gp.GeoDataFrame(
#             {"id": idx}, geometry=geometry, crs=self.ref_crs
#         )

#         # All points formatted as a multipoint object:
#         self._as_multipoint = self._df_points.geometry.unary_union

#         xx = [row.coords[0][0] for row in self._df_points.geometry]
#         yy = [row.coords[0][1] for row in self._df_points.geometry]
#         self._kdtree = KDTree(np.array(list(zip(xx, yy))))

#     def transform(self, geometry, crs):
#         # Project geometry to the reference crs. Returns a tuple with the new
#         # geometry coordinates and its crs.
#         project = partial(
#             pyproj.transform,
#             pyproj.Proj(init=crs["init"]),
#             pyproj.Proj(init=self.ref_crs["init"]),
#         )
#         return sp.ops.transform(project, geometry), self.ref_crs

#     def nearest_feature(self, point, point_crs={"init": "epsg:4326"}, kdtree=True):
#         # Find the id of the feature nearest to a specified point. The KDTree
#         # method is used by default.

#         if point_crs != self.ref_crs:
#             # Transform point to the ref_crs
#             point, point_crs = self.transform(point, point_crs)

#         distance = None  # Not calculated for brute force method.
#         if kdtree:
#             _, ii = self._kdtree.query(point.coords[0], 2)
#             id = self._df_points.loc[ii[0], "id"]

#             # Calculate offset distance:
#             p1 = np.array(self._df_points.loc[ii[0], "geometry"].coords)
#             p2 = np.array(self._df_points.loc[ii[1], "geometry"].coords)
#             p3 = np.array(point.coords)
#             distance = (np.abs(np.cross(p2-p1, p1-p3)) / norm(p2-p1))[0]
#         else:
#             _n = (
#                 self._df_points.geometry
#                 == sp.ops.nearest_points(point, self._as_multipoint)[1]
#             )
#             id = list(self._df_points.loc[_n, "id"])[0]

#         row = self._df_features[self._df_features["carr_way_no"] == id].index[0]

#         return row, distance

#     def displacement(self, point, point_crs={"init": "epsg:4326"}, kdtree=True):
#         try:
#             # Find the position along the line that is closest to the specified
#             # point. Also returns the road_id.

#             if point_crs != self.ref_crs:
#                 # Transform point to the ref_crs:
#                 point, point_crs = self.transform(point, point_crs)

#             # Find the nearest line feature to the specified point:
#             row, offset_m = self.nearest_feature(point, point_crs, kdtree)

#             start_m = self._df_features.loc[row, "carrway_start_m"]
#             length_m = self._df_features.loc[row, "length_m"]

#             position = self._df_features.geometry[row].project(point, True)

#             return (
#                 start_m + position * length_m,
#                 self._df_features.loc[row, "road_id"],
#                 self._df_features.loc[row, "carr_way_no"],
#                 offset_m
#             )
#         except Exception as e:
#             return None, None, None, None

#     def append_geometry(self, df, geometry_type="wkt"):
#         """
#         Append geometry to dataframe. Dataframe must contain road_id, start_m and end_m.

#         Parameters
#         ----------
#         df : pd.DataFrame

#         geometry_type : str
#             Use "wkt" to return a well-known-text string. Use "coord" to return the
#             coordinates of the start_m position.

#         """
#         if geometry_type not in ["wkt", "coord"]:
#             raise exceptions.AttributeError

#         geometry = []
#         for _, row in df.iterrows():
#             geometry.append(
#                 self.extract_geometry(row["road_id"], row["start_m"], row["end_m"])
#             )
#         if geometry_type == "wkt":
#             df["wkt"] = [gg.wkt for gg in geometry]
#         elif geometry_type == "coord":
#             coords = [gg.coords[0] for gg in geometry]
#             df["easting"] = [cc[0] for cc in coords]
#             df["northing"] = [cc[1] for cc in coords]
#         return df

#     def extract_geometry(self, road_id, start_m, end_m):
#         """Extract the part of the centreline that corresponds to the section
#         of interest.

#         Parameters
#         ----------
#         road_id : int

#         start_m : float

#         end_m : float

#         Returns
#         -------
#         sp.geometry.LineString
#             Geometry object.

#         """

#         # TODO: move to __init__ (it may break the Cpx.route_position method).
#         centreline = self._df_features.set_index("carr_way_no")

#         selected_cways = centreline.loc[
#             (centreline["road_id"] == road_id)
#             & (centreline["carrway_end_m"] > start_m)
#             & (centreline["carrway_start_m"] < end_m)
#         ].sort_values("carrway_start_m")

#         cway_no = selected_cways.index.tolist()
#         if len(cway_no) == 0:
#             return None

#         # Find start:
#         current_cway = centreline.loc[cway_no[0]]
#         ref_pos = (start_m - current_cway["carrway_start_m"]) / current_cway["length_m"]
#         extracted_coords = [
#             current_cway["geometry"].interpolate(ref_pos, normalized=True).coords[0]
#         ]

#         for ii, cc in enumerate(current_cway["geometry"].coords):
#             pos = current_cway["geometry"].project(
#                 sp.geometry.Point(cc), normalized=True
#             )
#             if pos > ref_pos:
#                 break

#         if len(selected_cways) == 1:
#             # There both start_m and end_m lie on the same carr_way elements.

#             # Find the end point:
#             ref_pos = (end_m - current_cway["carrway_start_m"]) / current_cway[
#                 "length_m"
#             ]
#             for jj, cc in enumerate(current_cway["geometry"].coords):
#                 pos = current_cway["geometry"].project(
#                     sp.geometry.Point(cc), normalized=True
#                 )
#                 if pos > ref_pos:
#                     break

#             # Build geometry:
#             extracted_coords += current_cway["geometry"].coords[ii:jj]
#             extracted_coords.append(
#                 current_cway["geometry"].interpolate(ref_pos, normalized=True).coords[0]
#             )

#         else:
#             # Find the end point.
#             # Select the last carr_way element
#             current_cway = centreline.loc[cway_no[-1]]
#             ref_pos = (end_m - current_cway["carrway_start_m"]) / current_cway[
#                 "length_m"
#             ]
#             for jj, cc in enumerate(current_cway["geometry"].coords):
#                 pos = current_cway["geometry"].project(
#                     sp.geometry.Point(cc), normalized=True
#                 )
#                 if pos > ref_pos:
#                     break

#             # Build geometry.
#             # Coords from first carr_way element:
#             extracted_coords += centreline.loc[cway_no[0]]["geometry"].coords[ii:]

#             if len(selected_cways) > 2:
#                 # There are additional (complete) carr_way elements between the
#                 # first and last carr_way elements:
#                 for kk in cway_no[1:-1]:
#                     # The first point is shared with the previous carr_way
#                     # element, so don't append it.
#                     extracted_coords += centreline.loc[kk]["geometry"].coords[1:]

#             # Add the points from the last carr_way element. Ignore the first
#             # point (shared with previous carr_way element).
#             extracted_coords += centreline.loc[cway_no[-1]]["geometry"].coords[1:jj]
#             # Finally add the last point (interpolated).
#             extracted_coords.append(
#                 centreline.loc[cway_no[-1]]["geometry"]
#                 .interpolate(ref_pos, normalized=True)
#                 .coords[0]
#             )

#         return sp.geometry.LineString(extracted_coords)

#     def limited_centreline(self, road_id):
#         """Return a limited version of the centreline object, with only the
#         specified road_ids present.

#         Parameters
#         ----------
#         road_id : list or int
#             target road_id or list of target road_ids.

#         Returns
#         -------
#         altramm.geom.Centreline
#             Limited version of centreline.

#         """
#         if type(road_id) is not list:
#             road_id = [road_id]

#         return Centreline(
#             self._df_features.loc[self._df_features.road_id.isin(road_id)]
#         )
