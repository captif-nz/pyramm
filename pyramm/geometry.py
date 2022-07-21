from io import UnsupportedOperation
import pyproj
import pandas as pd
import numpy as np

from collections import deque
from functools import lru_cache
from numpy.linalg import norm
from scipy.spatial import KDTree
from shapely import ops
from shapely.wkt import loads  # Load into geometry namespace
from shapely.geometry import (
    Point,
    Polygon,
    LineString,
    MultiPoint,
    MultiPolygon,
    MultiLineString,
)
from shapely.geometry.base import BaseGeometry
from typing import List, Optional

from pyramm.helpers import _records_to_grid, _extract_records_from_grid


ROADNAME_COLUMNS = [
    "sh_ne_unique",
    "sh_state_hway",
    "sh_element_type",
    "sh_ref_station_no",
    "sh_rp_km",
    "sh_direction",
    "road_region",
    "road_type",
]


@lru_cache(maxsize=5)
def project(from_crs, to_crs):
    return pyproj.Transformer.from_crs(
        pyproj.CRS(f"epsg:{from_crs:d}"), pyproj.CRS(f"epsg:{to_crs:d}"), always_xy=True
    ).transform


def _transform_single(geometry, from_crs=4326, to_crs=2193):
    return ops.transform(project(from_crs, to_crs), geometry)


def transform(geometry, from_crs=4326, to_crs=2193):
    if isinstance(geometry, list):
        if isinstance(geometry[0], Polygon):
            geometry = MultiPolygon(geometry)
        elif isinstance(geometry[0], LineString):
            geometry = MultiLineString(geometry)
        else:
            geometry = MultiPoint(geometry)
    return _transform_single(geometry, from_crs, to_crs)


def _build_point_layer(df):
    geometry, idx = [], []
    offset = 0.1  # Start/end offset in metres.
    for i, row in df.iterrows():
        # Add additional points over the first/last 20 metres to avoid
        # incorrect snapping at gaps.
        coords = deque(list(row.geometry.coords)[1:-2])
        for rr in range(20, 0, -5):
            coords.appendleft(
                row.geometry.interpolate(
                    rr / row.geometry.length, normalized=True
                ).coords[0]
            )
            coords.append(
                row.geometry.interpolate(
                    1 - rr / row.geometry.length, normalized=True
                ).coords[0]
            )

        # Modify the lines so that consecutive lines don't start/end right
        # on top of one another:
        coords.appendleft(
            row.geometry.interpolate(
                offset / row.geometry.length, normalized=True
            ).coords[0]
        )
        coords.append(
            row.geometry.interpolate(
                1 - offset / row.geometry.length, normalized=True
            ).coords[0]
        )

        # Extract the points and id for each feature:
        geometry += [Point(xy) for xy in coords]
        idx += [row.name] * len(coords)

    # Store individual points with corresponding id in a GeoDataFrame:
    return pd.DataFrame({"id": idx, "geometry": geometry})


def _coords(df):
    return [gg.coords[0] for gg in df.geometry.tolist()]


def _x_coords(coords):
    return [cc[0] for cc in coords]


def _y_coords(coords):
    return [cc[1] for cc in coords]


def _build_kdtree(df):
    return KDTree(np.array(list(zip(_x_coords(_coords(df)), _y_coords(_coords(df))))))


class Centreline(object):
    def __init__(self, df: pd.DataFrame):
        """
        Used to find the displacement value of a point projected onto the nearest road feature. The length of the target line is specified and does not need to match the length of the geometry feature (this is usually the case with RAMM features).

        df can be a GeoDataFrame, or a standard DataFrame containing the RAMM carr_way table with the 'sh_direction' and 'sh_element_type' columns from the RAMM roadname table.

        The reference crs is used when projecting the point onto a line.

        """
        self.ref_crs = 2193
        self._df_features = df.drop_duplicates(
            ["road_id", "carrway_start_m", "carrway_end_m"]
        )
        self._df_points = _build_point_layer(self._df_features)
        self._kdtree = _build_kdtree(self._df_points)

    def nearest_feature(
        self,
        point: Point,
        point_crs: int = 4326,
    ):
        """
        Find the id of the feature nearest to a specified point.

        """

        if point_crs != self.ref_crs:
            point = transform(point, point_crs, self.ref_crs)

        _, ii = self._kdtree.query(point.coords[0], 2)
        carr_way_no = self._df_points.loc[ii[0], "id"]

        # Calculate offset distance:
        p1 = np.array(self._df_points.loc[ii[0], "geometry"].coords)
        p2 = np.array(self._df_points.loc[ii[1], "geometry"].coords)
        p3 = np.array(point.coords)

        offset_m = (np.abs(np.cross(p2 - p1, p1 - p3)) / norm(p2 - p1))[0]

        return carr_way_no, offset_m

    def displacement(*args, **kwargs):
        raise UnsupportedOperation(
            "Centreline.displacement() has been replaced by the Centreline.position() "
            "method."
        )

    def position(
        self,
        point: Point,
        point_crs: int = 4326,
    ):
        """
        Find the position along the line that is closest to the specified point. Returns
        a dictionary with `road_id`, `position_m` and `search_offset_m` keys.

        """
        if point_crs != self.ref_crs:
            point = transform(point, point_crs, self.ref_crs)

        # Find the nearest line feature to the specified point:
        carr_way_no, offset_m = self.nearest_feature(point, self.ref_crs)

        start_m = self._df_features.loc[carr_way_no, "carrway_start_m"]
        length_m = self._df_features.loc[carr_way_no, "length_m"]

        position = self._df_features.geometry[carr_way_no].project(point, True)

        return {
            "position_m": start_m + position * length_m,
            "road_id": self._df_features.loc[carr_way_no, "road_id"],
            "search_offset_m": offset_m,
        }

    def append_geometry(
        self, df: pd.DataFrame, geometry_type: str = "wkt"
    ) -> pd.DataFrame:
        """
        Append geometry to dataframe.

        :param df: dataframe containing road_id, start_m and end_m columns
        :param geometry_type: geometry type can be "wkt" for well-known-text string or "coord" for the coordinates of the start_m position, defaults to "wkt"
        :return: dataframe with geometry appended

        """
        if geometry_type not in ["wkt", "coord"]:
            raise AttributeError

        geometry = []
        for _, row in df.iterrows():
            geometry.append(
                self.extract_geometry(row["road_id"], row["start_m"], row["end_m"])
            )
        if geometry_type == "wkt":
            df["wkt"] = self._extract_wkt_from_list_of_geometry_objects(geometry)
        elif geometry_type == "coord":
            coords = [gg.coords[0] for gg in geometry]
            df["easting"] = [cc[0] for cc in coords]
            df["northing"] = [cc[1] for cc in coords]
        return df

    @staticmethod
    def _extract_wkt_from_list_of_geometry_objects(
        geometry: List[Optional[BaseGeometry]],
    ) -> List[Optional[str]]:
        return [gg.wkt if gg else None for gg in geometry]

    def extract_geometry(
        self, road_id: int, start_m: float, end_m: float
    ) -> LineString:
        """
        Extract the part of the centreline that corresponds to the section of interest.

        :param road_id: road ID
        :param start_m: start position (m)
        :param end_m: end position (m)
        :return: linestring geometry of road section

        """
        centreline = self._df_features

        selected_cways = centreline.loc[
            (centreline["road_id"] == road_id)
            & (centreline["carrway_end_m"] > start_m)
            & (centreline["carrway_start_m"] < end_m)
        ].sort_values("carrway_start_m")

        cway_no = selected_cways.index.tolist()
        if len(cway_no) == 0:
            return None

        # Find start:
        current_cway = centreline.loc[cway_no[0]]
        ref_pos = (start_m - current_cway["carrway_start_m"]) / current_cway["length_m"]
        extracted_coords = [
            current_cway["geometry"].interpolate(ref_pos, normalized=True).coords[0]
        ]

        for ii, cc in enumerate(current_cway["geometry"].coords):
            pos = current_cway["geometry"].project(Point(cc), normalized=True)
            if pos > ref_pos:
                break

        if len(selected_cways) == 1:
            # There both start_m and end_m lie on the same carr_way elements.

            # Find the end point:
            ref_pos = (end_m - current_cway["carrway_start_m"]) / current_cway[
                "length_m"
            ]
            for jj, cc in enumerate(current_cway["geometry"].coords):
                pos = current_cway["geometry"].project(Point(cc), normalized=True)
                if pos > ref_pos:
                    break

            # Build geometry:
            extracted_coords += current_cway["geometry"].coords[ii:jj]
            extracted_coords.append(
                current_cway["geometry"].interpolate(ref_pos, normalized=True).coords[0]
            )

        else:
            # Find the end point.
            # Select the last carr_way element
            current_cway = centreline.loc[cway_no[-1]]
            ref_pos = (end_m - current_cway["carrway_start_m"]) / current_cway[
                "length_m"
            ]
            for jj, cc in enumerate(current_cway["geometry"].coords):
                pos = current_cway["geometry"].project(Point(cc), normalized=True)
                if pos > ref_pos:
                    break

            # Build geometry.
            # Coords from first carr_way element:
            extracted_coords += centreline.loc[cway_no[0]]["geometry"].coords[ii:]

            if len(selected_cways) > 2:
                # There are additional (complete) carr_way elements between the
                # first and last carr_way elements:
                for kk in cway_no[1:-1]:
                    # The first point is shared with the previous carr_way
                    # element, so don't append it.
                    extracted_coords += centreline.loc[kk]["geometry"].coords[1:]

            # Add the points from the last carr_way element. Ignore the first
            # point (shared with previous carr_way element).
            extracted_coords += centreline.loc[cway_no[-1]]["geometry"].coords[1:jj]
            # Finally add the last point (interpolated).
            extracted_coords.append(
                centreline.loc[cway_no[-1]]["geometry"]
                .interpolate(ref_pos, normalized=True)
                .coords[0]
            )

        return LineString(extracted_coords)


def build_chainage_layer(centreline, road_id, length_m=1000, width_m=300):
    selected = _extract_centreline(centreline, road_id)
    chainage_base = centreline.append_geometry(
        _build_chainage_base_table(selected, length_m)
    )
    return build_label_layer(chainage_base, width_m)


def build_label_layer(df, width_m=300):
    df = df.copy()
    for ii, row in df.iterrows():
        df.loc[ii, "wkt"] = _generate_perpendicular_geometry(
            loads(row["wkt"]), row["direction"], width_m
        )
    return df


def build_partial_centreline(centreline, roadnames, lengths: dict):
    df_features = centreline._df_features

    records = []
    for road_id, start_end in lengths.items():
        selected_features = df_features.loc[df_features["road_id"] == road_id]

        if start_end is None:
            start_m = selected_features["carrway_start_m"].min()
            end_m = selected_features["carrway_end_m"].max()
        else:
            start_m, end_m = start_end
            end_m = selected_features["carrway_end_m"].max() if end_m is None else end_m

        records.append({"road_id": road_id, "start_m": start_m, "end_m": end_m})

    df = (
        centreline.append_geometry(pd.DataFrame(records))
        .rename(columns={"start_m": "carrway_start_m", "end_m": "carrway_end_m"})
        .join(roadnames[ROADNAME_COLUMNS], on="road_id")
    )
    df["length_m"] = abs(df["carrway_end_m"] - df["carrway_start_m"])
    df["geometry"] = [loads(ww) for ww in df["wkt"]]

    return Centreline(df)


def _generate_perpendicular_geometry(linestring, direction, width_m):
    points = [np.array(pp) for pp in zip(*linestring.xy)]
    pt1, pt2 = points[0], points[-1]
    m = -1 / ((pt2[1] - pt1[1]) / (pt2[0] - pt1[0]))
    theta = np.arctan(m)
    dx = width_m * np.cos(theta)
    dy = width_m * np.sin(theta)
    if direction == "D":
        pt3 = pt1 - np.array([dx, dy])
    else:
        pt3 = pt1 + np.array([dx, dy])
    return LineString([pt1, pt3])


def _build_chainage_base_table(selected, length_m):
    groupby = ["road_id", "sh_state_hway", "sh_ref_station_no", "sh_direction"]
    df = pd.DataFrame()
    for (road_id_, sh, rs, direction), gg in selected.groupby(groupby):
        df_ = pd.DataFrame(
            {
                "road_id": road_id_,
                "start_m": np.arange(*_carrway_start_end_m(gg), length_m),
            }
        )
        df_["end_m"] = df_["start_m"] + 1
        df_["direction"] = direction
        df_["label"] = _generate_rsrp_labels(
            sh, rs, direction, df_["start_m"].to_list()
        )
        df = pd.concat([df, df_], ignore_index=True)
    return df


def _generate_rsrp_labels(sh, rs, direction, rps):
    rs = float(rs)
    rps = [float(rp) for rp in rps]
    return [f"{sh}-{rs:04.0f}/{rp/1000:05.2f}-{direction}" for rp in rps]


def _carrway_start_end_m(df):
    return df.carrway_start_m.min(), df.carrway_end_m.max()


def _extract_centreline(centreline_obj, road_id):
    if isinstance(road_id, int):
        road_id = [road_id]
    return centreline_obj._df_features.loc[
        centreline_obj._df_features["road_id"].isin(road_id)
    ]


def combine_continuous_segments(
    df: pd.DataFrame,
    groupby: list = ["road_id"],
) -> pd.DataFrame:
    """Combines road segments with consecutive start_m and end_m values. This is useful
    as a preparation step before appending centreline geometry where a continuous segment
    is desired over its components.

    By default segments are grouped by "road_id". This can be adjusted using the
    `group_by` parameter. The groupby values are included in the resulting dataframe.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing the segments to be combined with "road_id", "start_m" and
        "end_m" columns. If the `groupby` parameter is used these columns must also be
        present.
    groupby : list, optional
        Column names used to group the segments prior to combining, by default
        ["road_id"]

    Returns
    -------
    pd.DataFrame
        Combined road segments.
    """
    sort_columns = ["road_id", "start_m", "end_m"]
    combined = pd.DataFrame()
    for values, gg in df.groupby(groupby):
        gg = (
            gg.drop_duplicates(["start_m", "end_m"])
            .sort_values(["start_m", "end_m"])
            .set_index(pd.Index([1] * len(gg)))
        )
        records = _extract_records_from_grid(_records_to_grid(gg)).drop(columns="id")

        values = [values] if not isinstance(values, tuple) else values
        for kk, vv in zip(groupby, values):
            records[kk] = vv

        combined = pd.concat([combined, records], axis=0, ignore_index=True)

    return combined[sort_columns + [cc for cc in groupby if cc not in sort_columns]]
