from collections import defaultdict
from typing import List
import numpy as np
import pandas as pd

from pyramm.tables import TopSurface


def build_top_surface(tables: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Generate the top_surface table from several RAMM-style surface tables (with
    overlapping surface records. If the table contains "full_width_flag" only full width
    surface records are kept.

    Surfaces are chosen in the following order: "surface_date", "start_m", "end_m". I.e.
    Newer surfaces will take priority, followed by surfaces with a higher "start_m" (if
    "surface_date" is the same), followed by surfaces with a higher "end_m" (if
    "surface_date" and "start_m" are the same).

    :param tables: List of DataFrames containing RAMM-style surface tables.
    :return: new top_surface table with non-overlapping surface records.

    """
    tables = [TopSurface.from_frame(tt) for tt in tables]
    tables = [_reset_surface_table_index(tt) for tt in tables]
    tables = [_limit_to_full_width(tt) for tt in tables]

    df = pd.concat(tables, ignore_index=True)
    top_surface = pd.DataFrame()
    for _, gg in df.groupby("road_id"):
        gg = gg.sort_values(["surface_date", "start_m", "end_m"]).reset_index(drop=True)
        gg.index += 1
        grid = _surface_records_to_grid(gg)
        surfaces = _extract_surface_records_from_grid(grid)
        surfaces = surfaces.join(
            gg[[cc for cc in gg.columns if cc not in ["start_m", "end_m"]]],
            on="surface_id",
        ).drop(columns="surface_id")
        top_surface = pd.concat([top_surface, surfaces], ignore_index=True)
    return top_surface.set_index(["road_id", "start_m", "end_m"]).sort_index()


def append_surface_details_to_segments(df: pd.DataFrame, top_surface: pd.DataFrame):
    """
    Append surface details to each road segment.

    :param df: Road segments. Must contain "road_id", "start_m", "end_m" columns.
    :param top_surface: RAMM-style top_surface table with non-overlapping surface records.
    :return: Road segments with surfaces details appended.

    """
    top_surface = TopSurface.from_frame(top_surface)
    top_surface = _reset_surface_table_index(top_surface)

    surface_columns = [
        cc for cc in top_surface.columns if cc not in ["road_id", "start_m", "end_m"]
    ]
    df[surface_columns] = None

    valid_surfaces = top_surface.loc[top_surface["road_id"].isin(df["road_id"])]
    if len(valid_surfaces) == 0:
        return df

    for _, row in valid_surfaces.iterrows():
        # Limit to the road segments that are fully within the surface section.
        # Using less / greater than ensures that a join on the boundary results in
        # the two straddling segments being excluded.
        selected_segments = df.loc[
            (df["road_id"] == row["road_id"])
            & (df["start_m"] > row["start_m"])
            & (df["end_m"] < row["end_m"])
        ]

        # Append the surface details to the appropriate rows:
        for cc in surface_columns:
            df.loc[selected_segments.index, cc] = row[cc]

    df = _fix_na_columns(df)
    return df


def _limit_to_full_width(df):
    if "full_width_flag" not in df.columns:
        return df
    return df.loc[df["full_width_flag"].str.lower() == "y"]


def _reset_surface_table_index(df):
    return df.reset_index() if df.index.names != [None] else df


def _fix_na_columns(df):
    for cc in df.columns:
        fill_value = 0 if df[cc].dtype.type == np.float64 else ""
        df[cc].fillna(fill_value, inplace=True)
    return df


def _record_to_grid(start_m, end_m, template=None):
    ii_start, ii_end = int(round(start_m)), int(round(end_m)) - 1
    n_points = int(round(end_m))
    grid = np.zeros(n_points) if template is None else np.copy(template)
    grid[ii_start : (ii_end + 1)] = 1
    return grid


def _surface_records_to_grid(df):
    n_points = int(round(df["end_m"].max()))
    template = np.zeros(n_points)
    return np.array(
        [
            surface_id * _record_to_grid(row.start_m, row.end_m, template=template)
            for surface_id, row in df.iterrows()
        ]
    ).max(axis=0)


def _extract_surface_records_from_grid(grid):
    groups = (
        (np.insert(np.diff(grid), 0, 1 if grid[0] > 0 else 0) != 0).astype(int).cumsum()
    )
    surfaces_dict = defaultdict(list)
    for ii_group in set(groups):
        ii_section = np.where(groups == ii_group)[0]
        ii_start, ii_end = ii_section[0], ii_section[-1]
        surface_id = grid[ii_start]
        if surface_id == 0:
            continue
        surfaces_dict["surface_id"].append(surface_id)
        surfaces_dict["start_m"].append(ii_start)
        surfaces_dict["end_m"].append(ii_end + 1)
    return pd.DataFrame(surfaces_dict)
