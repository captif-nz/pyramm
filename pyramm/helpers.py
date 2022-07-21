import re
import numpy as np
import pandas as pd
from collections import defaultdict


def _map_json(json_dict):
    return {_convert(kk): vv for kk, vv in json_dict.items()}


def _convert(name):
    ss = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", ss).lower()


def _record_to_grid(row, template=None):
    ii_start, ii_end = int(round(row.start_m)), int(round(row.end_m)) - 1
    n_points = int(round(row.end_m))
    grid = np.zeros(n_points) if template is None else np.copy(template)
    grid[ii_start : (ii_end + 1)] = 1
    return grid


def _records_to_grid(df):
    n_points = int(round(df["end_m"].max()))
    template = np.zeros(n_points)
    return np.array(
        [ii * _record_to_grid(row, template=template) for ii, row in df.iterrows()]
    ).max(axis=0)


def _extract_records_from_grid(grid):
    groups = (
        (np.insert(np.diff(grid), 0, 1 if grid[0] > 0 else 0) != 0).astype(int).cumsum()
    )
    results_dict = defaultdict(list)
    for ii_group in set(groups):
        ii_section = np.where(groups == ii_group)[0]
        ii_start, ii_end = ii_section[0], ii_section[-1]
        ii = grid[ii_start]
        if ii == 0:
            continue
        results_dict["id"].append(ii)
        results_dict["start_m"].append(ii_start)
        results_dict["end_m"].append(ii_end + 1)
    return pd.DataFrame(results_dict)
