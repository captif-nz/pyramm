import pytest
import pandas as pd
from pandas.testing import assert_frame_equal
from shapely.geometry import Point, MultiPoint

from pyramm.geometry import (
    build_partial_centreline,
    combine_continuous_segments,
    build_chainage_layer,
)


@pytest.mark.parametrize(
    "point,lengths,road_id,position_m",
    [
        (Point((172.608406, -43.451023)), {3656: None}, 3656, 179.462),
        (Point((172.608406, -43.451023)), {3656: None, 3654: None}, 3654, 241.879),
        (Point((172.608406, -43.451023)), {3656: [10, 100]}, 3656, 100),
        (Point((172.608406, -43.451023)), {3656: [200, None]}, 3656, 200),
        (Point((172.631957, -43.436542)), {1716: None}, 1716, 3371.371),
    ],
)
def test_build_partial_centreline(
    point, lengths, road_id, position_m, centreline, roadnames
):
    partial_centreline = build_partial_centreline(centreline, roadnames, lengths)
    position = partial_centreline.position(point)
    assert position["road_id"] == road_id
    assert round(position["position_m"], 3) == position_m


def test_combine_continuous_segments():
    df = pd.DataFrame(
        [
            {"road_id": 1, "start_m": 40, "end_m": 50},
            {"road_id": 1, "start_m": 50, "end_m": 60},
            {"road_id": 1, "start_m": 60, "end_m": 70},
            {"road_id": 1, "start_m": 100, "end_m": 110},
            {"road_id": 2, "start_m": 1000, "end_m": 1010},
            {"road_id": 2, "start_m": 1000, "end_m": 1010},  # test with duplicate row
            {"road_id": 2, "start_m": 1010, "end_m": 1020},
            {"road_id": 3, "start_m": 0, "end_m": 20},
        ]
    )
    expected = pd.DataFrame(
        [
            {"road_id": 1, "start_m": 40, "end_m": 70},
            {"road_id": 1, "start_m": 100, "end_m": 110},
            {"road_id": 2, "start_m": 1000, "end_m": 1020},
            {"road_id": 3, "start_m": 0, "end_m": 20},
        ]
    )
    new = combine_continuous_segments(df)
    assert_frame_equal(new, expected)


def test_combine_continuous_segments_groupby():
    df = pd.DataFrame(
        [
            {"road_id": 1, "start_m": 40, "end_m": 50, "c_surface_id": 1},
            {"road_id": 1, "start_m": 50, "end_m": 60, "c_surface_id": 1},
            {"road_id": 1, "start_m": 60, "end_m": 70, "c_surface_id": 2},
        ]
    )
    expected = pd.DataFrame(
        [
            {"road_id": 1, "start_m": 40, "end_m": 60, "c_surface_id": 1},
            {"road_id": 1, "start_m": 60, "end_m": 70, "c_surface_id": 2},
        ]
    )
    new = combine_continuous_segments(df, groupby=["road_id", "c_surface_id"])
    assert_frame_equal(new, expected)


def test_build_chainage_layer(centreline):
    df = build_chainage_layer(centreline, [3664, 3670, 3667])
    assert df[
        [
            "is_start",
            "is_end",
            "is_ramp",
            "is_2000s",
            "is_1000s",
            "is_500s",
            "is_200s",
            "is_100s",
            "label",
        ]
    ].to_dict("records") == [
        {
            "is_start": True,
            "is_end": False,
            "is_ramp": False,
            "is_2000s": False,
            "is_1000s": False,
            "is_500s": False,
            "is_200s": False,
            "is_100s": False,
            "label": "01S-0333/01.40-D",
        },
        {
            "is_start": False,
            "is_end": False,
            "is_ramp": False,
            "is_2000s": True,
            "is_1000s": True,
            "is_500s": True,
            "is_200s": True,
            "is_100s": True,
            "label": "01S-0333/02.00-D",
        },
        {
            "is_start": False,
            "is_end": False,
            "is_ramp": False,
            "is_2000s": False,
            "is_1000s": True,
            "is_500s": True,
            "is_200s": True,
            "is_100s": True,
            "label": "01S-0333/03.00-D",
        },
        {
            "is_start": False,
            "is_end": False,
            "is_ramp": False,
            "is_2000s": True,
            "is_1000s": True,
            "is_500s": True,
            "is_200s": True,
            "is_100s": True,
            "label": "01S-0333/04.00-D",
        },
        {
            "is_start": False,
            "is_end": True,
            "is_ramp": False,
            "is_2000s": False,
            "is_1000s": False,
            "is_500s": False,
            "is_200s": False,
            "is_100s": False,
            "label": "01S-0333/04.13-D",
        },
        {
            "is_start": True,
            "is_end": False,
            "is_ramp": True,
            "is_2000s": True,
            "is_1000s": True,
            "is_500s": True,
            "is_200s": True,
            "is_100s": True,
            "label": "01S-0333/00.00-R2",
        },
        {
            "is_start": False,
            "is_end": True,
            "is_ramp": True,
            "is_2000s": False,
            "is_1000s": False,
            "is_500s": False,
            "is_200s": False,
            "is_100s": False,
            "label": "01S-0333/00.25-R2",
        },
    ]


def test_build_limited_centreline(centreline):
    points = MultiPoint(
        [
            (172.608406, -43.451023),
            (172.631957, -43.436542),
        ]
    )
    limited_centreline = centreline.build_limited_centreline(
        points=points,
    )
    assert len(limited_centreline._df_features) == 8


def test_nearest_feature_kdtree(centreline):
    limited_centreline = centreline.build_limited_centreline(
        points=MultiPoint([(172.618567, -43.441594)]),
    )
    carr_way_no, offset_m = limited_centreline.nearest_feature(
        point=Point((172.618567, -43.441594)),
        method="kdtree",
    )
    assert carr_way_no == 11263
    assert round(offset_m, 1) == 26.1
    assert limited_centreline._kdtree is not None


def test_nearest_feature_shortest_line(centreline):
    carr_way_no, offset_m = centreline.nearest_feature(
        point=Point((172.618567, -43.441594)),
        method="shortest line",
    )
    assert carr_way_no == 11263
    assert round(offset_m, 1) == 26.1


def test_nearest_feature_shortest_line_road_id(centreline):
    carr_way_no, offset_m = centreline.nearest_feature(
        point=Point((175.090586493333, -40.83538405)),
        method="shortest line",
        road_id=3816,
    )
    assert carr_way_no == 11747
    assert round(offset_m, 1) == 2.9


def test_nearest_feature_shortest_line_road_id_2(centreline):
    carr_way_no, offset_m = centreline.nearest_feature(
        point=Point((175.090586493333, -40.83538405)),
        method="shortest line",
        road_id=3563,
    )
    assert carr_way_no == 10771
    assert round(offset_m, 1) == 2.9


def test_nearest_feature_shortest_line_coincident_with_centreline(centreline):
    from shapely.geometry import Point

    point = Point(1776931.317417459, 5478527.211259831)
    carr_way_no, offset_m = centreline.nearest_feature(
        point,
        point_crs=2193,
        method="shortest_line",
    )
    assert carr_way_no == 11747
    assert offset_m == pytest.approx(0)


def test_extract_geometry(centreline):
    geometry = centreline.extract_geometry(
        road_id=3589,
        start_m=5330,
        end_m=5350,
    )
    assert len(geometry.coords) == 3
    assert geometry.coords[0] == pytest.approx(
        (1564198.6479590088, 5186205.344215191),
    )
    assert geometry.coords[-1] == pytest.approx(
        (1564189.0309959787, 5186187.792160582),
    )


def test_extract_geometry_ends_only(centreline):
    geometry = centreline.extract_geometry(
        road_id=3589,
        start_m=5330,
        end_m=5350,
        ends_only=True,
    )
    assert len(geometry.coords) == 2
    assert geometry.coords[0] == pytest.approx(
        (1564198.6479590088, 5186205.344215191),
    )
    assert geometry.coords[-1] == pytest.approx(
        (1564189.0309959787, 5186187.792160582),
    )
