import pytest
from shapely.geometry import Point

from pyramm.geometry import build_partial_centreline


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
