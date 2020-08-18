import pytest
import pandas as pd

from pyramm.api import parse_filters, Connection
from pyramm.geometry import Centreline


@pytest.fixture
def conn():
    return Connection()


@pytest.fixture
def centreline(conn):
    return conn.centreline()


def test_parse_filters():
    f1 = {"columnName": "road_id", "operator": "EqualTo", "value": "1111"}
    f2 = {"columnName": "latest", "operator": "EqualTo", "value": "L"}
    assert parse_filters() == []
    assert parse_filters(road_id=1111) == [f1]
    assert parse_filters(latest=True) == [f2]
    assert parse_filters(1111, True) == [f2, f1]


class TestConnection:
    def test_table_names(self, conn):
        valid_table_names = [
            "roadnames",
            "carr_way",
            "c_surface",
            "top_surface",
            "surf_material",
            "surf_category",
            "minor_structure",
            "hsd_rough_hdr",
            "hsd_rough",
            "hsd_rutting_hdr",
            "hsd_rutting",
            "hsd_texture_hdr",
            "hsd_texture",
        ]

        table_names = conn.table_names()

        assert isinstance(table_names, list)
        assert len(table_names) > 0
        assert all((tt in table_names for tt in valid_table_names))

    def test_column_names(self, conn):
        valid_column_names = ["road_id", "sh_ne_unique", "road_name"]
        column_names = conn.column_names("roadnames")

        assert isinstance(column_names, list)
        assert len(column_names) > 0
        assert all((tt in column_names for tt in valid_column_names))

    def test_roadnames(self, conn):
        df = conn.roadnames()
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    def test_carr_way(self, conn):
        df = conn.carr_way()
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    def test_carr_way_with_road_id(self, conn):
        road_id = 3315
        df = conn.carr_way()
        df = df.loc[df.road_id == road_id]
        selected = conn.carr_way(road_id)
        assert set(selected.index) == set(df.index)


class TestCentreline:
    def test_centreline(self, centreline):
        assert isinstance(centreline, Centreline)
