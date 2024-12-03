import pytest
import pandas as pd
from shapely.geometry.point import Point

import pyramm
from pyramm.api import TableRemovedError, parse_filters
from pyramm.geometry import Centreline


def test_version():
    assert pyramm.__version__ == "1.40"


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

    def test__rows(self, conn):
        n_rows = conn._rows("roadnames")
        assert isinstance(n_rows, int)
        assert n_rows > 0

    def test_top_surface(self, conn):
        """
        top_surface is no longer available following upgrade to AMDS.
        """
        with pytest.raises(TableRemovedError):
            conn.top_surface()


class TestCentreline:
    def test_centreline(self, centreline):
        assert isinstance(centreline, Centreline)

    def test_partial_centreline(self, conn):
        partial_centreline = conn.centreline({3656: None})
        assert isinstance(partial_centreline, Centreline)
        assert len(partial_centreline._df_features) == 1

    def test_nearest_feature(self, centreline):
        point = Point((172.618567, -43.441594))
        carr_way_no, offset_m = centreline.nearest_feature(point)

        assert carr_way_no == 11263
        assert round(offset_m, 1) == 26.1

    def test_position(self, centreline):
        point = Point((172.618567, -43.441594))
        position = centreline.position(point)

        assert round(position["position_m"], 1) == 4504.9
        assert position["road_id"] == 1715
        assert round(position["search_offset_m"], 1) == 26.1

    def test_position_with_road_id(self, centreline):
        point = Point((172.618567, -43.441594))
        position = centreline.position(point, road_id=1716)

        assert round(position["position_m"], 1) == 4597.2
        assert position["road_id"] == 1716
        assert round(position["search_offset_m"], 1) == 38.8

    @pytest.mark.skip
    def test_append_geometry(self, centreline, top_surface):
        df = top_surface.reset_index()
        df = centreline.append_geometry(df)
        assert "wkt" in df.columns

    def test_append_geometry_fast(self, centreline, surface_structure_cleaned):
        df = surface_structure_cleaned.reset_index().iloc[:100]
        df = centreline.append_geometry(df)
        assert "wkt" in df.columns

    def test_extract_wkt_from_list_of_geometry_objects(self):
        geometry = [Point(0, 0), Point(1, 2), None]
        wkt = Centreline._extract_wkt_from_list_of_geometry_objects(geometry)

        assert wkt == ["POINT (0 0)", "POINT (1 2)", None]
