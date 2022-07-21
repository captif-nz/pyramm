import pytest
import pandas as pd

from pandas import Timestamp
from datetime import date

from pyramm.ops.top_surface import build_top_surface, append_surface_details_to_segments


@pytest.fixture
def original():
    return pd.DataFrame.from_records(
        [
            {
                "road_id": 100,
                "start_m": 20,
                "end_m": 200,
                "surface_date": date(2020, 1, 1),
                "name": "A",
            },
            {
                "road_id": 100,
                "start_m": 20,
                "end_m": 60,
                "surface_date": date(2020, 1, 1),
                "name": "A",
            },
            {
                "road_id": 101,
                "start_m": 230,
                "end_m": 500,
                "surface_date": date(2021, 1, 1),
                "name": "B",
            },
            {
                "road_id": 101,
                "start_m": 230,
                "end_m": 260,
                "surface_date": date(2021, 1, 1),
                "name": "C",
            },
            {
                "road_id": 102,
                "start_m": 0,
                "end_m": 9999,
                "surface_date": date(2018, 6, 6),
                "name": "D",
            },
        ]
    ).set_index(["road_id", "start_m", "end_m"])


class TestTopSurface:
    class TestBuildTopSurface:
        def test_build_top_surface(self, original):
            index_names = ["road_id", "start_m", "end_m"]
            custom = pd.DataFrame.from_records(
                [
                    {
                        "road_id": 100,
                        "start_m": 0,
                        "end_m": 30,
                        "surface_date": date(2020, 1, 2),
                        "name": "1",
                    },
                    {
                        "road_id": 100,
                        "start_m": 20,
                        "end_m": 30,
                        "surface_date": date(2019, 12, 1),
                        "name": "2",
                    },
                    {
                        "road_id": 100,
                        "start_m": 60,
                        "end_m": 100,
                        "surface_date": date(2020, 1, 5),
                        "name": "3",
                    },
                    {
                        "road_id": 100,
                        "start_m": 190,
                        "end_m": 220,
                        "surface_date": date(2020, 1, 10),
                        "name": "4",
                    },
                    {
                        "road_id": 100,
                        "start_m": 70,
                        "end_m": 90,
                        "surface_date": date(2020, 1, 5),
                        "name": "5",
                    },
                ]
            )
            new = build_top_surface([original, custom])
            expected = (
                pd.DataFrame.from_records(
                    [
                        {
                            "road_id": 100,
                            "start_m": 0,
                            "end_m": 30,
                            "surface_date": Timestamp(2020, 1, 2),
                            "name": "1",
                        },
                        {
                            "road_id": 100,
                            "start_m": 30,
                            "end_m": 60,
                            "surface_date": Timestamp(2020, 1, 1),
                            "name": "A",
                        },
                        {
                            "road_id": 100,
                            "start_m": 60,
                            "end_m": 70,
                            "surface_date": Timestamp(2020, 1, 5),
                            "name": "3",
                        },
                        {
                            "road_id": 100,
                            "start_m": 70,
                            "end_m": 90,
                            "surface_date": Timestamp(2020, 1, 5),
                            "name": "5",
                        },
                        {
                            "road_id": 100,
                            "start_m": 90,
                            "end_m": 100,
                            "surface_date": Timestamp(2020, 1, 5),
                            "name": "3",
                        },
                        {
                            "road_id": 100,
                            "start_m": 100,
                            "end_m": 190,
                            "surface_date": Timestamp(2020, 1, 1),
                            "name": "A",
                        },
                        {
                            "road_id": 100,
                            "start_m": 190,
                            "end_m": 220,
                            "surface_date": Timestamp(2020, 1, 10),
                            "name": "4",
                        },
                        {
                            "road_id": 101,
                            "start_m": 230,
                            "end_m": 500,
                            "surface_date": Timestamp(2021, 1, 1),
                            "name": "B",
                        },
                        {
                            "road_id": 102,
                            "start_m": 0,
                            "end_m": 9999,
                            "surface_date": Timestamp(2018, 6, 6),
                            "name": "D",
                        },
                    ]
                )
                .set_index(index_names)
                .sort_index()
            )
            assert new.equals(expected)

        def test_build_top_surface_real_table(self, top_surface):
            top_surface_limited = top_surface.iloc[:100]
            new = build_top_surface([top_surface_limited])
            expected = top_surface_limited.loc[
                top_surface_limited["full_width_flag"].str.lower() == "y"
            ]
            assert new.equals(expected)

    class TestAppendSurfaceDetailsToSegments:
        def test_append_surface_details_to_segments(self, original):
            df = pd.DataFrame.from_records(
                [
                    {"road_id": 99, "start_m": 0, "end_m": 20},
                    {"road_id": 100, "start_m": 0, "end_m": 20},
                    {"road_id": 100, "start_m": 20, "end_m": 40},
                    {"road_id": 100, "start_m": 40, "end_m": 60},
                    {"road_id": 101, "start_m": 200, "end_m": 220},
                    {"road_id": 101, "start_m": 220, "end_m": 240},
                    {"road_id": 101, "start_m": 240, "end_m": 260},
                ]
            )
            original = build_top_surface(
                [original]
            )  # Removes overlapping surface records
            df = append_surface_details_to_segments(df, original)
            expected = df = pd.DataFrame.from_records(
                [
                    {
                        "road_id": 99,
                        "start_m": 0,
                        "end_m": 20,
                        "surface_date": "",
                        "name": "",
                    },
                    {
                        "road_id": 100,
                        "start_m": 0,
                        "end_m": 20,
                        "surface_date": "",
                        "name": "",
                    },
                    {
                        "road_id": 100,
                        "start_m": 20,
                        "end_m": 40,
                        "surface_date": Timestamp(2020, 1, 1),
                        "name": "A",
                    },
                    {
                        "road_id": 100,
                        "start_m": 40,
                        "end_m": 60,
                        "surface_date": Timestamp(2020, 1, 1),
                        "name": "A",
                    },
                    {
                        "road_id": 101,
                        "start_m": 200,
                        "end_m": 220,
                        "surface_date": "",
                        "name": "",
                    },
                    {
                        "road_id": 101,
                        "start_m": 220,
                        "end_m": 240,
                        "surface_date": "",
                        "name": "",
                    },
                    {
                        "road_id": 101,
                        "start_m": 240,
                        "end_m": 260,
                        "surface_date": Timestamp(2021, 1, 1),
                        "name": "C",
                    },
                ]
            )
            assert df.equals(expected)
