from time import sleep
from typing import Optional
from requests import get, post
from urllib.parse import urlencode
from numpy import arange, ceil
from pandas import DataFrame, concat
from functools import lru_cache
from os import environ
from unsync import unsync

from pyramm.cache import file_cache, freezeargs
from pyramm.config import config
from pyramm.logging import logger
from pyramm.tables import (
    TableSchema,
    Roadnames,
    Carrway,
    CSurface,
    TopSurface,
    SurfMaterial,
    SurfCategory,
    MinorStructure,
    HsdRoughness,
    HsdRoughnessHdr,
    HsdRutting,
    HsdRuttingHdr,
    HsdTexture,
    HsdTextureHdr,
    SkidResistance,
)
from pyramm.geometry import Centreline, ROADNAME_COLUMNS, build_partial_centreline


class RequestError(Exception):
    pass


class LoginError(Exception):
    pass


class Connection:
    url = "https://apps.ramm.co.nz/RammApi6.1/v1"
    chunk_size = 2000

    def __init__(
        self,
        username=config().get(
            "RAMM", "USERNAME", fallback=environ.get("RAMM_USERNAME")
        ),
        password=config().get(
            "RAMM", "PASSWORD", fallback=environ.get("RAMM_PASSWORD")
        ),
        database="SH New Zealand",
    ):

        if username is None:
            username, password = self._get_credentials()

        authorization_key = self._get_auth_token(
            username=username, password=password, database=database
        )

        self.headers = {
            "Content-type": "application/json",
            "referer": "https://test.com",
            "Authorization": f"Bearer {authorization_key}",
        }

    @staticmethod
    def _get_credentials():
        username = input("Username: ")
        password = input("Password: ")
        return username, password

    def _get_auth_token(self, **auth_params):
        response = post(
            f"{self.url}/authenticate/login?{urlencode(auth_params)}",
        )
        if response.status_code == 200:
            return response.json()
        raise LoginError(response)

    def _get(self, endpoint):
        response = get(f"{self.url}/{endpoint}", headers=self.headers)
        if response.status_code == 200:
            return response.json()
        raise RequestError(response)

    def _post(self, endpoint, body):
        response = post(f"{self.url}/{endpoint}", headers=self.headers, json=body)
        if response.status_code == 200:
            return response.json()
        raise RequestError(response)

    @staticmethod
    def _request_body(
        filters=[],
        table_name="sh_detail",
        skip=0,
        take=1,
        get_geometry=False,
        expand_lookups=False,
    ):
        return {
            "filters": filters,
            "expandLookups": expand_lookups,
            "getGeometry": get_geometry,
            "isLongitudeLatitude": True,
            "gridPaging": {"skip": skip, "take": take},
            "excludeReplacedData": True,
            "returnEntityId": False,
            "tableName": table_name,
            "loadType": "All",
        }

    def _query(self, table_name, filters=[], skip=0, take=1, get_geometry=False):
        return self._post(
            "/data/table",
            self._request_body(filters, table_name, skip, take, get_geometry),
        )

    def _rows(self, table_name, filters=[]):
        return self._query(table_name, filters=filters)["total"]

    def _geometry_table(self, table_name):
        return len(self._query(table_name, get_geometry=True)["rows"]) > 0

    @unsync
    def _get_data_partial(
        self,
        table_name,
        filters,
        get_geometry,
        column_names,
        start_row,
        end_row,
        chunk_size,
    ):
        df = DataFrame()
        for skip in range(start_row, end_row, chunk_size):
            sleep(1)
            logger.debug(f"getting rows {skip:.0f} to {skip+chunk_size:.0f}")
            response = self._query(
                table_name,
                filters=filters,
                skip=skip,
                take=chunk_size,
                get_geometry=get_geometry,
            )["rows"]
            df = concat(
                [
                    df,
                    DataFrame([rr["values"] for rr in response], columns=column_names),
                ],
                ignore_index=True,
            )
        return df

    def _get_data(self, table_name, filters=[], get_geometry=False, threads=4):
        """
        Parameters
        ----------
        filters: list
            List containing dict entries of the following format:
            {'columnName': 'latest', 'operator': 'EqualTo', 'value': 'L'}

        """
        if get_geometry:
            get_geometry = [False, get_geometry][self._geometry_table(table_name)]

        column_names = self.column_names(table_name)
        if get_geometry:
            column_names.append("wkt")

        # Retrieve data from the RAMM database and return a DataFrame.
        total_rows = self._rows(table_name, filters)
        if total_rows == 0:
            logger.info(f"no rows to retrieve from {table_name}")
            return DataFrame(columns=column_names)

        logger.info(f"retrieving {total_rows:.0f} rows from {table_name}")
        logger.debug(f"using {threads} threads")

        rows_per_thread = int(
            ceil((total_rows / threads) / self.chunk_size) * self.chunk_size
        )
        tasks = [
            self._get_data_partial(
                table_name=table_name,
                filters=filters,
                get_geometry=get_geometry,
                column_names=column_names,
                start_row=int(rr),
                end_row=int(rr + rows_per_thread),
                chunk_size=self.chunk_size,
            )
            for rr in arange(0, total_rows, rows_per_thread)
        ]
        return concat([tt.result() for tt in tasks], ignore_index=True)

    @lru_cache(maxsize=10)
    @file_cache()
    def get_data(
        self,
        table_name: str,
        road_id: Optional[int] = None,
        latest: bool = False,
        get_geometry: bool = False,
        threads: int = 4,
    ):
        threads = 1 if threads < 1 else threads
        return self._get_data(
            table_name,
            filters=parse_filters(road_id, latest),
            get_geometry=get_geometry,
            threads=threads,
        )

    def column_names(self, table_name):
        return self.table_schema(table_name).column_names()

    @lru_cache(maxsize=10)
    def table_schema(self, table_name):
        # Returns the RAMM schema details for a given table:
        return TableSchema.from_schema(self._get(f"schema/{table_name}?loadType=3"))

    @lru_cache(maxsize=1)
    def table_names(self):
        # Returns a list of valid tables:
        return [table["tableName"] for table in self._get("data/tables?tableTypes=255")]

    @freezeargs
    @lru_cache(maxsize=1)
    @file_cache("centreline")
    def centreline(self, lengths: Optional[dict] = None):
        """
        Parameters
        ----------
        lengths: dict
            Dict with road_ids as keys and start/end position pairs (or None) as the
            value. This limits the returned Centreline object to a subset of the full
            RAMM centreline.

            Examples:
            1.  lengths={3565: None, 3566: None} - centreline limited to road_id 3565
                and 3566.
            2.  lengths={3565: [100, 200]} - centreline limited to road_id 3565 between
                position 100 metres and 200 metres.
            3.  lengths={3565: [500, None]} - centreline limited to road_id 3565 between
                position 500 metres and the end of the road_id element.

        """
        df = self.carr_way().join(self.roadnames()[ROADNAME_COLUMNS], on="road_id")
        if lengths is None:
            return Centreline(df)
        return build_partial_centreline(
            self.centreline(), self.roadnames(), lengths=lengths
        )

    def roadnames(self):
        return Roadnames(self).df

    def carr_way(self, road_id=None):
        return Carrway(self, road_id).df

    def c_surface(self, road_id=None):
        return CSurface(self, road_id).df

    def top_surface(self):
        return TopSurface(self).df

    def surf_material(self):
        return SurfMaterial(self).df

    def surf_category(self):
        return SurfCategory(self).df

    def minor_structure(self):
        return MinorStructure(self).df

    def hsd_roughness_hdr(self):
        return HsdRoughnessHdr(self).df

    def hsd_roughness(self, road_id, latest=True, survey_year=None):
        return HsdRoughness(self, road_id, latest, survey_year).df

    def hsd_rutting_hdr(self):
        return HsdRuttingHdr(self).df

    def hsd_rutting(self, road_id, latest=True, survey_year=None):
        return HsdRutting(self, road_id, latest, survey_year).df

    def hsd_texture_hdr(self):
        return HsdTextureHdr(self).df

    def hsd_texture(self, road_id, latest=True, survey_year=None):
        return HsdTexture(self, road_id, latest, survey_year).df

    def skid_resistance(self, road_id, latest=True, survey_year=None):
        return SkidResistance(self, road_id, latest, survey_year).df


def parse_filters(road_id=None, latest=False):
    filters = []
    if latest:
        filters.append({"columnName": "latest", "operator": "EqualTo", "value": "L"})
    if road_id:
        operator, value = "EqualTo", str(int(road_id))
        if isinstance(road_id, list):
            operator, value = "In", ",".join([str(rr) for rr in road_id])
        filters.append({"columnName": "road_id", "operator": operator, "value": value})
    return filters
