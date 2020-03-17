from requests import get, post
from urllib.parse import urlencode
from numpy import ceil
from pandas import DataFrame, concat
from functools import lru_cache
from .config import config
from .tables import (
    TableSchema,
    Roadnames,
    Carrway,
    HsdRoughness,
    HsdRoughnessHdr,
    HsdRutting,
    HsdRuttingHdr,
    HsdTexture,
    HsdTextureHdr,
)


class RequestError(Exception):
    pass


class LoginError(Exception):
    pass


class Connection:
    url = "https://apps.ramm.co.nz/RammApi6.1/v1"
    chunk_size = 2000

    def __init__(
        self,
        username=config().get("RAMM", "USERNAME", fallback=None),
        password=config().get("RAMM", "PASSWORD", fallback=None),
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
        response = post(f"{self.url}/authenticate/login?{urlencode(auth_params)}",)
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

    def _chunks(self, table_name, filters):
        n_rows = self._query(table_name, filters=filters)["total"]
        n_chunks = int(ceil(1.0 * n_rows / self.chunk_size))
        yield from range(n_chunks)

    def _geometry_table(self, table_name):
        return len(self._query(table_name, get_geometry=True)["rows"]) > 0

    def _get_data(self, table_name, filters=[], get_geometry=False):
        """
        Parameters
        ----------
        filters: list
            List containing dict entries of the following format:
            {'columnName': 'latest', 'operator': 'EqualTo', 'value': 'L'}

        """
        get_geometry = [False, get_geometry][self._geometry_table(table_name)]
        column_names = self.column_names(table_name)
        if get_geometry:
            column_names.append("wkt")

        # Retrieve data from the RAMM database and return a DataFrame.
        df = DataFrame()
        for i_chunk in self._chunks(table_name, filters):
            # Get data in chunks:
            response = self._query(
                table_name,
                filters=filters,
                skip=i_chunk * self.chunk_size,
                take=self.chunk_size,
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

    @lru_cache(maxsize=10)
    def get_data(self, table_name, road_id=None, latest=False, get_geometry=False):
        return self._get_data(
            table_name,
            filters=parse_filters(road_id, latest),
            get_geometry=get_geometry,
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

    def roadnames(self):
        return Roadnames(self).df

    def carr_way(self, road_id=None):
        return Carrway(self, road_id).df

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


def parse_filters(road_id=None, latest=False):
    filters = []
    if latest:
        filters.append({"columnName": "latest", "operator": "EqualTo", "value": "L"})
    if road_id:
        filters.append(
            {"columnName": "road_id", "operator": "EqualTo", "value": road_id}
        )
    return filters
