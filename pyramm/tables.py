
from pandas import DataFrame, to_datetime

from .helpers import _map_json
from .geometry import transform, loads


class BaseTable():
    table_name = None
    index_name = None
    get_geometry = False
    date_columns = []

    def __init__(self, ramm, filters=[]):
        self.df = DataFrame()
        self.ramm = ramm
        self._set_column_names()
        self._get_data(filters)
        self._convert_dates()
        if self.index_name:
            self.df.set_index(self.index_name, drop=True, inplace=True)

    def _set_column_names(self):
        for cc in self.ramm.table_schema(self.table_name):
            self.df[cc.column_name] = None
        if self.get_geometry:
            self.df["wkt"] = None

    def _get_data(self, filters):
        self.df[self.df.columns] = self.ramm.get_data(
            self.table_name,
            column_names=self.df.columns.tolist(),
            filters=filters
        )[self.df.columns]
        if self.get_geometry:
            self.df["geometry"] = [transform(loads(ww)) for ww in self.df["wkt"]]

    def _convert_dates(self):
        for cc in self.date_columns:
            self.df[cc] = to_datetime(self.df[cc])


class Roadnames(BaseTable):
    table_name = "roadnames"
    index_name = "road_id"


class Carrway(BaseTable):
    table_name = "carr_way"
    index_name = "carr_way_no"
    get_geometry = True


class HsdTable(BaseTable):
    """
    Generic high speed data table.

    """
    hdr_table_cls = None
    hdr_table = None
    index_name = ["survey_number", "road_id", "lane", "start_m", "end_m"]
    date_columns = ["reading_date"]

    def __init__(self, ramm, filters=[], survey_year=None):
        super().__init__(ramm, filters)
        self._get_hdr_table()
        self._append_survey_year()
        self._limit_to_year(survey_year)

    def _get_hdr_table(self):
        if self.hdr_table_cls:
            self.hdr_table = self.hdr_table_cls(self.ramm)

    def _append_survey_year(self):
        self.df["survey_year"] = self.df.join(
            self.hdr_table.df["survey_date"].dt.year
        )["survey_date"]

    def _limit_to_year(self, survey_year):
        if survey_year:
            self.df = self.df.loc[self.df["survey_year"] == survey_year]


class HsdHdrTable(BaseTable):
    """
    Generic high speed data header table.

    """
    date_columns = ["survey_date", "added_on", "chgd_on"]
    index_name = "survey_number"


class HsdRoughnessHdr(HsdHdrTable):
    table_name = "hsd_rough_hdr"


class HsdRoughness(HsdTable):
    table_name = "hsd_rough"
    hdr_table_cls = HsdRoughnessHdr


class HsdRuttingHdr(HsdHdrTable):
    table_name = "hsd_rutting_hdr"


class HsdRutting(HsdTable):
    table_name = "hsd_rutting"
    hdr_table_cls = HsdRuttingHdr


class HsdTextureHdr(HsdHdrTable):
    table_name = "hsd_texture"


class HsdTexture(HsdTable):
    table_name = "hsd_texture"
    hdr_table_cls = HsdTextureHdr




class Schema:
    def __iter__(self):
        yield from self.__dict__.values()


class ColumnInfo(Schema):
    def __init__(self, def_dict):
        def_dict = _map_json(def_dict)
        for kk, vv in def_dict.items():
            self.__setattr__(kk, vv)


class TableSchema(Schema):
    def __init__(self, column_info):
        for cc in column_info:
            self.__setattr__(cc.column_name, cc)

    @staticmethod
    def from_schema(schema):
        return TableSchema([ColumnInfo(cc) for cc in schema])
