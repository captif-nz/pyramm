import warnings
from pandas import DataFrame, to_datetime, read_csv, notnull

from pyramm.helpers import _map_json
from pyramm.geometry import transform, loads


DEFAULT_DATE_COLUMNS = ["added_on", "chgd_on"]


class BaseTable:
    table_name = None
    index_name = None
    get_geometry = False
    date_columns = []

    def __init__(self, ramm, road_id=None, latest=False):
        self.df = DataFrame()

        if ramm is None:
            return

        self._get_data(ramm, road_id, latest)
        self._convert_dates()
        self._replace_nan()

        if self.index_name:
            self.df.set_index(self.index_name, drop=True, inplace=True)

    def _get_data(self, ramm, road_id, latest):
        self.df = ramm.get_data(
            self.table_name, road_id, latest, self.get_geometry
        ).copy()
        if "wkt" in self.df.columns:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore")
                self.df["geometry"] = [transform(loads(ww)) for ww in self.df["wkt"]]

    def _convert_dates(self):
        date_columns = set(self.date_columns + DEFAULT_DATE_COLUMNS)
        for cc in date_columns:
            try:
                self.df[cc] = to_datetime(self.df[cc])
            except KeyError:
                pass

    def _replace_nan(self):
        self.df = self.df.where(notnull(self.df), None)

    @classmethod
    def from_csv(cls, path):
        new = cls(None)
        new.df = read_csv(path, index_col=cls.index_name, float_precision="%g")
        if "wkt" in new.df.columns:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore")
                new.df["geometry"] = [transform(loads(ww)) for ww in new.df["wkt"]]
        new._convert_dates()
        new._replace_nan()
        return new.df

    @classmethod
    def from_frame(cls, df):
        new = cls(None)
        new.df = df.copy()
        if new.df.index.names == [None]:
            new.df.set_index(cls.index_name, drop=True, inplace=True)
        new._convert_dates()
        new._replace_nan()
        return new.df


class Roadnames(BaseTable):
    table_name = "roadnames"
    index_name = "road_id"


class Carrway(BaseTable):
    table_name = "carr_way"
    index_name = "carr_way_no"
    get_geometry = True


class CSurface(BaseTable):
    table_name = "c_surface"
    index_name = ""


class TopSurface(BaseTable):
    table_name = "top_surface"
    index_name = ["road_id", "start_m", "end_m"]
    date_columns = ["surface_date"]


class SurfMaterial(BaseTable):
    table_name = "surf_material"
    index_name = "surf_material"


class SurfCategory(BaseTable):
    table_name = "surf_category"
    index_name = "surf_category"


class MinorStructure(BaseTable):
    table_name = "minor_structure"
    index_name = "minor_structure_id"
    get_geometry = True


class HsdTable(BaseTable):
    """
    Generic high speed data table.

    """

    hdr_table_cls = None
    hdr_table = None
    index_name = ["survey_number", "road_id", "lane", "start_m", "end_m"]
    date_columns = ["reading_date"]

    def __init__(self, ramm, road_id, latest, survey_year=None):
        if survey_year:
            latest = False
        super().__init__(ramm, road_id, latest)
        self._get_hdr_table(ramm)
        self._append_survey_year()
        self._limit_to_year(survey_year)

    def _get_hdr_table(self, ramm):
        if self.hdr_table_cls:
            self.hdr_table = self.hdr_table_cls(ramm)

    def _append_survey_year(self):
        self.df["survey_year"] = self.df.join(self.hdr_table.df["survey_date"].dt.year)[
            "survey_date"
        ]

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
    table_name = "hsd_texture_hdr"


class HsdTexture(HsdTable):
    table_name = "hsd_texture"
    hdr_table_cls = HsdTextureHdr


class SkidResistanceHdr(HsdHdrTable):
    table_name = "skid_Resistance_hd"


class SkidResistance(HsdTable):
    table_name = "skid_resistance"
    hdr_table_cls = SkidResistanceHdr


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

    def column_names(self):
        return [cc.column_name for cc in self]
