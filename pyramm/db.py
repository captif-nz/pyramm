from contextlib import suppress
import pandas as pd
from datetime import date
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

from pyramm.constants import DEFAULT_SQLITE_PATH


def from_sqlite(
    sql,
    path=DEFAULT_SQLITE_PATH,
    date_columns=[],
    index_columns=[],
):
    with suppress(OperationalError):
        engine = create_engine(f"sqlite:///{path.absolute()}")
        df = pd.read_sql(sql, engine, parse_dates=[date_columns])
        for cc in date_columns:
            try:
                df[cc] = pd.to_datetime(df[cc]).dt.date
            except KeyError:
                pass
        if index_columns:
            df.set_index(index_columns, inplace=True)
        return df
    return None


def to_sqlite(
    df,
    table_name,
    path=DEFAULT_SQLITE_PATH,
    if_exists="replace",
):
    engine = create_engine(f"sqlite:///{path.absolute()}")
    df.to_sql(
        table_name,
        engine,
        if_exists=if_exists,
        index=False,
    )


def read_table_status_from_sqlite(
    path=DEFAULT_SQLITE_PATH,
):
    return from_sqlite(
        "_table_status",
        path=path,
        date_columns=["date_retrieved"],
        index_columns=["database", "table_name"],
    )


def update_table_status_in_sqlite(
    database,
    table_name,
    entire_table,
    path=DEFAULT_SQLITE_PATH,
):
    # Read the table status from the SQLite database or create a new one if it
    # doesn't exist:
    table_status = read_table_status_from_sqlite(path)
    if table_status is None:
        table_status = pd.DataFrame(
            columns=["database", "table_name", "full_retrieval", "date_retrieved"]
        ).set_index(["database", "table_name"])

    table_status.loc[(database, table_name), "full_retrieval"] = entire_table
    table_status.loc[(database, table_name), "date_retrieved"] = date.today()

    engine = create_engine(f"sqlite:///{path.absolute()}")
    table_status.to_sql("_table_status", engine, if_exists="replace", index=True)
