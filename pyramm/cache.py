import os
import pickle

import pandas as pd

from copy import copy
from datetime import datetime
from frozendict import frozendict
from functools import wraps
from pathlib import Path
from tempfile import gettempdir

from sqlalchemy import create_engine

from pyramm.version import __version__
from pyramm.logging import logger


TEMP_DIRECTORY = Path(gettempdir()).joinpath("pyramm")
DEFAULT_SQLITE_PATH = Path().home() / "pyramm.sqlite"


def setup_temp_directory():
    TEMP_DIRECTORY.mkdir(exist_ok=True)
    date_str = f"{datetime.now():%Y%m%d}"

    for temp_file in TEMP_DIRECTORY.glob("*"):
        if date_str in temp_file.stem:
            continue
        os.remove(temp_file)


def generate_cache_file_path(name=None, func_args=[], func_kwargs={}):
    prefix = [f"{datetime.now():%Y%m%d}"]
    if name is not None:
        prefix.append(name)

    args = [copy(vv) for vv in func_args]
    kwargs = {kk: copy(vv) for kk, vv in func_kwargs.items()}

    if len(args) > 0:
        # If the first argument is a Connection object, use the database name
        # as part of the cache file name and remove the Connection object from
        # the list of arguments:
        if type(args[0]).__name__ == "Connection":
            kwargs["database"] = args[0].database
            args = args[1:]

    return TEMP_DIRECTORY.joinpath(
        "_".join(
            prefix
            + [str(vv) for vv in args]
            + [str(vv) for vv in kwargs.values()]
            + [pd.__version__]
            + [__version__]
        )
    )


def file_cache(name=None):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                cache_file_path = generate_cache_file_path(name, args, kwargs)
                if cache_file_path.exists():
                    logger.debug("reading table from file cache")
                    return pickle.load(cache_file_path.open("rb"))

                result = func(*args, **kwargs)
                pickle.dump(result, cache_file_path.open("wb"))
                return result

            except Exception:
                return func(*args, **kwargs)

        return wrapper

    return decorator


def is_latest_table_in_sqlite(
    database,
    table_name,
    road_id,
    latest,
    path=DEFAULT_SQLITE_PATH,
):
    engine = create_engine(f"sqlite:///{path}")
    try:
        metadata = pd.read_sql(
            "SELECT * FROM _metadata;", engine, parse_dates=["last_updated"]
        ).set_index(("database", "table_name", "road_id", "latest"))
    except Exception:
        return False

    try:
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        road_id = road_id if road_id is not None else "all"
        last_updated = metadata.loc[
            (database, table_name, road_id, latest),
            "last_updated",
        ]
        if last_updated == today:
            return True

    except KeyError:
        pass

    return False


def update_metadata_in_sqlite(
    database,
    table_name,
    road_id,
    latest,
    path=DEFAULT_SQLITE_PATH,
):
    engine = create_engine(f"sqlite:///{path}")
    try:
        metadata = pd.read_sql(
            "SELECT * FROM _metadata;", engine, parse_dates=["last_updated"]
        ).set_index("table_name")
    except Exception:
        metadata = pd.DataFrame(
            columns=[
                "table_name",
                "database",
                "road_id",
                "latest",
                "last_updated",
            ]
        ).set_index("table_name")

    road_id = road_id if road_id is not None else "all"
    metadata.loc[table_name] = [
        database,
        road_id,
        int(latest),
        datetime.now().replace(hour=0, minute=0, second=0, microsecond=0),
    ]
    metadata.to_sql("_metadata", engine, if_exists="replace", index=True)


def sqlite_cache(path=DEFAULT_SQLITE_PATH):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                database = args[0].database
                table_name = (
                    args[1]
                    if "table_name" not in kwargs.keys()
                    else kwargs["table_name"]
                )
                road_id = (
                    args[2]
                    if (len(args) > 2 and "road_id" not in kwargs.keys())
                    else kwargs.get("road_id")
                )
                latest = (
                    args[3]
                    if (len(args) > 3 and "latest" not in kwargs.keys())
                    else kwargs.get("latest", False)
                )

                engine = create_engine(f"sqlite:///{path}")

                if is_latest_table_in_sqlite(
                    database=database,
                    table_name=table_name,
                    road_id=road_id,
                    latest=latest,
                    path=path,
                ):
                    logger.debug("reading table from sqlite cache")
                    return pd.read_sql(table_name, engine)

                result = func(*args, **kwargs)
                result.to_sql(table_name, engine, if_exists="replace", index=False)
                update_metadata_in_sqlite(
                    database=database,
                    table_name=table_name,
                    road_id=road_id,
                    latest=latest,
                    path=path,
                )
                return result

            except Exception:
                return func(*args, **kwargs)

        return wrapper

    return decorator


setup_temp_directory()


def freezeargs(func):
    """
    Transform mutable dictionnary into immutable.
    https://stackoverflow.com/questions/6358481/using-functools-lru-cache-with-dictionary-arguments

    """

    @wraps(func)
    def wrapped(*args, **kwargs):
        args = tuple(
            [frozendict(arg) if isinstance(arg, dict) else arg for arg in args]
        )
        kwargs = {
            k: frozendict(v) if isinstance(v, dict) else v for k, v in kwargs.items()
        }
        return func(*args, **kwargs)

    return wrapped
