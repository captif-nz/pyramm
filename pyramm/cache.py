import os
import pickle

import pandas as pd

from datetime import date
from frozendict import frozendict
from functools import wraps
from pathlib import Path
from tempfile import gettempdir

from pyramm.version import __version__
from pyramm.logging import logger


TEMP_DIRECTORY = Path(gettempdir()).joinpath("pyramm")


def setup_temp_directory():
    TEMP_DIRECTORY.mkdir(exist_ok=True)
    date_str = f"{date.today():%Y%m%d}"

    for temp_file in TEMP_DIRECTORY.glob("*"):
        if date_str in temp_file.stem:
            continue
        os.remove(temp_file)


def generate_cache_file_path(name=None, func_args=[], func_kwargs={}):
    prefix = [f"{date.today():%Y%m%d}"]
    if name is not None:
        prefix.append(name)

    func_args = func_args[1:] if len(func_args) > 1 else []

    return TEMP_DIRECTORY.joinpath(
        "_".join(
            prefix
            + [str(vv) for vv in func_args]
            + [str(vv) for vv in func_kwargs.values()]
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
