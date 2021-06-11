import os
import pickle

from datetime import date
from functools import wraps
from pathlib import Path
from tempfile import gettempdir


TEMP_DIRECTORY = Path(gettempdir()).joinpath("pyramm")


def setup_temp_directory():
    TEMP_DIRECTORY.mkdir(exist_ok=True)
    date_str = f"{date.today():%Y%m%d}"

    for temp_file in TEMP_DIRECTORY.glob("*"):
        if date_str in temp_file.stem:
            continue
        os.remove(temp_file)


def file_ref(*args, **kwargs):
    args = args[1:] if len(args) > 1 else []
    file_ref_str = "_".join(
        [f"{date.today():%Y%m%d}"]
        + [str(vv) for vv in args]
        + [str(vv) for vv in kwargs.values()]
    )
    return file_ref_str


def file_cache(name=None):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                cache_file_path = TEMP_DIRECTORY.joinpath(
                    file_ref(*args, **kwargs)
                    if name is None
                    else f"{date.today():%Y%m%d}_{name}"
                )
                if cache_file_path.exists():
                    return pickle.load(cache_file_path.open("rb"))

                result = func(*args, **kwargs)
                pickle.dump(result, cache_file_path.open("wb"))
                return result

            except Exception:
                return func(*args, **kwargs)

        return wrapper

    return decorator


setup_temp_directory()
