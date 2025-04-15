import pytest
from pathlib import Path

from pyramm.api import Connection


@pytest.fixture(scope="session")
def base_path():
    return Path(__file__).parent


@pytest.fixture(scope="session")
def data_path(base_path):
    return base_path / "data"


@pytest.fixture(scope="session")
def conn():
    return Connection(skip_table_name_check=True)


@pytest.fixture(scope="session")
def centreline(conn):
    return conn.centreline()


@pytest.fixture(scope="session")
def roadnames(conn):
    return conn.roadnames()


@pytest.fixture(scope="session")
def surface_structure_cleaned(conn):
    return conn.surface_structure_cleaned()
