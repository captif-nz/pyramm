import pytest

from pyramm.api import Connection


@pytest.fixture(scope="session")
def conn():
    return Connection()


@pytest.fixture(scope="session")
def centreline(conn):
    return conn.centreline()


@pytest.fixture(scope="session")
def roadnames(conn):
    return conn.roadnames()


@pytest.fixture(scope="session")
def surface_structure_cleaned(conn):
    return conn.surface_structure_cleaned()
