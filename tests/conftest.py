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
def top_surface(conn):
    return conn.top_surface()
