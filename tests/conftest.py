import pytest

from pyramm.api import Connection


@pytest.fixture
def conn():
    return Connection()


@pytest.fixture
def centreline(conn):
    return conn.centreline()


@pytest.fixture
def top_surface(conn):
    return conn.top_surface()
