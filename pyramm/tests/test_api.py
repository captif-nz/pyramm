from ..api import parse_filters


def test_parse_filters():
    f1 = {"columnName": "road_id", "operator": "EqualTo", "value": 1111}
    f2 = {"columnName": "latest", "operator": "EqualTo", "value": "L"}
    assert parse_filters() == []
    assert parse_filters(road_id=1111) == [f1]
    assert parse_filters(latest=True) == [f2]
    assert parse_filters(1111, True) == [f2, f1]
