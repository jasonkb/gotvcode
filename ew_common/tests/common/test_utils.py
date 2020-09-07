from ew_common.utils import nested_get


def test_nested_get():
    d = {"a": {"b": {"c": 1}}}
    assert nested_get(d, "a", "b", "c") == 1
    assert nested_get(d, "a", "b") == {"c": 1}
    assert nested_get(d, "x", "y") is None
    assert nested_get(d, "x", default=17) == 17
