import json

import pytest
from flask import url_for

from common.basic_auth import mock_basic_auth


@pytest.fixture
def mock_generic_kv_read_auth():
    return mock_basic_auth("generic_kv_read")


@pytest.fixture
def mock_generic_kv_write_auth():
    return mock_basic_auth("generic_kv_write")


def test_put_with_read_auth(
    client, mock_generic_kv_read_auth, mock_generic_kv_write_auth
):
    res = client.put(
        url_for("generic_kv.value_put", k="test key"),
        headers={"Authorization": mock_generic_kv_read_auth},
        data=b"test value",
    )
    assert res.status_code == 401


def test_put_get(client, mock_generic_kv_read_auth, mock_generic_kv_write_auth):
    res = client.put(
        url_for("generic_kv.value_put", k="test key"),
        headers={"Authorization": mock_generic_kv_write_auth},
        data=b"test value",
    )
    print(f"res: {res}")
    assert res.status_code == 204

    res = client.get(
        url_for("generic_kv.value_get", k="test key"),
        headers={"Authorization": mock_generic_kv_read_auth},
    )
    assert res.status_code == 200
    assert res.data == b"test value"
