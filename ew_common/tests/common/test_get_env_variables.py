import os
from unittest import mock

import boto3
import pytest
import responses
from moto import mock_ssm

from ew_common.get_env_variables import _get_boto_client_or_create, get_env_var


def test_default_variables():
    assert get_env_var("nope", optional=True, default="result") == "result"


def test_non_default_variables():
    with pytest.raises(KeyError):
        get_env_var("nope")


@mock.patch.dict("os.environ", {"VAR": "1"})
def test_value_not_in_ssm():
    assert get_env_var("VAR", optional=True, default="E") == "1"


@mock.patch.dict("os.environ", {"VAR": "ssm:VAR"})
@mock_ssm
def test_value_in_ssm():
    ssm_mock_client = boto3.client("ssm", region_name="us-east-1")
    ssm_mock_client.put_parameter(
        Name="VAR", Description="A test parameter", Value="2", Type="SecureString"
    )
    assert get_env_var("VAR", optional=True, default="E") == "2"


@mock.patch.dict("os.environ", {"VAR2": "ssm:VAR2"})
@mock_ssm
def test_value_not_in_ssm_store_throws():
    with pytest.raises(Exception):
        get_env_var("VAR2", optional=True, default="E")


@mock_ssm
def test_creates_only_one_client():
    client = _get_boto_client_or_create()
    client2 = _get_boto_client_or_create()
    assert client == client2
