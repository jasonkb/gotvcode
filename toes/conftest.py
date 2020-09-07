import os

import pytest
from flask import Response
from moto import mock_s3  # This must come before including models

from models.models import MODELS
from toes_app import app as toes_app


os.environ["INFRASTRUCTURE"] = "test"  # used by CaucusAppEvent


@pytest.fixture
def app():
    toes_app.debug = True
    toes_app.response_class = Response
    return toes_app


@pytest.fixture(autouse=True)
def clear_dynamodb(request):
    for model in MODELS:
        model.Meta.host = os.environ["DYNAMODB_URL"]

        if model.exists():
            # We used to just .delete_table() here, but that would
            # cause intermittent errors from trying to use the table
            # while it was being deleted (there's no wait=True param
            # for delete_table like there is for create_table) so
            # instead we just delete everything in the table.
            for item in model.scan():
                item.delete()

        model.setup()
