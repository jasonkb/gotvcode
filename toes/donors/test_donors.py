import json
import os

import freezegun
import pytest
import responses
from flask import url_for

from common.settings import settings
from models.donor import Donor


@pytest.fixture(autouse=True)
def mock_secrets():
    settings.override_cached_property("bsd_api_username", "user")
    settings.override_cached_property("bsd_api_password", "pass")
    settings.override_cached_property("donor_id_salt", "salt")


@pytest.fixture
def sample_email_request():
    with open(
        os.path.join(os.path.dirname(__file__), "sample_email_request.json")
    ) as f:
        return f.read()


@pytest.fixture
def sample_email_request_capitalized():
    with open(
        os.path.join(os.path.dirname(__file__), "sample_email_request_capitalized.json")
    ) as f:
        return f.read()


FROZEN_TIME = "2019-09-20T20:32:24Z"

# This request has payload that specifies jasonkatzbrown@gmail.com's donor_id.
EXPECTED_SEND_URL_EXISTING_DONOR = "https://warren.cp.bsd.net/page/api/mailer/send_triggered_email?api_id=user&api_ts=1569011544&api_ver=2&email=jasonkatzbrown%40gmail.com&email_opt_in=0&mailing_id=UlQPDQ&trigger_values=%257B%2522email%2522%253A%2520%2522jasonkatzbrown%2540gmail.com%2522%252C%2520%2522donor_id%2522%253A%2520%2522jCn0YZP86rCh%2522%257D&api_mac=7a6e2d34c42924804332d058927ddb5a4447fa8b"

EXPECTED_SEND_URL_EXISTING_DONOR_CAPITALIZED = "https://warren.cp.bsd.net/page/api/mailer/send_triggered_email?api_id=user&api_ts=1569011544&api_ver=2&email=Jasonkatzbrown%40gmail.com&email_opt_in=0&mailing_id=UlQPDQ&trigger_values=%257B%2522email%2522%253A%2520%2522Jasonkatzbrown%2540gmail.com%2522%252C%2520%2522donor_id%2522%253A%2520%2522jCn0YZP86rCh%2522%257D&api_mac=da84136ebcddceb8b2c83f0f6bc8b018071166ff"

# This request's payload does not include a donor_id.
EXPECTED_SEND_URL_NEW_DONOR = "https://warren.cp.bsd.net/page/api/mailer/send_triggered_email?api_id=user&api_ts=1569011544&api_ver=2&email=jasonkatzbrown%40gmail.com&email_opt_in=0&mailing_id=UlQPDQ&trigger_values=%257B%2522email%2522%253A%2520%2522jasonkatzbrown%2540gmail.com%2522%257D&api_mac=56c54cba03e12c75218fcfde23b003e5dd6534c6"

EXPECTED_DEFERRED_RESULT_URL = "https://warren.cp.bsd.net/page/api/get_deferred_results?api_id=user&api_ts=1569011544&api_ver=2&deferred_id=urrrr&api_mac=de3bbf70d9ead151f47446d4815628b242bf10ed"


@responses.activate
@freezegun.freeze_time(FROZEN_TIME)
def test_resend_donor_wall_link_existing_donor(client, sample_email_request):
    d = Donor.get_or_create_donor("jasonkatzbrown@gmail.com")
    d.set_donor_id(settings.donor_id_salt)
    d.save()

    responses.add(
        responses.POST,
        EXPECTED_SEND_URL_EXISTING_DONOR,
        body='{"deferred_task_id": "urrrr"}',
        status=202,
    )

    responses.add(responses.GET, EXPECTED_DEFERRED_RESULT_URL, body="")

    res = client.post(
        url_for("donors.resend_donor_wall_link"), data=sample_email_request
    )
    assert len(responses.calls) == 2
    assert res.status_code == 204


@responses.activate
@freezegun.freeze_time(FROZEN_TIME)
def test_resend_donor_wall_link_existing_donor_capitalized(
    client, sample_email_request_capitalized
):
    d = Donor.get_or_create_donor("jasonkatzbrown@gmail.com")
    d.set_donor_id(settings.donor_id_salt)
    d.save()

    responses.add(
        responses.POST,
        EXPECTED_SEND_URL_EXISTING_DONOR_CAPITALIZED,
        body='{"deferred_task_id": "urrrr"}',
        status=202,
    )

    responses.add(responses.GET, EXPECTED_DEFERRED_RESULT_URL, body="")

    res = client.post(
        url_for("donors.resend_donor_wall_link"), data=sample_email_request_capitalized
    )
    assert len(responses.calls) == 2
    assert res.status_code == 204


@responses.activate
@freezegun.freeze_time(FROZEN_TIME)
def test_resend_donor_wall_link_new_donor(client, sample_email_request):
    responses.add(
        responses.POST,
        EXPECTED_SEND_URL_NEW_DONOR,
        body='{"deferred_task_id": "urrrr"}',
        status=202,
    )

    responses.add(responses.GET, EXPECTED_DEFERRED_RESULT_URL, body="")

    res = client.post(
        url_for("donors.resend_donor_wall_link"), data=sample_email_request
    )
    assert len(responses.calls) == 2
    assert res.status_code == 204
