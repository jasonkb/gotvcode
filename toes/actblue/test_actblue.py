import datetime
import json
import os

import boto3
import freezegun
import pytest
import responses
from flask import url_for
from moto import mock_cloudwatch, mock_s3

from common.basic_auth import mock_basic_auth, mock_wrong_auth
from common.settings import settings
from models.donor import Donor

# In the sample, the donation was made at 2019-06-07T15:49:32-04:00
LAGGED_ALLOWED_WEBHOOK_NOTIFICATION_TIME = "2019-06-07T20:32:24Z"
LAGGED_DISALLOWED_WEBHOOK_NOTIFICATION_TIME = "2019-06-08T01:32:24Z"

# We'll also try a donation at midnight with fast webhook invocation;
# despite being in the middle of the night this should still be allowed.
NOT_LAGGED_MIDNIGHT_WEBHOOK_NOTIFICATION_TIME = "2019-06-07T04:01:24Z"

MOBILE_COMMONS_PROFILE_RESPONSE = """
<response success="true">
  <profile id="338948442">
    <first_name>Jason</first_name>
    <last_name>Katz-Brown</last_name>
    <phone_number>15105016227</phone_number>
  </profile>
</response>
"""

MOBILE_COMMONS_PROFILE_NOT_EXIST_RESPONSE = """
<response success="false">
  <error id="5" message="Invalid phone number"/>
</response>
"""

MOBILE_COMMONS_PROFILE_EXISTS_BUT_NOT_SUBSCRIBED_RESPONSE = """
<response success="true">
  <profile id="344794931">
    <first_name/>
    <last_name/>
    <phone_number>15102194255</phone_number>
    <email/>
    <status>Profiles with no Subscriptions</status>
  </profile>
</response>
"""

MOBILE_COMMONS_PROFILE_UPDATE_RESPONSE = """
<response success="true">
  <profile id="345304097">
    <first_name>Mary</first_name>
    <last_name>Smith</last_name>
    <phone_number>15105016227</phone_number>
    <email>marysmithexample@gmail.com</email>
    <status>Active Subscriber</status>
    <created_at>2019-06-06 19:33:03 UTC</created_at>
    <updated_at>2019-06-06 19:33:03 UTC</updated_at>
    <opted_out_at/>
    <opted_out_source/>
    <source type="API" name="Civis Sync" email="jkatzbrown+mcommonsapi@elizabethwarren.com"/>
    <address>
      <street1>20 Belvedere Ave.</street1>
      <street2/>
      <city>Richmond</city>
      <state>CA</state>
      <postal_code>94801</postal_code>
      <country>US</country>
    </address>
    <location>
      <latitude>45.507856</latitude>
      <longitude>-122.690794</longitude>
      <precision>place</precision>
      <city>Richmond</city>
      <state>CA</state>
      <postal_code>94801</postal_code>
      <country>US</country>
    </location>
    <districts></districts>
    <custom_columns></custom_columns>
    <subscriptions>
      <subscription campaign_id="189358" campaign_name="National" campaign_description="" opt_in_path_id="279022" status="Active" opt_in_source="Civis Sync" created_at="2019-06-06T19:33:03Z" activated_at="2019-06-06T19:33:03Z" opted_out_at="" opt_out_source=""/>
    </subscriptions>
    <integrations></integrations>
    <clicks></clicks>
  </profile>
</response>
"""


@pytest.fixture(autouse=True)
def mock_donor_id_salt():
    settings.override_cached_property("donor_id_salt", "salt")


@pytest.fixture
def sample_donation():
    with open(os.path.join(os.path.dirname(__file__), "sample_donation.json")) as f:
        return f.read()


@pytest.fixture
def sample_donation_no_phone():
    with open(os.path.join(os.path.dirname(__file__), "sample_donation.json")) as f:
        d = json.load(f)
        d["donor"]["phone"] = None
        return json.dumps(d)


@pytest.fixture
def sample_donation_followup():
    with open(
        os.path.join(os.path.dirname(__file__), "sample_donation_followup.json")
    ) as f:
        return f.read()


@pytest.fixture
def sample_donation_followup_recurring():
    with open(
        os.path.join(
            os.path.dirname(__file__), "sample_donation_followup_recurring.json"
        )
    ) as f:
        return f.read()


@pytest.fixture
def sample_donation_different_person():
    with open(
        os.path.join(os.path.dirname(__file__), "sample_donation_different_person.json")
    ) as f:
        return f.read()


@pytest.fixture
def sample_cancellation():
    with open(os.path.join(os.path.dirname(__file__), "sample_cancellation.json")) as f:
        return f.read()


@pytest.fixture
def mock_actblue_webhook_auth():
    settings.override_cached_property("mobile_commons_username", "test_mc_user")
    settings.override_cached_property("mobile_commons_password", "test_mc_password")
    return mock_basic_auth("actblue_webhook")


@pytest.fixture
def mock_actblue_webhook_wrong_auth():
    return mock_wrong_auth("actblue_webhook")


def setup_mock_s3():
    settings.override_cached_property(
        "actblue_donations_incoming_s3_bucket", "ew-actblue-donations-incoming-dev"
    )
    resource = boto3.resource("s3", region_name="us-east-1")
    resource.create_bucket(Bucket=settings.actblue_donations_incoming_s3_bucket)
    return resource


@responses.activate
@freezegun.freeze_time(LAGGED_ALLOWED_WEBHOOK_NOTIFICATION_TIME)
def test_invalid_auth(client, sample_donation, mock_actblue_webhook_wrong_auth):
    res = client.post(
        url_for("actblue.donation"),
        headers={"Authorization": mock_actblue_webhook_wrong_auth},
        data=sample_donation,
    )
    assert res.status_code == 401


@mock_s3
@mock_cloudwatch
@freezegun.freeze_time(LAGGED_ALLOWED_WEBHOOK_NOTIFICATION_TIME)
def test_s3_upload(client, sample_donation_no_phone, mock_actblue_webhook_auth):
    s3_resource = setup_mock_s3()
    res = client.post(
        url_for("actblue.donation"),
        headers={"Authorization": mock_actblue_webhook_auth},
        data=sample_donation_no_phone,
    )
    assert res.status_code == 204

    expected_key = f"donations/2019-06-07_20:32:24_2019-06-07_19:49:39_AB999999.json"
    body = (
        s3_resource.Object(settings.actblue_donations_incoming_s3_bucket, expected_key)
        .get()["Body"]
        .read()
        .decode("utf-8")
    )
    assert body == sample_donation_no_phone


@mock_s3
@mock_cloudwatch
@responses.activate
@freezegun.freeze_time(LAGGED_ALLOWED_WEBHOOK_NOTIFICATION_TIME)
def test_valid_auth_no_phone(
    client, sample_donation_no_phone, mock_actblue_webhook_auth
):
    setup_mock_s3()
    res = client.post(
        url_for("actblue.donation"),
        headers={"Authorization": mock_actblue_webhook_auth},
        data=sample_donation_no_phone,
    )
    assert len(responses.calls) == 0  # No Mobile Commons requests
    assert res.status_code == 204


@mock_s3
@mock_cloudwatch
@responses.activate
@freezegun.freeze_time(LAGGED_ALLOWED_WEBHOOK_NOTIFICATION_TIME)
def test_mobile_commons_profile_already_exists(
    client, sample_donation, mock_actblue_webhook_auth
):
    setup_mock_s3()
    responses.add(
        responses.POST,
        "https://secure.mcommons.com/api/profile",
        body=MOBILE_COMMONS_PROFILE_RESPONSE,
        match_querystring=True,
    )

    res = client.post(
        url_for("actblue.donation"),
        headers={"Authorization": mock_actblue_webhook_auth},
        data=sample_donation,
    )
    assert len(responses.calls) == 1  # No profile_update request.
    assert res.status_code == 204


@mock_s3
@mock_cloudwatch
@responses.activate
@freezegun.freeze_time(LAGGED_ALLOWED_WEBHOOK_NOTIFICATION_TIME)
def test_mobile_commons_profile_already_exists_no_subscriptions(
    client, sample_donation, mock_actblue_webhook_auth
):
    setup_mock_s3()
    responses.add(
        responses.POST,
        "https://secure.mcommons.com/api/profile",
        body=MOBILE_COMMONS_PROFILE_EXISTS_BUT_NOT_SUBSCRIBED_RESPONSE,
        match_querystring=True,
    )

    # This test case shows that we don't opt-in the phone number if they
    # have a profile, but that profile has never opted in to any campaign.
    # TODO: We might want to opt-in in this case too?

    res = client.post(
        url_for("actblue.donation"),
        headers={"Authorization": mock_actblue_webhook_auth},
        data=sample_donation,
    )
    assert len(responses.calls) == 1  # No profile_update request.
    assert res.status_code == 204


@mock_s3
@mock_cloudwatch
@responses.activate
@freezegun.freeze_time(LAGGED_ALLOWED_WEBHOOK_NOTIFICATION_TIME)
def test_mobile_commons_profile_upload(
    client, sample_donation, mock_actblue_webhook_auth
):
    setup_mock_s3()
    responses.add(
        responses.POST,
        "https://secure.mcommons.com/api/profile",
        body=MOBILE_COMMONS_PROFILE_NOT_EXIST_RESPONSE,
        match_querystring=True,
    )

    def check_mobile_commons_request_body(request):
        body = json.loads(request.body)
        expected = {
            "phone_number": "15105016227",
            "email": "marysmithexample@gmail.com",
            "postal_code": "94801",
            "first_name": "Mary",
            "last_name": "Smith",
            "street1": "20 Belvedere Ave.",
            "city": "Richmond",
            "state": "CA",
            "country": "US",
            "opt_in_path_id": "279022",
        }
        for k, v in expected.items():
            assert body[k] == v
        return (200, {}, MOBILE_COMMONS_PROFILE_UPDATE_RESPONSE)

    responses.add_callback(
        responses.POST,
        "https://secure.mcommons.com/api/profile_update",
        callback=check_mobile_commons_request_body,
        match_querystring=True,
    )

    res = client.post(
        url_for("actblue.donation"),
        headers={"Authorization": mock_actblue_webhook_auth},
        data=sample_donation,
    )
    assert len(responses.calls) == 2
    assert res.status_code == 204


@mock_s3
@mock_cloudwatch
@responses.activate
@freezegun.freeze_time(NOT_LAGGED_MIDNIGHT_WEBHOOK_NOTIFICATION_TIME)
def test_mobile_commons_profile_upload_not_lagged_at_midnight(
    client, sample_donation, mock_actblue_webhook_auth
):
    setup_mock_s3()
    responses.add(
        responses.POST,
        "https://secure.mcommons.com/api/profile",
        body=MOBILE_COMMONS_PROFILE_NOT_EXIST_RESPONSE,
        match_querystring=True,
    )

    d = json.loads(sample_donation)
    d["lineitems"][-1]["paidAt"] = "2019-06-07T00:00:00-04:00"
    sample_donation_at_midnight = json.dumps(d)
    responses.add(
        responses.POST,
        "https://secure.mcommons.com/api/profile_update",
        body=MOBILE_COMMONS_PROFILE_UPDATE_RESPONSE,
        match_querystring=True,
    )

    res = client.post(
        url_for("actblue.donation"),
        headers={"Authorization": mock_actblue_webhook_auth},
        data=sample_donation_at_midnight,
    )
    assert len(responses.calls) == 2
    assert res.status_code == 204


@mock_s3
@mock_cloudwatch
@responses.activate
@freezegun.freeze_time(LAGGED_DISALLOWED_WEBHOOK_NOTIFICATION_TIME)
def test_mobile_commons_profile_disallowed_send_time(
    client, sample_donation, mock_actblue_webhook_auth
):
    setup_mock_s3()
    responses.add(
        responses.POST,
        "https://secure.mcommons.com/api/profile",
        body=MOBILE_COMMONS_PROFILE_NOT_EXIST_RESPONSE,
        match_querystring=True,
    )

    res = client.post(
        url_for("actblue.donation"),
        headers={"Authorization": mock_actblue_webhook_auth},
        data=sample_donation,
    )
    assert len(responses.calls) == 0  # Short circuits before making any MC requests.
    assert res.status_code == 204


@mock_s3
@freezegun.freeze_time(LAGGED_ALLOWED_WEBHOOK_NOTIFICATION_TIME)
def test_s3_upload_cancellation(client, sample_cancellation, mock_actblue_webhook_auth):
    s3_resource = setup_mock_s3()
    res = client.post(
        url_for("actblue.cancellation"),
        headers={"Authorization": mock_actblue_webhook_auth},
        data=sample_cancellation,
    )
    assert res.status_code == 204

    expected_key = (
        f"cancellations/2019-06-07_20:32:24_2019-06-10_17:48:26_AB999999.json"
    )
    body = (
        s3_resource.Object(settings.actblue_donations_incoming_s3_bucket, expected_key)
        .get()["Body"]
        .read()
        .decode("utf-8")
    )
    assert body == json.dumps(json.loads(sample_cancellation))


@mock_s3
@mock_cloudwatch
@responses.activate
@freezegun.freeze_time(LAGGED_ALLOWED_WEBHOOK_NOTIFICATION_TIME)
def test_create_donor_object(
    client,
    sample_donation_no_phone,
    sample_donation_followup,
    sample_donation_followup_recurring,
    mock_actblue_webhook_auth,
):
    setup_mock_s3()
    res = client.post(
        url_for("actblue.donation"),
        headers={"Authorization": mock_actblue_webhook_auth},
        data=sample_donation_no_phone,
    )

    d = Donor.get_or_create_donor("marysmithexample@gmail.com")
    assert d.first_name == "Mary"
    assert d.last_name == "Smith"
    assert d.donor_id == "auAhIJZkUafh"
    assert d.city == "Richmond"
    assert d.state == "CA"
    assert d.zip == "94801"
    assert d.last_donation_dt == "2019-06-07"
    assert d.last_donation_ts == datetime.datetime(
        2019, 6, 7, 19, 49, 39, tzinfo=datetime.timezone.utc
    )
    assert d.last_donation_amount == 50.0
    assert d.last_donation_type == "actblue"
    assert d.total_donation_amount == 50.0
    assert d.badges == set()
    assert res.status_code == 204

    # Making same request twice is a no-op.
    res = client.post(
        url_for("actblue.donation"),
        headers={"Authorization": mock_actblue_webhook_auth},
        data=sample_donation_no_phone,
    )
    d = Donor.get_or_create_donor("marysmithexample@gmail.com")
    assert d.total_donation_amount == 50.0
    assert d.badges == set()

    res = client.post(
        url_for("actblue.donation"),
        headers={"Authorization": mock_actblue_webhook_auth},
        data=sample_donation_followup,
    )
    d = Donor.get_or_create_donor("marysmithexample@gmail.com")
    assert d.first_name == "Mary"
    assert d.donor_id == "auAhIJZkUafh"
    assert d.last_donation_dt == "2019-06-26"
    assert d.last_donation_ts == datetime.datetime(
        2019, 6, 26, 19, 49, 39, tzinfo=datetime.timezone.utc
    )
    assert d.last_donation_amount == 75.0
    assert d.total_donation_amount == 125.0
    assert d.badges == {"debate", "eoq"}
    assert res.status_code == 204

    res = client.post(
        url_for("actblue.donation"),
        headers={"Authorization": mock_actblue_webhook_auth},
        data=sample_donation_followup_recurring,
    )
    d = Donor.get_or_create_donor("marysmithexample@gmail.com")
    assert d.first_name == "Mary"
    assert d.donor_id == "auAhIJZkUafh"
    assert d.last_donation_dt == "2019-07-26"
    assert d.last_donation_ts == datetime.datetime(
        2019, 7, 26, 19, 49, 39, tzinfo=datetime.timezone.utc
    )
    assert d.last_donation_amount == 5.0
    assert d.total_donation_amount == 130.0
    assert d.badges == {"debate", "eoq", "eom", "recurring"}
    assert res.status_code == 204


@mock_s3
@mock_cloudwatch
@responses.activate
@freezegun.freeze_time(LAGGED_ALLOWED_WEBHOOK_NOTIFICATION_TIME)
def test_create_multiple_donors(
    client,
    sample_donation_no_phone,
    sample_donation_different_person,
    mock_actblue_webhook_auth,
):
    setup_mock_s3()
    res = client.post(
        url_for("actblue.donation"),
        headers={"Authorization": mock_actblue_webhook_auth},
        data=sample_donation_no_phone,
    )

    d = Donor.get_or_create_donor("marysmithexample@gmail.com")
    assert d.first_name == "Mary"
    assert d.last_name == "Smith"
    assert d.donor_id == "auAhIJZkUafh"
    assert d.city == "Richmond"
    assert d.state == "CA"
    assert d.zip == "94801"
    assert d.last_donation_dt == "2019-06-07"
    assert d.last_donation_ts == datetime.datetime(
        2019, 6, 7, 19, 49, 39, tzinfo=datetime.timezone.utc
    )
    assert d.last_donation_amount == 50.0
    assert d.last_donation_type == "actblue"
    assert d.total_donation_amount == 50.0
    assert d.badges == set()
    assert res.status_code == 204

    res = client.post(
        url_for("actblue.donation"),
        headers={"Authorization": mock_actblue_webhook_auth},
        data=sample_donation_different_person,
    )
    d = Donor.get_or_create_donor("johnsmithexample@gmail.com")
    assert d.first_name == "John"
    assert d.last_name == "Smith"
    assert d.donor_id == "vAIiTVlF8jOS"
    assert d.last_donation_dt == "2019-06-10"
    assert d.last_donation_ts == datetime.datetime(
        2019, 6, 10, 19, 49, 39, tzinfo=datetime.timezone.utc
    )
    assert d.last_donation_amount == 15.0
    assert d.last_donation_type == "actblue"
    assert d.total_donation_amount == 15.0
    assert d.badges == set()
    assert res.status_code == 204
