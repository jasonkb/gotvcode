import datetime
import json

import pytest
import responses
from flask import url_for

from models.chat_referral import ChatReferral

DEFAULT_PROFILE = {"phone": "15105016227", "profile_email": "jasonkatzbrown@gmail.com"}

DEFAULT_PROFILE_WITH_LOTS_INFO = {
    **DEFAULT_PROFILE,
    "profile_first_and_last_name": "Jason Katz-Brown",
    "profile_first_name": "Jason",
    "profile_last_name": "Katz-Brown",
    "profile_postal_code": "94801",
    "profile_techsandbox_income_last_year": "120000",
    "profile_techsandbox_outstanding_student_loan_debt": "60000",
}

MOBILE_COMMONS_SEND_MESSAGE_RESPONSE = """
<response success="true">
<message id="6755704007" type="generic" status="queued">
  <phone_number>15102194255</phone_number>
  <profile>338948442</profile>
  <body>Hi! Your friend Jason thought you'd be interested in Elizabeth Warren's plan to cancel student loan debt. Reply SANDBOXDEBT to see how much you'd save.</body>
  <sent_at>2019-05-31 05:04:17 UTC</sent_at>
  <message_template_id/>
  <campaign id="189718" active="true">
    <name>Tech Sandbox</name>
  </campaign>
</message>
</response>
"""


@responses.activate
def test_refer_invalid_request(client):
    params = {}
    res = client.get(url_for("mdata.refer", **params))
    assert res.json["message"] == "Sorry, I've hiccupped! Try writing to me tomorrow."


@responses.activate
def test_refer(client):
    params = {**DEFAULT_PROFILE, "args": ""}
    res = client.get(url_for("mdata.refer", **params))
    assert (
        res.json["message"]
        == "Let's get more people to join in this fight!\nFirst, in one quick sentence, why are you supporting Elizabeth? We'll share this with people you refer."
    )

    params = {
        **DEFAULT_PROFILE,
        "args": "The system is rigged *for* me in so many ways; now it's time For big structural change!",
    }
    res = client.get(url_for("mdata.refer", **params))
    assert res.json["message"] == "What's your first and last name?"

    params = {**DEFAULT_PROFILE, "args": "Jason Katz-Brown"}
    res = client.get(url_for("mdata.refer", **params))
    assert res.json["message"] == "And what's your zip code?"

    params = {**DEFAULT_PROFILE, "args": "94801"}
    res = client.get(url_for("mdata.refer", **params))
    assert res.json["message"] == "What's your friend's first and last name?"

    params = {**DEFAULT_PROFILE, "args": "Richard Katz"}
    res = client.get(url_for("mdata.refer", **params))
    assert res.json["message"] == "Cool, what's Richard Katz's phone number?"

    params = {**DEFAULT_PROFILE, "args": "5102194255"}
    res = client.get(url_for("mdata.refer", **params))
    assert (
        res.json["message"]
        == "What city and state does Richard live in? For example: Reno NV"
    )

    def check_mobile_commons_request_body(request):
        body = json.loads(request.body)
        expected = {
            "campaign_id": 189718,
            "phone_number": "15102194255",
            "body": [
                "Your friend Jason Katz-Brown invited you to join the fight with Elizabeth Warren. Richard, we need you in this fight too. Reply FIGHT to join.",
                'Jason supports Elizabeth because: "The system is rigged *for* me in so many ways; now it\'s time For big structural change!"',
            ],
        }
        for k, v in expected.items():
            if isinstance(v, list):
                assert body[k] in v
            else:
                assert body[k] == v
        return (200, {}, MOBILE_COMMONS_SEND_MESSAGE_RESPONSE)

    responses.add_callback(
        responses.POST,
        "https://secure.mcommons.com/api/send_message",
        callback=check_mobile_commons_request_body,
        match_querystring=True,
    )

    params = {**DEFAULT_PROFILE, "args": "Richmond CA"}
    res = client.get(url_for("mdata.refer", **params))
    assert (
        res.json["message"]
        == "Great, sent an invite to Richard. What's the name of another person you'd like to invite to join the fight?"
    )

    referral = ChatReferral.get_or_create_referral("15102194255", "15105016227")
    assert referral.first_and_last_name == "Richard Katz"
    assert referral.first_name == "Richard"
    assert referral.last_name == "Katz"
    assert referral.city == "Richmond"
    assert referral.state == "CA"
    assert referral.referrer_email == "jasonkatzbrown@gmail.com"
    assert referral.referrer_first_and_last_name == "Jason Katz-Brown"
    assert referral.referrer_first_name == "Jason"
    assert referral.referrer_last_name == "Katz-Brown"
    assert referral.referrer_postal_code == "94801"
    assert (
        datetime.datetime.now(datetime.timezone.utc) - referral.created_at
    ).seconds < 3

    params = {**DEFAULT_PROFILE, "args": "Daniel  Matza-Brown"}
    res = client.get(url_for("mdata.refer", **params))
    assert res.json["message"] == "Cool, what's Daniel Matza-Brown's phone number?"

    params = {**DEFAULT_PROFILE, "args": ""}
    res = client.get(url_for("mdata.refer", **params))
    assert (
        res.json["message"]
        == "Let's get more people to join in this fight!\nWhat's your friend's first and last name?"
    )

    params = {**DEFAULT_PROFILE, "args": "Francis Farewell Starlite"}
    res = client.get(url_for("mdata.refer", **params))
    assert res.json["message"] == "Cool, what's Francis Starlite's phone number?"


@responses.activate
def test_debt_calculation(client):
    params = {**DEFAULT_PROFILE_WITH_LOTS_INFO, "args": "120000"}
    res = client.get(url_for("mdata.debt", **params))
    assert (
        res.json["message"]
        == "Great news! You'll have $43333 of debt cancelled under Elizabeth's plan, bringing your outstanding student debt down to $16667.\n\nIf you know someone else who might benefit from student debt cancellation, what's their phone number?"
    )


@responses.activate
def test_debt_referral(client):
    def check_mobile_commons_request_body(request):
        body = json.loads(request.body)
        expected = {
            "campaign_id": 189718,
            "phone_number": "15102194255",
            "body": "Hi! Your friend Jason thought you'd be interested in Elizabeth Warren's plan to cancel student loan debt. Reply SANDBOXDEBT to see how much you'd save.",
        }
        for k, v in expected.items():
            assert body[k] == v
        return (200, {}, MOBILE_COMMONS_SEND_MESSAGE_RESPONSE)

    responses.add_callback(
        responses.POST,
        "https://secure.mcommons.com/api/send_message",
        callback=check_mobile_commons_request_body,
        match_querystring=True,
    )

    params = {**DEFAULT_PROFILE_WITH_LOTS_INFO, "args": "5102194255"}
    res = client.get(url_for("mdata.debt", **params))
    assert (
        res.json["message"]
        == "Great, sent! If you know others still with student loan debt, reply with their phone number."
    )
