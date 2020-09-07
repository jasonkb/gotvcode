import os

import pytest
import responses
from flask import url_for

DEFAULT_PARAMS = {
    "ts_start": "1564567200",
    "ts_end": "1564545600",
    "event_types": "COMMUNITY,HOUSE_PARTY",
    "max_dist": "30",
    "utm_source": "SMSMdata",
}

DEFAULT_MOBILIZE_AMERICA_API_URL = "https://events.mobilizeamerica.io/api/v1/organizations/3510/events?per_page=3&exclude_full=true&zipcode={zipcode}&timeslot_start=gte_1564567200&timeslot_start=lte_1564545600&max_dist=30&event_types=COMMUNITY&event_types=HOUSE_PARTY"


def mock_mobilize_america_response_default(zipcode):
    mock_filename = f"mock_mobilize_america_debate_watch_party_response_{zipcode}.json"
    with open(os.path.join(os.path.dirname(__file__), mock_filename)) as f:
        responses.add(
            responses.GET,
            DEFAULT_MOBILIZE_AMERICA_API_URL.format(zipcode=zipcode),
            body=f.read(),
        )


@responses.activate
def test_events(client):
    mock_mobilize_america_response_default("94801")
    res = client.get(
        url_for("mobilize_america.events", zipcode="94801", **DEFAULT_PARAMS)
    ).json
    assert res["data"][0]["timeslots"][0]["formatted_time"] == "Tue, Jul 30 at 5:00 PM"
    assert (
        res["data"][0]["browser_url"]
        == "https://events.elizabethwarren.com/event/103621/?utm_source=SMSMdata"
    )


@responses.activate
def test_events_without_data(client):
    mock_mobilize_america_response_default("00000")
    res = client.get(
        url_for("mobilize_america.events", zipcode="00000", **DEFAULT_PARAMS)
    ).json
    print(res)
    assert res["count"] == 0


@responses.activate
def test_events_with_words_in_zip(client):
    mock_mobilize_america_response_default("94801")
    res = client.get(
        url_for(
            "mobilize_america.events", zipcode="looking in 94801.", **DEFAULT_PARAMS
        )
    ).json
    assert (
        res["data"][0]["title"]
        == "Berkeley Elizabeth Warren Debate Watch Party, part 2"
    )
