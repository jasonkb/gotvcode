import datetime
import urllib.parse

import pytz
import requests
from flask import Blueprint, jsonify, request

from ew_common.input_validation import extract_postal_code

mod = Blueprint("mobilize_america", __name__)

EVENTS_URL_BASE = "https://events.mobilizeamerica.io/api/v1/organizations/3510/events?per_page=3&exclude_full=true"

# No actual whitelist for now
ORGANIZATION_ID_WHITELIST = []


def format_event_start_time(t):
    """Formats datetime into e.g. Tue, Jul 30 at 5:00 PM"""
    strftime_format = "%a, %b %-d at %-I:%M %p"
    return t.strftime(strftime_format)


def timestamp_to_datetime_in_zone(timestamp, tz):
    return pytz.utc.localize(datetime.datetime.utcfromtimestamp(timestamp)).astimezone(
        tz
    )


@mod.route("/events", methods=["GET"])
def events():
    """Wrapper around Mobilize America events API.

    Returns 1 event within 50 miles from given zipcode. Appropriate for using
    as a Mobile Commons mdata endpoint.

    Adds 'formatted_time' field to each event with human-readable date and time
    of the event in the local timezone. This is crucial to being able to
    render event details in a simple template in a Mobile Commons flow.

    Optionally adds utm_source param to the 'browser_url' of each event.

    Required params:
      - ts_start, ts_end: seconds since UNIX epoch defining timeframe in which event must *start*.
      - event_types: comma-separated list of Mobilize America all-caps event types
      - zipcode: search origin (unclear where Mobilize America defines center as zipcode, exactly)
      - max_dist: maximum distance from zipcode center in miles (default 50)
    """
    ts_start = request.args.get("ts_start", "").strip()
    ts_end = request.args.get("ts_end", "").strip()
    event_types = request.args.get("event_types", "").strip()
    zipcode = extract_postal_code(request.args.get("zipcode", "").strip())
    max_dist = request.args.get("max_dist", "50").strip()
    utm_source = request.args.get("utm_source", "").strip()

    if not (ts_start and ts_end and event_types and zipcode):
        return ("Events lookup requires ts_start, ts_end, event_types, zipcode", 400)

    # We compose URL manually (without using a params hash) because some
    # of the params (like event_types) can be specified more than once,
    # and that can be tricky to produce as intended.
    url = EVENTS_URL_BASE
    url += f"&zipcode={zipcode}&timeslot_start=gte_{ts_start}&timeslot_start=lte_{ts_end}&max_dist={max_dist}"
    for event_type in event_types.split(","):
        url += f"&event_types={event_type.strip()}"

    result = requests.get(url).json()

    if result.get("data"):
        result["data"] = [
            massage_event(e, utm_source) for e in result["data"] if should_keep_event(e)
        ]

    data = result.get("data", [])
    result["count"] = len(data) if data else 0

    return jsonify(result)


def should_keep_event(event):
    if ORGANIZATION_ID_WHITELIST:
        return event["sponsor"]["id"] in ORGANIZATION_ID_WHITELIST
    return True


def massage_event(event, utm_source):
    tz = pytz.timezone(event["timezone"])
    for timeslot in event["timeslots"]:
        timeslot["formatted_time"] = format_event_start_time(
            timestamp_to_datetime_in_zone(timeslot["start_date"], tz)
        )
    event["browser_url"] = add_utm_source(event["browser_url"], utm_source)
    return event


def add_utm_source(browser_url, utm_source):
    if not utm_source:
        return browser_url

    if "?" in browser_url:
        print(
            f"Mobilize America unexpectedly returned query params in event URL: {browser_url}"
        )
        return browser_url

    return f"{browser_url}?utm_source={urllib.parse.quote(utm_source)}"
