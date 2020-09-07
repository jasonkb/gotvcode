# ActBlue webhook handler.
#
# Fulfils two main roles:
# 1. Uploads incoming donation payloads to power some realtime
#    dashboards.
# 2. Uploads new phone numbers to Mobile Common profiles, to opt them
#    into our text message list.
#
# For reference, ActBlue webhook documentation is here:
#   https://secure.actblue.com/docs/webhooks

import datetime
import json
import logging

import boto3
import dateutil
import pytz
from flask import Blueprint, request
from nameparser import HumanName
from zappa.asynchronous import task

from common.basic_auth import requires_auth
from common.cloudwatch import cloudwatch_put_metric
from common.settings import settings
from ew_common.input_validation import extract_phone_number, normalize_name
from ew_common.mobile_commons import (
    create_or_update_mobile_commons_profile,
    profile_exists,
)
from models.donor import Donor

mod = Blueprint("actblue", __name__)

OPT_IN_PATH_ID = "279022"
# WHITELIST_ONLY_PHONE = ['15105016227', '18287138291', '17862830447']  # (11 digits with leading 1)
WHITELIST_ONLY_PHONE = []  # Empty whitelist allows all numbers.

# If there is more lag than this between donation creation and webhook
# invocation, we'll limit sending to 9am-9pm Eastern.
LOTS_OF_LAG_THRESHOLD_SECONDS = 5 * 60
LATEST_SEND_HOUR_UTC = 1  # 9pm EDT
EARLIEST_SEND_HOUR_UTC = 13  # 9am EDT

DEBATE_DONOR_DAYS = [
    datetime.date(2019, 6, 26),
    datetime.date(2019, 6, 27),
    datetime.date(2019, 7, 30),
    datetime.date(2019, 7, 31),
    datetime.date(2019, 9, 12),
    datetime.date(2019, 9, 13),
    datetime.date(2019, 10, 15),
    datetime.date(2019, 10, 16),
    datetime.date(2019, 11, 20),
    datetime.date(2019, 11, 21),
    datetime.date(2019, 12, 19),
    datetime.date(2019, 12, 20),
]

Donor.Meta.table_name = settings.donors_table_name


@mod.route("/donation", methods=["POST"])
@requires_auth("actblue_webhook")
def donation():
    req_body = request.data
    try:
        event = json.loads(req_body)
    except ValueError:
        logging.exception(f"ActBlue - Bad Donation Request - data: {req_body}")
        return ("Bad Request", 400)

    process_donation(event)
    return ("", 204)


@task
def process_donation(event):
    upload_to_mobilecommons(event)
    log_metrics(event)
    write_donation_to_s3(event)

    try:
        create_donor_object(event)
    except Exception as e:
        logging.exception("Got exception in create_donor_object", e)


def upload_to_mobilecommons(event):
    """Given an incoming donation, creates new profile on Mobile Commons if appropriate.

    - Noop if donation does not have phone number.
    - Does not attempt to create/update profile if one already exists in Mobile Commons.
    - Opts the phone number in to OPT_IN_PATH_ID opt-in path on Mobile Commons.
    - Normalizes first/last name in case it comes in as all lowercase/uppercase.
    """
    donor = event["donor"]
    if "phone" not in donor or not donor["phone"]:
        return
    phone_number = extract_phone_number(donor["phone"])
    if not phone_number:
        return

    logging.info(f"ActBlue webhook with phone number.")

    if WHITELIST_ONLY_PHONE and phone_number not in WHITELIST_ONLY_PHONE:
        return

    lag_in_seconds = lag_since_donation_in_seconds(event)
    if (
        webhook_notification_was_significantly_lagged(lag_in_seconds)
        and not allowed_sending_time()
    ):
        logging.warning(
            f"Webhook notification was lagged and now we are not in allowed sending time. Not creating profile for {phone_number}."
        )
        return

    if profile_exists(
        settings.mobile_commons_username, settings.mobile_commons_password, phone_number
    ):
        logging.info(f"Profile already exists for number {phone_number}")
        return

    create_or_update_mobile_commons_profile(
        settings.mobile_commons_username,
        settings.mobile_commons_password,
        profile_payload(phone_number, donor),
    )


def log_metrics(event):
    lag_in_seconds = lag_since_donation_in_seconds(event)
    log_lag(lag_in_seconds)


def lag_since_donation_in_seconds(event):
    paid_at_str = event["lineitems"][-1]["paidAt"]
    paid_at = dateutil.parser.parse(paid_at_str)
    webhook_invoked_at = datetime.datetime.now(datetime.timezone.utc)
    lag_in_seconds = (webhook_invoked_at - paid_at).seconds
    logging.info(
        f"Lag between donation creation {paid_at_str} (parsed to {paid_at}) and webhook invocation {webhook_invoked_at}: {lag_in_seconds} seconds"
    )
    return lag_in_seconds


def log_lag(lag_in_seconds):
    cloudwatch_put_metric(
        [
            {
                "MetricName": "lag_between_donation_and_webhook",
                "Dimensions": [],
                "Unit": "Seconds",
                "Value": lag_in_seconds,
            }
        ]
    )


def webhook_notification_was_significantly_lagged(lag_in_seconds):
    return lag_in_seconds > LOTS_OF_LAG_THRESHOLD_SECONDS


def allowed_sending_time():
    """Returns true if the time of day is roughly sane for sending text messages in the US.

    Ideally ActBlue would hit our webhook immediately after a donation
    is sent; but in reality, there can be a significant lag between
    donation and webhook. So we double check that it's not the middle of
    the night and allow sending only between 9am EDT and 11pm EDT.
    """
    hour_utc = datetime.datetime.utcnow().time().hour
    return hour_utc >= EARLIEST_SEND_HOUR_UTC or hour_utc < LATEST_SEND_HOUR_UTC


def profile_payload(phone_number, donor):
    """Prepares payload for profile_update Mobile Commons API endpoint.

    As described in
      https://community.uplandsoftware.com/hc/en-us/articles/204494185-REST-API#ProfileUpdate
    """
    first_name, last_name = normalize_name(
        donor.get("firstname", ""), donor.get("lastname", "")
    )
    payload = {
        "phone_number": phone_number,
        "email": donor.get("email", ""),
        "postal_code": donor.get("zip", ""),
        "first_name": first_name,
        "last_name": last_name,
        "street1": donor.get("addr1", ""),
        "city": donor.get("city", ""),
        "state": donor.get("state", ""),
        "country": "US",
        "opt_in_path_id": OPT_IN_PATH_ID,
    }

    # Don't upload null or empty fields.
    keys_to_delete = [k for k, v in payload.items() if not v]
    for k in keys_to_delete:
        del payload[k]

    return payload


def write_donation_to_s3(event):
    write_to_s3(event, event["lineitems"][-1]["paidAt"], "donations/")


@mod.route("/cancellation", methods=["POST"])
@requires_auth("actblue_webhook")
def cancellation():
    req_body = request.data
    try:
        event = json.loads(req_body)
    except ValueError:
        logging.exception(f"ActBlue - Bad Cancellation Request - data: {req_body}")
        return ("Bad Request", 400)

    process_cancellation(event)
    return ("", 204)


@task
def process_cancellation(event):
    write_cancellation_to_s3(event)


def write_cancellation_to_s3(event):
    write_to_s3(event, event["contribution"]["cancelledAt"], "cancellations/")


def write_to_s3(event, comparison_timestamp, object_prefix):
    comparison_datetime_utc = dateutil.parser.parse(comparison_timestamp).astimezone(
        pytz.utc
    )
    comparison_timestamp_utc = comparison_datetime_utc.strftime("%Y-%m-%d_%H:%M:%S")

    time_now = datetime.datetime.now(datetime.timezone.utc)
    primary_timestamp = time_now.strftime("%Y-%m-%d_%H:%M:%S")

    order_number = event["contribution"]["orderNumber"]

    key = f"{object_prefix}{primary_timestamp}_{comparison_timestamp_utc}_{order_number}.json"

    s3 = boto3.resource("s3")
    body = json.dumps(event)
    logging.info(f"Writing to s3: {key}")
    s3.Bucket(settings.actblue_donations_incoming_s3_bucket).put_object(
        Key=key, Body=body
    )


def create_donor_object(event):
    """Given an incoming donation, creates new Donor object in DynamoDB."""
    donor_ab = event["donor"]
    email = donor_ab["email"]
    contribution = event["contribution"]
    line_item = event["lineitems"][-1]

    donor = Donor.get_or_create_donor(email)

    paid_at = dateutil.parser.parse(line_item["paidAt"])
    if donor.last_donation_ts and paid_at <= donor.last_donation_ts:
        logging.warning(
            f"This seems like a duplicate webhook notification, with identical paidAt {paid_at} "
            f"compared to existing last_donation_ts {donor.last_donation_ts}"
        )
        return

    if not donor.donor_id:
        donor.set_donor_id(settings.donor_id_salt)

    donor.first_name, donor.last_name = normalize_name(
        donor_ab.get("firstname", ""), donor_ab.get("lastname", "")
    )
    donor.city = donor_ab.get("city")
    donor.state = donor_ab.get("state")
    donor.zip = donor_ab.get("zip")

    if is_eom(paid_at):
        donor.badges.add(Donor.BadgeId.EOM.value)
    if is_eoq(paid_at):
        donor.badges.add(Donor.BadgeId.EOQ.value)
    if is_debate(paid_at):
        donor.badges.add(Donor.BadgeId.DEBATE.value)
    if is_recurring(contribution):
        donor.badges.add(Donor.BadgeId.RECURRING.value)

    donor.last_donation_ts = dateutil.parser.parse(line_item["paidAt"])
    donor.last_donation_dt = str(
        donor.last_donation_ts.astimezone(pytz.timezone("US/Eastern")).date()
    )
    donor.last_donation_amount = float(line_item["amount"])
    donor.last_donation_type = "actblue"
    donor.total_donation_amount += donor.last_donation_amount
    donor.save()


def is_eom(paid_at):
    return paid_at.day >= 26 and paid_at.month % 3 != 0


def is_eoq(paid_at):
    return paid_at.day >= 26 and paid_at.month % 3 == 0


def is_debate(paid_at):
    return paid_at.date() in DEBATE_DONOR_DAYS


def is_recurring(contribution):
    return contribution["isRecurring"]
