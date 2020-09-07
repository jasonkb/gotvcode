# Endpoints to support the Grassroots Donor Walla.
#
# /donors/resend_donor_wall_link takes an email address and sends a
# triggered email that includes a link to find yourself on the
# grassroots donor wall.
import json

from flask import Blueprint, request
from zappa.asynchronous import task

from common.bsd_triggered_email import send_triggered_email
from common.settings import settings
from models.donor import Donor

mod = Blueprint("donors", __name__)

DONOR_WALL_LINK_MAILING_ID = "UlQPDQ"

Donor.Meta.table_name = settings.donors_table_name


@mod.route("/resend_donor_wall_link", methods=["POST"])
def resend_donor_wall_link():
    req_body = request.data
    try:
        event = json.loads(req_body)
    except ValueError:
        print(f"resend_donor_wall_link - bad request - data: {req_body}")
        return ("Bad Request", 400)

    if "email" not in event:
        print(f"resend_donor_wall_link - missing email - data: {req_body}")
        return ("Bad Request", 400)

    process_resend(event)
    return ("", 204)


@task
def process_resend(event):
    email = event["email"].strip()
    d = Donor.get_or_create_donor(email)
    payload = {"email": email}

    if not d.donor_id:
        email_lower = email.lower()
        if email_lower != email:
            # In case the user typed in their email address with novel
            # capitalization, also check for their email in lowercase.
            d = Donor.get_or_create_donor(email_lower)

    if d.donor_id:
        payload["donor_id"] = d.donor_id

    send_triggered_email(email, DONOR_WALL_LINK_MAILING_ID, payload)
