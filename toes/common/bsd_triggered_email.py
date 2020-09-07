import json
import time
import urllib

from bsdapi.BsdApi import Factory as BsdApiFactory

from common.settings import settings

BSD_HOST = "warren.cp.bsd.net"


def send_triggered_email(email, mailing_id, payload):
    bsd_api = BsdApiFactory().create(
        api_id=settings.bsd_api_username,
        secret=settings.bsd_api_password,
        host=BSD_HOST,
        port=80,
        securePort=443,
    )

    payload_str = json.dumps(payload)
    payload_str_urlencoded = urllib.parse.quote(payload_str)
    params = {
        "mailing_id": mailing_id,
        "email": email,
        "email_opt_in": 0,
        "trigger_values": payload_str_urlencoded,
    }

    print(f"Making triggered email request with params {params}")
    deferred_id = None
    resp = bsd_api.doRequest("/mailer/send_triggered_email", params, bsd_api.POST)
    while resp.http_status == 202:
        if not deferred_id and resp.body:
            deferred_id = json.loads(resp.body)["deferred_task_id"]
        else:
            # Sleep between calls to getDeferredResults.
            time.sleep(1)
        print(f"Making followup request with deferred id: {deferred_id}")
        resp = bsd_api.getDeferredResults(deferred_id)

    print(f"Got API result body: {resp.body}")
