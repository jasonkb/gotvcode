import datetime
import os
import time
import xml.etree.ElementTree
from collections import defaultdict

import civis
import pandas as pd
import xmltodict
from bsdapi.BsdApi import Factory as BsdApiFactory
from nameparser import HumanName
from email_validator import validate_email, EmailNotValidError

RESUBSCRIBE_FORM_ID = 1059
RESUBSCRIBE_FORM_EMAIL_FIELD_ID = 16362

with open(os.path.join(os.path.dirname(__file__), "export.sql")) as f:
    GET_EMAILS_SQL = f.read()


def epoch_to_datetime(epoch):
    if epoch is None or pd.isnull(epoch):
        return None

    return datetime.datetime.fromtimestamp(epoch, datetime.timezone.utc)


def in_groups_of(n, l):
    for i in range(0, len(l), n):
        yield l[i : i + n]


# Exports people with email addresses from Civis to BSD constituents using the
# BSD API.
#
# - Uploads Mobilize America signups to BSD constituents
# - Uploads Mobilize.io signups to BSD constituents
# - Uploads Shopify purchasers to BSD constituents
class BsdExport:
    CONS_EMAIL_EMAIL_TYPE = "personal"
    EMAIL_IS_SUBSCRIBED = "1"

    PHONE_TYPE = "home"
    PHONE_IS_SUBSCRIBED = "0"

    def __init__(
        self,
        database,
        bsd_host,
        bsd_api_id,
        bsd_secret,
        email_types_and_cons_group_ids_arg,
        cons_group_email,
        cons_group_added_via_civis_sync,
        custom_field_id_email_type,
        limit_per_type,
        dry_run,
    ):
        self.database = database
        self.bsd_api = BsdApiFactory().create(
            api_id=bsd_api_id, secret=bsd_secret, host=bsd_host, port=80, securePort=443
        )

        # To test that we uploaded a past constituent as intended : )
        # resp = self.bsd_api.cons_getConstituentsById([4853942], bundles=['cons_addr'])
        # print(f'Got response about constituent: {resp.http_status} {resp.body}')

        self.email_types_and_cons_group_ids = self.parse_email_types_and_cons_group_ids_arg(
            email_types_and_cons_group_ids_arg
        )
        self.cons_group_email = cons_group_email
        self.cons_group_added_via_civis_sync = cons_group_added_via_civis_sync
        self.custom_field_id_email_type = custom_field_id_email_type
        self.limit_per_type = limit_per_type

        self.detailed_logs = []
        self.rejected_emails = []
        self.domain_deliverability_cache = {}

        self.dry_run = dry_run

    # Computes emails that are in various tables in Civis but not in any BSD
    # constituent. Creates BSD constituents for such email addresses.
    def sync_emails(self):
        emails = self.get_emails()
        counts_by_type = defaultdict(int)
        for profile in emails.itertuples():
            counts_by_type[profile.ext_type] += 1
        print("Counts by type:", counts_by_type)

        try:
            log_rows = []
            for (
                email_type,
                email_type_cons_group_id,
            ) in self.email_types_and_cons_group_ids:
                emails_of_type = emails[emails["ext_type"] == email_type]
                print(f"Exporting {len(emails_of_type)} emails from {email_type}")
                num_new_cons = self.export_emails(
                    emails_of_type,
                    email_type,
                    email_type_cons_group_id,
                    self.limit_per_type,
                )

                log_rows.append((email_type, num_new_cons))
        except Exception as e:
            print("Caught an exception; flushing logs and then re-throwing")
            self.insert_detailed_logs()
            raise e

        self.insert_detailed_logs()
        self.update_rejected_emails_table()

        if not self.dry_run:
            self.insert_logs(log_rows)

    # Given a list of (type, count) tuples, inserts those tuples into the log tables
    def insert_logs(self, log_rows):
        now_str = (
            datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
        )

        formatted_values = []
        for (email_type, num_new_cons) in log_rows:
            formatted_values.append(f"('{now_str}', '{email_type}', {num_new_cons})")

        # Table schema:
        # CREATE TABLE bsd.cons_insert_logs (
        #     timestamp timestamptz SORTKEY,
        #     type text,
        #     num_new_cons int,
        #     PRIMARY KEY (timestamp, type)
        # );
        sql_str = f"INSERT INTO bsd.cons_insert_logs (timestamp, type, num_new_cons) VALUES {','.join(formatted_values)};"

        print(f"Inserting logs: {sql_str}")

        result = civis.io.query_civis(sql=sql_str, database=self.database).result()
        print(f"Inserted log rows: {result}")

    def insert_detailed_logs(self):
        df = pd.DataFrame(self.detailed_logs,
                          columns=[
                              "processed_at",
                              "is_resubscribe",
                              "was_new",
                              "email",
                              "first_name",
                              "last_name",
                              "postal_code",
                              "addr1",
                              "addr2",
                              "city",
                              "state_cd",
                              "phone_number",
                              "ext_type",
                              "ext_id",
                              "source",
                              "subsource",
                              "resubscribeable_action_time_epoch",
                              "unsub_epoch",
                              "dry_run",
                              "did_exist",
                              "bsd_response",
                              "bsd_did_error",
                          ],
                          )

        print(f"Inserting detailed logs")
        print(df)

        # Table schema:
        # CREATE TABLE bsd.detailed_cons_insert_logs (
        #     processed_at TIMESTAMPTZ,
        #     is_resubscribe BOOLEAN,
        #     was_new BOOLEAN,
        #     email VARCHAR(256),
        #     first_name VARCHAR(256),
        #     last_name VARCHAR(256),
        #     postal_code VARCHAR(256),
        #     addr1 VARCHAR(256),
        #     addr2 VARCHAR(256),
        #     city VARCHAR(256),
        #     state_cd VARCHAR(256),
        #     phone_number VARCHAR(256),
        #     ext_type VARCHAR(256),
        #     ext_id VARCHAR(256),
        #     source VARCHAR(256),
        #     subsource VARCHAR(256),
        #     resubscribeable_action_time_epoch TIMESTAMPTZ,
        #     unsub_epoch TIMESTAMPTZ,
        #     dry_run BOOLEAN,
        #     did_exist BOOLEAN,
        #     bsd_response VARCHAR(512),
        #     bsd_did_error BOOLEAN
        # );
        insert = civis.io.dataframe_to_civis(
            df,
            "Warren for MA",
            "bsd.detailed_cons_insert_logs",
            existing_table_rows="append",
        )

        insert.result()

    def update_rejected_emails_table(self):
        df = pd.DataFrame(self.rejected_emails, columns=[
            "email",
            "is_resubscribe",
            "first_name",
            "last_name",
            "postal_code",
            "addr1",
            "addr2",
            "city",
            "state_cd",
            "phone_number",
            "ext_type",
            "ext_id",
            "source",
            "subsource",
            "resubscribeable_action_time_epoch",
            "unsub_epoch",
        ])

        # Table schema:
        # CREATE TABLE bsd.cons_insert_rejected_emails (
        #     email VARCHAR(256),
        #     is_resubscribe BOOLEAN,
        #     first_name VARCHAR(256),
        #     last_name VARCHAR(256),
        #     postal_code VARCHAR(256),
        #     addr1 VARCHAR(256),
        #     addr2 VARCHAR(256),
        #     city VARCHAR(256),
        #     state_cd VARCHAR(256),
        #     phone_number VARCHAR(256),
        #     ext_type VARCHAR(256),
        #     ext_id VARCHAR(256),
        #     source VARCHAR(256),
        #     subsource VARCHAR(256),
        #     resubscribeable_action_time_epoch TIMESTAMPTZ,
        #     unsub_epoch TIMESTAMPTZ
        # );
        print(f"Dropping/inserting rejected emails")
        print(df)

        if not df.empty:
            insert = civis.io.dataframe_to_civis(
                df,
                "Warren for MA",
                "bsd.cons_insert_rejected_emails"
                if not self.dry_run
                else "bsd.cons_insert_rejected_emails_dry_run",
                existing_table_rows="truncate",
            )

            insert.result()

    # Given array of arrays like
    #   [ext_id, first_name, last_name, postal_code, phone_number, email]
    # Creates corresponding BSD constituents.
    def export_emails(self, emails, email_type, email_type_cons_group_id, limit):
        i = 0
        num_new_cons = 0
        num_resub_cons = 0
        num_rejected = 0

        # Especially for dry runs, but also to verify BSD's behavior during live
        # runs, we first query for whether each emails exists in BSD and log that
        # along with the subscription result.
        email_existence = self.get_email_existence(
            [
                e.email
                for e in emails.itertuples()
                if e.email is not None and not pd.isnull(e.email)
            ]
        )

        for email in emails.itertuples():
            if i >= limit:
                print(f"Hit limit of {limit} emails")
                break

            if i % 100 == 0:
                enable_log = True
                print(f"Exporting type {email_type}: {i} / {len(emails)}")
            else:
                enable_log = False

            is_resubscribe = email.is_resubscribe and email.is_resubscribe != "f"

            if not self.validate_email(email.email):
                if enable_log:
                    print(f"Rejecting email: {email.email}")

                num_rejected += 1
                self.rejected_emails.append(
                    [
                        email.email,
                        is_resubscribe,
                        email.first_name,
                        email.last_name,
                        email.postal_code,
                        email.addr1,
                        email.addr2,
                        email.city,
                        email.state_cd,
                        email.phone_number,
                        email.ext_type,
                        email.ext_id,
                        email.source,
                        email.subsource,
                        epoch_to_datetime(email.resubscribeable_action_time_epoch),
                        epoch_to_datetime(email.unsub_epoch),
                    ]
                )
                continue

            response_snippet = ""
            was_error = False
            if self.dry_run:
                was_new = not email_existence[email.email.strip().lower()]
                if enable_log:
                    print("Dry run")

            elif is_resubscribe:
                # Turned off for now, pending more logic around logging resubscribes
                if enable_log:
                    print("Not handling resubscribes at the moment")

                was_new = None

                # form_xml = self.compose_resubscribe_form_xml(email)
                # num_resub_cons += self.resubscribe_cons(form_xml, enable_log)
            else:
                cons_xml = self.compose_cons_xml(
                    email, email_type, email_type_cons_group_id
                )
                was_new, was_error, response_snippet = self.create_cons(
                    cons_xml, enable_log
                )

                if was_new and not was_error:
                    num_new_cons += 1

            self.detailed_logs.append(
                [
                    datetime.datetime.now(tz=datetime.timezone.utc),
                    is_resubscribe,
                    was_new,
                    email.email,
                    email.first_name,
                    email.last_name,
                    email.postal_code,
                    email.addr1,
                    email.addr2,
                    email.city,
                    email.state_cd,
                    email.phone_number,
                    email.ext_type,
                    email.ext_id,
                    email.source,
                    email.subsource,
                    epoch_to_datetime(email.resubscribeable_action_time_epoch),
                    epoch_to_datetime(email.unsub_epoch),
                    self.dry_run,
                    email_existence[email.email.strip().lower()],
                    response_snippet,
                    was_error,
                ]
            )

            i += 1
        print(
            f"Done with {email_type}. Created {num_new_cons} new cons and resubscribed {num_resub_cons} cons (out of {len(emails)} emails). Rejected {num_rejected} emails."
        )
        return num_new_cons + num_resub_cons

    # Returns emails that are in various tables in Civis but not in any BSD
    # constituent.
    # Returns array of arrays like
    #   [ext_id, first_name, last_name, postal_code, phone_number, email]
    def get_emails(self):
        try:
            df = civis.io.read_civis_sql(GET_EMAILS_SQL, self.database, use_pandas=True)
        except civis.base.EmptyResultError:
            df = pd.DataFrame()
        print("Got email rows:")
        print(df)

        return df

    # Returns XML string according to this spec:
    #   https://secure.bluestatedigital.com/page/api/doc#-------------Incoming-XML-Constituent-Records---------
    #
    # Adds to appropriate cons groups.
    def compose_cons_xml(self, email, email_type, email_type_cons_group_id):
        top = xml.etree.ElementTree.Element("api")
        cons = xml.etree.ElementTree.SubElement(top, "cons")

        if email.first_name and not pd.isnull(email.first_name):
            first_name = str(email.first_name)
        else:
            first_name = ""
        if email.last_name and not pd.isnull(email.last_name):
            last_name = str(email.last_name)
        else:
            last_name = ""
        if first_name or last_name:
            first_name, last_name = self.normalize_name(first_name, last_name)

        if first_name:
            xml.etree.ElementTree.SubElement(cons, "firstname").text = first_name
        if last_name:
            xml.etree.ElementTree.SubElement(cons, "lastname").text = last_name
        xml.etree.ElementTree.SubElement(cons, "is_banned").text = "0"
        if email.source and not pd.isnull(email.source):
            xml.etree.ElementTree.SubElement(cons, "source").text = str(email.source)
        if email.subsource and not pd.isnull(email.subsource):
            xml.etree.ElementTree.SubElement(cons, "subsource").text = str(
                email.subsource
            )

        cons_addr = xml.etree.ElementTree.SubElement(cons, "cons_addr")
        if email.addr1 and not pd.isnull(email.addr1):
            xml.etree.ElementTree.SubElement(cons_addr, "addr1").text = str(email.addr1)
        if email.addr2 and not pd.isnull(email.addr2):
            xml.etree.ElementTree.SubElement(cons_addr, "addr2").text = str(email.addr2)
        if email.city and not pd.isnull(email.city):
            xml.etree.ElementTree.SubElement(cons_addr, "city").text = str(email.city)
        if email.state_cd and not pd.isnull(email.state_cd):
            xml.etree.ElementTree.SubElement(cons_addr, "state_cd").text = str(
                email.state_cd
            )
        if email.postal_code and not pd.isnull(email.postal_code):
            xml.etree.ElementTree.SubElement(cons_addr, "zip").text = str(
                email.postal_code
            )
        xml.etree.ElementTree.SubElement(cons_addr, "country").text = "US"

        if email.phone_number and not pd.isnull(email.phone_number):
            primary_cons_phone = xml.etree.ElementTree.SubElement(cons, "cons_phone")
            xml.etree.ElementTree.SubElement(primary_cons_phone, "phone").text = str(
                email.phone_number
            )
            xml.etree.ElementTree.SubElement(
                primary_cons_phone, "phone_type"
            ).text = self.PHONE_TYPE
            xml.etree.ElementTree.SubElement(
                primary_cons_phone, "is_subscribed"
            ).text = self.PHONE_IS_SUBSCRIBED
            xml.etree.ElementTree.SubElement(
                primary_cons_phone, "is_primary"
            ).text = "1"

        primary_cons_email = xml.etree.ElementTree.SubElement(cons, "cons_email")
        xml.etree.ElementTree.SubElement(primary_cons_email, "email").text = str(
            email.email
        )
        xml.etree.ElementTree.SubElement(
            primary_cons_email, "email_type"
        ).text = self.CONS_EMAIL_EMAIL_TYPE
        xml.etree.ElementTree.SubElement(
            primary_cons_email, "is_subscribed"
        ).text = self.EMAIL_IS_SUBSCRIBED
        xml.etree.ElementTree.SubElement(primary_cons_email, "is_primary").text = "1"

        xml.etree.ElementTree.SubElement(
            cons, "cons_group", {"id": self.cons_group_email}
        )
        xml.etree.ElementTree.SubElement(
            cons, "cons_group", {"id": self.cons_group_added_via_civis_sync}
        )
        xml.etree.ElementTree.SubElement(
            cons, "cons_group", {"id": email_type_cons_group_id}
        )

        cons_field = xml.etree.ElementTree.SubElement(
            cons, "cons_field", {"id": self.custom_field_id_email_type}
        )
        xml.etree.ElementTree.SubElement(cons_field, "value").text = email_type

        cons_xml = xml.etree.ElementTree.tostring(top)

        return b'<?xml version="1.0" encoding="utf-8"?>' + cons_xml

    def create_cons(self, cons_xml, enable_log):
        if enable_log:
            print(f"create_cons({cons_xml})")

        deferred_id = None
        resp = self.bsd_api.cons_upsertConstituentData(cons_xml)

        if enable_log:
            print(f"Got response: {resp.http_status} {resp.body}")

        while resp.http_status == 202:
            if not deferred_id and resp.body:
                deferred_id = resp.body

            if enable_log:
                print(f"Making followup request with deferred id: {deferred_id}")

            time.sleep(1)
            resp = self.bsd_api.getDeferredResults(deferred_id)

        if enable_log:
            print(f"Got response: {resp.http_status} {resp.body}")

        d = xmltodict.parse(resp.body, attr_prefix="", cdata_key="value")

        # Possible responses:
        #
        # Invalid email domain:
        #
        # <api>
        #   <errors>    <error>      <message>Invalid field data, No MX record found for bpostelle@ncquariums.com</message>
        #       <field>email</field>
        #       <id type="email">bpostelle@ncquariums.com</id>
        #     </error>
        #   </errors>
        # </api>
        #
        # Cons already exists with that email:
        #
        # <api>
        #   <cons is_new="0" id="4227037">
        #     <guid>9egfEDpTVzRcLbZ8Ctph8GA</guid>
        #   </cons>
        # </api>
        #
        # New cons with that email:
        #
        # <api>
        #   <cons is_new="1" id="5816206">
        #     <guid>pXT0UVNw52hdeTw1h9AFsjA</guid>
        #   </cons>
        # </api>

        was_new = d["api"].get("cons", {}).get("is_new") == "1"
        was_error = d["api"].get("errors") != None
        body_snippet = resp.body[:500]
        return (was_new, was_error, body_snippet)

    def normalize_name(self, first_name, last_name):
        name = HumanName()
        name.first = first_name
        name.last = last_name
        name.capitalize()
        return (name.first, name.last)

    def parse_email_types_and_cons_group_ids_arg(
        self, email_types_and_cons_group_ids_arg
    ):
        """Parses email_types_and_cons_group_ids_arg argument into list of email types and cons group IDs.

        Given arg like
          'mobilizeamerica,278006,shopify,30384'
        Returns list of pairs like
          [('mobilizeamerica','278006'), ('shopify','30384')]
        """
        pieces = email_types_and_cons_group_ids_arg.split(",")
        return zip(pieces[0::2], pieces[1::2])

    def get_email_existence(self, emails):
        existence = defaultdict(bool)

        # We have to cram all the emails into the URL, so we break it
        # into chunks or BSD will error on the URL being to long
        for email_chunk in in_groups_of(100, emails):
            # The BSD python API doesn't provide access to
            # get_constituents_by_email, so we use _generateRequest
            # to manually generate the URL for the request (so we
            # can use the python API's hmac code)
            bsd_url = self.bsd_api._generateRequest(
                "/cons/get_constituents_by_email",
                {
                    "emails": ",".join(email_chunk),
                    "deferred": "1",
                    "bundles": "cons_email",
                },
            )
            resp = self.bsd_api._makeGETRequest(bsd_url)

            deferred_id = None
            while resp.http_status == 202:
                if not deferred_id and resp.body:
                    deferred_id = resp.body

                time.sleep(1)
                resp = self.bsd_api.getDeferredResults(deferred_id)

            d = xmltodict.parse(resp.body, attr_prefix="", cdata_key="value")

            api_res = d["api"]
            if api_res is None:
                # No emails in this batch
                continue

            cons_records = api_res.get("cons", [])
            if not isinstance(cons_records, list):
                cons_records = [cons_records]

            for cons_record in cons_records:
                cons_emails = cons_record.get("cons_email", [])
                if not isinstance(cons_emails, list):
                    cons_emails = [cons_emails]

                for cons_email in cons_emails:
                    existence[cons_email["email"].strip().lower()] = True

        return existence

    def validate_email(self, email):
        try:
            metadata = validate_email(email, check_deliverability=False)
        except EmailNotValidError:
            return False

        domain = metadata["domain"]

        if domain not in self.domain_deliverability_cache:
            # Check domain deliverability
            try:
                metadata = validate_email(email, check_deliverability=True)

                # We *could* also reject domains that are deliverable but have an A
                # record rather than an MX record. When we manually reviewed these, we
                # found that many were undeliverable domains but a fair number were also
                # legitimate domains for small businesses that just use A record for their
                # mail config.
                #
                # if metadata.get("mx-fallback"):
                #     print(f"Rejecting email {email} due to valid domain but missing MX")
                #     self.domain_deliverability_cache[domain] = False

                self.domain_deliverability_cache[domain] = True
            except EmailNotValidError:
                self.domain_deliverability_cache[domain] = False

        return self.domain_deliverability_cache[domain]
