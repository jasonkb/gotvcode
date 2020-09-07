import base64
import datetime
import hashlib
import random
import string
from enum import Enum

from pynamodb.attributes import (
    ListAttribute,
    NumberAttribute,
    UnicodeAttribute,
    UnicodeSetAttribute,
    UTCDateTimeAttribute,
)
from pynamodb.models import Model


class Donor(Model):
    class BadgeId(Enum):
        EOM = "eom"
        EOQ = "eoq"
        RECURRING = "recurring"
        MERCH = "merch"
        DEBATE = "debate"
        HQWALL = "hqwall"
        FOUNDING = "founding"

    class Meta:
        # Must set table_name before using this model.

        # Fake provisioned capacity only required to workaround pynamodb so we
        # can create table during tests.
        read_capacity_units = 1
        write_capacity_units = 1

    email = UnicodeAttribute(hash_key=True)
    donor_id = UnicodeAttribute(null=True)
    first_name = UnicodeAttribute(null=True)
    last_name = UnicodeAttribute(null=True)
    city = UnicodeAttribute(null=True)
    state = UnicodeAttribute(null=True)
    zip = UnicodeAttribute(null=True)
    last_donation_dt = UnicodeAttribute(null=True)
    last_donation_ts = UTCDateTimeAttribute(null=True)
    last_donation_amount = NumberAttribute(null=True)
    last_donation_type = UnicodeAttribute(null=True)
    total_donation_amount = NumberAttribute(default=0)
    badges = UnicodeSetAttribute(default=set)
    created_at = UTCDateTimeAttribute()
    updated_at = UTCDateTimeAttribute()

    @staticmethod
    def setup():
        if not Donor.exists():
            Donor.create_table(wait=True)

    @staticmethod
    def get_or_create_donor(email):
        if not email:
            raise ValueError("Invalid email in get_or_create_donor: {email}")
        try:
            donor = Donor.get(email)
            print(f"Got existing donor: {donor}")
        except Donor.DoesNotExist:
            donor = Donor(email)
            donor.set_created_at()

        return donor

    DONOR_ID_LENGTH = 12

    @staticmethod
    def compute_donor_id(email, salt):
        salted_email = salt + email
        md5_digest = hashlib.md5(bytes(salted_email, encoding="utf-8")).digest()
        base64_digest = base64.urlsafe_b64encode(md5_digest).decode("utf-8")
        return base64_digest[: Donor.DONOR_ID_LENGTH]

    def set_donor_id(self, salt):
        """Sets self.donor_id to a salted hash of self.email."""
        self.donor_id = Donor.compute_donor_id(self.email, salt)

    def set_created_at(self):
        now = datetime.datetime.utcnow()
        self.created_at = now
        self.updated_at = now

    def set_updated_at(self):
        self.updated_at = datetime.datetime.utcnow()
