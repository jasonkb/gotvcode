import datetime

from pynamodb.attributes import UnicodeAttribute, UTCDateTimeAttribute
from pynamodb.models import Model

from ew_common.input_validation import extract_phone_number


class ChatReferral(Model):
    class Meta:
        table_name = "ChatReferral"

    phone_number = UnicodeAttribute(hash_key=True)
    referrer_phone_number = UnicodeAttribute(range_key=True)

    first_and_last_name = UnicodeAttribute(null=True)
    first_name = UnicodeAttribute(null=True)
    last_name = UnicodeAttribute(null=True)
    city = UnicodeAttribute(null=True)
    state = UnicodeAttribute(null=True)

    referrer_first_and_last_name = UnicodeAttribute(null=True)
    referrer_first_name = UnicodeAttribute(null=True)
    referrer_last_name = UnicodeAttribute(null=True)
    referrer_postal_code = UnicodeAttribute(null=True)
    referrer_email = UnicodeAttribute(null=True)

    created_at = UTCDateTimeAttribute()

    @staticmethod
    def setup():
        if not ChatReferral.exists():
            ChatReferral.create_table(
                read_capacity_units=1, write_capacity_units=1, wait=True
            )

    @staticmethod
    def get_or_create_referral(phone_number_input, referrer_phone_number_input):
        phone_number = extract_phone_number(phone_number_input)
        if not phone_number:
            raise ValueError(
                f"Invalid phone number in get_or_create_profile: {phone_number_input}"
            )
        referrer_phone_number = extract_phone_number(referrer_phone_number_input)
        if not referrer_phone_number:
            raise ValueError(
                f"Invalid phone number in get_or_create_profile: {referrer_phone_number_input}"
            )
        try:
            chat_referral = ChatReferral.get(phone_number, referrer_phone_number)
        except ChatReferral.DoesNotExist:
            chat_referral = ChatReferral(phone_number, referrer_phone_number)
            chat_referral.set_created_at()

        return chat_referral

    def set_created_at(self):
        self.created_at = datetime.datetime.utcnow()
