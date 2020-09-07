import datetime

from pynamodb.attributes import UnicodeAttribute, UTCDateTimeAttribute
from pynamodb.models import Model

from ew_common.input_validation import extract_phone_number


class ChatProfile(Model):
    class Meta:
        table_name = "ChatProfile"

    phone_number = UnicodeAttribute(hash_key=True)
    email = UnicodeAttribute(null=True)
    first_and_last_name = UnicodeAttribute(null=True)
    first_name = UnicodeAttribute(null=True)
    last_name = UnicodeAttribute(null=True)
    last_state = UnicodeAttribute(null=True)
    postal_code = UnicodeAttribute(null=True)
    refer_personal_reason = UnicodeAttribute(null=True)
    refer_referee_first_and_last_name = UnicodeAttribute(null=True)
    refer_referee_first_name = UnicodeAttribute(null=True)
    refer_referee_last_name = UnicodeAttribute(null=True)
    refer_referee_phone_number = UnicodeAttribute(null=True)
    refer_referee_city = UnicodeAttribute(null=True)
    refer_referee_state = UnicodeAttribute(null=True)
    created_at = UTCDateTimeAttribute()
    updated_at = UTCDateTimeAttribute()

    @staticmethod
    def setup():
        if not ChatProfile.exists():
            ChatProfile.create_table(
                read_capacity_units=1, write_capacity_units=1, wait=True
            )

    @staticmethod
    def get_or_create_profile(phone_number_input):
        phone_number = extract_phone_number(phone_number_input)
        if not phone_number:
            raise ValueError(
                "Invalid phone number in get_or_create_profile: {phone_number_input}"
            )
        try:
            chat_state = ChatProfile.get(phone_number)
        except ChatProfile.DoesNotExist:
            chat_state = ChatProfile(phone_number)
            chat_state.set_created_at()

        return chat_state

    def set_created_at(self):
        self.created_at = datetime.datetime.utcnow()

    def set_updated_at(self):
        self.updated_at = datetime.datetime.utcnow()
