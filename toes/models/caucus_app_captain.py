from pynamodb.attributes import UnicodeAttribute
from pynamodb.models import Model

from common.settings import settings


class CaucusAppCaptain(Model):
    class Meta:
        table_name = f"caucus-app-captain-{settings.infrastructure or 'local'}"

    phone_number = UnicodeAttribute(hash_key=True)
    precinct_id = UnicodeAttribute(null=False)

    @staticmethod
    def setup():
        if not CaucusAppCaptain.exists():
            # Note: pynamodb doesn't appear to support setting on-demand scaling
            # through this API, so we set it through the AWS console after creating
            # the table.
            CaucusAppCaptain.create_table(
                wait=True, read_capacity_units=1, write_capacity_units=1
            )
