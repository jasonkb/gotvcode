import datetime

from pynamodb.attributes import UnicodeAttribute, UTCDateTimeAttribute
from pynamodb.models import Model


class GenericKV(Model):
    class Meta:
        table_name = "GenericKV"

    k = UnicodeAttribute(hash_key=True)
    v = UnicodeAttribute(null=True)
    created_at = UTCDateTimeAttribute()
    updated_at = UTCDateTimeAttribute()

    @staticmethod
    def setup():
        if not GenericKV.exists():
            GenericKV.create_table(
                read_capacity_units=1, write_capacity_units=1, wait=True
            )

    @staticmethod
    def get_or_create(k, consistent_read=True):
        if not k:
            raise ValueError("No key specified.")
        try:
            generic_kv = GenericKV.get(k, consistent_read=consistent_read)
        except GenericKV.DoesNotExist:
            generic_kv = GenericKV(k)
            generic_kv.set_created_at()

        return generic_kv

    @staticmethod
    def put_value(k, v):
        generic_kv = GenericKV.get_or_create(k)
        generic_kv.v = v
        generic_kv.set_updated_at()
        return generic_kv.save()

    def set_created_at(self):
        self.created_at = datetime.datetime.utcnow()

    def set_updated_at(self):
        self.updated_at = datetime.datetime.utcnow()
