import time
import xml.etree.ElementTree

from bsdapi.BsdApi import Factory as BsdApiFactory
import pandas as pd
import civis

class BsdUpdateCustomFields:
    def __init__(
            self,
            database,
            schema,
            bsd_host,
            bsd_api_id,
            bsd_secret,
            custom_fields,
            custom_field_bsd_ids,
            start,
            limit,
    ):
        self.database = database
        self.schema = schema
        self.bsd_api = BsdApiFactory().create(
            api_id=bsd_api_id,
            secret=bsd_secret,
            host=bsd_host,
            port=80,
            securePort=443
        )

        self.limit = limit
        self.custom_fields = custom_fields
        self.custom_field_bsd_ids = custom_field_bsd_ids
        self.start = start
        assert len(self.custom_fields) == len(self.custom_field_bsd_ids)

    def run(self):
        df = self.get_custom_fields_to_update()
        i = 0
        rows = []
        print(f'Will iterate over {len(df)} rows.')
        for row in df.itertuples(index=False):
            rows.append(row)
            i += 1
            if i % 100 == 0:
                cons_xml = self.compose_cons_xml(rows)
                self.update_cons(cons_xml)
                rows = []
                print(f'Row number {i} is {row}')
                if self.limit and i >= self.limit:
                    print(f'Hit limit of {self.limit} cons')
                    break
        if rows:
            cons_xml = self.compose_cons_xml(rows)
            self.update_cons(cons_xml)
        print('Done.')

    def prepare_query(self):
        query = f"SELECT * FROM {self.schema}.custom_fields_to_update_in_bsd"
        if self.start:
            query += f" WHERE donation_last_dt > '{self.start}'"
        query += f" ORDER BY cons_id"
        query += ";"
        return query

    def get_custom_fields_to_update(self):
        sql = self.prepare_query()
        print(f'Querying: {sql}');
        df = civis.io.read_civis_sql(sql, self.database, use_pandas=True, polling_interval=0.35)

        return df

    def update_cons(self, cons_xml):
        # For sanity checking:
        #print(f'update_cons({cons_xml})')

        deferred_id = None
        resp = self.bsd_api.cons_upsertConstituentData(cons_xml)

        # We don't wait around for the deferred result to finish.

    def compose_cons_xml(self, rows):
        top = xml.etree.ElementTree.Element('api')
        for row in rows:
            cons = xml.etree.ElementTree.SubElement(top, 'cons', id=str(row.cons_id))
            for custom_field, custom_field_bsd_id in zip(self.custom_fields, self.custom_field_bsd_ids):
                cons_field = xml.etree.ElementTree.SubElement(cons, 'cons_field', id=str(custom_field_bsd_id))

                v = getattr(row, custom_field)
                if v and not pd.isnull(v):
                    # All numbers come out as floats from the dataframe; if we
                    # take their str() representation as-is, we'll upload e.g.
                    # 24.0 for donation_num_gifts_wfp, which should be an
                    # integer. So we'll avoid uploading explicit decimals where
                    # the value is a whole number.
                    if isinstance(v, float) and abs(round(v) - v) < 0.000001:
                        v = round(v)
                    xml.etree.ElementTree.SubElement(cons_field, 'value').text = str(v)
                else:
                    # If we set no 'value', BSD will set value to empty.
                    pass


        cons_xml = xml.etree.ElementTree.tostring(top)

        return b'<?xml version="1.0" encoding="utf-8"?>' + cons_xml
