import os

import pandas as pd
import civis

from mobilecommons.client import MobileCommonsAPI

# Update fundraising-related custom fields in Mobile Commons profiles.
class MobileCommonsUpdateCustomFields:
    def __init__(
            self,
            database,
            schema,
            api_username,
            api_password,
            custom_fields,
            custom_fields_ints_on_mobile_commons,
            custom_fields_yesno_on_mobile_commons,
            custom_fields_dates_on_mobile_commons,
            custom_fields_nulls_on_mobile_commons,
            start,
            update_order,
            limit,
    ):
        self.database = database
        self.schema = schema
        self.mobilecommons_api = MobileCommonsAPI(api_username, api_password, schema, database)
        self.limit = limit
        self.custom_fields = custom_fields
        self.custom_fields_ints_on_mobile_commons = custom_fields_ints_on_mobile_commons
        self.custom_fields_yesno_on_mobile_commons = custom_fields_yesno_on_mobile_commons
        self.custom_fields_dates_on_mobile_commons = custom_fields_dates_on_mobile_commons
        self.custom_fields_nulls_on_mobile_commons = custom_fields_nulls_on_mobile_commons
        self.start = start
        self.update_order = update_order

    # Given DataFrame of profiles creates corresponding Mobile Commons profiles.
    def update_profiles(self):
        print(f'update_profiles()')
        df = self.get_custom_fields_to_update()
        print(f'Will update {len(df)} profiles.')
        i = 0
        for row in df.itertuples():
            if self.limit and i >= self.limit:
                print(f'Hit limit of {self.limit} rows')
                break
            try:
                profile_payload = self.profile_payload(row, self.custom_fields)
            except Exception as e:
                profile_payload = None
                print('Exception while preparing profile payload:')
                print(e)
                print(f'Row was: {row}')
                raise e
            if profile_payload:
                try:
                    self.mobilecommons_api.create_or_update_profile(profile_payload)
                except Exception as e:
                    if 'Phone is not textable' in str(e):
                        # This is mostly landline numbers, we don't need to see that error message.
                        pass
                    else:
                        print(e)
            i += 1
            if i % 100 == 0:
                print(f'Row #{i} was {row}')
        print('Done.')

    def prepare_query(self):
        query = f"SELECT * FROM {self.schema}.custom_fields_to_update_in_mobilecommons"
        if self.start:
            query += f" WHERE donation_last_dt > '{self.start}'"
        query += f" ORDER BY donation_last_dt {self.update_order}"
        query += ";"
        return query

    # Returns emails that are in various tables in Civis but not in any BSD
    # constituent.
    # Returns array of arrays like
    #   [user_id, first_name, last_name, postal_code, phone_number, email]
    def get_custom_fields_to_update(self):
        sql = self.prepare_query()
        print(f'Querying: {sql}');
        df = civis.io.read_civis_sql(sql, self.database, use_pandas=True)

        return df


    # Returns POST payload according to
    #   https://community.uplandsoftware.com/hc/en-us/articles/204494185-REST-API#ProfileUpdate
    def profile_payload(self, row, custom_fields):
        payload = {
            'phone_number': f'1{row.phone_number}',
        }

        for custom_field in custom_fields:
            if not hasattr(row, custom_field):
                print(f'Custom field {custom_field} not in row {row}')
                continue
            v = getattr(row, custom_field)
            if pd.isnull(v) or v == '' or v is None:
                continue
            if custom_field in self.custom_fields_ints_on_mobile_commons:
                v = round(v)
            if custom_field in self.custom_fields_yesno_on_mobile_commons:
                if v in (0, '0', 'No'):
                    v = 'No'
                elif v in (1, '1', 'Yes'):
                    v = 'Yes'
            payload[custom_field] = v

        return payload
