from collections import defaultdict
import datetime
import os

from nameparser import HumanName
import pandas as pd
import civis
import jinja2

from mobilecommons.client import MobileCommonsAPI

# For phone numbers which we are sure have recent SMS opt-in, exports profiles
# from Civis to Mobile Commons using the Mobile Commons API.
#
# Currently exports only from new ActBlue donors.
class MobileCommonsExport:
    # We'll abort if we would be sending messages outside these hours.
    # i.e we'll only send messages after 11am and before 8pm.
    # The daily job in Civis runs every noontime so messages *should* go out
    # between noon-1pm Eastern.
    earliest_send_hour_utc = 15  # 11am EDT
    latest_send_hour_utc = 24  # 8pm EDT

    def __init__(
            self,
            database,
            schema,
            api_username,
            api_password,
            opt_in_path_ids_arg,
            limit_per_type,
    ):
        self.database = database
        self.schema = schema
        self.mobilecommons_api = MobileCommonsAPI(api_username, api_password, schema, database)
        self.opt_in_path_ids = self.parse_opt_in_path_ids_arg(opt_in_path_ids_arg)
        self.limit_per_type = limit_per_type

        self.read_query_sql()

    def parse_opt_in_path_ids_arg(self, opt_in_path_ids_arg):
        """Parses opt_in_path_ids argument into list of opt in paths and IDs.

        Given arg like
          'bsd_contribution,279022,mobilizeamerica,278006'
        Returns list of pairs like
          [('bsd_contribution','279022'), ('mobilizeamerica','278006')]
        """
        pieces = opt_in_path_ids_arg.split(',')
        return zip(pieces[0::2], pieces[1::2])

    def read_query_sql(self):
        jinja_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(os.path.dirname(__file__)),
        )
        self.phone_numbers_to_export_query = jinja_env.get_template('phone_numbers_to_export.sql').render()

    def allowed_sending_time(self):
        hour_utc = datetime.datetime.utcnow().time().hour
        return hour_utc >= self.earliest_send_hour_utc and hour_utc < self.latest_send_hour_utc


    # Computes emails that are in various tables in Civis but not in any BSD
    # constituent. Creates BSD constituents for such email addresses.
    def sync_profiles(self):
        if self.limit_per_type != 0 and not self.allowed_sending_time():
            print('It is outside of allowed sending times (3pm-midnight UTC). Aborting sync.')
            return
        df = self.get_profiles()
        counts_by_type = defaultdict(int)
        for profile in df.itertuples():
            counts_by_type[profile.action_type] += 1
        print('Counts by type:', counts_by_type)
        for action_type, opt_in_path_id in self.opt_in_path_ids:
            profiles_of_type = df[df['action_type'] == action_type]
            print(f'Exporting to Mobile Commons {len(profiles_of_type)} profiles from {action_type}')
            self.export_profiles(profiles_of_type, opt_in_path_id, self.limit_per_type)


    # Given DataFrame of profiles creates corresponding Mobile Commons profiles.
    def export_profiles(self, df, opt_in_path_id, limit):
        i = 0
        for profile in df.itertuples():
            if limit and i >= limit:
                print(f'Hit limit of {limit} profiles')
                break
            if not self.allowed_sending_time():
                print('It is outside of allowed sending times (3pm-midnight UTC). Aborting profile creation.')
                break

            try:
                profile_payload = self.profile_payload(profile, opt_in_path_id)
            except Exception as e:
                profile_payload = None
                print('Exception while preparing profile payload:')
                print(e)
                print(f'Profile was: {profile}')
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
        print('Done.')


    # Returns emails that are in various tables in Civis but not in any BSD
    # constituent.
    # Returns array of arrays like
    #   [user_id, first_name, last_name, postal_code, phone_number, email]
    def get_profiles(self):
        sql = self.phone_numbers_to_export_query
        print(f'Querying: {sql}');
        df = civis.io.read_civis_sql(sql, self.database, use_pandas=True)

        return df


    # Returns POST payload according to
    #   https://community.uplandsoftware.com/hc/en-us/articles/204494185-REST-API#ProfileUpdate
    def profile_payload(self, profile, opt_in_path_id):
        first_name = profile.first_name
        last_name = profile.last_name
        if pd.isnull(first_name):
            first_name = ''
        if pd.isnull(last_name):
            last_name = ''

        postal_code = profile.postal_code
        if pd.isnull(postal_code):
            postal_code = ''
        if not isinstance(postal_code, str):
            # The dataframe sometimes turns e.g. 02145 into 2145.
            # Or hallucinates floats... :-/
            postal_code = str(int(postal_code)).zfill(5)

        first_name, last_name = self.normalize_name(str(first_name), str(last_name))
        payload = {
            'phone_number': profile.phone_number,
            'email': profile.email,
            'postal_code': postal_code,
            'first_name': first_name,
            'last_name': last_name,
            'street1': profile.addr1,
            'street2': profile.addr2,
            'city': profile.city,
            'state': profile.state,
            'country': profile.country,
            'opt_in_path_id': opt_in_path_id,
        }

        # Don't upload null or empty fields.
        keys_to_delete = [k for k, v in payload.items() if not v or pd.isnull(v)]
        for k in keys_to_delete:
            del payload[k]

        return payload

    def normalize_name(self, first_name, last_name):
        name = HumanName()
        name.first = first_name
        name.last = last_name
        name.capitalize()
        return (name.first, name.last)
