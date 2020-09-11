import os
from collections import defaultdict
from typing import DefaultDict, Set

import pandas
import jinja2

from integrations.civis import CivisDatabase

from .user import User, has_value

USER_JOIN_TABLE = os.environ.get(
    "USER_JOIN_TABLE", "bsd_mc_group.tmp_bsd_mc_group_users"
)


class UserJoiner:
    """
    UserJoiner is responsible for taking users (that have emails and/or phone numbers)
    and finding their BSD cons ID and MobileCommon profile
    """

    def __init__(self):
        self.bsd_cons_ids: DefaultDict[int, Set[str]] = defaultdict(set)
        self.mc_phone_numbers: DefaultDict[int, Set[str]] = defaultdict(set)

        self.jinja_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(os.path.dirname(__file__))
        )

    async def join_users(self, users: Set[User], db: CivisDatabase):
        # Create a table in Civis that has all of the users we want to join
        df = pandas.DataFrame(
            [
                {"id": user.id, "email": user.email, "phone": user.phone}
                for user in users
            ]
        )[["id", "email", "phone"]]

        print(df)

        await db.dataframe_to_civis(
            df=df, table=USER_JOIN_TABLE, existing_table_rows="drop"
        )

        # Join that table against MC and BSD users
        sql = self.jinja_env.get_template("join_users.sql").render(
            env={"USER_JOIN_TABLE": USER_JOIN_TABLE}
        )
        result_df = await db.read_civis_sql(sql)

        # Map user IDs to cons/phone numbers
        for p in result_df.itertuples(index=False):
            if has_value(p.cons_id):
                self.bsd_cons_ids[int(p.id)].add(p.cons_id)

            if has_value(p.phone):
                self.mc_phone_numbers[int(p.id)].add(p.phone)

    def phones_for(self, user: User) -> Set[str]:
        return self.mc_phone_numbers[user.id]

    def cons_ids_for(self, user: User) -> Set[str]:
        return self.bsd_cons_ids[user.id]
