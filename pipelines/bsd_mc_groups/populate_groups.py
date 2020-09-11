import asyncio
import os
from datetime import datetime, timezone

import aiohttp
import jinja2
from dateutil import parser as dateparser
import pandas

from integrations.bsd.api import BsdApi
from integrations.mobilecommons.client import MobileCommonsAPIClient

from .groups import (
    ACTION_TYPES,
    EVENT_HOSTS,
    EVENT_HOSTS_GROUP,
    IA_CAUCUS_ORG_GROUPS_MC,
    IA_CAUCUS_ORG_GROUP_NAMES,
    NEXT_WEEK_CUTOFF,
    NEXT_WEEK_RSVPS,
    PAST_ATTENDEES,
    PAST_ATTENDEES_GROUP,
    PAST_RSVPS,
    PAST_RSVPS_GROUP,
    PAST_RSVPS_TYPES,
    EVENT_HOST_TYPES,
    NEXT_THREE_DAYS_RSVPS,
    NEXT_THREE_DAYS_CUTOFF,
    EARLY_STATE_RSVPS,
    PAST_NGP_VAN_SIGNUPS,
)
from .sync_groups import SyncGroups
from .user import find_or_create_user

# Just print out what groups would be created/updated and how many
# items would be added to each group; don't actually do it.
#
# WARNING!
# BSD cons groups will still be created if they don't exist, but no
# cons will be added to them.
SIMULATE_ONLY = False


class PopulateGroups:
    def __init__(
        self,
        civis,
        bsd_host,
        bsd_api_id,
        bsd_secret,
        mc_username,
        mc_password,
        bsd_only,
    ):
        self.civis = civis
        self.bsd_api = BsdApi(bsd_api_id, bsd_secret, bsd_host, 80, 443)
        self.mc_api = MobileCommonsAPIClient(mc_username, mc_password)
        self.bsd_only = bsd_only

        self.jinja_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(os.path.dirname(__file__))
        )

        self.groups = SyncGroups()

        # We configure the mobilecommon ID later (because it requires hitting the
        # mobilecommons API to create the new group), but we sync to BSD before
        # we sync to mobilecommons so we have to do this initial configuration here
        # to avoid syncing this group to BSD.
        self.groups.configure_group(NEXT_WEEK_RSVPS, sync_to_bsd=False)

        self.groups.configure_group(PAST_RSVPS, mobilecommons_group_id=PAST_RSVPS_GROUP)
        self.groups.configure_group(
            PAST_ATTENDEES,
            sync_to_bsd=False,
            mobilecommons_group_id=PAST_ATTENDEES_GROUP,
        )
        self.groups.configure_group(
            EVENT_HOSTS, mobilecommons_group_id=EVENT_HOSTS_GROUP
        )

        for name, mc_id in PAST_RSVPS_TYPES.items():
            self.groups.configure_group(name, mobilecommons_group_id=mc_id)

        for name, mc_id in EVENT_HOST_TYPES.items():
            self.groups.configure_group(name, mobilecommons_group_id=mc_id)

        for name, mc_id in ACTION_TYPES.items():
            self.groups.configure_group(f"Action {name}", mobilecommons_group_id=mc_id)

        for name, mc_id in PAST_NGP_VAN_SIGNUPS.items():
            self.groups.configure_group(name, mobilecommons_group_id=mc_id)

        for name, mc_id in IA_CAUCUS_ORG_GROUPS_MC.items():
            self.groups.configure_group(
                name, mobilecommons_group_id=mc_id, sync_to_bsd=False
            )

    async def run(self, ds, limit):
        async with aiohttp.ClientSession(loop=asyncio.get_event_loop()) as session:
            await self.run_with_session(session, ds, limit)

    async def run_with_session(self, session, ds, limit):
        print("Loading upcoming mobilize america attendees")
        await self.populate_upcoming_mobilizeamerica_groups(session, ds)

        print("Loading past mobilize america attendees")
        await self.populate_past_mobilizeamerica_groups(session, ds)

        print("Loading mobilize america hosts")
        await self.populate_mobilizeamerica_hosts()

        print("Loading upcoming NGP/VAN attendees")
        await self.populate_upcoming_ngpvan_attendees(ds)

        print("Loading past NGP/VAN attendees")
        await self.populate_past_ngpvan_attendees(ds)

        print("Loading actions")
        await self.populate_user_actions()

        print("Loading IA caucus organizing groups")
        await self.populate_ia_caucus_organizing_groups()

        print("Joining against MC profiles and BSD cons")
        joiner = await self.groups.get_joiner(self.civis)

        if self.bsd_only:
            created_mc_groups = []
        else:
            print("Syncing to Mobile Commons")
            created_mc_groups = await self.create_mobilecommons_groups(joiner, limit)

        print("Syncing to BSD")
        created_bsd_groups = await self.create_bsd_groups(session, joiner, limit)

        print("Updating master group lists")
        await self.update_group_lists(created_bsd_groups, created_mc_groups)

    async def populate_upcoming_mobilizeamerica_groups(self, session, ds):
        """Creates one group per upcoming Mobilize America event.

        Each group is named for the ID and location of the event and
        contains the constituents who are RSVP'ed to the event.
        """
        query_template = self.jinja_env.get_template(
            "upcoming_mobilize_america_rsvps.sql"
        )
        sql = query_template.render(env={"DS": ds})
        df = await self.civis.read_civis_sql(sql)

        print("Got upcoming event RSVPs:")
        print(df)

        for p in df.itertuples(index=False):
            user = find_or_create_user(email=p.email, phone=p.phone)

            # Add the upcoming RSVP to the dynamic BSD group created per-event
            cons_group_name = (
                f"Mobilize America RSVPs {p.event_id} {p.event_city} {p.event_state}"
            )
            self.groups.add_user(cons_group_name, user)

            # If it's in the next week, add it to the MC group for next-week RSVPs
            if dateparser.parse(p.start_timestamp) < NEXT_WEEK_CUTOFF:
                self.groups.add_user(NEXT_WEEK_RSVPS, user)

            # If it's in the next three days, add it to the BSD group for next-three-days RSVPs
            if dateparser.parse(p.start_timestamp) < NEXT_THREE_DAYS_CUTOFF:
                self.groups.add_user(NEXT_THREE_DAYS_RSVPS, user)

                # Break out early states as well
                if p.event_state in EARLY_STATE_RSVPS:
                    self.groups.add_user(EARLY_STATE_RSVPS[p.event_state], user)

    async def populate_past_mobilizeamerica_groups(self, session, ds):
        """Creates one groups per Mobilize America event type.

        Each group is named for the event type and contains all
        attendees who have ever attended an event of that type. Also
        creates one  group with all attendees who have ever
        attended an event of any type.
        """
        query_template = self.jinja_env.get_template("past_mobilize_america_rsvps.sql")
        sql = query_template.render(env={"DS": ds})
        df = await self.civis.read_civis_sql(sql)

        print("Got past event RSVPs:")
        print(df)

        for p in df.itertuples(index=False):
            user = find_or_create_user(email=p.email, phone=p.phone)

            group_name = f"Past Mobilize America RSVPs {p.event_type}"

            self.groups.add_user(PAST_RSVPS, user)
            self.groups.add_user(group_name, user)

            if p.attended == "1":
                self.groups.add_user(PAST_ATTENDEES, user)

    async def populate_mobilizeamerica_hosts(self):
        """
        Creates a group for all Mobilize America event hosts
        """
        query_template = self.jinja_env.get_template("mobilize_america_hosts.sql")
        sql = query_template.render()
        df = await self.civis.read_civis_sql(sql)

        print("Got event hosts:")
        print(df)

        for p in df.itertuples(index=False):
            user = find_or_create_user(email=p.email, phone=p.phone)
            self.groups.add_user(EVENT_HOSTS, user)
            self.groups.add_user(f"Mobilize America Event Hosts {p.event_type}", user)

    async def populate_upcoming_ngpvan_attendees(self, ds):
        """
        Create groups for upcoming NGP and VAN sign-ups
        """

        query_template = self.jinja_env.get_template("upcoming_ngp_van_signups.sql")
        sql = query_template.render(env={"DS": ds})
        df = await self.civis.read_civis_sql(sql)

        print("Got upcoming NGP/VAN Signups:")
        print(df)

        for p in df.itertuples(index=False):
            user = find_or_create_user(email=p.email, phone=p.phone)

            # If it's in the next week, add it to the MC group for next-week RSVPs
            if dateparser.parse(p.start_timestamp) < NEXT_WEEK_CUTOFF:
                self.groups.add_user(NEXT_WEEK_RSVPS, user)

            # If it's in the next three days, add it to the BSD group for next-three-days RSVPs
            if dateparser.parse(p.start_timestamp) < NEXT_THREE_DAYS_CUTOFF:
                self.groups.add_user(NEXT_THREE_DAYS_RSVPS, user)

                # Break out early states as well
                if p.event_state in EARLY_STATE_RSVPS:
                    self.groups.add_user(EARLY_STATE_RSVPS[p.event_state], user)

    async def populate_past_ngpvan_attendees(self, ds):
        """
        Create groups for past NGP and VAN sign-ups
        """

        query_template = self.jinja_env.get_template("past_ngp_van_signups.sql")
        sql = query_template.render(env={"DS": ds})
        df = await self.civis.read_civis_sql(sql)

        print("Got past NGP/VAN Signups:")
        print(df)

        for p in df.itertuples(index=False):
            all_group = f"Past {p.ngp_or_van} Signups"
            type_group = f"Past {p.ngp_or_van} Signups {p.event_type.upper()}"

            # Check that it's in the map -- warn if there's a new signup type that we
            # haven't created a Mobile Commons group for yet.
            for group_name in (all_group, type_group):
                if group_name not in PAST_NGP_VAN_SIGNUPS:
                    print(
                        f"Warning: {group_name} does not have a Mobile Commons group ID; will be synced to BSD only"
                    )

            user = find_or_create_user(email=p.email, phone=p.phone)
            self.groups.add_user(all_group, user)
            self.groups.add_user(type_group, user)

    async def populate_user_actions(self):
        """
        Creates a group for each action type with all the users who have ever taken that action
        """
        query_template = self.jinja_env.get_template("user_actions.sql")
        sql = query_template.render()
        df = await self.civis.read_civis_sql(sql)

        print("Got actions:")
        print(df)

        for p in df.itertuples(index=False):
            if p.action in ACTION_TYPES:
                user = find_or_create_user(email=p.email, phone=p.phone)
                self.groups.add_user(f"Action {p.action}", user)

    async def populate_ia_caucus_organizing_groups(self):
        """
        Creates groups for specific IA caucus organizing groups
        based on VAN survey questions
        """
        query_template = self.jinja_env.get_template("ia_caucus_org_groups.sql")
        sql = query_template.render()
        df = await self.civis.read_civis_sql(sql)

        print("Got IA caucus org groups:")
        print(df)

        for p in df.itertuples(index=False):
            user = find_or_create_user(email=p.email, phone=p.phone)
            group_name = IA_CAUCUS_ORG_GROUP_NAMES[
                (int(p.survey_question_id), int(p.survey_response_id))
            ]

            self.groups.add_user(group_name, user)

    async def create_bsd_groups(self, session, joiner, limit):
        created_groups = []
        i = 0
        for cons_group_name, cons_ids in self.groups.bsd_sync_data(joiner).items():
            created_groups.append(
                await self.create_bsd_group(session, cons_group_name, cons_ids)
            )
            i += 1
            if i >= limit:
                break

        print(f"Created {i} cons groups.")
        return created_groups

    async def create_bsd_group(self, session, cons_group_name, cons_ids):
        cons_group_id = await self.bsd_api.createConsGroup(session, cons_group_name)
        print(
            f"Putting {len(cons_ids)} cons into {cons_group_name} ({cons_group_id}): {', '.join(cons_ids[0:3])}..."
        )

        if SIMULATE_ONLY:
            old_cons_ids = set(
                await self.bsd_api.getConsIdsForGroup(session, cons_group_id)
            )
            new_cons_ids = set(cons_ids)

            print(
                f"  -> Add {len(new_cons_ids - old_cons_ids)}; Remove {len(old_cons_ids - new_cons_ids)}; Old Count {len(old_cons_ids)}; New Count {len(new_cons_ids)}"
            )
        else:
            await self.bsd_api.setConsIdsForGroup(session, cons_group_id, cons_ids)

        return (cons_group_name, cons_group_id)

    async def create_mobilecommons_groups(self, joiner, limit):
        created_groups = []

        async with self.mc_api.session_context():
            # First, create the daily upcoming RSVP group
            if SIMULATE_ONLY:
                upcoming_rsvp_group_id = -1
            else:
                upcoming_rsvp_group_id = await self.mc_api.create_group(NEXT_WEEK_RSVPS)

            print(f"Created upcoming RSVP group {upcoming_rsvp_group_id}")

            self.groups.configure_group(
                NEXT_WEEK_RSVPS,
                mobilecommons_group_id=upcoming_rsvp_group_id,
                sync_to_bsd=False,
            )

            # Now, update all the groups
            i = 0
            for mc_group_id, phones in self.groups.mobilecommons_sync_data(
                joiner
            ).items():
                mc_group_name = self.groups.mobilecommons_group_name(mc_group_id)
                print(
                    f"Putting {len(phones)} phones into MC group {mc_group_name} ({mc_group_id}): {', '.join(phones[0:3])}..."
                )

                if not SIMULATE_ONLY:
                    await self.mc_api.add_group_members(mc_group_id, phones)

                created_groups.append((mc_group_name, mc_group_id))

                i += 1
                if i >= limit:
                    break

        return created_groups

    async def update_group_lists(self, created_bsd_groups, created_mc_groups):
        now = datetime.now(tz=timezone.utc).isoformat()

        df = await self.civis.read_civis_sql(
            "SELECT id, group_name, group_id, group_type, created_at FROM bsd_mc_group.created_groups"
        )

        print(f"Got existing groups")
        print(df)

        existing_group_ids = set([row.id for _, row in df.iterrows()])
        new_created_groups = []

        for group_type, created_groups in (
            ("bsd", created_bsd_groups),
            ("mobilecommons", created_mc_groups),
        ):
            new_created_groups_of_type = [
                {
                    "id": f"{group_type}:{group_id}",
                    "group_id": group_id,
                    "group_name": group_name,
                    "group_type": group_type,
                    "created_at": now,
                }
                for group_name, group_id in created_groups
                if f"{group_type}:{group_id}" not in existing_group_ids
            ]

            print(
                f"{len(new_created_groups_of_type)} {group_type} groups created for the first time"
            )

            new_created_groups += new_created_groups_of_type

        if len(new_created_groups) > 0:
            if not SIMULATE_ONLY:
                await self.civis.upload_async(
                    df=pandas.DataFrame(new_created_groups),
                    table="bsd_mc_group.created_groups",
                    primary_key="id",
                    merge_style="delsert",
                )
