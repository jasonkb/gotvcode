# Unfortunately, the Mobile Commons API doesn't have a way to find groups by name, and the "list groups"
# endpoint is unpaginated and times out before it returns anything. So we have no way to dynamically
# create the groups because we can't implement a find-or-create on top of the API :(
#
# Instead, when we want to add more groups to the maps in groups.py:
# - Add the group to groups.py with an ID of -1
# - Run this script
#
# This script will create the new groups and print out the new map with filled-in group IDs that you can
# copy-paste back into groups.py.

import asyncio
import os
from pprint import pformat

from integrations.mobilecommons.client import MobileCommonsAPIClient

from .groups import (
    ACTION_TYPES,
    IA_CAUCUS_ORG_GROUPS_MC,
    PAST_RSVPS_TYPES,
    EVENT_HOST_TYPES,
    PAST_NGP_VAN_SIGNUPS,
    IA_PRECINCTS,
    IA_COUNTIES,
)

mc_api = MobileCommonsAPIClient(
    os.environ["MOBILECOMMONS_USERNAME"], os.environ["MOBILECOMMONS_PASSWORD"]
)


async def populate_dict(const_name, prefix, d):
    new_d = {}
    for k, v in d.items():
        if v >= 0:
            new_d[k] = v
            continue

        new_d[k] = await mc_api.create_group(f"{prefix}{k}")

    print(f"{const_name} = {pformat(new_d)}")


async def main():
    async with mc_api.session_context():
        await populate_dict("PAST_RSVPS_TYPES", "", PAST_RSVPS_TYPES)
        await populate_dict("ACTION_TYPES", "Action ", ACTION_TYPES)
        await populate_dict("EVENT_HOST_TYPES", "", EVENT_HOST_TYPES)
        await populate_dict("PAST_NGP_VAN_SIGNUPS", "", PAST_NGP_VAN_SIGNUPS)

        await populate_dict("IA_PRECINCTS", "IA Precinct ", IA_PRECINCTS)
        await populate_dict("IA_COUNTIES", "IA County ", IA_COUNTIES)

        await populate_dict("IA_CAUCUS_ORG_GROUPS_MC", "", IA_CAUCUS_ORG_GROUPS_MC)


if __name__ == "__main__":
    asyncio.run(main())
