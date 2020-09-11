from collections import defaultdict
from dataclasses import dataclass, field
from typing import DefaultDict, Dict, List, Optional, Set

from integrations.civis import CivisDatabase
from projects.bsd_mc_groups.user_joiner import UserJoiner

from .user import User


@dataclass
class Group:
    # If this is true, the group should be synced to BSD
    sync_to_bsd: bool = True

    # If this is set to a mobilecommon group ID, the
    # group should be syncded to mobilecommons (we can't just
    # use a boolean flag like with BSD because the mobilecommons
    # API doesn't let you search by name, and the endpoint
    # for listing all groups isn't paginated and times out before
    # it returns)
    mobilecommons_group_id: Optional[int] = None

    users: Set[User] = field(default_factory=set)


class SyncGroups:
    """
    Collects data to be synced to both BSD and MobileCommons
    """

    def __init__(self):
        self.groups: DefaultDict[str, Group] = defaultdict(Group)
        self.mobilecommons_group_names: Dict[int, str] = {}

    def configure_group(
        self,
        name: str,
        sync_to_bsd: Optional[bool] = None,
        mobilecommons_group_id: Optional[int] = None,
    ):
        """
        By default, groups are syned to BSD and not to MobileCommons. If that's
        not the desired behavior for a particular group, call this method with
        sync_to_bsd=True and/or mobilecommons_group_id=<group id> to configure
        the sync destinations.

        Caution: MobileCommons groups are currently append-only. Existing
        group members will not be removed from a group; new members will just
        be added. BSD is a real sync; existing members will be removed.
        """
        group = self.groups[name]

        if sync_to_bsd is not None:
            group.sync_to_bsd = sync_to_bsd

        if mobilecommons_group_id is not None:
            group.mobilecommons_group_id = mobilecommons_group_id
            self.mobilecommons_group_names[mobilecommons_group_id] = name

    def add_user(self, group: str, user: User):
        """
        Adds a user to the group
        """
        self.groups[group].users.add(user)

    async def get_joiner(self, db: CivisDatabase) -> UserJoiner:
        """
        Joins the users that have been added to any group against their
        MC profile and/or BSD cons in Civis, and return a UserJoiner
        that can be passed to mobilecommons_sync_data and bsd_sync_data
        """
        users = set()
        for group in self.groups.values():
            for user in group.users:
                users.add(user)

        joiner = UserJoiner()
        await joiner.join_users(users, db)

        return joiner

    def mobilecommons_sync_data(self, joiner: UserJoiner) -> Dict[int, List[str]]:
        """
        Returns a dictionary of mobilecommons groups IDs and the phone numbers
        that should be in them
        """
        data: Dict[int, List[str]] = {}
        for group in self.groups.values():
            if group.mobilecommons_group_id is not None:
                group_phones = set()
                for user in group.users:
                    for phone in joiner.phones_for(user):
                        group_phones.add(phone)

                data[group.mobilecommons_group_id] = list(group_phones)

        return data

    def bsd_sync_data(self, joiner: UserJoiner) -> Dict[str, List[str]]:
        """
        Returns a dictionary of BSD group names and the cons_ids that should be
        in them
        """
        data: Dict[str, List[str]] = {}
        for group_name, group in self.groups.items():
            if group.sync_to_bsd:
                group_cons_ids = set()
                for user in group.users:
                    for cons_id in joiner.cons_ids_for(user):
                        group_cons_ids.add(cons_id)

                # It's possible there really is nobody left in this group -- but
                # it's much more likely that we're just not loading the data
                # correctly so we safe-guard against emptying a group erroneously
                if len(group_cons_ids) == 0:
                    print(f"WARNING: no cons in {group_name}; skipping BSD sync")
                else:
                    data[group_name] = list(group_cons_ids)

        return data

    def mobilecommons_group_name(self, group_id: int) -> Optional[str]:
        return self.mobilecommons_group_names.get(group_id)
