from .sync_groups import SyncGroups
from .user import User
from .user_joiner import UserJoiner


# Helper to sort sync data so it has a stable order for
# assertions
def sort_sync_data(data):
    for val in data.values():
        val.sort()

    return data


def test_sync_groups():
    groups = SyncGroups()
    joiner = UserJoiner()

    # Explicit group A: sync to both
    groups.configure_group("A", sync_to_bsd=True, mobilecommons_group_id=111)

    groups.add_user("A", User(id=1))
    joiner.bsd_cons_ids[1].add("a1")

    groups.add_user("A", User(id=2))
    joiner.bsd_cons_ids[2].add("a2")
    joiner.bsd_cons_ids[2].add("a3")
    joiner.mc_phone_numbers[2].add("111-111-1111")

    # Explicit group b: BSD only
    groups.configure_group("B")
    groups.add_user("B", User(id=3))
    joiner.bsd_cons_ids[3].add("b1")
    joiner.mc_phone_numbers[3].add("222-222-2222")

    # Explicit group c: MobileCommons only
    groups.configure_group("C", sync_to_bsd=False, mobilecommons_group_id=222)

    groups.add_user("C", User(id=4))
    joiner.bsd_cons_ids[4].add("c1")
    joiner.mc_phone_numbers[4].add("333-333-3333")

    groups.add_user("C", User(id=5))
    joiner.mc_phone_numbers[5].add("333-333-4444")

    # Implicit group d: BSD only
    groups.add_user("D", User(id=6))
    joiner.bsd_cons_ids[6].add("d1")
    joiner.mc_phone_numbers[6].add("444-444-4444")

    assert sort_sync_data(groups.mobilecommons_sync_data(joiner)) == {
        111: ["111-111-1111"],
        222: ["333-333-3333", "333-333-4444"],
    }

    assert sort_sync_data(groups.bsd_sync_data(joiner)) == {
        "A": ["a1", "a2", "a3"],
        "B": ["b1"],
        "D": ["d1"],
    }
