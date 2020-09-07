import json
import os

import pytest

from ew_common.mobile_commons import Profile


@pytest.fixture
def sample_profile():
    with open(
        os.path.join(os.path.dirname(__file__), "sample_mobile_commons_profile.json")
    ) as f:
        return json.loads(f.read())


def test_profile_dataclass_parsing(sample_profile):
    p = Profile.from_xml_dict(sample_profile)
    assert p.id == 345213473
    assert p.first_name == "FakeFirstName"
    assert p.last_name == "FakeLastName"
    assert p.phone_number == "15555555555"
    assert p.campaign_ids == {189358}
    assert {m.id for m in p.messages} == {
        6786806624,
        1338175333,
        7329031201,
        1338800116,
    }
    assert p.custom_fields["donation_hpc"] == "12345"
    assert p.custom_fields.get("SC_Vol_Ask") is None


def test_profile_message_sorting(sample_profile):
    p = Profile.from_xml_dict(sample_profile)
    assert p.messages[0].id == 6786806624
    assert not p.messages_are_sorted
    p.sort_messages_desc()
    assert p.messages_are_sorted
    assert [m.id for m in p.messages] == [
        1338800116,
        7329031201,
        1338175333,
        6786806624,
    ]
    assert p.most_recent_message().body == "Test"


def test_parsing_repeated_field(sample_profile):
    sample_profile["messages"]["message"] = sample_profile["messages"]["message"][0]
    p = Profile.from_xml_dict(sample_profile)
    assert len(p.messages) == 1


def test_parsing_no_messages(sample_profile):
    del sample_profile["messages"]
    p = Profile.from_xml_dict(sample_profile)
    assert p.messages == []
