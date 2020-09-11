import math

import numpy as np
import pytest

from .user import User, find_or_create_user, has_value


@pytest.mark.parametrize(
    "test_input,expected",
    [(None, False), (np.nan, False), (math.nan, False), ("", True), (0, True)],
)
def test_has_value(test_input, expected):
    assert has_value(test_input) == expected


def test_user_hash():
    # same email/phone -> same
    assert hash(User(email="A", phone="B", id=1)) == hash(
        User(email="A", phone="B", id=2)
    )

    # different email
    assert hash(User(email="A", phone="B", id=1)) != hash(
        User(email="A2", phone="B", id=1)
    )

    # different phone
    assert hash(User(email="A", phone="B", id=1)) != hash(
        User(email="A", phone="B2", id=1)
    )


def test_find_or_create_user():
    # same email/phone -> same user
    assert (
        find_or_create_user(email="A", phone="111-222-3333").id
        == find_or_create_user(email="A", phone="111-222-3333").id
    )

    # different email
    assert (
        find_or_create_user(email="A", phone="111-222-3333").id
        != find_or_create_user(email="A2", phone="111-222-3333").id
    )

    # different phone
    assert (
        find_or_create_user(email="A", phone="111-222-3333").id
        != find_or_create_user(email="A", phone="444-555-6666").id
    )

    # nan becomes null
    u1 = find_or_create_user(email=np.nan)
    assert u1.phone is None
    assert u1.email is None

    u2 = find_or_create_user(phone=np.nan, email="A")
    assert u2.phone is None
    assert u2.email == "a"

    # Emails and phones are cleaned up
    u3 = find_or_create_user(email="  FoO@example.com    \n", phone="(123) 456-7890")
    assert u3.email == "foo@example.com"
    assert u3.phone == "11234567890"
