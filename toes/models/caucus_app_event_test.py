from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from models.caucus_app_event import CaucusAppEvent

test_date = datetime(2020, 1, 9, 19, 12, 13, 4567, tzinfo=timezone.utc)
test_date_str = "01/09/2020 13:12:13"


def blanks(n, value=""):
    return [value] * n


@pytest.fixture
def mock_s3_client():
    mock = MagicMock()

    def generate_presigned_url(method, Params=None, ExpiresIn=None):
        assert method == "get_object"
        assert ExpiresIn == timedelta(hours=12).total_seconds()
        return f"signed:{Params['Bucket']}:{Params['Key']}"

    mock.generate_presigned_url.side_effect = generate_presigned_url

    return mock


def test_basic_metadata(mock_s3_client):
    assert CaucusAppEvent(
        id="A",
        precinct_id="B",
        phone_number="C",
        event_type="login",
        attendee_count=123,
        results=[],
        notes="E",
        s3_object_key=None,
        created_at=test_date,
    ).to_spreadsheet_row(mock_s3_client, "some-bucket") == [
        "A",
        "B",
        "C",
        "login",
        test_date_str,
        "E",
        "",
        123,
        *blanks(51),
    ]


def test_empty_event(mock_s3_client):
    # fully empty event
    assert CaucusAppEvent().to_spreadsheet_row(mock_s3_client, "some-bucket") == [
        *blanks(7),
        0,
        *blanks(51),
    ]

    # for login/final, counts should be blank
    assert CaucusAppEvent(event_type="login").to_spreadsheet_row(
        mock_s3_client, "some-bucket"
    ) == [*blanks(3), "login", *blanks(3), 0, *blanks(51)]

    assert CaucusAppEvent(event_type="final").to_spreadsheet_row(
        mock_s3_client, "some-bucket"
    ) == [*blanks(3), "final", *blanks(3), 0, *blanks(51)]

    # for alignment/realignment, counts should be 0
    assert CaucusAppEvent(event_type="alignment").to_spreadsheet_row(
        mock_s3_client, "some-bucket"
    ) == [*blanks(3), "alignment", *blanks(3), *blanks(27, value=0), *blanks(25)]

    assert CaucusAppEvent(event_type="realignment").to_spreadsheet_row(
        mock_s3_client, "some-bucket"
    ) == [*blanks(3), "realignment", *blanks(3), *blanks(27, value=0), *blanks(25)]


def test_media_url(mock_s3_client):
    assert CaucusAppEvent(
        id="A",
        precinct_id="B",
        phone_number="C",
        event_type="final",
        attendee_count=123,
        results=[],
        notes="E",
        s3_object_key="asdf",
        created_at=test_date,
    ).to_spreadsheet_row(mock_s3_client, "some-bucket") == [
        "A",
        "B",
        "C",
        "final",
        test_date_str,
        "E",
        "signed:some-bucket:asdf",
        123,
        *blanks(51),
    ]


def test_results_not_all_candidates(mock_s3_client):
    assert CaucusAppEvent(
        id="A",
        precinct_id="B",
        phone_number="C",
        event_type="alignment",
        attendee_count=123,
        results=[
            {"name": "Yang", "caucusers": 18, "delegates": 1},
            {"name": "Buttigieg", "caucusers": 13, "delegates": 2},
            {"name": "🌭", "caucusers": 22, "delegates": 3},
        ],
        notes="E",
        s3_object_key="asdf",
        created_at=test_date,
    ).to_spreadsheet_row(mock_s3_client, "some-bucket") == [
        "A",
        "B",
        "C",
        "alignment",
        test_date_str,
        "E",
        "signed:some-bucket:asdf",
        123,
        *blanks(6, value=0),
        13,
        2,
        *blanks(4, value=0),
        18,
        1,
        *blanks(12, value=0),
        "🌭",
        22,
        3,
        *blanks(22),
    ]


def test_results_all_candidates_and_writeins(mock_s3_client):
    assert CaucusAppEvent(
        id="A",
        precinct_id="B",
        phone_number="C",
        event_type="alignment",
        attendee_count=123,
        results=[
            {"name": "Yang", "caucusers": 16, "delegates": 160},
            {"name": "Buttigieg", "caucusers": 13, "delegates": 130},
            {"name": "🌭", "caucusers": 23, "delegates": 230},
            {"name": "Steyer", "caucusers": 18, "delegates": 180},
            {"name": "Sanders", "caucusers": 12, "delegates": 120},
            {"name": "Bennet", "caucusers": 19, "delegates": 190},
            {"name": "Uncommitted", "caucusers": 22, "delegates": 220},
            {"name": "Klobuchar", "caucusers": 15, "delegates": 150},
            {"name": "Biden", "caucusers": 11, "delegates": 110},
            {"name": "Bloomberg", "caucusers": 14, "delegates": 140},
            {"name": "Delaney", "caucusers": 20, "delegates": 200},
            {"name": "Patrick", "caucusers": 21, "delegates": 210},
            {"name": "Warren", "caucusers": 10, "delegates": 100},
            {"name": "Gabbard", "caucusers": 17, "delegates": 170},
            {"name": "Hot Dogs", "caucusers": 24, "delegates": 240},
        ],
        notes="E",
        s3_object_key="asdf",
        created_at=test_date,
    ).to_spreadsheet_row(mock_s3_client, "some-bucket") == [
        "A",
        "B",
        "C",
        "alignment",
        test_date_str,
        "E",
        "signed:some-bucket:asdf",
        123,
        10,
        100,
        11,
        110,
        12,
        120,
        13,
        130,
        14,
        140,
        15,
        150,
        16,
        160,
        17,
        170,
        18,
        180,
        19,
        190,
        20,
        200,
        21,
        210,
        22,
        220,
        "Hot Dogs",
        24,
        240,
        "🌭",
        23,
        230,
        *blanks(19),
    ]


def test_write_in_overflow(mock_s3_client):
    assert CaucusAppEvent(
        id="A",
        precinct_id="B",
        phone_number="C",
        event_type="alignment",
        attendee_count=123,
        results=[
            {"name": "Bratwurst", "caucusers": 26, "delegates": 260},
            {"name": "Klobuchar", "caucusers": 15, "delegates": 150},
            {"name": "Andouille", "caucusers": 24, "delegates": 240},
            {"name": "🌭", "caucusers": 22, "delegates": 220},
            {"name": "🌮", "caucusers": 28, "delegates": 280},
            {"name": "Biden", "caucusers": 11, "delegates": 110},
            {"name": "Warren", "caucusers": 10, "delegates": 100},
            {"name": "Uncommitted", "caucusers": 21, "delegates": 210},
            {"name": "Sanders", "caucusers": 12, "delegates": 120},
            {"name": "Delaney", "caucusers": 20, "delegates": 200},
            {"name": "Gabbard", "caucusers": 17, "delegates": 170},
            {"name": "Hot Dogs", "caucusers": 23, "delegates": 230},
            {"name": "Steyer", "caucusers": 18, "delegates": 180},
            {"name": "Bennet", "caucusers": 19, "delegates": 190},
            {"name": "Kielbasa", "caucusers": 27, "delegates": 270},
            {"name": "Bloomberg", "caucusers": 14, "delegates": 140},
            {"name": "Yang", "caucusers": 16, "delegates": 160},
            {"name": "Chorizo", "caucusers": 25, "delegates": 250},
            {"name": "Buttigieg", "caucusers": 13, "delegates": 130},
            {"name": "Patrick", "caucusers": 29, "delegates": 290},
        ],
        notes="E",
        s3_object_key="asdf",
        created_at=test_date,
    ).to_spreadsheet_row(mock_s3_client, "some-bucket") == [
        "A",
        "B",
        "C",
        "alignment",
        test_date_str,
        "E",
        "signed:some-bucket:asdf",
        123,
        10,
        100,
        11,
        110,
        12,
        120,
        13,
        130,
        14,
        140,
        15,
        150,
        16,
        160,
        17,
        170,
        18,
        180,
        19,
        190,
        20,
        200,
        29,
        290,
        21,
        210,
        "Andouille",
        24,
        240,
        "Bratwurst",
        26,
        260,
        "Chorizo",
        25,
        250,
        "Kielbasa",
        27,
        270,
        "🌮",
        28,
        280,
        "Hot Dogs: 23 -> 230; 🌭: 22 -> 220",
        45,
        450,
        *blanks(7),
    ]


def test_ties(mock_s3_client):
    assert CaucusAppEvent(
        id="A",
        precinct_id="B",
        phone_number="C",
        event_type="realignment",
        attendee_count=123,
        results=[
            {
                "name": "Warren",
                "caucusers": 14,
                "delegates": 3,
                "ties": ["Buttigieg"],
                "tieStatus": "won",
            },
            {
                "name": "Buttigieg",
                "caucusers": 14,
                "delegates": 2,
                "ties": ["Warren"],
                "tieStatus": "lost",
            },
            {
                "name": "🌭",
                "caucusers": 12,
                "delegates": 2,
                "ties": ["🍕", "🌮"],
                "tieStatus": "won",
            },
            {
                "name": "🌮",
                "caucusers": 12,
                "delegates": 1,
                "ties": ["🍕", "🌭"],
                "tieStatus": "lost",
            },
            {
                "name": "🍕",
                "caucusers": 12,
                "delegates": 2,
                "ties": ["🌭", "🌮"],
                "tieStatus": "won",
            },
            {"name": "Sanders", "caucusers": 8, "delegates": 0},
        ],
        notes="E",
        s3_object_key="asdf",
        created_at=test_date,
    ).to_spreadsheet_row(mock_s3_client, "some-bucket") == [
        "A",
        "B",
        "C",
        "realignment",
        test_date_str,
        "E",
        "signed:some-bucket:asdf",
        123,
        14,
        3,
        0,
        0,
        8,
        0,
        14,
        2,
        *blanks(18, value=0),
        "🌭",
        12,
        2,
        "🌮",
        12,
        1,
        "🍕",
        12,
        2,
        *blanks(9),
        "Buttigieg Warren",
        "Warren",
        "🌭 🌮 🍕",
        "🌭 🍕",
        *blanks(3),
    ]


def test_tie_overflow(mock_s3_client):
    assert CaucusAppEvent(
        id="A",
        precinct_id="B",
        phone_number="C",
        event_type="realignment",
        attendee_count=123,
        results=[
            {
                "name": "Warren",
                "caucusers": 14,
                "delegates": 140,
                "ties": ["Biden"],
                "tieStatus": "won",
            },
            {
                "name": "Biden",
                "caucusers": 14,
                "delegates": 140,
                "ties": ["Warren"],
                "tieStatus": "lost",
            },
            {
                "name": "Sanders",
                "caucusers": 12,
                "delegates": 120,
                "ties": ["Buttigieg"],
                "tieStatus": "won",
            },
            {
                "name": "Buttigieg",
                "caucusers": 12,
                "delegates": 120,
                "ties": ["Sanders"],
                "tieStatus": "lost",
            },
            {
                "name": "Bloomberg",
                "caucusers": 10,
                "delegates": 100,
                "ties": ["Bennet"],
                "tieStatus": "won",
            },
            {
                "name": "Bennet",
                "caucusers": 10,
                "delegates": 100,
                "ties": ["Bloomberg"],
                "tieStatus": "lost",
            },
            {
                "name": "Klobuchar",
                "caucusers": 8,
                "delegates": 80,
                "ties": ["Delaney"],
                "tieStatus": "won",
            },
            {
                "name": "Delaney",
                "caucusers": 8,
                "delegates": 80,
                "ties": ["Klobuchar"],
                "tieStatus": "lost",
            },
            {
                "name": "Yang",
                "caucusers": 6,
                "delegates": 60,
                "ties": ["Gabbard"],
                "tieStatus": "won",
            },
            {
                "name": "Gabbard",
                "caucusers": 6,
                "delegates": 60,
                "ties": ["Yang"],
                "tieStatus": "lost",
            },
        ],
        notes="E",
        s3_object_key="asdf",
        created_at=test_date,
    ).to_spreadsheet_row(mock_s3_client, "some-bucket") == [
        "A",
        "B",
        "C",
        "realignment",
        test_date_str,
        "E",
        "signed:some-bucket:asdf",
        123,
        14,
        140,
        14,
        140,
        12,
        120,
        12,
        120,
        10,
        100,
        8,
        80,
        6,
        60,
        6,
        60,
        0,
        0,
        10,
        100,
        8,
        80,
        *blanks(4, value=0),
        *blanks(18),
        "Bennet Bloomberg",
        "Bloomberg",
        "Biden Warren",
        "Warren",
        "Buttigieg Sanders",
        "Sanders",
        "Delaney Klobuchar: Klobuchar; Gabbard Yang: Yang",
    ]
