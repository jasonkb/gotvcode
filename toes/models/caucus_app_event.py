import os
from collections import defaultdict
from os import write

from pynamodb.attributes import (
    BooleanAttribute,
    JSONAttribute,
    NumberAttribute,
    UnicodeAttribute,
    UTCDateTimeAttribute,
)
from pynamodb.models import Model
from pytz import timezone

from common.settings import settings

IOWA_TIMEZONE = timezone("US/Central")


def make_id(phone, precinct_id, created_at):
    return "|".join([phone, precinct_id, str(int(1000 * created_at.timestamp()))])


def format_date_for_iowa(dt):
    if dt is None:
        return None

    return dt.astimezone(IOWA_TIMEZONE).strftime("%m/%d/%Y %H:%M:%S")


# Named candidates in the order from the sheet
MAIN_CANDIDATES = [
    "Warren",
    "Biden",
    "Sanders",
    "Buttigieg",
    "Bloomberg",
    "Klobuchar",
    "Yang",
    "Gabbard",
    "Steyer",
    "Bennet",
    "Delaney",
    "Patrick",
    "Uncommitted",
]

# S3 Presigned URL expiration for google sheet
# This is 12 hours becaue our Lambda credentials expire after 12 hours, so
# making it longer than that doesn't actually extend the valdidity.
S3_PRESIGNED_URL_EXPIRATION_SECONDS = 60 * 60 * 12


class CaucusAppEvent(Model):
    class Meta:
        table_name = f"caucus-app-event-{settings.infrastructure or 'local'}"

    id = UnicodeAttribute(hash_key=True)
    precinct_id = UnicodeAttribute(null=False)
    phone_number = UnicodeAttribute(null=False)
    event_type = UnicodeAttribute(null=False)
    attendee_count = NumberAttribute(null=True)
    results = JSONAttribute(null=True)
    notes = UnicodeAttribute(null=True)
    s3_object_key = UnicodeAttribute(null=True)
    created_at = UTCDateTimeAttribute(null=False)
    is_old_data = BooleanAttribute(null=True)

    @staticmethod
    def setup():
        if not CaucusAppEvent.exists():
            # Note: pynamodb doesn't appear to support setting on-demand scaling
            # through this API, so we set it through the AWS console after creating
            # the table.
            CaucusAppEvent.create_table(
                wait=True, read_capacity_units=1, write_capacity_units=1
            )

    def to_spreadsheet_row(self, s3_client, s3_bucket):
        if self.s3_object_key:
            media_url = s3_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": s3_bucket, "Key": self.s3_object_key},
                ExpiresIn=S3_PRESIGNED_URL_EXPIRATION_SECONDS,
            )
        else:
            media_url = None

        row = [
            self.id,
            self.precinct_id,
            self.phone_number,
            self.event_type,
            format_date_for_iowa(self.created_at),
            self.notes,
            media_url,
            self.attendee_count or 0,
        ]

        # Map of main candidates -> (caucusers, delegates)
        main_candidate_counts = {}

        # List of (name, caucusers, delegates) for non-main candidates ("write-ins")
        write_in_candidates = []

        # Map of (tie key - sorted list of members) -> (tie winners)
        tie_dict = defaultdict(set)

        for result in self.results or []:
            name = result["name"]
            caucusers = result["caucusers"]
            delegates = result["delegates"]

            if name in MAIN_CANDIDATES:
                main_candidate_counts[name] = [caucusers, delegates]
            else:
                write_in_candidates.append([name, caucusers, delegates])

            if result.get("tieStatus") == "won":
                tie_key = " ".join(sorted([name, *result.get("ties", [])]))
                tie_dict[tie_key].add(name)

        # Add main candidates to the row
        # Fill in 0 by default for alignment and realignment
        for name in MAIN_CANDIDATES:
            row.extend(
                main_candidate_counts.get(
                    name,
                    [0, 0]
                    if self.event_type in ("alignment", "realignment")
                    else [None, None],
                )
            )

        # Sort the write-in candidates by number of votes
        write_in_candidates.sort(key=lambda x: -x[1])

        # Get the top 5 write-in candidates, alphabetize them, and add them to the sheet
        top_write_in_candidates = write_in_candidates[0:5]
        top_write_in_candidates.sort(key=lambda x: x[0])

        for name, count, delegates in top_write_in_candidates:
            row.append(name)
            row.append(count)
            row.append(delegates)

        # If less than 5 write-ins, pad with None
        for _ in range(len(top_write_in_candidates), 5):
            row.append(None)
            row.append(None)
            row.append(None)

        # If more than 5 write-ins, construct the overflow data
        overflow_candidates = write_in_candidates[5:]
        overflow_data = None
        overflow_total = None
        overflow_delegates = None
        if len(overflow_candidates) > 0:
            overflow_data = "; ".join(
                [
                    f"{name}: {count} -> {delegates}"
                    for name, count, delegates in overflow_candidates
                ]
            )
            overflow_total = sum(
                [count for name, count, delegates in overflow_candidates]
            )
            overflow_delegates = sum(
                [delegates for name, count, delegates in overflow_candidates]
            )

        row.append(overflow_data)
        row.append(overflow_total)
        row.append(overflow_delegates)

        # Convert the map of ties into a list of (tie name, tie winners)
        ties = []
        for tie_key, tie_winners in tie_dict.items():
            tie_value = " ".join(sorted(tie_winners))

            ties.append((tie_key, tie_value))

        ties.sort()

        # Get the first three ties and add them to the sheet
        for name, winners in ties[0:3]:
            row.append(name)
            row.append(winners)

        # If fewer than 3 ties, pad with None
        for _ in range(len(ties), 3):
            row.append(None)
            row.append(None)

        # If more than 3 ties, construct the overflow data
        # Note that the frontend can't submit data that overflow ties --
        # a candidate needs 15% of the vote to be viable so there's
        # no way to have more than 3 ties within the viable candidates
        overflow_ties = ties[3:]
        overflow_tie_data = None
        if len(overflow_ties) > 0:
            overflow_tie_data = "; ".join(
                [f"{name}: {winners}" for name, winners in overflow_ties]
            )

        row.append(overflow_tie_data)

        # Convert None to empty string
        return [cell if cell is not None else "" for cell in row]
