import argparse
import csv

from help_scout import client

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload a csv of tags to Help Scout")
    parser.add_argument(
        "--csv",
        help="The csv file to upload, must include 'conversation_id' and 'tags' headers",
    )
    parser.add_argument(
        "--tag_separator",
        default="|",
        help="Separator to use when uploading multiple tags per conversation",
    )
    args = parser.parse_args()
    with open(args.csv, "r") as f, client.HelpScoutClient() as help_scout:
        reader = csv.DictReader(f)
        for row in reader:
            c_id = row["conversation_id"]
            tags = row["tags"].split(args.tag_separator)
            try:
                res = help_scout.update_conversation_tags(c_id, add=tags)
                print(f"Success: conversation_id: {c_id} tags: {tags}")
            except Exception as e:
                print("Failed to sync", c_id, e)

    print("Done!")
