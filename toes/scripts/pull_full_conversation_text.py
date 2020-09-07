import argparse
import csv

from help_scout import api, client

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download the whole text of every inbound email for a list of conversation ids"
    )
    parser.add_argument("--id_file", help="Newline delimited list of conversation ids")
    parser.add_argument("--output_file", help="csv")

    args = parser.parse_args()
    with open(args.id_file, "r") as infile, open(
        args.output_file, "w+"
    ) as outfile, client.HelpScoutClient() as help_scout:
        writer = csv.DictWriter(
            outfile, fieldnames=["conversation_id", "thread_id", "email", "text"]
        )
        c = 0
        for i in infile:
            c += 1
            if c % 250 == 0:
                print(c)
            json = help_scout.get_threads_for_conversation(int(i)).json()
            for thread in json["_embedded"]["threads"]:
                if thread["type"] != "customer":
                    continue
                row = {
                    "conversation_id": int(i),
                    "thread_id": thread["id"],
                    "email": thread["customer"].get("email"),
                    "text": thread.get("body"),
                }
                writer.writerow(row)
    print("Done!")
