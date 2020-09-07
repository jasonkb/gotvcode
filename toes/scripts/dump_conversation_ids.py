import argparse

from help_scout import client

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Write a list of conversation ids in a mailbox to a file"
    )
    parser.add_argument("--mailbox", help="The mailbox id to dump")
    parser.add_argument("--file", help="The file to write to")
    args = parser.parse_args()

    with client.HelpScoutClient() as c, open(args.file, "w+") as f:
        for page in c.list_conversations(args.mailbox):
            json = page.json()
            print(f"Page {json['page']['number']} of {json['page']['totalPages']}")
            lines = [
                str(convo["id"]) + "\n" for convo in json["_embedded"]["conversations"]
            ]
            f.writelines(lines)
