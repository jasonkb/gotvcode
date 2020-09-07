import json

import boto3

SES_REGION_NAME = "us-east-1"


class EmailService:
    """ A service for sending emails in supportal via SES. To replace the tempalte
    service as we don't really need it anymore."""

    def __init__(self, ses_client=None):
        self.ses = (
            ses_client
            if ses_client
            else boto3.client("ses", region_name=SES_REGION_NAME)
        )

    def send_email(
        self,
        template_name,
        from_email,
        recipient,
        reply_to_email,
        configuration_set_name,
        payload,
        application_name,
        bcc_emails=[],
    ):

        request = {
            "Source": from_email,
            "Destination": {"ToAddresses": [recipient], "BccAddresses": bcc_emails},
            "ReplyToAddresses": [reply_to_email],
            "Template": template_name,
            "TemplateData": json.dumps(payload),
            "ConfigurationSetName": configuration_set_name,
            "Tags": [
                {"Name": "application", "Value": application_name},
                {
                    "Name": "mailing_identifier",
                    "Value": f"{application_name}-transactional",
                },
                {"Name": "template", "Value": template_name},
                {"Name": "mail_config", "Value": configuration_set_name},
            ],
        }

        return self.ses.send_templated_email(**request)

    def _generate_destinations(self, payload_array):
        destination_array = []

        for payload in payload_array:
            destination_to_ad = {
                "Destination": {"ToAddresses": [payload["email"]]},
                "ReplacementTemplateData": json.dumps(payload),
            }
            destination_array += [destination_to_ad]

        return destination_array

    def send_bulk_email(
        self,
        from_email,
        reply_to_email,
        configuration_set_name,
        template,
        payload_array,
        default_template_data,
        application_name,
    ):
        """Bulk emails need to be sent in groups of 50"""
        if len(payload_array) > 50:
            raise Exception("Can only send bulk emails in groups of 50")

        if len(payload_array) == 0:
            return

        request = {
            "Source": from_email,
            "ReplyToAddresses": [reply_to_email],
            "ConfigurationSetName": configuration_set_name,
            "Template": template,
            "Destinations": self._generate_destinations(payload_array),
            "DefaultTemplateData": json.dumps(default_template_data),
            "DefaultTags": [
                {"Name": "application", "Value": application_name},
                {
                    "Name": "mailing_identifier",
                    "Value": f"{application_name}-automated",
                },
                {"Name": "template", "Value": template},
                {"Name": "mail_config", "Value": configuration_set_name},
            ],
        }
        return self.ses.send_bulk_templated_email(**request)
