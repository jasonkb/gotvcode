from unittest.mock import MagicMock

import pytest

from ew_common.email_service import EmailService

SES_REGION_NAME = "us-east-1"


def test_send_email():
    mock_ses_client = MagicMock()
    email_service = EmailService(mock_ses_client)
    email_service.send_email(
        "test_template",
        from_email="no-reply@test.com",
        recipient="sgoldblatt@elizabethwarren.com",
        reply_to_email="no-reply@test.com",
        configuration_set_name="config",
        payload={},
        application_name="supportal",
    )

    mock_ses_client.send_templated_email.assert_called_once_with(
        ConfigurationSetName="config",
        Destination={
            "ToAddresses": ["sgoldblatt@elizabethwarren.com"],
            "BccAddresses": [],
        },
        ReplyToAddresses=["no-reply@test.com"],
        Source="no-reply@test.com",
        Template="test_template",
        TemplateData="{}",
        Tags=[
            {"Name": "application", "Value": "supportal"},
            {"Name": "mailing_identifier", "Value": "supportal-transactional"},
            {"Name": "template", "Value": "test_template"},
            {"Name": "mail_config", "Value": "config"},
        ],
    )


def test_send_bulk_email():
    mock_ses_client = MagicMock()
    email_service = EmailService(mock_ses_client)
    email_service.send_bulk_email(
        template="test_template",
        from_email="no-reply@test.com",
        reply_to_email="no-reply@test.com",
        configuration_set_name="config",
        payload_array=[{"email": "sgoldblatt@elizabethwarren.com", "test": "cats!"}],
        default_template_data={"test": "pets!"},
        application_name="supportal",
    )

    mock_ses_client.send_bulk_templated_email.assert_called_once_with(
        Source="no-reply@test.com",
        ReplyToAddresses=["no-reply@test.com"],
        ConfigurationSetName="config",
        Template="test_template",
        Destinations=[
            {
                "Destination": {"ToAddresses": ["sgoldblatt@elizabethwarren.com"]},
                "ReplacementTemplateData": '{"email": "sgoldblatt@elizabethwarren.com", "test": "cats!"}',
            }
        ],
        DefaultTemplateData='{"test": "pets!"}',
        DefaultTags=[
            {"Name": "application", "Value": "supportal"},
            {"Name": "mailing_identifier", "Value": "supportal-automated"},
            {"Name": "template", "Value": "test_template"},
            {"Name": "mail_config", "Value": "config"},
        ],
    )
