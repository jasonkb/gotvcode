import os

from werkzeug.utils import cached_property

from common.parameter_store import get_parameter


class Settings:
    STAGE_DEV = "dev"
    STAGE_PROD = "prod"

    ACTBLUE_DONATIONS_INCOMING_S3_BUCKETS = {
        STAGE_DEV: "ew-actblue-donations-incoming-dev",
        STAGE_PROD: "ew-actblue-donations-incoming",
    }

    DONORS_TABLE_NAMES = {STAGE_DEV: "donors-dev", STAGE_PROD: "donors"}

    @cached_property
    def stage(self):
        # Return default 'dev' if no env variable set.
        if not os.environ.get("STAGE"):
            return self.STAGE_DEV

        return os.environ.get("STAGE")

    @cached_property
    def infrastructure(self):
        return os.environ.get("INFRASTRUCTURE")

    @cached_property
    def mobile_commons_username(self):
        return os.environ["MOBILE_COMMONS_USERNAME"]

    @cached_property
    def mobile_commons_password(self):
        return os.environ["MOBILE_COMMONS_PASSWORD"]

    @cached_property
    def contentful_emails_access_token(self):
        return os.environ["CONTENTFUL_EMAILS_ACCESS_TOKEN"]

    @cached_property
    def contentful_emails_space_id(self):
        return os.environ["CONTENTFUL_EMAILS_SPACE_ID"]

    @cached_property
    def contentful_webhook_username(self):
        return os.environ["CONTENTFUL_USERNAME"]

    @cached_property
    def contentful_webhook_password(self):
        return os.environ["CONTENTFUL_PASSWORD"]

    @cached_property
    def actblue_webhook_username(self):
        return os.environ["ACTBLUE_WEBHOOK_USERNAME"]

    @cached_property
    def actblue_webhook_password(self):
        return os.environ["ACTBLUE_WEBHOOK_PASSWORD"]

    @cached_property
    def generic_kv_read_username(self):
        return os.environ["GENERIC_KV_READ_USERNAME"]

    @cached_property
    def generic_kv_read_password(self):
        return os.environ["GENERIC_KV_READ_PASSWORD"]

    @cached_property
    def generic_kv_write_username(self):
        return os.environ["GENERIC_KV_WRITE_USERNAME"]

    @cached_property
    def generic_kv_write_password(self):
        return os.environ["GENERIC_KV_WRITE_PASSWORD"]

    @cached_property
    def bsd_api_username(self):
        return os.environ["BSD_API_USERNAME"]

    @cached_property
    def bsd_api_password(self):
        return os.environ["BSD_API_PASSWORD"]

    @cached_property
    def actblue_donations_incoming_s3_bucket(self):
        return self.ACTBLUE_DONATIONS_INCOMING_S3_BUCKETS[self.stage]

    @cached_property
    def donors_table_name(self):
        return self.DONORS_TABLE_NAMES[self.stage]

    @cached_property
    def donor_id_salt(self):
        return os.environ["DONOR_ID_SALT"]

    @cached_property
    def help_scout_webhook_secret(self):
        return os.environ["HELPSCOUT_WEBHOOK_SECRET"]

    @cached_property
    def help_scout_contact_us_client_id(self):
        return os.environ["HELPSCOUT_CONTACT_US_CLIENT_ID"]

    @cached_property
    def help_scout_contact_us_secret(self):
        return os.environ["HELPSCOUT_CONTACT_US_SECRET"]

    @cached_property
    def help_scout_api_client_id(self):
        return os.environ["HELPSCOUT_API_CLIENT_ID"]

    @cached_property
    def help_scout_api_client_secret(self):
        return os.environ["HELPCLOUD_API_CLIENT_SECRET"]

    @cached_property
    def caucus_app_username(self):
        return os.environ["CAUCUS_APP_USERNAME"]

    @cached_property
    def caucus_app_password(self):
        return os.environ["CAUCUS_APP_PASSWORD"]

    @cached_property
    def zendesk_webhook_username(self):
        return os.environ["ZENDESK_WEBHOOK_USERNAME"]

    @cached_property
    def zendesk_webhook_password(self):
        return os.environ["ZENDESK_WEBHOOK_PASSWORD"]

    @cached_property
    def mobilizeio_api_key_username(self):
        return os.environ["MOBILIZEIO_API_KEY_USERNAME"]

    @cached_property
    def mobilizeio_api_key_password(self):
        return os.environ["MOBILIZEIO_API_KEY_PASSWORD"]

    @cached_property
    def mobilizeio_webhook_auth_code(self):
        return os.environ["MOBILIZEIO_WEBHOOK_AUTH_CODE"]

    @cached_property
    def caucus_data_spreadsheet_service_account(self):
        return os.environ["CAUCUS_DATA_SPREADSHEET_SERVICE_ACCOUNT"]

    @cached_property
    def zendesk_subdomain(self):
        return os.environ["ZENDESK_SUBDOMAIN"]

    @cached_property
    def zendesk_api_email(self):
        return os.environ["ZENDESK_API_EMAIL"]

    @cached_property
    def zendesk_api_token(self):
        return os.environ["ZENDESK_API_TOKEN"]

    @cached_property
    def twilio_account_sid(self):
        return os.environ["TWILIO_ACCOUNT_SID"]

    @cached_property
    def twilio_auth_token(self):
        return os.environ["TWILIO_AUTH_TOKEN"]

    @cached_property
    def bling_jwt_secret(self):
        return os.environ["BLING_JWT_SECRET"]

    def override_cached_property(self, name, value):
        """Helper for tests that allows a cached property to be overridden"""
        self.__dict__[name] = value


settings = Settings()
