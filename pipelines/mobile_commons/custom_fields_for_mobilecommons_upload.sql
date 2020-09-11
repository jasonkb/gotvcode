-- Queries that produce `custom_fields_to_update_in_mobilecommonsg` table, which
-- has one row per Mobile Commons profile for which we want to update a custom field.
--
-- Prepares table of existing custom fields per Mobile Commons profile based on
-- upstream Mobile Commons data; then compares with the latest custom field values
-- from `custom_fields_per_cons` to see which fields we should push updates to.

DROP TABLE IF EXISTS tmp_custom_fields_per_phone_number;
CREATE TEMP TABLE tmp_custom_fields_per_phone_number AS
SELECT
  *
FROM (
SELECT
  *
, ROW_NUMBER() OVER (PARTITION BY phone_number ORDER BY
    donation_total_wfp DESC NULLS LAST
  , is_recurring_donor DESC NULLS LAST
  , donation_total_senate DESC NULLS LAST
  , donation_last_dt DESC NULLS LAST
  , is_sms_signup DESC NULLS LAST
  , is_shopify_shopper DESC NULLS LAST) AS row_num
  FROM {{ env.SCHEMA }}.custom_fields_per_cons
 WHERE custom_fields_per_cons.ds = {{ env.DS | pprint }}
)
WHERE row_num = 1
;

{% set custom_fields = env.CUSTOM_FIELDS.split(',') %}
{% set custom_fields_ints_on_mobile_commons = env.CUSTOM_FIELDS_INTS_ON_MOBILECOMMONS.split(',') %}
{% set custom_fields_yesno_on_mobile_commons = env.CUSTOM_FIELDS_YESNO_ON_MOBILECOMMONS.split(',') %}
{% set custom_fields_dates_on_mobile_commons = env.CUSTOM_FIELDS_DATES_ON_MOBILECOMMONS.split(',') %}
{% set custom_fields_nulls_on_mobile_commons = env.CUSTOM_FIELDS_NULLS_ON_MOBILECOMMONS.split(',') %}
DROP TABLE IF EXISTS {{ env.SCHEMA }}.custom_fields_from_mobilecommons;
CREATE TABLE {{ env.SCHEMA }}.custom_fields_from_mobilecommons AS
SELECT
  profiles.profile_id
, RIGHT(profiles.phone_number, 10) AS phone_number
{% for custom_field in custom_fields %}
, custom_columns_{{ custom_field }}.value AS {{ custom_field }}
{% endfor %}

FROM mobilecommons.profiles

{% for custom_field in custom_fields %}
LEFT JOIN mobilecommons.profile_custom_columns AS custom_columns_{{ custom_field }}
  ON custom_columns_{{ custom_field }}.name = {{ custom_field | pprint }}
 AND custom_columns_{{ custom_field }}.profile_id = profiles.profile_id
{% endfor %}
;


DROP TABLE IF EXISTS {{ env.SCHEMA }}.custom_fields_to_update_in_mobilecommons;
CREATE TABLE {{ env.SCHEMA }}.custom_fields_to_update_in_mobilecommons AS
SELECT DISTINCT
  tmp_custom_fields_per_phone_number.phone_number
{% for custom_field in custom_fields %}
, CAST(tmp_custom_fields_per_phone_number.{{ custom_field }} AS CHARACTER VARYING(1024)) AS {{ custom_field }}
{% endfor %}

  FROM {{ env.SCHEMA }}.custom_fields_from_mobilecommons
  JOIN tmp_custom_fields_per_phone_number
    ON tmp_custom_fields_per_phone_number.ds = {{ env.DS | pprint }}
   AND tmp_custom_fields_per_phone_number.phone_number = custom_fields_from_mobilecommons.phone_number

 WHERE
{% if env.PHONE_WHITELIST %}
   custom_fields_from_mobilecommons.phone_number = {{ env.PHONE_WHITELIST | pprint }} AND
{% endif %}
(
    FALSE

-- We jump through some hoops to try to sync to Mobile Commons only custom
-- fields which actually need to be synced; i.e. where the value in Mobile
-- Commons for the profile is different from what we want it to be.

{% for custom_field in custom_fields %}
{% if custom_field not in custom_fields_nulls_on_mobile_commons %}
    OR (
      -- We don't want to upload if value to upload is 0 and it's null upstream.
      (NOT (tmp_custom_fields_per_phone_number.{{ custom_field }} = 0 AND custom_fields_from_mobilecommons.{{ custom_field }} IS NULL))
      AND (
        -- If value is null/empty upstream should be updated to be non-null.
        ((custom_fields_from_mobilecommons.{{ custom_field }} IS NULL OR custom_fields_from_mobilecommons.{{ custom_field }} = '') AND tmp_custom_fields_per_phone_number.{{ custom_field }} IS NOT NULL)

        -- If value is non-null/nonempty upstream should be null.
        OR (custom_fields_from_mobilecommons.{{ custom_field }} IS NOT NULL AND custom_fields_from_mobilecommons.{{ custom_field }} != '' AND tmp_custom_fields_per_phone_number.{{ custom_field }} IS NULL)

        {% if custom_field in custom_fields_ints_on_mobile_commons %}
          -- This CEILs ensure we round consistently when comparing on integer basis.
          OR CEIL(custom_fields_from_mobilecommons.{{ custom_field }}) != CEIL(tmp_custom_fields_per_phone_number.{{ custom_field }})
        {% elif custom_field in custom_fields_yesno_on_mobile_commons %}
          -- This logic accommodates Civis-side values being 1/0/NULL/empty and
          -- Mobile-Commons-side values being 'Yes'/'No'/NULL/empty.
          OR (custom_fields_from_mobilecommons.{{ custom_field }} IN ('1', 'Yes') AND (tmp_custom_fields_per_phone_number.{{ custom_field }} IS NULL OR tmp_custom_fields_per_phone_number.{{ custom_field }} = 0))
          OR ((custom_fields_from_mobilecommons.{{ custom_field }} IS NULL OR custom_fields_from_mobilecommons.{{ custom_field }} IN ('0', 'No', '')) AND tmp_custom_fields_per_phone_number.{{ custom_field }} = 1)
        {% elif custom_field in custom_fields_dates_on_mobile_commons %}
          OR custom_fields_from_mobilecommons.{{ custom_field }} != tmp_custom_fields_per_phone_number.{{ custom_field }}::DATE
        {% else %}
          OR custom_fields_from_mobilecommons.{{ custom_field }} != tmp_custom_fields_per_phone_number.{{ custom_field }}
        {% endif %}
      )
    )
{% endif %}
{% endfor %}
)
AND
(
    FALSE
{% for custom_field in custom_fields %}
{% if custom_field not in custom_fields_nulls_on_mobile_commons %}
    -- Only rows with at least one non-null custom field.
    OR tmp_custom_fields_per_phone_number.{{ custom_field }} IS NOT NULL
{% endif %}
{% endfor %}
)
;
