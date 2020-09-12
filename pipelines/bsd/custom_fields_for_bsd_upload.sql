-- Queries that produce `custom_fields_to_update_in_bsd` table, which
-- has one row per BSD cons for which we want to update a custom field.
--
-- Prepares table of existing custom fields per BSD constituent based on
-- upstream BSD data; then compares with the latest custom field values
-- from `custom_fields_per_cons` to see which fields we should push updates to.

DROP TABLE IF EXISTS tmp_cons_field_value_with_name;
CREATE TEMP TABLE tmp_cons_field_value_with_name AS
SELECT
  cons_field_value.cons_id
, cons_field_value.cons_field_id
, cons_field_value.value_varchar AS value
, cons_field.slug AS name
  FROM bsd.cons_field_value
  JOIN bsd.cons_field
    ON cons_field.cons_field_id = cons_field_value.cons_field_id
;


{% set custom_fields = env.CUSTOM_FIELDS.split(',') %}
DROP TABLE IF EXISTS {{ env.SCHEMA }}.custom_fields_from_bsd;
CREATE TABLE {{ env.SCHEMA }}.custom_fields_from_bsd AS
SELECT
  cons.cons_id
{% for custom_field in custom_fields %}
, custom_columns_{{ custom_field }}.value AS {{ custom_field }}
{% endfor %}

FROM bsd.cons

{% for custom_field in custom_fields %}
LEFT JOIN tmp_cons_field_value_with_name AS custom_columns_{{ custom_field }}
  ON custom_columns_{{ custom_field }}.name = {{ custom_field | pprint }}
 AND custom_columns_{{ custom_field }}.cons_id = cons.cons_id
{% endfor %}
;

DROP TABLE IF EXISTS {{ env.SCHEMA }}.custom_fields_to_update_in_bsd;
CREATE TABLE {{ env.SCHEMA }}.custom_fields_to_update_in_bsd AS
SELECT DISTINCT
  custom_fields_per_cons.cons_id
{% for custom_field in custom_fields %}
, CAST(custom_fields_per_cons.{{ custom_field }} AS CHARACTER VARYING(1024)) AS {{ custom_field }}
{% endfor %}

  FROM {{ env.SCHEMA }}.custom_fields_from_bsd
  JOIN {{ env.SCHEMA }}.custom_fields_per_cons
    ON custom_fields_per_cons.ds = {{ env.DS | pprint }}
   AND custom_fields_per_cons.cons_id = custom_fields_from_bsd.cons_id

 WHERE
(
    FALSE
{% for custom_field in custom_fields %}
    OR (
      -- We don't want to upload if value to upload is 0 and it's null upstream.
      (NOT (custom_fields_per_cons.{{ custom_field }} = 0 AND (custom_fields_from_bsd.{{ custom_field }} IS NULL OR custom_fields_from_bsd.{{ custom_field }} = '')))
      AND (
        -- If value is null/empty upstream should be updated to be non-null.
        ((custom_fields_from_bsd.{{ custom_field }} IS NULL OR custom_fields_from_bsd.{{ custom_field }} = '') AND custom_fields_per_cons.{{ custom_field }} IS NOT NULL)

        -- If value is non-null/nonempty upstream should be null.
        OR (custom_fields_from_bsd.{{ custom_field }} IS NOT NULL AND custom_fields_from_bsd.{{ custom_field }} != '' AND custom_fields_per_cons.{{ custom_field }} IS NULL)

        -- If the values are both non-null/nonempty and different.
        OR custom_fields_from_bsd.{{ custom_field }} != custom_fields_per_cons.{{ custom_field }}
      )
    )
{% endfor %}
)
AND
(
    FALSE
{% for custom_field in custom_fields %}
    -- Only rows with at least one non-null custom field.
    OR custom_fields_per_cons.{{ custom_field }} IS NOT NULL
{% endfor %}
)
;
