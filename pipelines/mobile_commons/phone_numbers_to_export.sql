/* All ActBlue donors in last 30 days */
DROP TABLE IF EXISTS tmp_sms_opt_in_phone_numbers_bsd_contribution;
CREATE TEMP TABLE tmp_sms_opt_in_phone_numbers_bsd_contribution AS
SELECT
    phone AS phone_number
  , MAX(firstname) AS first_name
  , MAX(lastname) AS last_name
  , MAX(email) AS email
  , MAX(CASE
        WHEN LPAD(LEFT(zip, 5), 5, '0') SIMILAR TO '[0-9]{5}'
          THEN LPAD(LEFT(zip, 5), 5, '0')
        ELSE NULL END) AS postal_code
  , MAX(addr1) AS addr1
  , MAX(addr2) AS addr2
  , MAX(city) AS city
  , MAX(state_cd) AS state
  , MAX(country) AS country
  , MAX(charge_dt) AS action_date
  , 'bsd_contribution' AS action_type
FROM bsd.stg_contribution
WHERE phone IS NOT NULL
  AND (phone SIMILAR TO '[2-9][0-9]{9}' OR phone SIMILAR TO '1[2-9][0-9]{9}')
  AND contribution_page_id = 350
  AND DATEDIFF(days, convert_timezone('America/New_York', charge_dt), convert_timezone('America/New_York', GETDATE())) <= 30
GROUP BY 1
;

/* The next two tables are for Mobilize America:
 * we want every SMS-opted-in phone number that's had Mobilize America activity
 * in the last 30 days. */
/* This first table is the most recent activity per email address.
 * We don't use user_id/person_id because... turns out they are not comparable!
 * I guess because mobilizeamerica.people.person_id comes from API and
 * mobilizeamerica.participations.user_id comes from DB mirror and somehow they
 * do not match. */
DROP TABLE IF EXISTS tmp_mobilizeamerica_last_action;
CREATE TEMP TABLE tmp_mobilizeamerica_last_action AS
SELECT
  email
, action_date AS last_action_date
, action_type AS last_action_type
FROM (
  SELECT
    email
  , action_date
  , action_type
  , ROW_NUMBER() OVER (PARTITION BY email ORDER BY action_date DESC) AS row_num
  FROM (
    SELECT
      TRIM(LOWER(email_addresses_address)) AS email
    , timestamp 'epoch' + created_date * interval '1 second' AS action_date
    , 'people_created_date' AS action_type
    FROM mobilizeamerica.people
    UNION ALL
    SELECT
      TRIM(LOWER(user__email_address)) AS email
    , modified_date as action_date
    , 'participations_modified_date' AS action_type
    FROM mobilizeamerica.participations
    UNION ALL
    SELECT
      TRIM(LOWER(owner__email_address)) AS email
    , owner__modified_date as action_date
    , 'events_owner__modified_date' AS action_type
    FROM mobilizeamerica.events
    UNION ALL
    SELECT
      TRIM(LOWER(creator__email_address)) AS email
    , creator__modified_date as action_date
    , 'events_creator__modified_date' AS action_type
    FROM mobilizeamerica.events
    UNION ALL
    SELECT
      TRIM(LOWER(reviewed_by__email_address)) AS email
    , reviewed_by__modified_date as action_date
    , 'events_reviewed_by__modified_date' AS action_type
    FROM mobilizeamerica.events
  )
  WHERE email IS NOT NULL
)
WHERE row_num = 1
ORDER BY last_action_date DESC
;


/* Take each phone number in mobilizeamerica.people, join the last activity
 * table by email address, and keep phone numbers that are opted-in and active
 * in last 30 days. */
DROP TABLE IF EXISTS tmp_sms_opt_in_phone_numbers_mobilizeamerica;
CREATE TEMP TABLE tmp_sms_opt_in_phone_numbers_mobilizeamerica AS
SELECT
    mobilizeamerica_people.phone_numbers_number AS phone_number
  , MAX(mobilizeamerica_people.given_name) AS first_name
  , MAX(mobilizeamerica_people.family_name) AS last_name
  , MAX(TRIM(LOWER(mobilizeamerica_people.email_addresses_address))) AS email
  , MAX(CASE
        WHEN LPAD(LEFT(mobilizeamerica_people.postal_addresses_postal_code, 5), 5, '0') SIMILAR TO '[0-9]{5}'
          THEN LPAD(LEFT(mobilizeamerica_people.postal_addresses_postal_code, 5), 5, '0')
        ELSE NULL END) AS postal_code
  , NULL AS addr1
  , NULL AS addr2
  , NULL AS city
  , NULL AS state
  , 'US' AS country
  , MAX(tmp_mobilizeamerica_last_action.last_action_date) AS action_date
  , 'mobilizeamerica' AS action_type
FROM mobilizeamerica.people AS mobilizeamerica_people
JOIN tmp_mobilizeamerica_last_action
  ON tmp_mobilizeamerica_last_action.email = TRIM(LOWER(mobilizeamerica_people.email_addresses_address))
WHERE mobilizeamerica_people.phone_numbers_number IS NOT NULL
  AND mobilizeamerica_people.sms_opt_in_status = 'OPT_IN'
  AND DATEDIFF(days, convert_timezone('America/New_York', tmp_mobilizeamerica_last_action.last_action_date::timestamp), convert_timezone('America/New_York', GETDATE())) <= 30
  AND (mobilizeamerica_people.phone_numbers_number SIMILAR TO '[2-9][0-9]{9}' OR mobilizeamerica_people.phone_numbers_number SIMILAR TO '1[2-9][0-9]{9}')
GROUP BY phone_number
;

/* Part 3a: SMS opt-in phone numbers from Iowa VAN */
DROP TABLE IF EXISTS tmp_sms_opt_in_phone_numbers_van_IA;
CREATE TEMP TABLE tmp_sms_opt_in_phone_numbers_van_IA AS
SELECT
  mc.phone_number
, first_name
, last_name
, e.email
, zip AS postal_code
, voting_address AS addr1
, NULL AS addr2
, city
, state
, country
, NULL::timestamp AS action_date
, 'van_IA' AS action_type
FROM phoenix_demswarren20_vansync.contacts_phones_myc mc
LEFT JOIN (
  SELECT mcp.myc_van_id
       , first_name
       , last_name
       , CASE WHEN first_name IS NOT NULL THEN 'US' ELSE NULL END AS country
  FROM phoenix_demswarren20_vansync.person_records_myc mcp
) USING(myc_van_id)
LEFT JOIN (
  SELECT myc_van_id
    , voting_address
    , city
    , state
    , zip
    , row_number()
        over(
          partition BY myc_van_id
              ORDER BY date_modified DESC) AS rn
    FROM phoenix_demswarren20_vansync_derived.addresses_myc
  ) AS a
  ON mc.myc_van_id = a.myc_van_id
  AND mc.state_code = a.state
  AND a.rn = 1
LEFT JOIN (
  SELECT myc_van_id
       , email
       , row_number()
           over(
             partition BY myc_van_id
                 ORDER BY datetime_modified desc) rn
    FROM phoenix_demswarren20_vansync.contacts_emails_myc
) e ON mc.myc_van_id = e.myc_van_id AND e.rn = 1
LEFT JOIN phoenix_phones_ref.wireless_blocks wb ON ((wb.npa || wb.nxx || wb.x_digit) = substring(mc.phone_number, 1, 7))
LEFT JOIN phoenix_phones_ref.wireless_from_landline wfl ON (wfl.phone_number = mc.phone_number)
WHERE phone_source_id = '2'  -- Only manually-added numbers.
AND (wfl.phone_number IS NOT NULL OR wb.npa IS NOT NULL)
AND mc.state_code = 'IA'
AND mc.myc_van_id IN (
  SELECT myc_van_id
  FROM analytics_ia.vansync_activists
  WHERE activist_code_name = 'Get Texts'  -- This activist code means SMS opt in.
    AND DATEDIFF(days, convert_timezone('America/New_York', datetime_created), convert_timezone('America/New_York', GETDATE())) <= 30
)
AND first_name IS NOT NULL
;

DROP TABLE IF EXISTS tmp_sms_opt_in_phone_numbers_van_NH;
CREATE TEMP TABLE tmp_sms_opt_in_phone_numbers_van_NH AS
SELECT DISTINCT
  contacts_phones_myc.phone_number
, person_records_myc.first_name
, person_records_myc.last_name
, email.email
, address.zip as postal_code
, address.voting_address AS addr1
, NULL AS addr2
, address.city
, address.state
, 'US' as country
, activist_myc.date_created as action_date
, 'van_NH' AS action_type
  FROM vansync_nh.activist_myc
  LEFT JOIN vansync_nh.contacts_phones_myc
    ON contacts_phones_myc.myc_van_id=activist_myc.myc_van_id
   AND DATE(contacts_phones_myc.datetime_created)=DATE(activist_myc.date_created)
  LEFT JOIN vansync_nh.person_records_myc
    ON activist_myc.myc_van_id=person_records_myc.myc_van_id
  LEFT JOIN (
  SELECT * FROM (
    SELECT
      myc_van_id
    , city
    , state
    , zip
    , voting_address
    , row_number() OVER (PARTITION BY myc_van_id ORDER BY datetime_modified DESC) AS rn
      FROM vansync_nh.contacts_addresses_myc
  ) WHERE rn=1
) address
    ON activist_myc.myc_van_id=address.myc_van_id
  LEFT JOIN (
  SELECT * FROM (
    SELECT
      myc_van_id
    , email
    , row_number() OVER (PARTITION BY myc_van_id ORDER BY datetime_modified DESC) AS rn
  FROM vansync_nh.contacts_emails_myc) WHERE rn=1
) email
    ON activist_myc.myc_van_id=email.myc_van_id
 WHERE activist_code_id IN ('4525863','4519733','4586636', '4616951') AND cell_status_id IN (1,2)
;

/* Part 3b: SMS opt-in phone numbers from the other three early states' VAN */
{% set early_states_and_activist_code_ids = [['SC', "'4530463'"], ['NV', "'4537068'"]] %}
{% for early_state_and_activist_code_id in early_states_and_activist_code_ids %}
{% set early_state = early_state_and_activist_code_id[0] %}
{% set activist_code_id = early_state_and_activist_code_id[1] %}

DROP TABLE IF EXISTS tmp_sms_opt_in_phone_numbers_van_{{ early_state }};
CREATE TEMP TABLE tmp_sms_opt_in_phone_numbers_van_{{ early_state }} AS
WITH people AS (

  SELECT
    DISTINCT mcp.myc_van_id
   , first_name
   , last_name
   , mcp.state_code
   , CASE WHEN first_name IS NOT NULL THEN 'US' ELSE NULL END AS country
  FROM phoenix_demswarren20_vansync.contacts_activist_codes_myc AS ac
  JOIN phoenix_demswarren20_vansync.person_records_myc AS mcp
    ON mcp.myc_van_id = ac.myc_van_id
    AND mcp.state_code = ac.state_code
  WHERE
    activist_code_id IN ({{ activist_code_id }})  -- optins
    AND mcp.state_code = {{ early_state | pprint }}
   -- opted in in the last 30 days
    AND DATEDIFF(
      days, convert_timezone('America/New_York', ac.datetime_created),
      convert_timezone('America/New_York', GETDATE())) <= 31
)

, add_info AS (

  SELECT
    DISTINCT mc.phone_number
    , people.myc_van_id
    , first_name
    , last_name
    , e.email
    , zip AS postal_code
    , voting_address AS addr1
    , NULL AS addr2
    , city
    , state
    , country
    , NULL::timestamp AS action_date
    , 'van_{{ early_state }}' AS action_type
    , phone_source_id
  FROM people
  -- get phones
  LEFT JOIN phoenix_demswarren20_vansync.contacts_phones_myc AS mc
    on people.myc_van_id = mc.myc_van_id
    AND people.state_code = mc.state_code
  -- remove wrong numbers
  LEFT JOIN phoenix_demswarren20_vansync.contacts_contacts_myc AS cc
    on people.myc_van_id = cc.myc_van_id
    AND people.state_code = cc.state_code
    AND mc.contacts_phone_id = cc.contacts_phone_id
  -- get emails
  LEFT JOIN (
    SELECT myc_van_id
      , email
      , state_code
      , row_number()
          over(
            partition BY myc_van_id
                ORDER BY datetime_modified DESC) AS rn
      FROM phoenix_demswarren20_vansync.contacts_emails_myc) AS e
   ON people.myc_van_id = e.myc_van_id
   AND people.state_code = e.state_code
   AND e.rn = 1
  -- get address
  LEFT JOIN (
    SELECT myc_van_id
      , voting_address
      , city
      , state
      , zip
      , row_number()
          over(
            partition BY myc_van_id
                ORDER BY date_modified DESC) AS rn
      FROM phoenix_demswarren20_vansync_derived.addresses_myc) AS a
    ON people.myc_van_id = a.myc_van_id
    AND people.state_code = a.state
    AND a.rn = 1
  WHERE cc.result_id <> '20'
)

, final AS (

  SELECT
      a.phone_number
    , first_name
    , last_name
    , email
    , postal_code
    , addr1
    , addr2
    , city
    , state
    , country
    , action_date
    , action_type
    , CASE WHEN wtl.phone_number IS NOT NULL THEN 'L'
           WHEN wfl.phone_number IS NOT NULL THEN 'C'
           WHEN wb.npa IS NOT NULL THEN 'C'
           ELSE NULL END AS phone_type
  FROM add_info AS a

  LEFT JOIN phoenix_phones_ref.wireless_blocks wb
    ON ((wb.npa || wb.nxx || wb.x_digit) = substring(a.phone_number, 1, 7))
  LEFT JOIN phoenix_phones_ref.wireless_from_landline wfl
    ON (wfl.phone_number = a.phone_number)
  LEFT JOIN phoenix_phones_ref.wireless_to_landline wtl
    ON (wtl.phone_number = a.phone_number)

  WHERE phone_source_id = '2'  -- Only manually-added numbers.
  AND phone_type = 'C'
  AND (wfl.phone_number IS NOT NULL OR wb.npa IS NOT NULL)
  AND state = {{ early_state | pprint }}
  AND first_name IS NOT NULL
)

SELECT
      phone_number
    , first_name
    , last_name
    , email
    , postal_code
    , addr1
    , addr2
    , city
    , state
    , country
    , action_date
    , action_type
FROM final;

{% endfor %}


/* Part 4: SMS opt-in phone numbers from Reach */
/* These committee IDs come from this query:
SELECT committee_id, COUNT(*)
  FROM reach.responses
 WHERE responses.survey_question ILIKE '%SMS%'
 GROUP BY committee_id
;
*/
{% set early_states_and_reach_committee_ids = [['national', '91'], ['IA', '95'], ['NV', '98']] %}
{% for early_state_and_reach_committee_id in early_states_and_reach_committee_ids %}
{% set early_state = early_state_and_reach_committee_id[0] %}
{% set reach_committee_id = early_state_and_reach_committee_id[1] %}

DROP TABLE IF EXISTS tmp_sms_opt_in_phone_numbers_reach_canvass_{{ early_state }};
CREATE TEMP TABLE tmp_sms_opt_in_phone_numbers_reach_canvass_{{ early_state }} AS
SELECT
DISTINCT
  people.phone AS phone_number
, people.first_name
, people.last_name
, people.email
, people.zip_code AS postal_code
, people.address_line_1 AS addr1
, people.address_line_2 AS addr2
, people.city
, people.state
, 'US' AS country
, responses.canvass_date AS action_date
, 'reach_canvass_{{ early_state }}' AS action_type
  FROM reach.people
  JOIN reach.responses ON responses.reach_id = people.reach_id
 WHERE responses.survey_question ILIKE '%SMS%'
   AND responses.survey_response ILIKE 'Yes'
   AND responses.committee_id = {{ reach_committee_id }}
   AND people.phone IS NOT NULL
;

{% endfor %}

-- Part 5: Shifter
DROP TABLE IF EXISTS tmp_sms_opt_in_phone_numbers_shifter;
CREATE TEMP TABLE tmp_sms_opt_in_phone_numbers_shifter AS
SELECT
  phone_number
, first_name
, last_name
, email
, postal_code
, addr1
, addr2
, city
, state
, country
, action_date
, action_type
FROM (
SELECT
    RIGHT(phone, 10) AS phone_number
  , given_name AS first_name
  , family_name AS last_name
  , email
  , CASE WHEN LPAD(LEFT(zip5, 5), 5, '0') SIMILAR TO '[0-9]{5}'
         THEN LPAD(LEFT(zip5, 5), 5, '0')
         ELSE NULL END AS postal_code
  , NULL AS addr1
  , NULL AS addr2
  , NULL AS city
  , NULL AS state
  , 'US' AS country
  , created_at AS action_date
  , 'shifter' AS action_type
  , ROW_NUMBER() OVER (PARTITION BY phone_number ORDER BY created_at DESC) AS row_num
FROM supportal.shifter_event_signup_attempts
WHERE phone IS NOT NULL
  AND sms_opt_in
  AND (RIGHT(phone, 10) SIMILAR TO '[2-9][0-9]{9}' OR RIGHT(phone, 10) SIMILAR TO '1[2-9][0-9]{9}')
)
WHERE row_num = 1
;


/* Part 4. Combine all tables from parts 1-3 */
DROP TABLE IF EXISTS tmp_sms_opt_in_phone_numbers_combined;
CREATE TEMP TABLE tmp_sms_opt_in_phone_numbers_combined AS
SELECT * FROM tmp_sms_opt_in_phone_numbers_bsd_contribution
UNION ALL
SELECT * FROM tmp_sms_opt_in_phone_numbers_mobilizeamerica
UNION ALL
SELECT * FROM tmp_sms_opt_in_phone_numbers_van_IA
UNION ALL
SELECT * FROM tmp_sms_opt_in_phone_numbers_van_NH
{% for early_state_and_activist_code_id in early_states_and_activist_code_ids %}
{% set early_state = early_state_and_activist_code_id[0] %}
UNION ALL
SELECT * FROM tmp_sms_opt_in_phone_numbers_van_{{ early_state }}
{% endfor %}
{% for early_state_and_reach_committee_id in early_states_and_reach_committee_ids %}
{% set early_state = early_state_and_reach_committee_id[0] %}
UNION ALL
SELECT * FROM tmp_sms_opt_in_phone_numbers_reach_canvass_{{ early_state }}
{% endfor %}
UNION ALL
SELECT * FROM tmp_sms_opt_in_phone_numbers_shifter
/* For testing, if you want to test with another number that hasn't made a donation or RSVP lately. */
/*
UNION ALL
SELECT
  '5105016227' AS phone_number
, 'Jason' AS first_name
, 'Katz-Brown' AS last_name
, 'jasonkatzbrown@gmail.com' AS email
, '02145' AS postal_code
, '10 Hathorn St' AS addr1
, NULL AS addr2
, 'Somerville' AS city
, 'MA' AS state
, 'US' AS country
, '2019-06-01' AS action_date
, 'bsd_contribution' AS action_type
*/
;

DROP TABLE IF EXISTS tmp_mobilecommons_phone_numbers;
CREATE TEMP TABLE tmp_mobilecommons_phone_numbers AS
SELECT RIGHT(profiles.phone_number, 10) AS phone_number
FROM mobilecommons.profiles
WHERE status != 'Profiles with no Subscriptions'
/* For testing on only one profile */
/* AND phone_number NOT IN ('15105016227', '12066056687', '14088323962') */
GROUP BY 1
;

SELECT tmp_sms_opt_in_phone_numbers_combined.*
FROM tmp_sms_opt_in_phone_numbers_combined
LEFT OUTER JOIN tmp_mobilecommons_phone_numbers ON tmp_mobilecommons_phone_numbers.phone_number = tmp_sms_opt_in_phone_numbers_combined.phone_number
WHERE tmp_mobilecommons_phone_numbers.phone_number IS NULL
/* For testing on only one profile */
/* AND tmp_sms_opt_in_phone_numbers_combined.phone_number IN ('5105016227', '2066056687', '4088323962') */
;
