-- mobilizeamerica
DROP TABLE IF EXISTS tmp_mobilizeamerica_participations;
CREATE TEMP TABLE tmp_mobilizeamerica_participations AS
SELECT
    user_id
  , EXTRACT(EPOCH FROM MAX(created_date)) AS resubscribeable_action_time_epoch
  FROM mobilizeamerica.participations
  GROUP BY user_id
UNION ALL
SELECT
    creator_id AS user_id
  , EXTRACT(EPOCH FROM MAX(created_date)) AS resubscribeable_action_time_epoch
  FROM mobilizeamerica.events
  GROUP BY creator_id;

DROP TABLE IF EXISTS tmp_mobilizeamerica_emails;
CREATE TEMP TABLE tmp_mobilizeamerica_emails AS
SELECT
  TRIM(LOWER(mobilizeamerica_people.email_addresses_address)) AS email
, MAX(mobilizeamerica_people.given_name) AS first_name
, MAX(mobilizeamerica_people.family_name) AS last_name
, MAX(mobilizeamerica_people.postal_addresses_postal_code) AS postal_code
, NULL AS addr1
, NULL AS addr2
, NULL AS city
, NULL AS state_cd
, MAX(mobilizeamerica_people.phone_numbers_number) AS phone_number
, 'mobilizeamerica' AS ext_type
, CAST(MAX(mobilizeamerica_people.person_id) AS CHARACTER VARYING(1024)) AS ext_id
, NULL AS source
, NULL AS subsource
, MAX(tmp_mobilizeamerica_participations.resubscribeable_action_time_epoch) AS resubscribeable_action_time_epoch
FROM
  mobilizeamerica.people AS mobilizeamerica_people
LEFT OUTER JOIN tmp_mobilizeamerica_participations
  ON tmp_mobilizeamerica_participations.user_id = mobilizeamerica_people.user_id
GROUP BY
  TRIM(LOWER(mobilizeamerica_people.email_addresses_address))
;


-- mobilizeio
DROP TABLE IF EXISTS tmp_mobilizeio_emails;
CREATE TEMP TABLE tmp_mobilizeio_emails AS
SELECT
  TRIM(LOWER(mobilizeio_users.email)) AS email
, mobilizeio_users.first_name
, mobilizeio_users.last_name
, mobilizeio_users.fields_zip_code AS postal_code
, NULL AS addr1  /* Address data from mobilize.io are unreliable. */
, NULL AS addr2
, NULL AS city
, NULL AS state_cd
, mobilizeio_users.fields_phone AS phone_number
, 'mobilizeio' AS ext_type
, CAST(mobilizeio_users.user_id AS CHARACTER VARYING(1024)) AS ext_id
, NULL AS source
, NULL AS subsource
, (mobilizeio_users.created_at::double precision / 1000) AS resubscribeable_action_time_epoch
FROM
  mobilizeio.users AS mobilizeio_users
WHERE
  mobilizeio_users.first_name IS NOT NULL
  AND mobilizeio_users.fields_zip_code IS NOT NULL
;


-- shopify
DROP TABLE IF EXISTS tmp_shopify_attributions;
CREATE TEMP TABLE tmp_shopify_attributions AS
SELECT * FROM (
    SELECT
      customer_id
    , created_at
    , landing_site_ref
    , ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at ASC) row_num
    FROM shopify.orders
    WHERE created_at >= '2018-12-30'
) WHERE row_num = 1;

DROP TABLE IF EXISTS tmp_shopify_order_optins;
CREATE TEMP TABLE tmp_shopify_order_optins AS
SELECT
    customer_id
  , EXTRACT(EPOCH FROM MAX(customer_accepts_marketing_updated_at::timestamptz)) AS resubscribeable_action_time_epoch
  FROM shopify.orders
  WHERE customer_accepts_marketing IS TRUE
  GROUP BY customer_id;

DROP TABLE IF EXISTS tmp_shopify_emails;
CREATE TEMP TABLE tmp_shopify_emails AS
SELECT
  TRIM(LOWER(shopify_customers.email)) AS email
, shopify_customers.first_name
, shopify_customers.last_name
, shopify_customers.default_address_zip AS postal_code
, shopify_customers.default_address_address1 AS addr1
, shopify_customers.default_address_address2 AS addr2
, shopify_customers.default_address_city AS city
, shopify_customers.default_address_province_code AS state_cd
, shopify_customers.default_address_phone AS phone_number
, 'shopify' AS ext_type
, CAST(shopify_customers.customer_id AS CHARACTER VARYING(1024)) AS ext_id
, SPLIT_PART(tmp_shopify_attributions.landing_site_ref, '#', 1) AS source
, SPLIT_PART(tmp_shopify_attributions.landing_site_ref, '#', 2) AS subsource
, tmp_shopify_order_optins.resubscribeable_action_time_epoch AS resubscribeable_action_time_epoch
FROM
  shopify.customers AS shopify_customers
LEFT OUTER JOIN tmp_shopify_attributions ON tmp_shopify_attributions.customer_id = shopify_customers.customer_id
LEFT OUTER JOIN tmp_shopify_order_optins ON tmp_shopify_order_optins.customer_id = shopify_customers.customer_id
WHERE shopify_customers.email IS NOT NULL
/* For testing */
/*
UNION
SELECT
  'jkatzbrown+shopifyexporttest4@elizabethwarren.com' AS email
, 'JASON' AS first_name
, 'KATZ-BROWN' AS last_name
, '94801' AS postal_code
, '20 Belvedere Ave' AS addr1
, '' AS addr2
, 'Richmond' AS city
, 'CA' AS state_cd
, '(510) 501 6227' AS phone_number
, 'shopify' AS ext_type
, 12345 AS ext_id
, '' AS source
, '' AS subsource
*/
;


-- ngp
DROP TABLE IF EXISTS tmp_ngp_emails;
CREATE TEMP TABLE tmp_ngp_emails AS
SELECT
  email
, first_name
, last_name
, postal_code
, addr1
, addr2
, city
, state_cd
, phone_number
, 'ngp' AS ext_type
, ext_id
, NULL AS source
, NULL AS subsource
, NULL::double precision AS resubscribeable_action_time_epoch
FROM (
    SELECT
      TRIM(LOWER(ed.email)) AS email
    , c.firstname AS first_name
    , c.lastname AS last_name
    , c.zip5 AS postal_code
    , CASE WHEN c.badvaddress = 0 THEN c.vaddress ELSE NULL END AS addr1
    , NULL AS addr2
    , c.city
    , c.state AS state_cd
    , c.phone AS phone_number
    , CAST(c.vanid AS CHARACTER VARYING(1024)) AS ext_id
    , ROW_NUMBER() OVER (PARTITION BY TRIM(LOWER(ed.email)) ORDER BY ed.datecreated DESC) as row_number
    FROM ngp.contacts c
    INNER JOIN ngp.emaildelta ed USING(vanid)
    INNER JOIN ngp.users u ON (ed.createdby = u.userid)
    WHERE ed.datesuppressed IS NULL
    AND c.datesuppressed IS NULL
    AND ed.emailsourcename = 'User Added'
    AND u.username NOT LIKE '%.api'
)
WHERE row_number = 1
;


-- People who answered "Yes" to email subscribe on Reach.
DROP TABLE IF EXISTS tmp_reach_emails;
CREATE TEMP TABLE tmp_reach_emails AS
SELECT
  TRIM(LOWER(reach_people.email)) AS email
, MAX(reach_people.firstname) AS first_name
, MAX(reach_people.lastname) AS last_name
, MAX(reach_people.ziporpostalcode) AS postal_code
, MAX(reach_people.addressline1) AS addr1
, NULL AS addr2
, MAX(reach_people.city) AS city
, MAX(reach_people.stateorprovince) AS state_cd
, MAX(reach_people.phonenumber) AS phone_number
, 'reach' AS ext_type
, MAX(CAST(reach_actions.pk AS CHARACTER VARYING(1024))) AS ext_id
, NULL AS source
, NULL AS subsource
, EXTRACT(EPOCH FROM MAX(reach_actions.datecanvassed::timestamptz)) AS resubscribeable_action_time_epoch
  FROM van.actions_all AS reach_actions
  JOIN van.people AS reach_people
    ON reach_people.source = reach_actions.source
   AND reach_people.instance = reach_actions.instance
   AND reach_people.pk = reach_actions.pk
   AND reach_people.van_mode = reach_actions.van_mode
 WHERE reach_actions.action IN ('us_email_subscribe', 'nh_ptv_email')
   AND reach_people.email IS NOT NULL
  GROUP BY TRIM(LOWER(reach_people.email))
;


-- van
DROP TABLE IF EXISTS tmp_van_emails;
CREATE TEMP TABLE tmp_van_emails AS
SELECT DISTINCT
  TRIM(LOWER(em.email)) AS email
, c.first_name
, c.last_name
, ad.zip AS postal_code
, ad.voting_address AS addr1
, NULL AS addr2
, ad.city
, ad.state AS state_cd
, ph.phone_number
, 'myc_van_id' AS ext_type
, CAST(c.state_code || '-' || c.myc_van_id AS CHARACTER VARYING(1024)) AS ext_id
, NULL as source
, NULL as subsource
, NULL::double precision AS resubscribeable_action_time_epoch
FROM vansync.contacts_emails_myc em
JOIN vansync.person_records_myc c USING(state_code, myc_van_id)
LEFT JOIN vansync.contacts_addresses_myc ad ON c.state_code = ad.state_code
  AND c.myc_van_id = ad.myc_van_id
  AND c.contacts_address_id = ad.contacts_address_id
  AND c.bad_voting_address = 0
  AND ad.datetime_suppressed IS NULL
LEFT JOIN vansync.contacts_phones_myc ph ON c.state_code = ph.state_code
  AND c.myc_van_id = ph.myc_van_id
  AND c.phone_id = ph.contacts_phone_id
  AND ph.datetime_suppressed IS NULL
LEFT JOIN vansync.users u ON em.created_by_user_id = u.user_id
-- Iowa-specific suppression list
LEFT JOIN (
  SELECT myc_van_id
    FROM analytics_ia.vansync_activists
   WHERE activist_code_name IN ('Campaign Staff','Hostile','Pol. Do Not Contact')
     AND myc_van_id IS NOT NULL
   UNION ALL
  SELECT myc_van_id
    FROM analytics_ia.vansync_candidate
   WHERE master_survey_response_name ILIKE '%GOP%'
     AND msq_mrr_all = 1
     AND myc_van_id IS NOT NULL
) ia_sup
    ON c.myc_van_id = ia_sup.myc_van_id AND c.state_code = 'IA'
-- NH-specific suppression list
LEFT JOIN (
  SELECT myc_van_id
    FROM phoenix_demswarren20_vansync_derived.activist_myc
   WHERE activist_code_id IN (4532077, 4519724, 4561237, 4598797)
     AND state_code = 'NH'
) nh_sup
    ON c.myc_van_id = nh_sup.myc_van_id AND c.state_code = 'NH'
-- NV-specific suppression list
LEFT JOIN (
  SELECT myc_van_id
    FROM phoenix_demswarren20_vansync_derived.activist_myc
   WHERE activist_code_id IN (4602009, 4537994, 4538015) AND state_code = 'NV'
) nv_sup
    ON c.myc_van_id = nv_sup.myc_van_id AND c.state_code = 'NV'
-- Attempt to remove emails added by known bulk uploaders
LEFT JOIN (
  SELECT DISTINCT user_id FROM (
    SELECT u.user_id
    , u.username
    , u.committee_state_code
    , em.datetime_created::date
    , COUNT(DISTINCT em.email) emails
    FROM vansync.users u
    JOIN vansync.contacts_emails_myc em
      ON em.created_by_user_id = u.user_id
      AND em.state_code = u.committee_state_code
    WHERE u.is_api_user = 0
    GROUP BY 1,2,3,4
  )
  WHERE emails > 120
) bulk ON em.created_by_user_id = bulk.user_id
WHERE ia_sup.myc_van_id IS NULL
  AND nh_sup.myc_van_id IS NULL
  AND nv_sup.myc_van_id IS NULL
  AND bulk.user_id IS NULL
-- API added should be captured by other email queries, anyway
  AND u.is_api_user = 0
;


-- mobilecommons
DROP TABLE IF EXISTS tmp_mobilecommons_emails;
CREATE TEMP TABLE tmp_mobilecommons_emails AS
SELECT
  TRIM(LOWER(email)) AS email
, MAX(first_name) AS first_name
, MAX(last_name) AS last_name
, MAX(address_postal_code) AS postal_code
, MAX(address_street1) AS addr1
, MAX(address_street2) AS addr2
, MAX(address_city) AS city
, MAX(address_state) AS state_cd
, MAX(phone_number) AS phone_number
, 'mobilecommons' AS ext_type
, MAX(CAST(profile_id AS CHARACTER VARYING(1024))) AS ext_id
, NULL AS source
, NULL AS subsource
, EXTRACT(EPOCH FROM MAX(created_at::timestamptz)) AS resubscribeable_action_time_epoch
  FROM mobilecommons.profiles
  WHERE email is NOT NULL
  GROUP BY TRIM(LOWER(email));


--- Create combined email list
DROP TABLE IF EXISTS tmp_combined_emails;
CREATE TEMP TABLE tmp_combined_emails AS
SELECT * FROM tmp_mobilizeamerica_emails
UNION ALL
SELECT * FROM tmp_mobilizeio_emails
UNION ALL
SELECT * FROM tmp_shopify_emails
UNION ALL
SELECT * FROM tmp_ngp_emails
UNION ALL
SELECT * FROM tmp_reach_emails
UNION ALL
SELECT * FROM tmp_van_emails
UNION ALL
SELECT * FROM tmp_mobilecommons_emails
;


-- get already-subscribed (or subscribed and then unsubscribed) emails
DROP TABLE IF EXISTS tmp_bsd_emails;
CREATE TEMP TABLE tmp_bsd_emails AS
SELECT
    TRIM(LOWER(cons_email.email)) AS email
  , EXTRACT(EPOCH FROM MAX(cons_email_chapter_subscription.unsub_dt)) AS unsub_epoch
  , BOOL_OR(cons_email_chapter_subscription.isunsub) AS isunsub
  FROM bsd.cons_email
  LEFT JOIN bsd.cons_email_chapter_subscription
    ON cons_email_chapter_subscription.cons_email_id = cons_email.cons_email_id
  GROUP BY TRIM(LOWER(cons_email.email))
;

-- Get blacklist of emails we will never resubscribe:
--  * Anybody who's unsubscribed or resubscribed more than once for any reason
--  * Or anybody who's ever unsubscribed because of hard bounce ("deadrcpt")
DROP TABLE IF EXISTS tmp_resubscribe_blacklist_emails;
CREATE TEMP TABLE tmp_resubscribe_blacklist_emails AS

-- People with more than one unsubscibe or resubscribe for any reason.
SELECT DISTINCT stg_unsubscribe.email
  FROM bsd.stg_unsubscribe
 GROUP BY email, is_resubscribe
HAVING COUNT(*) >= 2

 UNION DISTINCT

-- People with one or more hard bounce ever.
SELECT DISTINCT stg_unsubscribe.email
  FROM bsd.stg_unsubscribe
 WHERE stg_unsubscribe.reason ILIKE '%deadrcpt%'
;

-- Compute (all emails) - (already subscribed emails) + (unsubscribe date before resubscribe date)
SELECT *
  , tmp_bsd_emails.email IS NOT NULL AS is_resubscribe
  , tmp_bsd_emails.unsub_epoch
  FROM tmp_combined_emails
  LEFT OUTER JOIN tmp_bsd_emails
    ON REPLACE(TRIM(LOWER(tmp_bsd_emails.email)), '.', '') = REPLACE(TRIM(LOWER(tmp_combined_emails.email)), '.', '')
  LEFT OUTER JOIN tmp_resubscribe_blacklist_emails
    ON REPLACE(TRIM(LOWER(tmp_resubscribe_blacklist_emails.email)), '.', '') = REPLACE(TRIM(LOWER(tmp_combined_emails.email)), '.', '')
 WHERE
  (
    tmp_bsd_emails.email IS NULL
    OR (
      tmp_bsd_emails.isunsub
      AND tmp_bsd_emails.unsub_epoch IS NOT NULL
      AND (
        tmp_bsd_emails.unsub_epoch < tmp_combined_emails.resubscribeable_action_time_epoch
      )
    )
  )
  AND tmp_resubscribe_blacklist_emails.email IS NULL
  -- For testing on just one email address
  -- AND tmp_combined_emails.email = 'jkatzbrown@elizabethwarren.com'
;
