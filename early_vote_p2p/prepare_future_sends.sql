-- Jason Katz-Brown 2018
-- Hustle EVIP invite pipeline. Documentation:
-- https://docs.google.com/document/d/1CHJKSxkInuF3-rJq6q0ePh9jkC0Zxa8uTL4JAD7OU48/edit

-- This temp table, and the scores-related columns of registration_cfi
-- below, are not needed for Hustle; they're just for convenience so
-- these tables can be reused in other Iowa pipelines.
  DROP table IF EXISTS cfi_average_gotv_per_precinct;
CREATE LOCAL TEMP TABLE cfi_average_gotv_per_precinct
    ON COMMIT PRESERVE ROWS AS
SELECT precinct.vanprecinctid
     , count(registration.personid) AS persons
     , sum(gotv_scores.hubbell_gotv_score_v2) AS total_gotv_score
     , count(gotv_scores.hubbell_gotv_score_v2) AS persons_scored
     , sum(gotv_scores.hubbell_gotv_score_v2) / count(gotv_scores.hubbell_gotv_score_v2) AS avg_in_univ_gotv_score
  FROM golden_ia.registration
  LEFT JOIN sp_ia_katzbrownj.cfi_gotv_univ_latest AS cfi_univ
 USING (vanid)
  LEFT JOIN golden_ia.precinct
 USING (precinctid)
  LEFT JOIN scores_2018.clarity_dga_ia_gotv_v2_20181001 AS gotv_scores
    ON gotv_scores.personid = registration.personid
 WHERE cfi_univ.vanid IS NOT NULL
   AND registration.iscurrent
 GROUP BY 1
 ORDER BY 5 DESC;


-- Rollup of registration table.
-- Not all fields are necessary for Hustle creation. Some are optional;
-- e.g. we try to ballpark an "isStrongDem" later based on some signals
-- that indicate that we've Dem-ID'd the voter this cycle, and then
-- target our Hustles to Dem-ID'd voters to immediately tell voter about
-- EVIP site, and non-Dem-ID'd voters to do a Dem ID first.
-- And we have two in-universe flags that we use to compute who we
-- target for Hustles. Now we're targeting people in Campaign for Iowa
-- base universe plus more people in targeted state senate districts;
-- minus other exclusions (was in our most recent absentee ballot request)
  DROP TABLE IF EXISTS registration_cfi;
CREATE TABLE registration_cfi AS /*+direct*/
SELECT precinct.vanprecinctid
     , person.county_name
     , person.county_fips
     , registration.vanid
     , distinct_myc_id.myc_vanid
     , registration.regaddressid
     , person.address_type
     , CASE WHEN cfi_univ.vanid IS NULL THEN 0 ELSE 1 END AS in_univ
     , CASE WHEN cfi_pre_gotv_canv_univ.vanid IS NULL THEN 0 ELSE 1 END AS in_pre_gotv_canv_univ
     , CASE WHEN cfi_canv_univ.vanid IS NULL THEN 0 ELSE 1 END AS in_canv_univ
     , CASE WHEN cfi_gotv_univ.vanid IS NULL THEN 0 ELSE 1 END AS in_gotv_univ
     , CASE WHEN cfi_evip_hustle_univ.vanid IS NULL THEN 0 ELSE 1 END AS in_evip_hustle_univ
     , CASE WHEN cfi_orig_univ.vanid IS NULL THEN 0 ELSE 1 END AS in_orig_univ
     , CASE WHEN cfi_orig_univ.vanid IS NOT NULL AND cfi_canv_univ.vanid IS NOT NULL THEN 1 ELSE 0 END AS in_orig_univ_and_canv_univ
     , CASE WHEN cfi_abr_mail_flight_4.vanid IS NULL THEN 0 ELSE 1 END AS in_abr_mail_flight_4_univ
     , CASE WHEN absentee_any.vanid IS NULL THEN 0 ELSE 1 END AS abr_sos
     , CASE WHEN rvl_vbm_collected.vanid IS NULL THEN 0 ELSE 1 END AS abr_myv
     , CASE WHEN absentee_any.vanid IS NULL AND rvl_vbm_collected.vanid IS NULL THEN 1 ELSE 0 END AS no_abr
     , CASE WHEN absentee_voted.vanid IS NULL THEN 0 ELSE 1 END AS voted_sos
     , CASE WHEN absentee_voted.vanid IS NULL THEN 1 ELSE 0 END AS no_vote
     , gotv_scores.hubbell_gotv_score_v2 AS gotv_score_raw
     , CASE WHEN gotv_scores.hubbell_gotv_score_v2 IS NOT NULL THEN gotv_scores.hubbell_gotv_score_v2
            WHEN cfi_univ.vanid IS NOT NULL THEN nvl(cfi_average_gotv_per_precinct.avg_in_univ_gotv_score, 0)
            WHEN cfi_gotv_univ.vanid IS NOT NULL THEN nvl(cfi_average_gotv_per_precinct.avg_in_univ_gotv_score, 0)
            WHEN cfi_canv_univ.vanid IS NOT NULL THEN nvl(cfi_average_gotv_per_precinct.avg_in_univ_gotv_score, 0)
            ELSE 0 END AS gotv_score
     , support_scores.hubbell_support_score_v2 AS support_score
     , CASE WHEN rvl_strong_dem.contactssurveyresponseid IS NULL THEN 0 ELSE 1 END AS strong_dem
     , CASE WHEN rvl_vol_ask.contactssurveyresponseid IS NULL THEN 0 ELSE 1 END AS has_survey_vol_ask
     , CASE WHEN rvl_abr_interested.contactssurveyresponseid IS NULL THEN 0 ELSE 1 END AS has_survey_abr_interested
     , CASE WHEN registration.partyid = 1 THEN 1 ELSE 0 END AS registration_party_dem
     , CASE WHEN registration.partyid = 2 THEN 1 ELSE 0 END AS registration_party_repub
     , nvl(person_votes.vote_g2014_method_absentee, 0) AS vote_g2014_method_absentee
     , initcap(person.first_name) AS first_name
     , initcap(person.last_name) AS last_name
     , person.age
     , person.combined_ethnicity_full
     , person.us_cong_district_latest
     , person.state_senate_district_latest
     , person.state_house_district_latest
     , person.city
     , person.zipcode
     , person.latitude
     , person.longitude
     , person.out_of_state_ncoa
     , 'chat+' || registration.vanid || '@campaignforiowa.com' AS coalesced_email
  FROM golden_ia.registration
  LEFT JOIN cfi_univ_latest AS cfi_univ  -- The pre-GOTV base universe.
 USING (vanid)
  LEFT JOIN cfi_canv_univ_latest AS cfi_canv_univ  -- The Doors version of GOTV universe.
 USING (vanid)
  LEFT JOIN cfi_pre_gotv_canv_univ_latest AS cfi_pre_gotv_canv_univ  -- The pre-GOTV universe Doors target.
 USING (vanid)
  LEFT JOIN cfi_gotv_univ_latest AS cfi_gotv_univ
 USING (vanid)
  LEFT JOIN cfi_evip_hustle_univ_latest AS cfi_evip_hustle_univ
 USING (vanid)
  LEFT JOIN cfi_orig_univ_latest AS cfi_orig_univ
 USING (vanid)
  LEFT JOIN sp_ia_katzbrownj.cfi_abr_mail_flight_4
 USING (vanid)
  LEFT JOIN golden_ia.precinct
 USING (precinctid)
  LEFT JOIN org_sp_ia_vansync_live.dnc_contactsabsentees AS absentee_any
 USING (vanid)
  LEFT JOIN org_sp_ia_vansync_live.dnc_contactsabsentees AS absentee_voted
    ON absentee_voted.vanid = registration.vanid
   AND (absentee_voted.ballotreceived IS NOT NULL OR absentee_voted.earlyvoted IS NOT NULL)
  LEFT JOIN org_sp_ia_vansync_live.responses_voter_live AS rvl_vbm_collected
    ON rvl_vbm_collected.vanid = registration.vanid
   AND rvl_vbm_collected.committeeid IN (67923, 70588)
   AND rvl_vbm_collected.mastersurveyresponseid = 16552
   AND rvl_vbm_collected.msq_currency = 1
  LEFT JOIN org_sp_ia_vansync_live.responses_voter_live AS rvl_strong_dem
    ON rvl_strong_dem.vanid = registration.vanid
   AND rvl_strong_dem.mastersurveyresponseid = 16980
   AND rvl_strong_dem.msq_currency = 1
  LEFT JOIN org_sp_ia_vansync_live.responses_voter_live AS rvl_vol_ask
    ON rvl_vol_ask.vanid = registration.vanid
   AND (rvl_vol_ask.surveyquestionid = 287117  -- "CC Volunteer" question
     OR rvl_vol_ask.surveyquestionid = 270384  -- "2018 Volunteer" (House) question
     OR rvl_vol_ask.surveyquestionid = 253103  -- "2018: Sen Volunteer" question
     OR rvl_vol_ask.mastersurveyresponseid = 15574)  -- "DCCC Volunteer" "Yes" response
   AND rvl_vol_ask.msq_currency = 1
  LEFT JOIN org_sp_ia_vansync_live.responses_voter_live AS rvl_abr_interested
    ON rvl_abr_interested.vanid = registration.vanid
   AND rvl_abr_interested.surveyresponseid = 1290726  -- "CFI ABR Mail Chase" "Will mail back"
   AND rvl_abr_interested.msq_currency = 1
  LEFT JOIN scores_2018.clarity_dga_ia_gotv_v2_20181001 AS gotv_scores
    ON gotv_scores.personid = registration.personid
  LEFT JOIN scores_2018.clarity_dga_ia_support_v2_20181001 AS support_scores
    ON support_scores.personid = registration.personid
  LEFT JOIN analytics_ia.person_votes
    ON person_votes.personid = registration.personid
  LEFT JOIN cfi_average_gotv_per_precinct
    ON cfi_average_gotv_per_precinct.vanprecinctid = precinct.vanprecinctid
  LEFT JOIN analytics_ia.person
    ON person.personid = registration.personid
  LEFT JOIN (
    SELECT votervanid
         , max(vanid) AS myc_vanid
      FROM org_sp_ia_vansync_live.dnc_contacts_myc
     WHERE dnc_contacts_myc.personcommitteeid = 67923
     GROUP BY 1
  ) distinct_myc_id
    ON distinct_myc_id.votervanid = registration.vanid
 WHERE registration.iscurrent
 ORDER BY 1, 2, 3;


CREATE OR REPLACE FUNCTION compute_distance_between_points(lat0 FLOAT, lng0 FLOAT, lat1 FLOAT, lng1 FLOAT) RETURN FLOAT AS BEGIN
    RETURN 3959 * acos (
      cos(radians(lat0))
      * cos(radians(lat1))
      * cos(radians(lng1) - radians(lng0))
      + sin(radians(lat0))
      * sin(radians(lat1))
    );
END;


--Map every date to its phase (basically, week, except the first batch).
DROP table IF EXISTS cfi_evip_invite_phase_mapping;
CREATE LOCAL TEMP TABLE cfi_evip_invite_phase_mapping
    ON COMMIT PRESERVE ROWS AS
SELECT 'Phase' AS timeframe
     , d.date AS ds
     , MAX(phase_start_dates.date) AS timeframe_start
     , MIN(phase_end_dates.date) AS timeframe_end
  FROM dim.dates AS d
  JOIN dim.dates AS phase_start_dates
    ON phase_start_dates.date <= d.date
   AND phase_start_dates.date IN ('2018-11-05', '2018-11-03', '2018-10-29', '2018-10-27', '2018-10-22', '2018-10-15', '2018-10-11')
  JOIN dim.dates AS phase_end_dates
    ON phase_end_dates.date >= d.date
   AND phase_end_dates.date IN ('2018-11-06', '2018-11-04', '2018-11-02', '2018-10-28', '2018-10-26', '2018-10-21', '2018-10-14')
 WHERE d.date >= '2018-10-11'
   AND d.date <= '2018-11-06'
 GROUP BY 1, 2
 ORDER BY 1, 2 DESC;


--One row per phase.
DROP TABLE IF EXISTS cfi_evip_invite_phases;
CREATE LOCAL TEMP TABLE cfi_evip_invite_phases
    ON COMMIT PRESERVE ROWS AS
SELECT DISTINCT timeframe
     , timeframe_start
     , timeframe_end
  FROM cfi_evip_invite_phase_mapping
 ORDER BY 1, 2 DESC;


-- Our universe for EVIP Hustle!
-- Base Universe + Targeted SD Adds
-- Minus those with SoS absentee record
  DROP table IF EXISTS registration_cfi_for_evip_hustle;
CREATE LOCAL TEMP TABLE registration_cfi_for_evip_hustle
    ON COMMIT PRESERVE ROWS AS
SELECT *
  FROM registration_cfi
 WHERE in_evip_hustle_univ = 1
   AND abr_sos = 0
   AND NOT out_of_state_ncoa
  ;


-- week * EVIP invite universe * evip sites <= distances from evip sites open that week.
  DROP table IF EXISTS cfi_evip_sites_by_voter_by_phase;
CREATE LOCAL TEMP TABLE cfi_evip_sites_by_voter_by_phase
    ON COMMIT PRESERVE ROWS AS
SELECT cfi_evip_invite_phases.timeframe_start
     , cfi_evip_invite_phases.timeframe_end
     , registration_cfi_for_evip_hustle.county_name
     , registration_cfi_for_evip_hustle.vanprecinctid
     , compute_distance_between_points(registration_cfi_for_evip_hustle.latitude, registration_cfi_for_evip_hustle.longitude, cfi_evip_sites.latitude, cfi_evip_sites.longitude) AS distance_to_evip_site
     , registration_cfi_for_evip_hustle.vanid
     , registration_cfi_for_evip_hustle.regaddressid
     , cfi_evip_sites.iwillvoteid
     , cfi_evip_sites.location
     , cfi_evip_sites.address
     , cfi_evip_sites.hustle_date
     , cfi_evip_sites.start_date
     , cfi_evip_sites.end_date
     , cfi_evip_sites.start_time
     , cfi_evip_sites.end_time
     , cfi_evip_sites.dates_times_synopsis
     , cfi_evip_sites.rally_blurb
     , registration_cfi_for_evip_hustle.gotv_score
     , registration_cfi_for_evip_hustle.strong_dem
     , registration_cfi_for_evip_hustle.has_survey_vol_ask
     , registration_cfi_for_evip_hustle.has_survey_abr_interested
     , registration_cfi_for_evip_hustle.age
     , registration_cfi_for_evip_hustle.first_name
     , registration_cfi_for_evip_hustle.last_name
     , registration_cfi_for_evip_hustle.combined_ethnicity_full
     , registration_cfi_for_evip_hustle.us_cong_district_latest
     , registration_cfi_for_evip_hustle.state_senate_district_latest
     , registration_cfi_for_evip_hustle.state_house_district_latest
     , CASE WHEN registration_cfi_for_evip_hustle.state_senate_district_latest IN (
          '007', '013', '019', '027', '029', '039', '041', '047', '049'
       ) THEN 1 ELSE 0 END AS targeted_sd
     , registration_cfi_for_evip_hustle.city
     , registration_cfi_for_evip_hustle.zipcode
     , registration_cfi_for_evip_hustle.latitude AS registration_latitude
     , registration_cfi_for_evip_hustle.longitude AS registration_longitude
     , cfi_evip_sites.latitude AS evip_site_latitude
     , cfi_evip_sites.longitude AS evip_site_longitude
  FROM cfi_evip_invite_phases
 CROSS JOIN registration_cfi_for_evip_hustle
  JOIN cfi_evip_sites
    ON cfi_evip_sites.county = registration_cfi_for_evip_hustle.county_name
   AND DATE_LE(cfi_evip_sites.start_date, cfi_evip_invite_phases.timeframe_end)
   AND DATE_GE(cfi_evip_sites.start_date, cfi_evip_invite_phases.timeframe_start)
   AND DATE_GE(cfi_evip_sites.end_date, cfi_evip_invite_phases.timeframe_start);


-- The nearest EVIP site per voter, if there is one within 3 miles.
  DROP table IF EXISTS cfi_nearest_evip_site_to_voter_by_phase;
CREATE LOCAL TEMP TABLE cfi_nearest_evip_site_to_voter_by_phase
    ON COMMIT PRESERVE ROWS AS
SELECT cfi_evip_sites_by_voter_by_phase.timeframe_start
     , cfi_evip_sites_by_voter_by_phase.timeframe_end
     , cfi_evip_sites_by_voter_by_phase.county_name
     , cfi_evip_sites_by_voter_by_phase.vanprecinctid
     , cfi_evip_sites_by_voter_by_phase.vanid
     , cfi_evip_sites_by_voter_by_phase.regaddressid
     , cfi_evip_sites_by_voter_by_phase.iwillvoteid
     , cfi_evip_sites_by_voter_by_phase.location
     , cfi_evip_sites_by_voter_by_phase.address
     , cfi_evip_sites_by_voter_by_phase.hustle_date
     , cfi_evip_sites_by_voter_by_phase.start_date
     , cfi_evip_sites_by_voter_by_phase.end_date
     , cfi_evip_sites_by_voter_by_phase.start_time
     , cfi_evip_sites_by_voter_by_phase.end_time
     , cfi_evip_sites_by_voter_by_phase.dates_times_synopsis
     , cfi_evip_sites_by_voter_by_phase.rally_blurb
     , cfi_evip_sites_by_voter_by_phase.gotv_score
     , cfi_evip_sites_by_voter_by_phase.strong_dem
     , cfi_evip_sites_by_voter_by_phase.has_survey_vol_ask
     , cfi_evip_sites_by_voter_by_phase.has_survey_abr_interested
     , cfi_evip_sites_by_voter_by_phase.age
     , cfi_evip_sites_by_voter_by_phase.first_name
     , cfi_evip_sites_by_voter_by_phase.last_name
     , cfi_evip_sites_by_voter_by_phase.combined_ethnicity_full
     , cfi_evip_sites_by_voter_by_phase.us_cong_district_latest
     , cfi_evip_sites_by_voter_by_phase.state_senate_district_latest
     , cfi_evip_sites_by_voter_by_phase.state_house_district_latest
     , cfi_evip_sites_by_voter_by_phase.targeted_sd
     , cfi_evip_sites_by_voter_by_phase.city
     , cfi_evip_sites_by_voter_by_phase.zipcode
     , cfi_evip_sites_by_voter_by_phase.registration_latitude
     , cfi_evip_sites_by_voter_by_phase.registration_longitude
     , cfi_evip_sites_by_voter_by_phase.evip_site_latitude
     , cfi_evip_sites_by_voter_by_phase.evip_site_longitude
     , cfi_evip_sites_by_voter_by_phase.distance_to_evip_site
  FROM cfi_evip_sites_by_voter_by_phase
  LEFT OUTER JOIN cfi_evip_sites_by_voter_by_phase b
    ON b.timeframe_start = cfi_evip_sites_by_voter_by_phase.timeframe_start
   AND b.timeframe_end = cfi_evip_sites_by_voter_by_phase.timeframe_end
   AND b.vanid = cfi_evip_sites_by_voter_by_phase.vanid
   AND CASE WHEN ABS(cfi_evip_sites_by_voter_by_phase.distance_to_evip_site - b.distance_to_evip_site) < 0.00001 THEN cfi_evip_sites_by_voter_by_phase.start_date > b.start_date ELSE cfi_evip_sites_by_voter_by_phase.distance_to_evip_site > b.distance_to_evip_site END
 WHERE b.timeframe_start IS NULL
   AND b.timeframe_end IS NULL
   AND b.vanid IS NULL
   AND cfi_evip_sites_by_voter_by_phase.distance_to_evip_site < 500.0;


  DROP TABLE IF EXISTS cfi_nearest_evip_site_to_voter_by_phase_with_phone;
CREATE TABLE cfi_nearest_evip_site_to_voter_by_phase_with_phone AS /*+direct*/
 SELECT cfi_nearest_evip_site_to_voter_by_phase.*
      , best_van_phone.phone
  FROM cfi_nearest_evip_site_to_voter_by_phase
  LEFT JOIN (
    SELECT * FROM (
      SELECT van_phones.vanid
           , van_phones.phone
           , van_phones.phone_quality_rank
           , van_phones.phone_type
           , ROW_NUMBER() OVER (PARTITION BY van_phones.vanid ORDER BY van_phones.phone_quality_rank DESC) rank
      FROM sp_ia_katzbrownj.van_phones
      WHERE phone_type = 'C'
        AND wrongnumber < 12
    ) one_phone
    WHERE one_phone.rank = 1
  ) AS best_van_phone
    ON best_van_phone.vanid = cfi_nearest_evip_site_to_voter_by_phase.vanid
 WHERE best_van_phone.phone IS NOT NULL;


-- All upcoming, in Hustle export format.
  DROP TABLE IF EXISTS cfi_upcoming_hustle_evip_invites;
CREATE TABLE cfi_upcoming_hustle_evip_invites AS /*+direct*/
SELECT vanid AS "Custom ID"
     , first_name
     , last_name
     , phone
     , hustle_date
     , start_date
     , location AS evipLocation
     , address AS evipAddress
     , dates_times_synopsis AS evipDatesTimes
     , CASE WHEN LENGTH(rally_blurb) > 5 THEN 1 ELSE 0 END AS evipIsRally
     , rally_blurb AS evipRallyBlurb
     , county_name AS County
     , city AS City
     , zipcode AS Zip
     , us_cong_district_latest AS CD
     , state_senate_district_latest AS SD
     , state_house_district_latest AS HD
     , targeted_sd AS isTargetedSD
     , CASE WHEN strong_dem = 1 OR has_survey_vol_ask = 1 OR has_survey_abr_interested = 1 THEN 1 ELSE 0 END AS isStrongDem
  FROM cfi_nearest_evip_site_to_voter_by_phase_with_phone
 WHERE hustle_date >= current_date + 1;


  DROP TABLE IF EXISTS cfi_upcoming_hustle_evip_invites_nov2;
CREATE TABLE cfi_upcoming_hustle_evip_invites_nov2 AS /*+direct*/
SELECT *
  FROM cfi_upcoming_hustle_evip_invites;


-- For Hustling tomorrow
  DROP TABLE IF EXISTS cfi_latest_hustle_evip_invites;
CREATE TABLE cfi_latest_hustle_evip_invites AS /*+direct*/
SELECT *
  FROM cfi_upcoming_hustle_evip_invites
 WHERE hustle_date = current_date + 1;


-- For upload to Hustle!
SELECT *
  FROM cfi_latest_hustle_evip_invites;

-- Count
SELECT count(*)
  FROM cfi_latest_hustle_evip_invites;
