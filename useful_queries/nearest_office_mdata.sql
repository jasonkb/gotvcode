-- Shows example of general pattern of, given a universe of people, and
-- a universe of points of interest (e.g. offices, or events, or staging
-- locations...), computes for each person the closest point of interest.

-- (In this example, instead of people, we're matching incoming zip
-- codes to their nearest office.)

-- Given an ia_zips table with latitude and longitude, one row per zip
-- code in Iowa, and an ia_offices table with latitude and longitude and
-- other info, one row per office in Iowa, computes one row per ia_zip
-- and adds in all the info from the ia_office which is nearest to the
-- ia_zip. Also adds a column 'distance' to the output; you can then use
-- this if you e.g. want to select rows only with distance < 25 or so.

WITH offices_cross_people AS (
    SELECT
      ia_zips.zip AS origin_zip
    , ia_zips.latitude AS origin_latitude
    , ia_zips.longitude AS origin_longitude
    , ia_offices.*
    , ST_DistanceSphere(ST_Point(ia_zips.longitude, ia_zips.latitude), ST_Point(ia_offices.longitude, ia_offices.latitude)) / 1609 as distance
      FROM scratch.ia_zips
      CROSS JOIN scratch.ia_offices
)
SELECT origin_zip, 'Your nearest office is at ' || address || '!' FROM (
    SELECT * FROM (
        SELECT
          *
        , ROW_NUMBER() OVER (PARTITION BY offices_cross_people.origin_zip ORDER BY offices_cross_people.distance ASC) AS distance_rank
          FROM offices_cross_people
    )
     WHERE distance_rank = 1
     ORDER BY distance DESC
)
;
