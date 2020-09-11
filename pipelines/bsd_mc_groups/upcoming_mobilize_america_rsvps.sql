SELECT
  {{ env.DS | pprint }}::DATE AS ds
, events.id AS event_id
, events.location__locality AS event_city
, events.location__region AS event_state
, timeslots.start_date AS start_timestamp
, participations.email_at_signup AS email
, participations.phone_number_at_signup AS phone
  FROM mobilizeamerica.events
  JOIN mobilizeamerica.timeslots
    ON events.id = timeslots.event_id
   AND convert_timezone('America/New_York', timeslots.start_date::TIMESTAMP) >= {{ env.DS | pprint }}::DATE
  JOIN mobilizeamerica.participations
    ON participations.timeslot_id = timeslots.id
   AND participations.status != 'CANCELLED'
  WHERE events.deleted_date IS NULL
   AND timeslots.deleted_date IS NULL
   AND events.approval_status = 'APPROVED'
 ORDER BY start_timestamp ASC, email ASC, phone ASC
;
