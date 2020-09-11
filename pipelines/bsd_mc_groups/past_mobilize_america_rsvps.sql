SELECT
  events.event_type
, participations.phone_number_at_signup AS phone
, participations.email_at_signup AS email
, participations.attended AS attended
  FROM mobilizeamerica.events
  JOIN mobilizeamerica.timeslots
    ON events.id = timeslots.event_id
   AND convert_timezone('America/New_York', timeslots.start_date::TIMESTAMP) < {{ env.DS | pprint }}::DATE
  JOIN mobilizeamerica.participations
    ON participations.timeslot_id = timeslots.id
   AND participations.status != 'CANCELLED'
  WHERE events.deleted_date IS NULL
   AND timeslots.deleted_date IS NULL
   AND events.approval_status = 'APPROVED'
 GROUP BY 1, 2, 3, 4
 ORDER BY 1 ASC
;
