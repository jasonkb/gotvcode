SELECT
  owner__email_address AS email
, owner__phone_number  AS phone
, event_type
FROM mobilizeamerica.events
WHERE approval_status = 'APPROVED' AND deleted_date IS NULL
GROUP BY email, phone, event_type
;
