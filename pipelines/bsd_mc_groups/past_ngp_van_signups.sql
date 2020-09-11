SELECT DISTINCT
    'NGP' AS ngp_or_van
  , ev.eventcalendarname AS event_type
  , pd.phone
  , ed.email
  , el.state AS event_state
FROM ngp.events ev
JOIN ngp.eventshifts esh
  ON ev.eventid = esh.eventid
JOIN ngp.eventsignups es
  ON esh.eventshiftid = es.eventshiftid
JOIN ngp.eventsignupsstatuses ess
  ON es.currenteventsignupseventstatusid = ess.eventsignupseventstatusid
JOIN ngp.contacts c
  ON es.vanid = c.vanid
LEFT JOIN ngp.phonesdelta pd
  ON c.vanid = pd.vanid
  AND pd.datesuppressed IS NULL
LEFT JOIN ngp.emaildelta ed
  ON c.vanid = ed.vanid
  AND ed.datesuppressed IS NULL
LEFT JOIN ngp.locations el
  ON es.locationid = el.locationid
WHERE c.committeeid = 75379
  AND c.datesuppressed IS NULL
  AND ev.datesuppressed IS NULL
  AND esh.datesuppressed IS NULL
  AND es.datesuppressed IS NULL
  AND esh.datetimeoffsetbegin::DATE < {{ env.DS | pprint }}::DATE
  AND ess.eventstatusname NOT IN (
    'Declined',
    'Cancelled',
    'Resched',
    'Cancel-Web',
    'Excused',
    'No Show'
  )

UNION ALL

SELECT DISTINCT
    'VAN' AS ngp_or_van
  , ea.event_type_name AS event_type
  , ph.phone_number AS phone
  , em.email
  , el.state AS event_state
FROM vansync.event_attendees ea
LEFT JOIN vansync.contacts_phones_myc ph
  ON ea.state_code = ph.state_code
  AND ea.myc_van_id = ph.myc_van_id
  AND ph.datetime_suppressed IS NULL
LEFT JOIN vansync.contacts_emails_myc em
  ON ea.state_code = em.state_code
  AND ea.myc_van_id = em.myc_van_id
  AND em.datetime_suppressed IS NULL
LEFT JOIN vansync.locations el
  ON el.location_id = ea.event_location_id
 AND el.state_code = ea.state_code
WHERE event_date::DATE < {{ env.DS | pprint }}::DATE
 AND ((ea.attended IS NULL) OR (ea.attended = 1))
