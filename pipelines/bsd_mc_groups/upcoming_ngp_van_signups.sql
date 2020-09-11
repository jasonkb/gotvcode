SELECT DISTINCT
    'NGP' AS ngp_or_van
  , l.state AS event_state
  , esh.datetimeoffsetbegin::timestamptz AS start_timestamp
  , ed.email
  , pd.phone
FROM ngp.events ev
LEFT JOIN ngp.eventslocations el
  ON ev.eventid = el.eventid
LEFT JOIN ngp.locations l
  ON el.locationid = l.locationid
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
WHERE c.committeeid = 75379
  AND c.datesuppressed IS NULL
  AND ev.datesuppressed IS NULL
  AND esh.datesuppressed IS NULL
  AND es.datesuppressed IS NULL
  AND esh.datetimeoffsetbegin::DATE >= {{ env.DS | pprint }}::DATE
  AND ess.eventstatusname NOT IN ('Declined',
                                  'Cancelled',
                                  'Resched',
                                  'Cancel-Web',
                                  'Excused')

UNION ALL

SELECT DISTINCT
    'VAN' AS ngp_or_van
  , l.state AS event_state
  , ea.event_date::timestamptz AS start_timestamp
  , em.email
  , ph.phone_number AS phone
FROM vansync.event_attendees ea
LEFT JOIN vansync.locations l
  ON ea.state_code = l.state_code
  AND ea.event_location_id = l.location_id
LEFT JOIN vansync.contacts_phones_myc ph
  ON ea.state_code = ph.state_code
  AND ea.myc_van_id = ph.myc_van_id
  AND ph.datetime_suppressed IS NULL
LEFT JOIN vansync.contacts_emails_myc em
  ON ea.state_code = em.state_code
  AND ea.myc_van_id = em.myc_van_id
  AND em.datetime_suppressed IS NULL
WHERE event_date::DATE >= {{ env.DS | pprint }}::DATE
  AND ea.mrr_status_name NOT IN ('Declined',
                                 'Cancelled',
                                 'Resched',
                                 'Cancel-Web',
                                 'Excused')

;
