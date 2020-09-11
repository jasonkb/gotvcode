SELECT
	tmp_bsd_mc_group_users.id,
	cons_email.cons_id,
	COALESCE(mc_by_phone.phone_number, mc_by_email.phone_number) AS phone
FROM {{ env.USER_JOIN_TABLE }} AS tmp_bsd_mc_group_users
LEFT OUTER JOIN bsd.cons_email
    ON LOWER(TRIM(cons_email.email)) = tmp_bsd_mc_group_users.email
LEFT OUTER JOIN mobilecommons.profiles AS mc_by_phone
    ON  mc_by_phone.phone_number = tmp_bsd_mc_group_users.phone
    AND mc_by_phone.status = 'Active Subscriber'
    AND mc_by_phone.opted_out_at IS NULL
LEFT OUTER JOIN mobilecommons.profiles AS mc_by_email
    ON  LOWER(TRIM(mc_by_email.email)) = tmp_bsd_mc_group_users.email
    AND mc_by_email.status = 'Active Subscriber'
    AND mc_by_email.opted_out_at IS NULL
;
