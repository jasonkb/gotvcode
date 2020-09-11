SELECT DISTINCT
  people.phonenumber AS phone
, people.email
, actions.action
  FROM van.actions_all AS actions
  JOIN van.people AS people
    ON people.source = actions.source
      AND people.instance = actions.instance
      AND people.pk = actions.pk
      AND people.van_mode = actions.van_mode
 WHERE actions.action NOT IN ('result','donor','online_signup','mobilecommons_subscriber')
   AND actions.source != 'ngp'
;
