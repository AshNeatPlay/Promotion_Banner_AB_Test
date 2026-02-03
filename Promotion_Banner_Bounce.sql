-- STEP 4 FIXED: Bounce rate analysis - First-timers vs Returners on casino lobby
-- Fixed: ga_session_id is stored as INT_VALUE, not STRING_VALUE

WITH login_events AS (
  SELECT 
    user_id,
    TIMESTAMP_MICROS(event_timestamp) as event_timestamp,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp) as login_sequence
  FROM `nabuminds.analytics_271552729.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20260121' AND '20260128'
    AND event_name = 'login_success'
),

first_login AS (
  SELECT 
    user_id,
    event_timestamp as first_login_timestamp
  FROM login_events
  WHERE login_sequence = 1
),

casino_visits_raw AS (
  SELECT 
    user_id,
    TIMESTAMP_MICROS(event_timestamp) as event_timestamp,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as session_id,
    event_name
  FROM `nabuminds.analytics_271552729.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20260121' AND '20260128'
    AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') LIKE '%/tr/casino/%'
),

casino_sessions AS (
  SELECT 
    user_id,
    session_id,
    COUNT(*) as session_events,
    MIN(event_timestamp) as session_start_time,
    MAX(event_timestamp) as session_end_time
  FROM casino_visits_raw
  WHERE session_id IS NOT NULL
  GROUP BY user_id, session_id
),

user_classification AS (
  SELECT 
    c.user_id,
    c.session_id,
    c.session_events,
    CASE 
      WHEN fl.user_id IS NOT NULL 
        AND c.session_start_time >= fl.first_login_timestamp
        AND c.session_start_time <= TIMESTAMP_ADD(fl.first_login_timestamp, INTERVAL 1 DAY)
      THEN 'FIRST-TIME'
      ELSE 'RETURNING'
    END as user_type
  FROM casino_sessions c
  LEFT JOIN first_login fl ON c.user_id = fl.user_id
)

SELECT 
  user_type,
  COUNT(DISTINCT session_id) as total_sessions,
  COUNT(DISTINCT user_id) as unique_users,
  SUM(CASE WHEN session_events <= 2 THEN 1 ELSE 0 END) as low_engagement_sessions,
  ROUND(100 * SUM(CASE WHEN session_events <= 2 THEN 1 ELSE 0 END) / NULLIF(COUNT(DISTINCT session_id), 0), 2) as bounce_rate_pct,
  ROUND(AVG(session_events), 2) as avg_events_per_session,
  ROUND(APPROX_QUANTILES(session_events, 100)[OFFSET(50)], 2) as median_events_per_session
FROM user_classification
WHERE session_id IS NOT NULL
GROUP BY user_type
ORDER BY user_type DESC;
