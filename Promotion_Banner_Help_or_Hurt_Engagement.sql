-- 4-MONTH WEEKLY ANALYSIS: Do promotional banners help or hurt game engagement?
-- Period: Sept 29, 2025 to Jan 28, 2026
-- Banner clickers: measure if they return to casino within 5 min
-- Non-clickers: measure if they started a game
-- Breakdown by: First-timers vs Returners

WITH login_events AS (
  SELECT 
    user_id,
    TIMESTAMP_MICROS(event_timestamp) as event_timestamp,
    ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY event_timestamp) as login_sequence
  FROM `nabuminds.analytics_271552729.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20250929' AND '20260128'
    AND event_name = 'login_success'
),

first_login AS (
  SELECT 
    user_id,
    event_timestamp as first_login_timestamp
  FROM login_events
  WHERE login_sequence = 1
),

casino_visits AS (
  SELECT 
    user_id,
    user_pseudo_id,
    TIMESTAMP_MICROS(event_timestamp) as event_timestamp,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as session_id,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') as page_location,
    event_name,
    event_date
  FROM `nabuminds.analytics_271552729.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20250929' AND '20260128'
    AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') LIKE '%/tr/casino/%'
),

banner_clicks AS (
  SELECT 
    user_id,
    user_pseudo_id,
    TIMESTAMP_MICROS(event_timestamp) as banner_click_time,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as session_id
  FROM `nabuminds.analytics_271552729.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20250929' AND '20260128'
    AND event_name = 'dynamic_content'
    AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') LIKE '%/tr/casino/%'
),

gameplay_initiated AS (
  SELECT 
    user_id,
    TIMESTAMP_MICROS(event_timestamp) as gameplay_time,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as session_id
  FROM `nabuminds.analytics_271552729.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20250929' AND '20260128'
    AND event_name = 'gameplay_initiated'
),

-- Identify banner clickers per session
clickers_per_session AS (
  SELECT DISTINCT
    bc.user_id,
    bc.session_id,
    bc.banner_click_time,
    CASE 
      WHEN fl.user_id IS NOT NULL 
        AND bc.banner_click_time >= fl.first_login_timestamp
        AND bc.banner_click_time <= TIMESTAMP_ADD(fl.first_login_timestamp, INTERVAL 1 DAY)
      THEN 'FIRST-TIME'
      ELSE 'RETURNING'
    END as user_type
  FROM banner_clicks bc
  LEFT JOIN first_login fl ON bc.user_id = fl.user_id
),

-- Check if clickers return to casino within 5 minutes
clicker_returns AS (
  SELECT 
    cs.user_id,
    cs.session_id,
    cs.user_type,
    cs.banner_click_time,
    CASE 
      WHEN EXISTS (
        SELECT 1 FROM casino_visits cv
        WHERE cv.user_id = cs.user_id
          AND TIMESTAMP_DIFF(cv.event_timestamp, cs.banner_click_time, SECOND) > 0
          AND TIMESTAMP_DIFF(cv.event_timestamp, cs.banner_click_time, SECOND) <= 300
      ) THEN 1
      ELSE 0
    END as returned_within_5min
  FROM clickers_per_session cs
),

-- Identify non-clickers (visited casino but didn't click banner)
non_clickers_per_session AS (
  SELECT DISTINCT
    cv.user_id,
    cv.session_id,
    PARSE_DATE('%Y%m%d', CAST(cv.event_date AS STRING)) as event_date,
    CASE 
      WHEN fl.user_id IS NOT NULL 
        AND cv.event_timestamp >= fl.first_login_timestamp
        AND cv.event_timestamp <= TIMESTAMP_ADD(fl.first_login_timestamp, INTERVAL 1 DAY)
      THEN 'FIRST-TIME'
      ELSE 'RETURNING'
    END as user_type
  FROM casino_visits cv
  LEFT JOIN first_login fl ON cv.user_id = fl.user_id
  WHERE NOT EXISTS (
    SELECT 1 FROM banner_clicks bc
    WHERE bc.user_id = cv.user_id
      AND bc.session_id = cv.session_id
  )
),

-- Check if non-clickers initiated gameplay
non_clicker_gameplay AS (
  SELECT 
    ncs.user_id,
    ncs.session_id,
    ncs.user_type,
    ncs.event_date,
    CASE 
      WHEN EXISTS (
        SELECT 1 FROM gameplay_initiated gi
        WHERE gi.user_id = ncs.user_id
          AND gi.session_id = ncs.session_id
      ) THEN 1
      ELSE 0
    END as initiated_gameplay
  FROM non_clickers_per_session ncs
),

-- Weekly aggregation for clickers
clicker_weekly AS (
  SELECT 
    FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP_TRUNC(cr.banner_click_time, WEEK)) as week_start,
    cr.user_type,
    'BANNER_CLICKERS' as group_type,
    COUNT(DISTINCT cr.session_id) as sessions,
    COUNT(DISTINCT cr.user_id) as unique_users,
    SUM(cr.returned_within_5min) as returned_within_5min,
    ROUND(100 * SUM(cr.returned_within_5min) / COUNT(DISTINCT cr.session_id), 2) as return_rate_pct
  FROM clicker_returns cr
  GROUP BY week_start, cr.user_type
),

-- Weekly aggregation for non-clickers
non_clicker_weekly AS (
  SELECT 
    FORMAT_TIMESTAMP('%Y-%m-%d', TIMESTAMP_TRUNC(TIMESTAMP(ncg.event_date), WEEK)) as week_start,
    ncg.user_type,
    'NON_CLICKERS' as group_type,
    COUNT(DISTINCT ncg.session_id) as sessions,
    COUNT(DISTINCT ncg.user_id) as unique_users,
    SUM(ncg.initiated_gameplay) as initiated_gameplay,
    ROUND(100 * SUM(ncg.initiated_gameplay) / COUNT(DISTINCT ncg.session_id), 2) as gameplay_rate_pct
  FROM non_clicker_gameplay ncg
  GROUP BY week_start, ncg.user_type
)

-- Combined output
SELECT 
  week_start,
  user_type,
  group_type,
  sessions,
  unique_users,
  COALESCE(returned_within_5min, initiated_gameplay) as key_metric,
  COALESCE(return_rate_pct, gameplay_rate_pct) as engagement_rate_pct
FROM (
  SELECT 
    week_start, user_type, group_type, sessions, unique_users,
    returned_within_5min, return_rate_pct, NULL as initiated_gameplay, NULL as gameplay_rate_pct
  FROM clicker_weekly
  UNION ALL
  SELECT 
    week_start, user_type, group_type, sessions, unique_users,
    NULL, NULL, initiated_gameplay, gameplay_rate_pct
  FROM non_clicker_weekly
)
ORDER BY week_start DESC, user_type, group_type;
