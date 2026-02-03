-- STEP 1A: Brand + platform volumes for CASINO LOBBY ROOT sessions
-- Adjust date range if you want (this is last ~2.5 weeks of your last window).
WITH lobby_pageviews AS (
  SELECT
    user_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
    platform,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location
  FROM `nabuminds.analytics_271552729.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20250101' AND '20260128'
    AND event_name = 'page_view'
    AND REGEXP_CONTAINS(
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'),
      r'/tr/casino/?(\?.*)?$'   -- root casino lobby only
    )
),
lobby_sessions AS (
  SELECT DISTINCT
    user_id,
    session_id,
    platform,
    REGEXP_EXTRACT(page_location, r'https?://([^/]+)/') AS host
  FROM lobby_pageviews
  WHERE session_id IS NOT NULL
)
SELECT
  host,
  platform,
  COUNT(DISTINCT CONCAT(user_id, '-', CAST(session_id AS STRING))) AS sessions,
  COUNT(DISTINCT user_id) AS users
FROM lobby_sessions
GROUP BY host, platform
ORDER BY sessions DESC;




-- STEP 1B: What parameters exist on dynamic_content events in lobby sessions?
WITH lobby_sessions AS (
  SELECT DISTINCT
    user_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id
  FROM `nabuminds.analytics_271552729.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20250101' AND '20260128'
    AND event_name = 'page_view'
    AND REGEXP_CONTAINS(
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'),
      r'/tr/casino/?(\?.*)?$'
    )
),
dynamic_content_events AS (
  SELECT
    ep.key AS param_key
  FROM `nabuminds.analytics_271552729.events_*` e
  JOIN lobby_sessions ls
    ON e.user_id = ls.user_id
   AND (SELECT value.int_value FROM UNNEST(e.event_params) WHERE key = 'ga_session_id') = ls.session_id
  CROSS JOIN UNNEST(e.event_params) ep
  WHERE _TABLE_SUFFIX BETWEEN '20250101' AND '20260128'
    AND e.event_name = 'dynamic_content'
)
SELECT
  param_key,
  COUNT(*) AS occurrences
FROM dynamic_content_events
GROUP BY param_key
ORDER BY occurrences DESC;



-- STEP 2.1: Lobby-root sessions by host/brand + device category + OS
WITH lobby_pageviews AS (
  SELECT
    user_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
    platform,
    device.category AS device_category,
    device.operating_system AS operating_system,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'brand') AS brand,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
    REGEXP_EXTRACT(
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'),
      r'https?://([^/]+)/'
    ) AS host
  FROM `nabuminds.analytics_271552729.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20250101' AND '20260128'
    AND event_name = 'page_view'
    AND REGEXP_CONTAINS(
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'),
      r'/tr/casino/?(\?.*)?$'
    )
)
SELECT
  host,
  brand,
  platform,
  device_category,
  operating_system,
  COUNT(DISTINCT CONCAT(user_id, '-', CAST(session_id AS STRING))) AS sessions,
  COUNT(DISTINCT user_id) AS users
FROM lobby_pageviews
WHERE session_id IS NOT NULL
GROUP BY host, brand, platform, device_category, operating_system
ORDER BY sessions DESC;



-- STEP 2.2: Event inventory on lobby root page, MOBILE only
WITH lobby_mobile_events AS (
  SELECT
    e.event_name
  FROM `nabuminds.analytics_271552729.events_*` e
  WHERE _TABLE_SUFFIX BETWEEN '20250101' AND '20260128'
    AND e.platform = 'WEB'
    AND e.device.category IN ('mobile','tablet')
    AND REGEXP_CONTAINS(
      (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key = 'page_location'),
      r'/tr/casino/?(\?.*)?$'
    )
)
SELECT
  event_name,
  COUNT(*) AS event_count
FROM lobby_mobile_events
GROUP BY event_name
ORDER BY event_count DESC;




-- STEP 2.3: Top dynamic_content combinations on lobby root page, MOBILE only
WITH dc AS (
  SELECT
    (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key = 'brand') AS brand,
    (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key = 'content_type') AS content_type,
    (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key = 'user_interaction') AS user_interaction,
    (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key = 'click_target') AS click_target,
    (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key = 'navigation_selection') AS navigation_selection,
    (SELECT value.int_value FROM UNNEST(e.event_params) WHERE key = 'index') AS idx,
    COUNT(*) AS events
  FROM `nabuminds.analytics_271552729.events_*` e
  WHERE _TABLE_SUFFIX BETWEEN '20250101' AND '20260128'
    AND e.event_name = 'dynamic_content'
    AND e.platform = 'WEB'
    AND e.device.category IN ('mobile','tablet')
    AND REGEXP_CONTAINS(
      (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key = 'page_location'),
      r'/tr/casino/?(\?.*)?$'
    )
  GROUP BY brand, content_type, user_interaction, click_target, navigation_selection, idx
)
SELECT *
FROM dc
ORDER BY events DESC
LIMIT 100;


-- STEP 2A: Mobile lobby-root sessions by URL variant (casino vs tr/casino) + brand + host
WITH lobby_pageviews AS (
  SELECT
    user_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
    device.category AS device_category,
    device.operating_system AS operating_system,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'brand') AS brand,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page_location,
    REGEXP_EXTRACT(
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'),
      r'https?://([^/]+)/'
    ) AS host
  FROM `nabuminds.analytics_271552729.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20250101' AND '20260128'
    AND event_name = 'page_view'
    AND device.category IN ('mobile','tablet')
    AND REGEXP_CONTAINS(
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'),
      r'/(tr/)?casino/?(\?.*)?$'
    )
)
SELECT
  host,
  brand,
  CASE
    WHEN REGEXP_CONTAINS(page_location, r'/tr/casino/?(\?.*)?$') THEN 'tr/casino'
    WHEN REGEXP_CONTAINS(page_location, r'/casino/?(\?.*)?$') THEN 'casino'
    ELSE 'other'
  END AS lobby_path_variant,
  operating_system,
  COUNT(DISTINCT CONCAT(user_id, '-', CAST(session_id AS STRING))) AS sessions,
  COUNT(DISTINCT user_id) AS users
FROM lobby_pageviews
WHERE session_id IS NOT NULL
GROUP BY host, brand, lobby_path_variant, operating_system
ORDER BY sessions DESC;


-- STEP 2B: Session-level baseline metrics for MOBILE lobby-root sessions
-- Includes: gameplay conversion, time-to-first-gameplay, promo click rate, deposit guardrails

WITH lobby_sessions AS (
  SELECT
    user_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
    MIN(event_timestamp) AS lobby_first_ts,
    ANY_VALUE(device.operating_system) AS operating_system,
    ANY_VALUE(device.category) AS device_category,
    ANY_VALUE((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'brand')) AS brand,
    ANY_VALUE(REGEXP_EXTRACT(
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'),
      r'https?://([^/]+)/'
    )) AS host,
    MAX((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number')) AS ga_session_number
  FROM `nabuminds.analytics_271552729.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20250101' AND '20260122'
    AND event_name = 'page_view'
    AND device.category IN ('mobile','tablet')
    AND REGEXP_CONTAINS(
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'),
      r'/(tr/)?casino/?(\?.*)?$'
    )
  GROUP BY user_id, session_id
),
session_events AS (
  SELECT
    e.user_id,
    (SELECT value.int_value FROM UNNEST(e.event_params) WHERE key = 'ga_session_id') AS session_id,
    e.event_name,
    e.event_timestamp,
    (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key = 'navigation_selection') AS navigation_selection
  FROM `nabuminds.analytics_271552729.events_*` e
  JOIN lobby_sessions ls
    ON e.user_id = ls.user_id
   AND (SELECT value.int_value FROM UNNEST(e.event_params) WHERE key = 'ga_session_id') = ls.session_id
  WHERE _TABLE_SUFFIX BETWEEN '20250101' AND '20260122'
),
per_session AS (
  SELECT
    ls.user_id,
    ls.session_id,
    ls.brand,
    ls.host,
    ls.operating_system,
    CASE WHEN ls.ga_session_number = 1 THEN 'NEW_USER' ELSE 'RETURNING_USER' END AS user_type,
    ls.lobby_first_ts AS session_start_ts,

    -- gameplay
    MIN(IF(se.event_name = 'gameplay_initiated', se.event_timestamp, NULL)) AS first_gameplay_ts,
    COUNTIF(se.event_name = 'gameplay_initiated') AS gameplay_events,
    MAX(IF(se.event_name = 'gameplay_initiated', 1, 0)) AS has_gameplay,

    -- promo banner clicks (your confirmed mapping)
    COUNTIF(se.event_name = 'dynamic_content' AND se.navigation_selection = 'Promotion Area') AS promo_click_events,
    MAX(IF(se.event_name = 'dynamic_content' AND se.navigation_selection = 'Promotion Area', 1, 0)) AS has_promo_click,

    -- discovery/support actions
    MAX(IF(se.event_name = 'search_window_initiated', 1, 0)) AS has_search_window,
    MAX(IF(se.event_name = 'scroll', 1, 0)) AS has_scroll,

    -- deposit guardrails
    MAX(IF(se.event_name = 'deposit_window_opened', 1, 0)) AS has_deposit_window_opened,
    MAX(IF(se.event_name = 'deposit_initiated', 1, 0)) AS has_deposit_initiated,
    MAX(IF(se.event_name = 'deposit_success', 1, 0)) AS has_deposit_success

  FROM lobby_sessions ls
  LEFT JOIN session_events se
    ON ls.user_id = se.user_id
   AND ls.session_id = se.session_id
  GROUP BY
    ls.user_id, ls.session_id, ls.brand, ls.host, ls.operating_system, user_type, session_start_ts
)

SELECT
  user_type,
  operating_system,
  COUNT(*) AS lobby_sessions,

  AVG(has_gameplay) AS gameplay_session_rate,
  AVG(gameplay_events) AS avg_gameplay_events_per_session,

  AVG(has_promo_click) AS promo_click_session_rate,
  AVG(promo_click_events) AS avg_promo_clicks_per_session,

  AVG(has_scroll) AS pct_sessions_with_scroll,
  AVG(has_search_window) AS pct_sessions_with_search,

  AVG(has_deposit_window_opened) AS pct_sessions_deposit_window,
  AVG(has_deposit_success) AS pct_sessions_deposit_success,

  APPROX_QUANTILES(
    SAFE_DIVIDE(first_gameplay_ts - session_start_ts, 1000000),
    100
  )[OFFSET(50)] AS median_time_to_first_gameplay_sec,

  APPROX_QUANTILES(
    SAFE_DIVIDE(first_gameplay_ts - session_start_ts, 1000000),
    100
  )[OFFSET(75)] AS p75_time_to_first_gameplay_sec

FROM per_session
GROUP BY user_type, operating_system
ORDER BY lobby_sessions DESC;



-- STEP 3.1: Multi-host exposure risk for lobby users (mobile)
-- We build a stable "identity_key" in priority order and see how many hosts each identity hits.

WITH lobby_hits AS (
  SELECT DISTINCT
    COALESCE(
      NULLIF((SELECT value.string_value FROM UNNEST(event_params) WHERE key='enhancedUserId'), ''),
      NULLIF((SELECT value.string_value FROM UNNEST(event_params) WHERE key='customer_guid'), ''),
      NULLIF((SELECT value.string_value FROM UNNEST(event_params) WHERE key='ssguid'), ''),
      user_id
    ) AS identity_key,
    REGEXP_EXTRACT(
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'),
      r'https?://([^/]+)/'
    ) AS host
  FROM `nabuminds.analytics_271552729.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20250101' AND '20260128'
    AND event_name = 'page_view'
    AND device.category IN ('mobile','tablet')
    AND REGEXP_CONTAINS(
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'),
      r'/tr/casino/?(\?.*)?$'
    )
),
per_identity AS (
  SELECT
    identity_key,
    COUNT(DISTINCT host) AS distinct_hosts
  FROM lobby_hits
  GROUP BY identity_key
)
SELECT
  distinct_hosts,
  COUNT(*) AS identities
FROM per_identity
GROUP BY distinct_hosts
ORDER BY distinct_hosts;




-- STEP 3.2: "Fast game start" rates in lobby sessions (mobile)
WITH lobby_sessions AS (
  SELECT
    user_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
    MIN(event_timestamp) AS lobby_first_ts,
    ANY_VALUE(device.operating_system) AS operating_system,
    MAX((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_number')) AS ga_session_number
  FROM `nabuminds.analytics_271552729.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20250101' AND '20260128'
    AND event_name = 'page_view'
    AND device.category IN ('mobile','tablet')
    AND REGEXP_CONTAINS(
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'),
      r'/tr/casino/?(\?.*)?$'
    )
  GROUP BY user_id, session_id
),
session_events AS (
  SELECT
    e.user_id,
    (SELECT value.int_value FROM UNNEST(e.event_params) WHERE key = 'ga_session_id') AS session_id,
    e.event_name,
    e.event_timestamp,
    (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key = 'navigation_selection') AS navigation_selection
  FROM `nabuminds.analytics_271552729.events_*` e
  JOIN lobby_sessions ls
    ON e.user_id = ls.user_id
   AND (SELECT value.int_value FROM UNNEST(e.event_params) WHERE key = 'ga_session_id') = ls.session_id
  WHERE _TABLE_SUFFIX BETWEEN '20250101' AND '20260128'
),
per_session AS (
  SELECT
    ls.user_id,
    ls.session_id,
    ls.operating_system,
    CASE WHEN ls.ga_session_number = 1 THEN 'NEW_USER' ELSE 'RETURNING_USER' END AS user_type,
    ls.lobby_first_ts AS session_start_ts,

    MIN(IF(se.event_name = 'gameplay_initiated', se.event_timestamp, NULL)) AS first_gameplay_ts,
    MIN(IF(se.event_name = 'dynamic_content' AND se.navigation_selection = 'Promotion Area', se.event_timestamp, NULL)) AS first_promo_click_ts
  FROM lobby_sessions ls
  LEFT JOIN session_events se
    ON ls.user_id = se.user_id AND ls.session_id = se.session_id
  GROUP BY ls.user_id, ls.session_id, ls.operating_system, user_type, session_start_ts
),
scored AS (
  SELECT
    user_type,
    operating_system,
    SAFE_DIVIDE(first_gameplay_ts - session_start_ts, 1000000) AS ttf_gameplay_sec,
    SAFE_DIVIDE(first_promo_click_ts - session_start_ts, 1000000) AS ttf_promo_click_sec,
    first_gameplay_ts IS NOT NULL AS has_gameplay,
    first_promo_click_ts IS NOT NULL AS has_promo_click,
    (first_promo_click_ts IS NOT NULL AND first_gameplay_ts IS NOT NULL AND first_promo_click_ts < first_gameplay_ts) AS promo_before_gameplay
  FROM per_session
)
SELECT
  user_type,
  operating_system,
  COUNT(*) AS sessions,

  AVG(CAST(has_gameplay AS INT64)) AS gameplay_anytime_rate,

  AVG(CASE WHEN has_gameplay AND ttf_gameplay_sec <= 10 THEN 1 ELSE 0 END) AS gameplay_within_10s_rate,
  AVG(CASE WHEN has_gameplay AND ttf_gameplay_sec <= 15 THEN 1 ELSE 0 END) AS gameplay_within_15s_rate,
  AVG(CASE WHEN has_gameplay AND ttf_gameplay_sec <= 30 THEN 1 ELSE 0 END) AS gameplay_within_30s_rate,
  AVG(CASE WHEN has_gameplay AND ttf_gameplay_sec <= 60 THEN 1 ELSE 0 END) AS gameplay_within_60s_rate,

  AVG(CAST(has_promo_click AS INT64)) AS promo_click_anytime_rate,
  AVG(CAST(promo_before_gameplay AS INT64)) AS promo_click_before_gameplay_rate

FROM scored
GROUP BY user_type, operating_system
ORDER BY sessions DESC;



-- STEP 4.1: Coverage of stable identifiers in MOBILE lobby sessions
WITH lobby_sessions AS (
  SELECT
    user_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key='ga_session_id') AS session_id,
    MAX((SELECT value.int_value FROM UNNEST(event_params) WHERE key='ga_session_number')) AS ga_session_number,
    ANY_VALUE(device.operating_system) AS operating_system,

    -- presence flags (session-level, from the page_view)
    MAX(IF(NULLIF((SELECT value.string_value FROM UNNEST(event_params) WHERE key='enhancedUserId'), '') IS NOT NULL, 1, 0)) AS has_enhancedUserId,
    MAX(IF(NULLIF((SELECT value.string_value FROM UNNEST(event_params) WHERE key='customer_guid'), '') IS NOT NULL, 1, 0)) AS has_customer_guid,
    MAX(IF(NULLIF((SELECT value.string_value FROM UNNEST(event_params) WHERE key='ssguid'), '') IS NOT NULL, 1, 0)) AS has_ssguid

  FROM `nabuminds.analytics_271552729.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20260101' AND '20260128'
    AND event_name='page_view'
    AND device.category IN ('mobile','tablet')
    AND REGEXP_CONTAINS(
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key='page_location'),
      r'/tr/casino/?(\?.*)?$'
    )
  GROUP BY user_id, session_id
)
SELECT
  CASE WHEN ga_session_number = 1 THEN 'NEW_USER' ELSE 'RETURNING_USER' END AS user_type,
  operating_system,
  COUNT(*) AS lobby_sessions,

  AVG(has_enhancedUserId) AS pct_sessions_with_enhancedUserId,
  AVG(has_customer_guid) AS pct_sessions_with_customer_guid,
  AVG(has_ssguid) AS pct_sessions_with_ssguid
FROM lobby_sessions
WHERE session_id IS NOT NULL
GROUP BY user_type, operating_system
ORDER BY lobby_sessions DESC;




-- STEP 4.2: Weekly KPI trend for MOBILE lobby sessions (fast discovery + guardrails)

WITH lobby_sessions AS (
  SELECT
    user_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key='ga_session_id') AS session_id,
    MIN(event_timestamp) AS lobby_first_ts,
    ANY_VALUE(device.operating_system) AS operating_system,
    MAX((SELECT value.int_value FROM UNNEST(event_params) WHERE key='ga_session_number')) AS ga_session_number
  FROM `nabuminds.analytics_271552729.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20251001' AND '20260202'
    AND event_name = 'page_view'
    AND device.category IN ('mobile','tablet')
    AND REGEXP_CONTAINS(
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key='page_location'),
      r'/tr/casino/?(\?.*)?$'
    )
  GROUP BY user_id, session_id
),
session_events AS (
  SELECT
    e.user_id,
    (SELECT value.int_value FROM UNNEST(e.event_params) WHERE key='ga_session_id') AS session_id,
    e.event_name,
    e.event_timestamp,
    (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key='navigation_selection') AS navigation_selection
  FROM `nabuminds.analytics_271552729.events_*` e
  JOIN lobby_sessions ls
    ON e.user_id = ls.user_id
   AND (SELECT value.int_value FROM UNNEST(e.event_params) WHERE key='ga_session_id') = ls.session_id
  WHERE _TABLE_SUFFIX BETWEEN '20251001' AND '20260202'
    AND e.event_name IN (
      'gameplay_initiated',
      'dynamic_content',
      'deposit_success',
      'deposit_initiated',
      'deposit_window_opened',
      'scroll',
      'search_window_initiated'
    )
),
per_session AS (
  SELECT
    DATE_TRUNC(DATE(TIMESTAMP_MICROS(ls.lobby_first_ts)), WEEK(MONDAY)) AS week_start,
    ls.operating_system,
    CASE WHEN ls.ga_session_number = 1 THEN 'NEW_USER' ELSE 'RETURNING_USER' END AS user_type,
    ls.lobby_first_ts AS session_start_ts,

    MIN(IF(se.event_name='gameplay_initiated', se.event_timestamp, NULL)) AS first_gameplay_ts,
    MIN(IF(se.event_name='dynamic_content' AND se.navigation_selection='Promotion Area', se.event_timestamp, NULL)) AS first_promo_ts,

    MAX(IF(se.event_name='deposit_success', 1, 0)) AS has_deposit_success,
    MAX(IF(se.event_name='deposit_initiated', 1, 0)) AS has_deposit_initiated,
    MAX(IF(se.event_name='deposit_window_opened', 1, 0)) AS has_deposit_window_opened,

    MAX(IF(se.event_name='scroll', 1, 0)) AS has_scroll,
    MAX(IF(se.event_name='search_window_initiated', 1, 0)) AS has_search_window
  FROM lobby_sessions ls
  LEFT JOIN session_events se
    ON ls.user_id = se.user_id AND ls.session_id = se.session_id
  GROUP BY week_start, operating_system, user_type, session_start_ts
),
scored AS (
  SELECT
    week_start,
    operating_system,
    user_type,

    first_gameplay_ts IS NOT NULL AS has_gameplay,
    SAFE_DIVIDE(first_gameplay_ts - session_start_ts, 1000000) AS ttf_gameplay_sec,

    first_promo_ts IS NOT NULL AS has_promo_event,
    (first_promo_ts IS NOT NULL AND first_gameplay_ts IS NOT NULL AND first_promo_ts < first_gameplay_ts) AS promo_before_gameplay,

    has_deposit_success,
    has_deposit_initiated,
    has_deposit_window_opened,
    has_scroll,
    has_search_window
  FROM per_session
)
SELECT
  week_start,
  operating_system,
  user_type,
  COUNT(*) AS sessions,

  AVG(CAST(has_gameplay AS INT64)) AS gameplay_anytime_rate,
  AVG(CASE WHEN has_gameplay AND ttf_gameplay_sec <= 15 THEN 1 ELSE 0 END) AS gameplay_within_15s_rate,
  AVG(CASE WHEN has_gameplay AND ttf_gameplay_sec <= 30 THEN 1 ELSE 0 END) AS gameplay_within_30s_rate,

  AVG(CAST(has_promo_event AS INT64)) AS promo_event_rate,
  AVG(CAST(promo_before_gameplay AS INT64)) AS promo_before_gameplay_rate,

  AVG(has_deposit_window_opened) AS deposit_window_rate,
  AVG(has_deposit_initiated) AS deposit_initiated_rate,
  AVG(has_deposit_success) AS deposit_success_rate,

  AVG(has_scroll) AS scroll_rate,
  AVG(has_search_window) AS search_rate

FROM scored
GROUP BY week_start, operating_system, user_type
ORDER BY week_start DESC, sessions DESC;



-- STEP 5: Daily KPI + data quality check for MOBILE lobby sessions
-- Goal: find tracking outages / unstable periods and choose a stable baseline window.

WITH lobby_sessions AS (
  SELECT
    user_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key='ga_session_id') AS session_id,
    MIN(event_timestamp) AS lobby_first_ts,
    ANY_VALUE(device.operating_system) AS operating_system,
    MAX((SELECT value.int_value FROM UNNEST(event_params) WHERE key='ga_session_number')) AS ga_session_number
  FROM `nabuminds.analytics_271552729.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20251201' AND '20260122'
    AND event_name = 'page_view'
    AND device.category IN ('mobile','tablet')
    AND REGEXP_CONTAINS(
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key='page_location'),
      r'/tr/casino/?(\?.*)?$'
    )
  GROUP BY user_id, session_id
),
session_events AS (
  SELECT
    e.user_id,
    (SELECT value.int_value FROM UNNEST(e.event_params) WHERE key='ga_session_id') AS session_id,
    e.event_name,
    e.event_timestamp,
    (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key='navigation_selection') AS navigation_selection
  FROM `nabuminds.analytics_271552729.events_*` e
  JOIN lobby_sessions ls
    ON e.user_id = ls.user_id
   AND (SELECT value.int_value FROM UNNEST(e.event_params) WHERE key='ga_session_id') = ls.session_id
  WHERE _TABLE_SUFFIX BETWEEN '20251201' AND '20260122'
    AND e.event_name IN ('gameplay_initiated','dynamic_content','deposit_success','deposit_initiated','deposit_window_opened')
),
per_session AS (
  SELECT
    ls.user_id,
    ls.session_id,
    DATE(TIMESTAMP_MICROS(ls.lobby_first_ts)) AS activity_date,
    ls.operating_system,
    CASE WHEN ls.ga_session_number = 1 THEN 'NEW_USER' ELSE 'RETURNING_USER' END AS user_type,
    ls.lobby_first_ts AS session_start_ts,

    MIN(IF(se.event_name='gameplay_initiated', se.event_timestamp, NULL)) AS first_gameplay_ts,
    LOGICAL_OR(se.event_name='gameplay_initiated') AS has_gameplay,

    LOGICAL_OR(se.event_name='dynamic_content' AND se.navigation_selection='Promotion Area') AS has_promo_click,

    LOGICAL_OR(se.event_name='deposit_window_opened') AS has_deposit_window,
    LOGICAL_OR(se.event_name='deposit_initiated') AS has_deposit_initiated,
    LOGICAL_OR(se.event_name='deposit_success') AS has_deposit_success
  FROM lobby_sessions ls
  LEFT JOIN session_events se
    ON ls.user_id = se.user_id AND ls.session_id = se.session_id
  GROUP BY
    ls.user_id, ls.session_id,
    activity_date, operating_system, user_type, session_start_ts
),
scored AS (
  SELECT
    activity_date,
    operating_system,
    user_type,
    has_gameplay,
    SAFE_DIVIDE(CAST(first_gameplay_ts - session_start_ts AS FLOAT64), 1000000.0) AS ttf_gameplay_sec,
    has_promo_click,
    has_deposit_window,
    has_deposit_initiated,
    has_deposit_success
  FROM per_session
)

SELECT
  activity_date,
  operating_system,
  user_type,
  COUNT(*) AS sessions,

  AVG(CAST(has_gameplay AS INT64)) AS gameplay_anytime_rate,
  AVG(CASE WHEN has_gameplay AND ttf_gameplay_sec <= 15 THEN 1 ELSE 0 END) AS gameplay_within_15s_rate,
  AVG(CASE WHEN has_gameplay AND ttf_gameplay_sec <= 30 THEN 1 ELSE 0 END) AS gameplay_within_30s_rate,

  AVG(CAST(has_promo_click AS INT64)) AS promo_click_rate,

  AVG(CAST(has_deposit_window AS INT64)) AS deposit_window_rate,
  AVG(CAST(has_deposit_initiated AS INT64)) AS deposit_initiated_rate,
  AVG(CAST(has_deposit_success AS INT64)) AS deposit_success_rate

FROM scored
GROUP BY activity_date, operating_system, user_type
ORDER BY activity_date DESC, sessions DESC;

-- STEP 5 (user_pseudo_id): Daily KPI + data quality check for MOBILE lobby sessions

WITH lobby_sessions AS (
  SELECT
    user_pseudo_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key='ga_session_id') AS session_id,
    MIN(event_timestamp) AS lobby_first_ts,
    ANY_VALUE(device.operating_system) AS operating_system,
    MAX((SELECT value.int_value FROM UNNEST(event_params) WHERE key='ga_session_number')) AS ga_session_number
  FROM `nabuminds.analytics_271552729.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20251201' AND '20260128'
    AND event_name = 'page_view'
    AND device.category IN ('mobile','tablet')
    AND REGEXP_CONTAINS(
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key='page_location'),
      r'/tr/casino/?(\?.*)?$'
    )
  GROUP BY user_pseudo_id, session_id
),

session_events AS (
  SELECT
    e.user_pseudo_id,
    (SELECT value.int_value FROM UNNEST(e.event_params) WHERE key='ga_session_id') AS session_id,
    e.event_name,
    e.event_timestamp,
    (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key='navigation_selection') AS navigation_selection
  FROM `nabuminds.analytics_271552729.events_*` e
  JOIN lobby_sessions ls
    ON e.user_pseudo_id = ls.user_pseudo_id
   AND (SELECT value.int_value FROM UNNEST(e.event_params) WHERE key='ga_session_id') = ls.session_id
  WHERE _TABLE_SUFFIX BETWEEN '20251201' AND '20260128'
    AND e.event_name IN ('gameplay_initiated','dynamic_content','deposit_success','deposit_initiated','deposit_window_opened')
),

per_session AS (
  SELECT
    ls.user_pseudo_id,
    ls.session_id,

    -- compute directly (don’t rely on alias in GROUP BY)
    DATE(TIMESTAMP_MICROS(ls.lobby_first_ts)) AS activity_date,

    ls.operating_system,
    IF(ls.ga_session_number = 1, 'NEW_USER', 'RETURNING_USER') AS user_type,
    ls.lobby_first_ts AS session_start_ts,

    MIN(IF(se.event_name='gameplay_initiated', se.event_timestamp, NULL)) AS first_gameplay_ts,
    LOGICAL_OR(se.event_name='gameplay_initiated') AS has_gameplay,

    LOGICAL_OR(se.event_name='dynamic_content' AND se.navigation_selection='Promotion Area') AS has_promo_click,

    LOGICAL_OR(se.event_name='deposit_window_opened') AS has_deposit_window,
    LOGICAL_OR(se.event_name='deposit_initiated') AS has_deposit_initiated,
    LOGICAL_OR(se.event_name='deposit_success') AS has_deposit_success
  FROM lobby_sessions ls
  LEFT JOIN session_events se
    ON ls.user_pseudo_id = se.user_pseudo_id
   AND ls.session_id = se.session_id
  GROUP BY
    ls.user_pseudo_id,
    ls.session_id,
    DATE(TIMESTAMP_MICROS(ls.lobby_first_ts)),
    ls.operating_system,
    IF(ls.ga_session_number = 1, 'NEW_USER', 'RETURNING_USER'),
    ls.lobby_first_ts
),

scored AS (
  SELECT
    activity_date,
    operating_system,
    user_type,
    has_gameplay,
    SAFE_DIVIDE(CAST(first_gameplay_ts - session_start_ts AS FLOAT64), 1000000.0) AS ttf_gameplay_sec,
    has_promo_click,
    has_deposit_window,
    has_deposit_initiated,
    has_deposit_success
  FROM per_session
)

SELECT
  activity_date,
  operating_system,
  user_type,
  COUNT(*) AS sessions,

  AVG(CAST(has_gameplay AS INT64)) AS gameplay_anytime_rate,
  AVG(CASE WHEN has_gameplay AND ttf_gameplay_sec <= 15 THEN 1 ELSE 0 END) AS gameplay_within_15s_rate,
  AVG(CASE WHEN has_gameplay AND ttf_gameplay_sec <= 30 THEN 1 ELSE 0 END) AS gameplay_within_30s_rate,

  AVG(CAST(has_promo_click AS INT64)) AS promo_click_rate,

  AVG(CAST(has_deposit_window AS INT64)) AS deposit_window_rate,
  AVG(CAST(has_deposit_initiated AS INT64)) AS deposit_initiated_rate,
  AVG(CAST(has_deposit_success AS INT64)) AS deposit_success_rate
FROM scored
GROUP BY activity_date, operating_system, user_type
ORDER BY activity_date DESC, sessions DESC;

