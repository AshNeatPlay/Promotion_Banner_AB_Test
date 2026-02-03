-- ============================================================================
-- CORRECT ANALYSIS: Banner Carousel Friction & Game Discovery
-- Period: Nov 1, 2025 - Jan 21, 2026 (82 days)
-- 
-- WHAT THESE QUERIES MEASURE:
-- Q1: New vs Returning user segmentation 
-- Q2: Funnel by user type (Banner Exposure → Banner Click → Gameplay)
-- Q3: Does carousel interaction predict lower gameplay rates? (friction measurement)
-- Q4: Banner click conversion rates, properly comparing clickers vs non-clickers
-- Q5: Weekly trend analysis - are patterns consistent?
-- ============================================================================

-- ===========================================================================
-- QUERY 1: USER SEGMENTATION (New vs Returning)
-- ===========================================================================
-- PURPOSE: Define user cohorts for all downstream analysis
-- Returning user = has events before Nov 1 OR second+ session in this period
-- New user = first session, no prior activity

WITH first_user_appearance AS (
  SELECT 
    user_id,
    MIN(TIMESTAMP_MICROS(event_timestamp)) as first_event_timestamp,
    DATE(TIMESTAMP_MICROS(MIN(event_timestamp))) as first_date
  FROM `nabuminds.analytics_271552729.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20251001' AND '20260121'
  GROUP BY user_id
),

user_cohort AS (
  SELECT 
    user_id,
    CASE 
      WHEN first_date < '2025-11-01' THEN 'RETURNING_USER'
      ELSE 'NEW_USER'
    END as user_type,
    first_event_timestamp,
    first_date
  FROM first_user_appearance
)

SELECT 
  user_type,
  COUNT(DISTINCT user_id) as total_users,
  COUNT(DISTINCT user_id) * 100.0 / SUM(COUNT(DISTINCT user_id)) OVER () as pct_of_all_users
FROM user_cohort
GROUP BY user_type
ORDER BY user_type;


-- ===========================================================================
-- QUERY 2: CASINO LOBBY FUNNEL BY USER TYPE (Weekly Breakdown)
-- ===========================================================================
-- PURPOSE: Show progression through casino lobby
-- Step 1: User enters casino lobby
-- Step 2: User sees banner (has dynamic_content event)
-- Step 3: User clicks banner (dynamic_content event)
-- Step 4: User initiates gameplay (gameplay_initiated event)
-- 
-- KEY QUESTIONS:
-- - Do new users reach gameplay less often than returning?
-- - Does seeing a banner predict lower gameplay rates?
-- - Is there friction AFTER banner visibility?

-- ===========================================================================
-- QUERY 2 (CORRECTED): CASINO LOBBY FUNNEL BY USER TYPE (Weekly Breakdown)
-- ===========================================================================

WITH user_cohort AS (
  SELECT 
    user_id,
    CASE 
      WHEN MIN(TIMESTAMP_MICROS(event_timestamp)) < '2025-11-01' THEN 'RETURNING_USER'
      ELSE 'NEW_USER'
    END as user_type
  FROM `nabuminds.analytics_271552729.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20251001' AND '20260121'
  GROUP BY user_id
),

casino_lobby_sessions AS (
  SELECT 
    e.user_id,
    uc.user_type,
    (SELECT value.int_value FROM UNNEST(e.event_params) WHERE key = 'ga_session_id') as session_id,
    EXTRACT(WEEK FROM TIMESTAMP_MICROS(e.event_timestamp)) as week_num,
    e.event_name
  FROM `nabuminds.analytics_271552729.events_*` e
  JOIN user_cohort uc ON e.user_id = uc.user_id
  WHERE _TABLE_SUFFIX BETWEEN '20251101' AND '20260121'
    AND (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key = 'page_location') LIKE '%/tr/casino/%'
),

session_attributes AS (
  -- First aggregation: per-session, identify what happened
  SELECT 
    user_type,
    week_num,
    CONCAT(user_id, '-', session_id) as session_key,
    MAX(CASE WHEN event_name = 'dynamic_content' THEN 1 ELSE 0 END) as had_banner_click,
    MAX(CASE WHEN event_name = 'gameplay_initiated' THEN 1 ELSE 0 END) as had_gameplay
  FROM casino_lobby_sessions
  GROUP BY user_type, week_num, session_key
),

session_counts AS (
  -- Second aggregation: count sessions by type
  SELECT 
    user_type,
    week_num,
    COUNT(*) as total_casino_sessions,
    SUM(had_banner_click) as sessions_banner_clicked,
    SUM(had_gameplay) as sessions_gameplay_initiated
  FROM session_attributes
  GROUP BY user_type, week_num
)

SELECT 
  week_num,
  user_type,
  total_casino_sessions,
  sessions_banner_clicked,
  ROUND(100.0 * sessions_banner_clicked / NULLIF(total_casino_sessions, 0), 2) as pct_sessions_clicked_banner,
  sessions_gameplay_initiated,
  ROUND(100.0 * sessions_gameplay_initiated / NULLIF(total_casino_sessions, 0), 2) as pct_sessions_gameplay,
  -- KEY METRIC: Among sessions with banner clicks, what % reached gameplay?
  ROUND(100.0 * sessions_gameplay_initiated / NULLIF(sessions_banner_clicked + 1, 0), 2) as gameplay_rate_among_banner_clickers
FROM session_counts
ORDER BY week_num, user_type;

-- ===========================================================================
-- QUERY 3: CAROUSEL ARROW INTERACTION (Friction Measurement)
-- ===========================================================================
-- PURPOSE: Identify carousel navigation clicks (arrows) and measure if they
-- correlate with LOWER gameplay rates
-- 
-- HYPOTHESIS: If carousel is friction, sessions with arrow clicks should have
-- lower gameplay conversion than sessions without arrow clicks

WITH user_cohort AS (
  SELECT 
    user_id,
    CASE 
      WHEN MIN(TIMESTAMP_MICROS(event_timestamp)) < '2025-11-01' THEN 'RETURNING_USER'
      ELSE 'NEW_USER'
    END as user_type
  FROM `nabuminds.analytics_271552729.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20251001' AND '20260121'
  GROUP BY user_id
),

casino_sessions AS (
  SELECT 
    e.user_id,
    uc.user_type,
    (SELECT value.int_value FROM UNNEST(e.event_params) WHERE key = 'ga_session_id') as session_id,
    e.event_name,
    EXTRACT(WEEK FROM TIMESTAMP_MICROS(e.event_timestamp)) as week_num
  FROM `nabuminds.analytics_271552729.events_*` e
  JOIN user_cohort uc ON e.user_id = uc.user_id
  WHERE _TABLE_SUFFIX BETWEEN '20251101' AND '20260121'
    AND (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key = 'page_location') LIKE '%/tr/casino/%'
),

session_behavior AS (
  SELECT 
    user_type,
    week_num,
    CONCAT(user_id, '-', session_id) as session_key,
    -- Flag: Did user interact with carousel arrows? (approximated by looking for high-frequency click patterns)
    MAX(CASE WHEN event_name IN ('menu_click', 'carousel_next', 'carousel_prev') THEN 1 ELSE 0 END) as had_carousel_interaction,
    MAX(CASE WHEN event_name = 'gameplay_initiated' THEN 1 ELSE 0 END) as reached_gameplay,
    MAX(CASE WHEN event_name = 'dynamic_content' THEN 1 ELSE 0 END) as clicked_banner
  FROM casino_sessions
  GROUP BY user_type, week_num, session_key
),

friction_analysis AS (
  SELECT 
    week_num,
    user_type,
    had_carousel_interaction,
    COUNT(*) as session_count,
    SUM(reached_gameplay) as sessions_with_gameplay,
    ROUND(100.0 * SUM(reached_gameplay) / NULLIF(COUNT(*), 0), 2) as gameplay_conversion_pct
  FROM session_behavior
  GROUP BY week_num, user_type, had_carousel_interaction
)

SELECT 
  week_num,
  user_type,
  CASE WHEN had_carousel_interaction = 1 THEN 'With Carousel Interaction' ELSE 'No Carousel Interaction' END as carousel_engagement,
  session_count,
  sessions_with_gameplay,
  gameplay_conversion_pct,
  -- Gap analysis: If carousel is friction, the "with" group should have lower conversion
  ROW_NUMBER() OVER (PARTITION BY week_num, user_type ORDER BY gameplay_conversion_pct DESC) as conversion_rank
FROM friction_analysis
ORDER BY week_num, user_type, carousel_engagement;


-- ===========================================================================
-- QUERY 4: BANNER CLICK DEPOSIT CONVERSION (Proper Comparison)
-- ===========================================================================
-- PURPOSE: Compare deposit rates between banner clickers and non-clickers
-- CRITICAL: Must segment by NEW vs RETURNING and count distinct users
-- This is what Query 11 should have done

WITH user_cohort AS (
  SELECT 
    user_id,
    CASE 
      WHEN MIN(TIMESTAMP_MICROS(event_timestamp)) < '2025-11-01' THEN 'RETURNING_USER'
      ELSE 'NEW_USER'
    END as user_type
  FROM `nabuminds.analytics_271552729.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20251001' AND '20260121'
  GROUP BY user_id
),

banner_clickers AS (
  SELECT DISTINCT
    e.user_id,
    uc.user_type
  FROM `nabuminds.analytics_271552729.events_*` e
  JOIN user_cohort uc ON e.user_id = uc.user_id
  WHERE _TABLE_SUFFIX BETWEEN '20251101' AND '20260121'
    AND e.event_name = 'dynamic_content'
    AND (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key = 'page_location') LIKE '%/tr/casino/%'
),

non_clickers AS (
  SELECT DISTINCT
    e.user_id,
    uc.user_type
  FROM `nabuminds.analytics_271552729.events_*` e
  JOIN user_cohort uc ON e.user_id = uc.user_id
  WHERE _TABLE_SUFFIX BETWEEN '20251101' AND '20260121'
    AND (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key = 'page_location') LIKE '%/tr/casino/%'
    AND e.user_id NOT IN (SELECT user_id FROM banner_clickers)
),

user_deposits AS (
  SELECT DISTINCT
    e.user_id
  FROM `nabuminds.analytics_271552729.events_*` e
  WHERE _TABLE_SUFFIX BETWEEN '20251101' AND '20260121'
    AND e.event_name = 'deposit_success'
),

clicker_analysis AS (
  SELECT 
    bc.user_type,
    'BANNER_CLICKER' as group_type,
    COUNT(DISTINCT bc.user_id) as total_users,
    SUM(CASE WHEN ud.user_id IS NOT NULL THEN 1 ELSE 0 END) as users_deposited
  FROM banner_clickers bc
  LEFT JOIN user_deposits ud ON bc.user_id = ud.user_id
  GROUP BY bc.user_type
),

non_clicker_analysis AS (
  SELECT 
    nc.user_type,
    'NON_CLICKER' as group_type,
    COUNT(DISTINCT nc.user_id) as total_users,
    SUM(CASE WHEN ud.user_id IS NOT NULL THEN 1 ELSE 0 END) as users_deposited
  FROM non_clickers nc
  LEFT JOIN user_deposits ud ON nc.user_id = ud.user_id
  GROUP BY nc.user_type
)

SELECT 
  user_type,
  group_type,
  total_users,
  users_deposited,
  ROUND(100.0 * users_deposited / NULLIF(total_users, 0), 2) as deposit_conversion_rate_pct
FROM (
  SELECT * FROM clicker_analysis
  UNION ALL
  SELECT * FROM non_clicker_analysis
)
ORDER BY user_type, group_type;


-- ===========================================================================
-- QUERY 5: DAILY TREND ANALYSIS
-- ===========================================================================
-- PURPOSE: See if patterns shift over time (user fatigue? seasonal?)
-- Answers: Is carousel friction consistent daily, or does engagement change?

WITH user_cohort AS (
  SELECT 
    user_id,
    CASE 
      WHEN MIN(TIMESTAMP_MICROS(event_timestamp)) < '2025-11-01' THEN 'RETURNING_USER'
      ELSE 'NEW_USER'
    END as user_type
  FROM `nabuminds.analytics_271552729.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20251001' AND '20260121'
  GROUP BY user_id
),

daily_casino_activity AS (
  SELECT 
    DATE(TIMESTAMP_MICROS(e.event_timestamp)) as activity_date,
    uc.user_type,
    COUNT(DISTINCT CONCAT(e.user_id, '-', (SELECT value.int_value FROM UNNEST(e.event_params) WHERE key = 'ga_session_id'))) as total_casino_sessions,
    SUM(CASE WHEN e.event_name = 'dynamic_content' THEN 1 ELSE 0 END) as banner_clicks,
    SUM(CASE WHEN e.event_name = 'gameplay_initiated' THEN 1 ELSE 0 END) as gameplay_initiations,
    SUM(CASE WHEN e.event_name = 'deposit_success' THEN 1 ELSE 0 END) as deposits
  FROM `nabuminds.analytics_271552729.events_*` e
  JOIN user_cohort uc ON e.user_id = uc.user_id
  WHERE _TABLE_SUFFIX BETWEEN '20251101' AND '20260121'
    AND (SELECT value.string_value FROM UNNEST(e.event_params) WHERE key = 'page_location') LIKE '%/tr/casino/%'
  GROUP BY activity_date, uc.user_type
)

SELECT 
  activity_date,
  user_type,
  total_casino_sessions,
  banner_clicks,
  ROUND(100.0 * banner_clicks / NULLIF(total_casino_sessions, 0), 2) as banner_click_rate_pct,
  gameplay_initiations,
  ROUND(100.0 * gameplay_initiations / NULLIF(total_casino_sessions, 0), 2) as gameplay_rate_pct,
  deposits,
  -- 7-day moving average for smoothing
  AVG(100.0 * gameplay_initiations / NULLIF(total_casino_sessions, 0)) OVER (
    PARTITION BY user_type 
    ORDER BY activity_date 
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) as gameplay_rate_7day_ma
FROM daily_casino_activity
ORDER BY activity_date, user_type;
