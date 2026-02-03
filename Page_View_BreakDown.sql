WITH extracted_params AS (
  SELECT
    user_id,
    event_timestamp,
    event_name,
    param.key AS param_key,
    param.value.string_value AS param_string_value
  FROM `nabuminds.analytics_271552729.events_*`
  CROSS JOIN UNNEST(event_params) AS param
  WHERE 
    _TABLE_SUFFIX BETWEEN '20260121' AND '20260128'
    AND user_id IS NOT NULL
),
page_data AS (
  SELECT
    user_id,
    param_string_value AS page_location,
    COUNT(*) AS count
  FROM extracted_params
  WHERE param_key = 'page_location'
    AND param_string_value IS NOT NULL
  GROUP BY user_id, page_location
),
categorized AS (
  SELECT
    user_id,
    page_location,
    CASE 
      WHEN page_location LIKE 'https://jetbahis%.com/' 
        OR page_location LIKE 'https://jetbahis%.com/tr' 
        OR page_location LIKE 'https://jetbahis%.com/en'
        THEN 'Home'
      WHEN page_location LIKE '%/canli-casino%' THEN 'Live Casino'
      WHEN page_location LIKE '%/casino%' THEN 'Casino'
      WHEN page_location LIKE '%/bahis%' OR page_location LIKE '%/sportsbook%' THEN 'Sportsbook'
      WHEN page_location LIKE '%/canli-bahis%' THEN 'Live Betting'
      WHEN page_location LIKE '%/virtual-football%'
        OR page_location LIKE '%/oyun-penceresi%'
        OR page_location LIKE '%/game-iframe%'
        THEN 'Virtual Sports'
      WHEN page_location LIKE '%/account%' 
        OR page_location LIKE '%modal:deposit%'
        OR page_location LIKE '%modal:pending-withdrawals%'
        THEN 'Account'
      WHEN page_location LIKE '%/destek%' OR page_location LIKE '%/support%' THEN 'Support'
      WHEN page_location LIKE '%/kampanyalar%' OR page_location LIKE '%bonus%' THEN 'Promotions'
      WHEN page_location LIKE '%/kayit-ol%' THEN 'Registration'
      WHEN page_location LIKE '%/login%' OR page_location LIKE '%session_expired%' THEN 'Login/Session'
      ELSE 'Other'
    END AS page_category,
    count
  FROM page_data
)
SELECT
  page_category,
  COUNT(DISTINCT page_location) AS unique_pages,
  SUM(count) AS total_page_views,
  COUNT(DISTINCT user_id) AS unique_users_in_category,
  ROUND(100 * SUM(count) / SUM(SUM(count)) OVER (), 1) AS percent_of_all_page_views
FROM categorized
GROUP BY page_category
ORDER BY total_page_views DESC;
