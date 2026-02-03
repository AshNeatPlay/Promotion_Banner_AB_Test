WITH user_cohort AS (
  SELECT CAST(user_id AS STRING) as player_id_ga,
    CASE WHEN MIN(TIMESTAMP_MICROS(event_timestamp)) < '2025-11-01' THEN 'RETURNING_USER' ELSE 'NEW_USER' END as user_type
  FROM `nabuminds.analytics_271552729.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20251001' AND '20260128'
  GROUP BY user_id
),

successful_deposits AS (
  SELECT 
    player_id,
    DATE(CAST(date_created AS TIMESTAMP)) as deposit_date,
    uc.user_type,
    COUNT(*) as transactions,
    AVG(transaction_amount_EUR) as avg_amount,
    SUM(transaction_amount_EUR) as total_amount
  FROM `site_ods.avos_player_deposits` pd
  LEFT JOIN user_cohort uc ON pd.player_id = uc.player_id_ga
  WHERE DATE(CAST(pd.date_created AS TIMESTAMP)) BETWEEN '2025-11-01' AND '2026-01-28'
    AND business_unit_name = 'JETBAHIS.COM'
    AND payment_status IN ('CAPTURED', 'COMPLETED')
    AND uc.user_type IS NOT NULL
  GROUP BY player_id, deposit_date, user_type
)

SELECT 
  deposit_date,
  user_type,
  COUNT(DISTINCT player_id) as users_deposited,
  ROUND(AVG(transactions), 3) as avg_transactions_per_user,
  ROUND(AVG(avg_amount), 2) as avg_amount_per_user,
  ROUND(SUM(total_amount), 2) as total_revenue
FROM successful_deposits
GROUP BY deposit_date, user_type
ORDER BY deposit_date, user_type;
