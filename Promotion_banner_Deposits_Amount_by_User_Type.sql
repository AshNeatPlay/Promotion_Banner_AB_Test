# Query 6: Daily Deposit Amounts by User Type (with Payment Data)
/*
This query joins the GA4 user segmentation (NEW vs RETURNING) with payment transaction data to show whether average deposit amounts increased during the payment restriction period.
*/
## SQL Query

WITH user_cohort AS (
  -- Segment users as NEW (first appearance Nov 1+) or RETURNING (before Nov 1)
  SELECT 
    CAST(user_id AS STRING) as player_id_ga,
    CASE 
      WHEN MIN(TIMESTAMP_MICROS(event_timestamp)) < '2025-11-01' THEN 'RETURNING_USER'
      ELSE 'NEW_USER'
    END as user_type
  FROM `nabuminds.analytics_271552729.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20251001' AND '20260121'
  GROUP BY user_id
),

payment_data AS (
  -- Get successful deposits from JETBAHIS only (matching original analysis)
  SELECT 
    DATE(CAST(date_created AS TIMESTAMP)) as deposit_date,
    player_id,
    transaction_amount_EUR,
    payment_status,
    business_unit_name
  FROM `site_ods.avos_player_deposits`
  WHERE DATE(CAST(date_created AS TIMESTAMP)) BETWEEN '2025-11-01' AND '2026-01-29'
    AND business_unit_name = 'JETBAHIS.COM'
    AND payment_status IN ('CAPTURED', 'COMPLETED')  -- Only successful deposits
    AND transaction_amount_EUR > 0
),

daily_deposits_by_user_type AS (
  SELECT 
    pd.deposit_date,
    uc.user_type,
    COUNT(DISTINCT pd.player_id) as unique_depositors,
    COUNT(*) as total_deposit_transactions,
    ROUND(AVG(pd.transaction_amount_EUR), 2) as avg_deposit_amount_EUR,
    ROUND(SUM(pd.transaction_amount_EUR), 2) as total_deposited_EUR,
    ROUND(MIN(pd.transaction_amount_EUR), 2) as min_amount_EUR,
    ROUND(MAX(pd.transaction_amount_EUR), 2) as max_amount_EUR,
    ROUND(STDDEV(pd.transaction_amount_EUR), 2) as stddev_amount_EUR
  FROM payment_data pd
  LEFT JOIN user_cohort uc 
    ON pd.player_id = uc.player_id_ga
  WHERE uc.user_type IS NOT NULL  -- Only users we can classify
  GROUP BY pd.deposit_date, uc.user_type
)

SELECT 
  deposit_date,
  user_type,
  unique_depositors,
  total_deposit_transactions,
  avg_deposit_amount_EUR,
  total_deposited_EUR,
  min_amount_EUR,
  max_amount_EUR,
  stddev_amount_EUR
FROM daily_deposits_by_user_type
ORDER BY deposit_date, user_type;
/*
## What This Shows

- **avg_deposit_amount_EUR**: Did average deposit size increase during payment restrictions?
- **total_deposited_EUR**: Total money coming in (revenue impact)
- **unique_depositors**: How many users made deposits (depth of monetization)
- **total_deposit_transactions**: Transaction count (volume)

## Key Analysis Points

1. **Nov 1-7 baseline** → Average deposit size with no restrictions
2. **Nov 10-30 (restriction onset)** → Did average deposits increase?
3. **Dec-Jan (full restriction)** → Is the higher average size sustained?

If the hypothesis is correct:
- **NEW users**: Average deposit size might increase slightly, but transaction volume stays low
- **RETURNING users**: Average deposit size should increase significantly as they adapt to minimums

## How to Correlate with Query 5

Once you have these results:
- Join by `deposit_date` and `user_type`
- Compare `avg_deposit_amount_EUR` with `banner_click_rate_pct` from Query 5
- The 0.86 correlation for RETURNING users might disappear (or strengthen) once you normalize by deposit size

---

**Expected Result:**
If payment restrictions drove minimums UP:
- Nov 1-7: average ~20-30 EUR per deposit
- Dec-Jan: average ~40-60 EUR per deposit (adaptation to minimums)
- **This would prove:** The 88% transaction volume drop was offset by larger individual deposits (revenue stabilization)

If true: The banner's correlation with deposits becomes more meaningful (it's helping users justify larger amounts).
*/
