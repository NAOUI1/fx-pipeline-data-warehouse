-- =====================================================
-- FX DATA WAREHOUSE - VALIDATION QUERIES
-- =====================================================
-- These queries demonstrate that the dataset is usable
-- for analytics and can be easily joined with other DWH data
-- =====================================================

USE fx_dwh;

-- =====================================================
-- 1. DATA QUALITY CHECKS
-- =====================================================

-- 1.1 Check table row counts
SELECT 
    'dim_currencies' AS table_name, 
    COUNT(*) AS row_count 
FROM dim_currencies
UNION ALL
SELECT 
    'fact_fx_rates_daily', 
    COUNT(*) 
FROM fact_fx_rates_daily
UNION ALL
SELECT 
    'fact_fx_rates_ytd', 
    COUNT(*) 
FROM fact_fx_rates_ytd
UNION ALL
SELECT 
    'pipeline_execution_log', 
    COUNT(*) 
FROM pipeline_execution_log;


-- 1.2 Verify data freshness and coverage
SELECT 
    'Daily Rates' AS table_name,
    MIN(rate_date) AS oldest_date,
    MAX(rate_date) AS latest_date,
    COUNT(DISTINCT rate_date) AS unique_dates,
    COUNT(DISTINCT CONCAT(base_currency, '/', quote_currency)) AS unique_pairs
FROM fact_fx_rates_daily
UNION ALL
SELECT 
    'YTD Metrics',
    MIN(rate_date),
    MAX(rate_date),
    COUNT(DISTINCT rate_date),
    COUNT(DISTINCT CONCAT(base_currency, '/', quote_currency))
FROM fact_fx_rates_ytd;


-- 1.3 Verify all 42 currency pairs exist
SELECT 
    base_currency,
    quote_currency,
    COUNT(DISTINCT rate_date) AS days_available,
    MIN(rate_date) AS first_date,
    MAX(rate_date) AS last_date
FROM fact_fx_rates_daily
GROUP BY base_currency, quote_currency
ORDER BY base_currency, quote_currency;
-- Expected: 42 rows (7 currencies × 6 pairs each)


-- 1.4 Check for missing dates (weekends/holidays expected)
SELECT 
    rate_date,
    COUNT(DISTINCT CONCAT(base_currency, '/', quote_currency)) AS pair_count
FROM fact_fx_rates_daily
GROUP BY rate_date
HAVING pair_count < 42
ORDER BY rate_date DESC
LIMIT 10;
-- Expected: Empty or very few results (only major holidays)


-- 1.5 Check pipeline execution logs
SELECT 
    execution_date,
    pipeline_step,
    status,
    rows_processed,
    duration_seconds,
    error_message
FROM pipeline_execution_log
ORDER BY execution_date DESC
LIMIT 20;


-- =====================================================
-- 2. EXAMPLE QUERY 1: LOOKUP BY DATE AND CURRENCY
-- =====================================================

-- Get EUR/NOK exchange rate on a specific date
SELECT 
    rate_date,
    base_currency,
    quote_currency,
    exchange_rate,
    CONCAT(base_currency, '/', quote_currency) AS currency_pair,
    source
FROM fact_fx_rates_daily
WHERE base_currency = 'EUR'
  AND quote_currency = 'NOK'
  AND rate_date = '2024-12-15'
ORDER BY rate_date DESC;

-- Use case: "What was the EUR to NOK rate on December 15, 2024?"


-- =====================================================
-- 3. EXAMPLE QUERY 2: LATEST RATES FOR ALL PAIRS
-- =====================================================

-- Get the most recent exchange rate for each currency pair
SELECT 
    rate_date,
    CONCAT(base_currency, '/', quote_currency) AS currency_pair,
    exchange_rate,
    source
FROM vw_latest_fx_rates
WHERE base_currency = 'EUR'
ORDER BY quote_currency;

-- Use case: "Show me today's EUR exchange rates for all currencies"


-- =====================================================
-- 4. EXAMPLE QUERY 3: YTD METRICS (CORE REQUIREMENT)
-- =====================================================

-- Get Year-to-Date metrics for EUR/NOK
SELECT 
    rate_date,
    CONCAT(base_currency, '/', quote_currency) AS currency_pair,
    ytd_avg_rate AS avg_rate_ytd,
    ytd_min_rate AS min_rate_ytd,
    ytd_max_rate AS max_rate_ytd,
    ytd_first_rate AS rate_jan_1,
    ytd_last_rate AS rate_current,
    ytd_change_pct AS change_pct_ytd,
    ytd_days_count AS trading_days
FROM fact_fx_rates_ytd
WHERE base_currency = 'EUR'
  AND quote_currency = 'NOK'
ORDER BY rate_date DESC
LIMIT 1;

-- Use case: "What's the YTD performance of EUR/NOK?"
-- Interpretation:
-- - ytd_avg_rate: Average rate from Jan 1 to today
-- - ytd_change_pct: How much has the rate changed since Jan 1?
-- - ytd_min/max: Range of fluctuation this year


-- =====================================================
-- 5. EXAMPLE QUERY 4: COMPARE MULTIPLE PAIRS YTD
-- =====================================================

-- Compare YTD performance across all EUR pairs
SELECT 
    CONCAT(base_currency, '/', quote_currency) AS currency_pair,
    ytd_first_rate AS rate_jan_1,
    ytd_last_rate AS rate_latest,
    ytd_change_pct AS change_pct,
    ytd_min_rate AS min_ytd,
    ytd_max_rate AS max_ytd,
    ytd_avg_rate AS avg_ytd,
    ytd_days_count AS days
FROM fact_fx_rates_ytd
WHERE rate_date = (SELECT MAX(rate_date) FROM fact_fx_rates_ytd)
  AND base_currency = 'EUR'
ORDER BY ytd_change_pct DESC;

-- Use case: "Which currencies gained/lost the most against EUR this year?"


-- =====================================================
-- 6. EXAMPLE QUERY 5: TIME SERIES ANALYSIS
-- =====================================================

-- View EUR/NOK daily rates for the last 30 days with daily changes
SELECT 
    rate_date,
    exchange_rate,
    LAG(exchange_rate) OVER (ORDER BY rate_date) AS previous_rate,
    ROUND(
        (exchange_rate - LAG(exchange_rate) OVER (ORDER BY rate_date)) / 
        LAG(exchange_rate) OVER (ORDER BY rate_date) * 100, 
        2
    ) AS daily_change_pct
FROM fact_fx_rates_daily
WHERE base_currency = 'EUR'
  AND quote_currency = 'NOK'
  AND rate_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
ORDER BY rate_date DESC;

-- Use case: "Show me EUR/NOK trend for the last month"


-- =====================================================
-- 7. EXAMPLE QUERY 6: CROSS-PAIR VALIDATION
-- =====================================================

-- Verify cross-pair calculation accuracy
-- Compare EUR/NOK with calculated (EUR/SEK × SEK/NOK)
SELECT 
    d1.rate_date,
    d1.exchange_rate AS eur_nok_stored,
    ROUND(d2.exchange_rate * d3.exchange_rate, 8) AS eur_nok_calculated,
    ROUND(
        ABS(d1.exchange_rate - (d2.exchange_rate * d3.exchange_rate)) / d1.exchange_rate * 100,
        6
    ) AS difference_pct
FROM fact_fx_rates_daily d1
JOIN fact_fx_rates_daily d2 
    ON d1.rate_date = d2.rate_date 
    AND d2.base_currency = 'EUR' 
    AND d2.quote_currency = 'SEK'
JOIN fact_fx_rates_daily d3 
    ON d1.rate_date = d3.rate_date 
    AND d3.base_currency = 'SEK' 
    AND d3.quote_currency = 'NOK'
WHERE d1.base_currency = 'EUR'
  AND d1.quote_currency = 'NOK'
ORDER BY d1.rate_date DESC
LIMIT 10;

-- Use case: "Validate cross-pair calculation consistency"
-- Expected: difference_pct should be < 0.001%


-- =====================================================
-- 8. JOINING WITH OTHER DWH DATA (EXAMPLE)
-- =====================================================

-- Hypothetical: Join FX rates with a sales fact table
-- This demonstrates how FX data integrates with other warehouse tables

/*
-- Example if you had a sales table:
SELECT 
    s.sale_date,
    s.product_id,
    s.amount_local_currency,
    s.local_currency,
    fx.exchange_rate AS fx_to_eur,
    ROUND(s.amount_local_currency / fx.exchange_rate, 2) AS amount_eur
FROM sales_fact s
LEFT JOIN fact_fx_rates_daily fx
    ON s.sale_date = fx.rate_date
    AND s.local_currency = fx.quote_currency
    AND fx.base_currency = 'EUR'
WHERE s.sale_date >= '2024-01-01'
LIMIT 100;
*/

-- The FX schema is designed to be easily joined:
-- - Simple composite key (date + currency_pair)
-- - Dimension table for currency metadata
-- - Standard date column (DATE type, no timestamps)


-- =====================================================
-- 9. ANALYTICAL QUERIES: VOLATILITY ANALYSIS
-- =====================================================

-- Calculate rolling 30-day volatility (standard deviation)
SELECT 
    rate_date,
    base_currency,
    quote_currency,
    exchange_rate,
    ROUND(
        STDDEV(exchange_rate) OVER (
            PARTITION BY base_currency, quote_currency 
            ORDER BY rate_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ),
        6
    ) AS rolling_30d_stddev
FROM fact_fx_rates_daily
WHERE base_currency = 'EUR'
  AND quote_currency = 'NOK'
ORDER BY rate_date DESC
LIMIT 30;

-- Use case: "Which currency pairs are most volatile?"


-- =====================================================
-- 10. YTD METRICS: MONTH-BY-MONTH BREAKDOWN
-- =====================================================

-- Show YTD metrics at the end of each month
SELECT 
    LAST_DAY(rate_date) AS month_end,
    base_currency,
    quote_currency,
    ytd_avg_rate,
    ytd_change_pct,
    ytd_days_count
FROM fact_fx_rates_ytd
WHERE base_currency = 'EUR'
  AND quote_currency = 'NOK'
  AND rate_date = LAST_DAY(rate_date)  -- Only month-end dates
  AND YEAR(rate_date) = YEAR(CURDATE())  -- Current year only
ORDER BY month_end;

-- Use case: "Show me month-by-month YTD progression for EUR/NOK"


-- =====================================================
-- 11. CURRENCY STRENGTH INDEX
-- =====================================================

-- Calculate average performance vs. all other currencies
SELECT 
    base_currency,
    COUNT(DISTINCT quote_currency) AS pairs_count,
    AVG(ytd_change_pct) AS avg_ytd_change,
    MIN(ytd_change_pct) AS worst_pair,
    MAX(ytd_change_pct) AS best_pair
FROM fact_fx_rates_ytd
WHERE rate_date = (SELECT MAX(rate_date) FROM fact_fx_rates_ytd)
GROUP BY base_currency
ORDER BY avg_ytd_change DESC;

-- Use case: "Which currency is strongest/weakest overall this year?"


-- =====================================================
-- 12. DATA COMPLETENESS CHECK
-- =====================================================

-- Find any gaps in the time series
WITH date_spine AS (
    SELECT 
        MIN(rate_date) + INTERVAL n DAY AS check_date
    FROM fact_fx_rates_daily
    CROSS JOIN (
        SELECT 0 AS n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 
        UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9
    ) numbers
    WHERE MIN(rate_date) + INTERVAL n DAY <= (SELECT MAX(rate_date) FROM fact_fx_rates_daily)
)
SELECT 
    ds.check_date,
    COUNT(f.rate_date) AS records_found,
    DAYNAME(ds.check_date) AS day_of_week
FROM date_spine ds
LEFT JOIN fact_fx_rates_daily f
    ON ds.check_date = f.rate_date
    AND f.base_currency = 'EUR'
    AND f.quote_currency = 'NOK'
GROUP BY ds.check_date
HAVING records_found = 0
ORDER BY ds.check_date DESC
LIMIT 20;

-- Use case: "Are there any missing trading days?"
-- Expected: Only weekends and holidays (Sat/Sun)


-- =====================================================
-- 13. PERFORMANCE TEST QUERY
-- =====================================================

-- Test query performance on indexed columns
EXPLAIN
SELECT 
    rate_date,
    base_currency,
    quote_currency,
    exchange_rate
FROM fact_fx_rates_daily
WHERE rate_date BETWEEN '2024-01-01' AND '2024-12-31'
  AND base_currency = 'EUR'
  AND quote_currency IN ('NOK', 'SEK', 'PLN');

-- Should show "Using index" or "Using where" with index scan


-- =====================================================
-- END OF VALIDATION QUERIES
-- =====================================================
-- These queries demonstrate:
-- 1. Data quality and completeness
-- 2. Easy date/currency lookups
-- 3. YTD calculations (core requirement)
-- 4. Time series analysis
-- 5. Cross-pair validation
-- 6. Integration readiness with other DWH tables
-- =====================================================