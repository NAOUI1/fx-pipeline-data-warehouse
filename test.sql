-- Compter les taux quotidiens
SELECT COUNT(*) FROM fact_fx_rates_daily;

-- Voir les derniers taux EUR/NOK
SELECT * FROM fact_fx_rates_daily 
WHERE base_currency='EUR' AND quote_currency='NOK' 
ORDER BY rate_date DESC LIMIT 10;

-- Voir les métriques YTD les plus récentes
SELECT * FROM vw_latest_ytd 
WHERE base_currency='EUR' AND quote_currency='NOK';