-- CREATE DATABASE fx_dwh;
USE fx_dwh;


-- Table 1: Dimension des devises
-- =============================================
DROP TABLE IF EXISTS dim_currencies;

CREATE TABLE dim_currencies (
    currency_code VARCHAR(3) PRIMARY KEY,
    currency_name VARCHAR(100) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Insertion des 7 devises requises
INSERT INTO dim_currencies (currency_code, currency_name) VALUES
('NOK', 'Norwegian Krone'),
('EUR', 'Euro'),
('SEK', 'Swedish Krona'),
('PLN', 'Polish Zloty'),
('RON', 'Romanian Leu'),
('DKK', 'Danish Krone'),
('CZK', 'Czech Koruna');


-- Table 2: Taux de change quotidiens (Fact Table)
-- =============================================
DROP TABLE IF EXISTS fact_fx_rates_daily;

CREATE TABLE fact_fx_rates_daily (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    rate_date DATE NOT NULL,
    base_currency VARCHAR(3) NOT NULL,
    quote_currency VARCHAR(3) NOT NULL,
    exchange_rate DECIMAL(18, 8) NOT NULL,
    source VARCHAR(50) DEFAULT 'Frankfurter',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- Contraintes
    CONSTRAINT fk_base_currency FOREIGN KEY (base_currency) 
        REFERENCES dim_currencies(currency_code),
    CONSTRAINT fk_quote_currency FOREIGN KEY (quote_currency) 
        REFERENCES dim_currencies(currency_code),
    CONSTRAINT chk_different_currencies CHECK (base_currency != quote_currency),
    CONSTRAINT chk_positive_rate CHECK (exchange_rate > 0),
    
    -- Index unique pour éviter les doublons
    UNIQUE KEY uk_rate_date_pair (rate_date, base_currency, quote_currency),
    
    -- Index pour les recherches
    INDEX idx_rate_date (rate_date),
    INDEX idx_base_currency (base_currency),
    INDEX idx_quote_currency (quote_currency),
    INDEX idx_date_base (rate_date, base_currency)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- Table 3: Métriques YTD (Year-To-Date)
-- =============================================
DROP TABLE IF EXISTS fact_fx_rates_ytd;

CREATE TABLE fact_fx_rates_ytd (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    rate_date DATE NOT NULL,
    base_currency VARCHAR(3) NOT NULL,
    quote_currency VARCHAR(3) NOT NULL,
    
    -- Métriques YTD
    ytd_avg_rate DECIMAL(18, 8) NOT NULL,
    ytd_min_rate DECIMAL(18, 8) NOT NULL,
    ytd_max_rate DECIMAL(18, 8) NOT NULL,
    ytd_first_rate DECIMAL(18, 8) NOT NULL,
    ytd_last_rate DECIMAL(18, 8) NOT NULL,
    ytd_days_count INT NOT NULL,
    
    -- Calculs additionnels
    ytd_variance DECIMAL(18, 8),
    ytd_std_dev DECIMAL(18, 8),
    ytd_change_pct DECIMAL(10, 4),  -- (last - first) / first * 100
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- Contraintes
    CONSTRAINT fk_ytd_base_currency FOREIGN KEY (base_currency) 
        REFERENCES dim_currencies(currency_code),
    CONSTRAINT fk_ytd_quote_currency FOREIGN KEY (quote_currency) 
        REFERENCES dim_currencies(currency_code),
    
    -- Index unique
    UNIQUE KEY uk_ytd_date_pair (rate_date, base_currency, quote_currency),
    
    -- Index pour les recherches
    INDEX idx_ytd_date (rate_date),
    INDEX idx_ytd_base (base_currency),
    INDEX idx_ytd_quote (quote_currency)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


-- Table 4: Logs d'exécution du pipeline
-- =============================================
DROP TABLE IF EXISTS pipeline_execution_log;

CREATE TABLE pipeline_execution_log (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    execution_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    pipeline_step VARCHAR(50) NOT NULL,  -- 'extract', 'transform', 'load'
    status VARCHAR(20) NOT NULL,  -- 'success', 'failed', 'running'
    rows_processed INT DEFAULT 0,
    error_message TEXT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds INT,
    
    INDEX idx_execution_date (execution_date),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
dim_currencies

-- =============================================
-- Vues utiles pour l'analyse
-- =============================================

-- Vue: Derniers taux de change disponibles
DROP VIEW IF EXISTS vw_latest_fx_rates;

CREATE VIEW vw_latest_fx_rates AS
SELECT 
    f.rate_date,
    f.base_currency,
    f.quote_currency,
    f.exchange_rate,
    CONCAT(f.base_currency, '/', f.quote_currency) AS currency_pair
FROM fact_fx_rates_daily f
INNER JOIN (
    SELECT base_currency, quote_currency, MAX(rate_date) AS max_date
    FROM fact_fx_rates_daily
    GROUP BY base_currency, quote_currency
) latest ON f.base_currency = latest.base_currency 
    AND f.quote_currency = latest.quote_currency 
    AND f.rate_date = latest.max_date;


-- Vue: YTD le plus récent
DROP VIEW IF EXISTS vw_latest_ytd;

CREATE VIEW vw_latest_ytd AS
SELECT 
    y.rate_date,
    y.base_currency,
    y.quote_currency,
    CONCAT(y.base_currency, '/', y.quote_currency) AS currency_pair,
    y.ytd_avg_rate,
    y.ytd_min_rate,
    y.ytd_max_rate,
    y.ytd_change_pct,
    y.ytd_days_count
FROM fact_fx_rates_ytd y
INNER JOIN (
    SELECT base_currency, quote_currency, MAX(rate_date) AS max_date
    FROM fact_fx_rates_ytd
    GROUP BY base_currency, quote_currency
) latest ON y.base_currency = latest.base_currency 
    AND y.quote_currency = latest.quote_currency 
    AND y.rate_date = latest.max_date;


-- =============================================
-- Vérification de la création des tables
-- =============================================
SELECT 
    TABLE_NAME, 
    TABLE_ROWS, 
    CREATE_TIME 
FROM information_schema.TABLES 
WHERE TABLE_SCHEMA = 'fx_dwh' 
ORDER BY TABLE_NAME;