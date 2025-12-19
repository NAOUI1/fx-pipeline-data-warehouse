# FX Data Pipeline - Technical Case

A production-ready ETL pipeline that extracts foreign exchange rates for 7 currencies, calculates all cross-pairs, computes Year-to-Date metrics, and loads data into a MySQL data warehouse.

## 📋 Table of Contents

- [Overview](#overview)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Running the Pipeline](#running-the-pipeline)
- [Validating Outputs](#validating-outputs)
- [Project Structure](#project-structure)
- [Technical Specifications](#technical-specifications)

---

## 🎯 Overview

This pipeline implements a complete ETL process for FX rates:

**Extract** → Fetches daily FX rates from Frankfurter API  
**Transform** → Calculates 42 cross-currency pairs + YTD metrics  
**Load** → Inserts data into MySQL data warehouse

### Key Features

- **7 currencies**: NOK, EUR, SEK, PLN, RON, DKK, CZK
- **42 cross-pairs**: All possible combinations (e.g., EUR/NOK, NOK/SEK, PLN/DKK)
- **YTD calculations**: Running metrics from January 1st to each date
- **Modular design**: 3 independent scripts for flexible orchestration
- **Production-ready**: Logging, error handling, idempotent operations

---

## 📦 Prerequisites

- **Python 3.8+**
- **MySQL 8.0+** (or MySQL Workbench)
- **Internet connection** (for API access)

---

## 🚀 Installation

### 1. Clone/Download the project

```bash
cd Fx-pipeline
```

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

**Dependencies:**
- `requests` - HTTP calls to Frankfurter API
- `pandas` - Data manipulation
- `pymysql` - MySQL connector
- `sqlalchemy` - Database ORM
- `python-dotenv` - Environment variable management

### 3. Create the MySQL database

Open MySQL Workbench and run:

```sql
CREATE DATABASE fx_dwh CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
```

### 4. Create tables

Execute the entire `create_tables.sql` script in MySQL Workbench.

This creates:
- `dim_currencies` - Currency dimension table
- `fact_fx_rates_daily` - Daily exchange rates (fact table)
- `fact_fx_rates_ytd` - Year-to-date metrics (fact table)
- `pipeline_execution_log` - Pipeline execution tracking
- Views for easier querying

### 5. Configure environment variables

Copy `.env.example` to `.env` and update with your credentials:

```env
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=your_mysql_password
DB_NAME=fx_dwh
START_DATE=2024-01-01
```

---

## ⚙️ Configuration

### Database Connection

Edit `.env` file with your MySQL credentials:

```env
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=your_password_here
DB_NAME=fx_dwh
```

### Pipeline Parameters

- **START_DATE**: First date to extract (default: `2024-01-01`)
- **TEMP_DIR**: Temporary CSV storage (default: `./temp`)

---

## 🏃 Running the Pipeline

### Option A: Run Complete Pipeline (All 3 Steps)

```bash
python run_pipeline.py
```

This executes:
1. Extract → `temp/raw_fx_data.csv`
2. Transform → `temp/transformed_fx_data.csv` + `temp/ytd_metrics.csv`
3. Load → MySQL tables

**Expected output:**
```
============================================================
🚀 STARTING COMPLETE FX PIPELINE
============================================================
📅 Start: 2024-12-16 14:30:00

====================================================================
STEP 1/3: EXTRACTION
====================================================================
🚀 Executing: python scripts/extract.py
📥 Extracting data from 2024-01-01 to 2024-12-16
🌐 API Call: https://api.frankfurter.dev/v1/2024-01-01..2024-12-16
✅ 1,512 records extracted in 2s
💾 Data saved: temp/raw_fx_data.csv
✅ Extraction completed successfully

====================================================================
STEP 2/3: TRANSFORMATION
====================================================================
🚀 Executing: python scripts/transform.py
🔄 Calculating cross-pairs...
✅ 10,584 cross-pairs calculated in 3s
📊 Calculating YTD metrics...
✅ 10,584 YTD records calculated
✅ Transformation completed successfully

====================================================================
STEP 3/3: LOADING
====================================================================
🚀 Executing: python scripts/load.py
💾 Loading 10,584 daily rates...
✅ 10,584 rates inserted
💾 Loading 10,584 YTD metrics...
✅ 10,584 YTD metrics inserted
✅ Loading completed successfully

============================================================
✅ PIPELINE COMPLETED SUCCESSFULLY
============================================================
📅 End: 2024-12-16 14:30:15
⏱️  Total duration: 15s (0.2 min)
============================================================
```

---

### Option B: Run Steps Individually

Useful for debugging or orchestration tools like Azure Data Factory.

#### Step 1: Extract

```bash
python scripts/extract.py
```

**Arguments:**
```bash
python scripts/extract.py --start-date 2024-01-01 --end-date 2024-12-31 --output ./temp/custom.csv
```

**Output:** CSV file with columns:
- `rate_date` - Trading date
- `base_currency` - Base currency (EUR)
- `quote_currency` - Quote currency
- `exchange_rate` - Exchange rate

#### Step 2: Transform

```bash
python scripts/transform.py
```

**Arguments:**
```bash
python scripts/transform.py --input ./temp/raw_fx_data.csv --output-cross ./temp/cross.csv --output-ytd ./temp/ytd.csv
```

**Outputs:**
- `transformed_fx_data.csv` - All 42 cross-pairs
- `ytd_metrics.csv` - YTD metrics for each pair

#### Step 3: Load

```bash
python scripts/load.py
```

**Arguments:**
```bash
python scripts/load.py --input-cross ./temp/cross.csv --input-ytd ./temp/ytd.csv
```

**Result:** Data loaded into MySQL tables

---

## ✅ Validating Outputs

### 1. Check File Outputs (After Transform)

```bash
# Check that CSV files were created
ls -lh temp/

# Expected files:
# raw_fx_data.csv          (~150 KB)
# transformed_fx_data.csv  (~1.2 MB)
# ytd_metrics.csv          (~1.5 MB)

# Preview first 10 rows of cross-pairs
head -n 10 temp/transformed_fx_data.csv
```

### 2. Verify Database Loading

Open MySQL Workbench and run:

```sql
-- 1. Check table row counts
SELECT 
    'fact_fx_rates_daily' AS table_name, COUNT(*) AS row_count 
FROM fact_fx_rates_daily
UNION ALL
SELECT 
    'fact_fx_rates_ytd', COUNT(*) 
FROM fact_fx_rates_ytd;

-- Expected: ~10,000-15,000 rows per table (depends on date range)
```

```sql
-- 2. Verify data freshness
SELECT 
    'Daily Rates' AS table_name,
    MIN(rate_date) AS oldest_date,
    MAX(rate_date) AS latest_date,
    COUNT(DISTINCT rate_date) AS date_count
FROM fact_fx_rates_daily
UNION ALL
SELECT 
    'YTD Metrics',
    MIN(rate_date),
    MAX(rate_date),
    COUNT(DISTINCT rate_date)
FROM fact_fx_rates_ytd;

-- Expected: 
-- oldest_date = 2024-01-01 (or your START_DATE)
-- latest_date = today or yesterday (latest trading day)
```

```sql
-- 3. Verify currency coverage
SELECT 
    base_currency,
    quote_currency,
    COUNT(*) AS days_available
FROM fact_fx_rates_daily
GROUP BY base_currency, quote_currency
ORDER BY base_currency, quote_currency;

-- Expected: 42 rows (7×6 currency pairs), each with ~250+ days
```

```sql
-- 4. Check pipeline execution logs
SELECT 
    execution_date,
    pipeline_step,
    status,
    rows_processed,
    duration_seconds
FROM pipeline_execution_log
ORDER BY execution_date DESC
LIMIT 10;

-- Expected: 'success' status for extract, transform, load steps
```

### 3. Example Validation Queries

See the **Example Queries** section below for functional validation.

---

## 📊 Example Queries

### Query 1: Lookup Exchange Rate by Date and Currency Pair

```sql
-- Get EUR/NOK rate on a specific date
SELECT 
    rate_date,
    base_currency,
    quote_currency,
    exchange_rate,
    CONCAT(base_currency, '/', quote_currency) AS currency_pair
FROM fact_fx_rates_daily
WHERE base_currency = 'EUR'
  AND quote_currency = 'NOK'
  AND rate_date = '2024-12-15'
ORDER BY rate_date DESC;
```

**Expected output:**
```
+-----------+---------------+----------------+---------------+---------------+
| rate_date | base_currency | quote_currency | exchange_rate | currency_pair |
+-----------+---------------+----------------+---------------+---------------+
| 2024-12-15| EUR           | NOK            |    11.7234000 | EUR/NOK       |
+-----------+---------------+----------------+---------------+---------------+
```

---

### Query 2: Get Latest Rates for All Cross-Pairs

```sql
-- View most recent exchange rates for all currency pairs
SELECT 
    rate_date,
    CONCAT(base_currency, '/', quote_currency) AS currency_pair,
    exchange_rate,
    source
FROM vw_latest_fx_rates
WHERE base_currency IN ('EUR', 'NOK', 'SEK')
  AND quote_currency IN ('EUR', 'NOK', 'SEK')
ORDER BY base_currency, quote_currency;
```

**Expected output:**
```
+-----------+---------------+---------------+-------------+
| rate_date | currency_pair | exchange_rate | source      |
+-----------+---------------+---------------+-------------+
| 2024-12-16| EUR/NOK       |   11.72340000 | Frankfurter |
| 2024-12-16| EUR/SEK       |   11.54320000 | Frankfurter |
| 2024-12-16| NOK/EUR       |    0.08530000 | Frankfurter |
| 2024-12-16| NOK/SEK       |    0.98460000 | Frankfurter |
| 2024-12-16| SEK/EUR       |    0.08663000 | Frankfurter |
| 2024-12-16| SEK/NOK       |    1.01563000 | Frankfurter |
+-----------+---------------+---------------+-------------+
```

---

### Query 3: Year-to-Date (YTD) Metrics

```sql
-- Get YTD metrics for EUR/NOK on the latest available date
SELECT 
    rate_date,
    base_currency,
    quote_currency,
    ytd_avg_rate,
    ytd_min_rate,
    ytd_max_rate,
    ytd_first_rate,
    ytd_last_rate,
    ytd_change_pct,
    ytd_days_count
FROM fact_fx_rates_ytd
WHERE base_currency = 'EUR'
  AND quote_currency = 'NOK'
ORDER BY rate_date DESC
LIMIT 1;
```

**Expected output:**
```
+-----------+---------------+----------------+--------------+--------------+--------------+----------------+---------------+----------------+----------------+
| rate_date | base_currency | quote_currency | ytd_avg_rate | ytd_min_rate | ytd_max_rate | ytd_first_rate | ytd_last_rate | ytd_change_pct | ytd_days_count |
+-----------+---------------+----------------+--------------+--------------+--------------+----------------+---------------+----------------+----------------+
| 2024-12-16| EUR           | NOK            |  11.62345000 |  11.32100000 |  11.95600000 |    11.45200000 |   11.72340000 |          2.3700|            252 |
+-----------+---------------+----------------+--------------+--------------+--------------+----------------+---------------+----------------+----------------+
```

**Interpretation:**
- **ytd_avg_rate**: Average EUR/NOK rate from Jan 1 to Dec 16, 2024
- **ytd_min_rate**: Lowest rate in 2024
- **ytd_max_rate**: Highest rate in 2024
- **ytd_first_rate**: Rate on January 1st (first trading day)
- **ytd_last_rate**: Rate on December 16th (latest date)
- **ytd_change_pct**: +2.37% change from Jan 1 to Dec 16
- **ytd_days_count**: 252 trading days with data

---

### Query 4: Compare Multiple Currency Pairs YTD

```sql
-- Compare YTD performance across different currency pairs
SELECT 
    CONCAT(base_currency, '/', quote_currency) AS currency_pair,
    ytd_first_rate AS rate_jan_1,
    ytd_last_rate AS rate_latest,
    ytd_change_pct AS change_pct,
    ytd_avg_rate AS avg_rate,
    ytd_days_count AS days
FROM fact_fx_rates_ytd
WHERE rate_date = (SELECT MAX(rate_date) FROM fact_fx_rates_ytd)
  AND base_currency = 'EUR'
  AND quote_currency IN ('NOK', 'SEK', 'PLN', 'DKK')
ORDER BY ytd_change_pct DESC;
```

**Expected output:**
```
+---------------+-------------+-------------+------------+-------------+------+
| currency_pair | rate_jan_1  | rate_latest | change_pct | avg_rate    | days |
+---------------+-------------+-------------+------------+-------------+------+
| EUR/PLN       |  4.34500000 |  4.45600000 |     2.5500 |  4.39850000 |  252 |
| EUR/NOK       | 11.45200000 | 11.72340000 |     2.3700 | 11.62345000 |  252 |
| EUR/DKK       |  7.45380000 |  7.45940000 |     0.0800 |  7.45550000 |  252 |
| EUR/SEK       | 11.23400000 | 11.54320000 |     2.7500 | 11.38950000 |  252 |
+---------------+-------------+-------------+------------+-------------+------+
```

---

### Query 5: Time Series Analysis (Daily Rates Over Time)

```sql
-- View daily EUR/NOK rates for the last 30 days
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
```

---

### Query 6: Find Cross-Pair Arbitrage Opportunities (Example)

```sql
-- Compare direct vs. calculated cross rates
-- Example: EUR/NOK vs. (EUR/SEK × SEK/NOK)
SELECT 
    d1.rate_date,
    d1.exchange_rate AS eur_nok_direct,
    ROUND(d2.exchange_rate * d3.exchange_rate, 8) AS eur_nok_calculated,
    ROUND(
        ABS(d1.exchange_rate - (d2.exchange_rate * d3.exchange_rate)) / d1.exchange_rate * 100,
        4
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
```

---

## 📁 Project Structure

```
Fx-pipeline/
│
├── config/
│   └── config.py                  # Centralized configuration
│
├── scripts/
│   ├── extract.py                 # Step 1: API extraction
│   ├── transform.py               # Step 2: Cross-pairs + YTD
│   └── load.py                    # Step 3: MySQL loading
│
├── sql/
│   ├── create_tables.sql          # DDL for tables
│   └── validation_queries.sql     # Example queries
│
├── temp/                          # Temporary CSV files (auto-created)
│   ├── raw_fx_data.csv
│   ├── transformed_fx_data.csv
│   └── ytd_metrics.csv
│
├── .env                           # Environment variables
├── .env.example                   # Template for .env
├── requirements.txt               # Python dependencies
├── run_pipeline.py                # Orchestrator script
├── README.md                      # This file
└── DESIGN.md                      # Architecture decisions
```

---

## 🔧 Technical Specifications

### Data Source

- **API**: [Frankfurter](https://www.frankfurter.app/) - Free, open-source FX data API
- **Source**: European Central Bank reference rates
- **Update frequency**: Daily (around 16:00 CET)
- **Historical data**: Available from 1999-01-04
- **No API key required**

### Currencies

7 currencies: **NOK, EUR, SEK, PLN, RON, DKK, CZK**

### Cross-Pairs

42 currency pairs calculated using triangulation:
```
Formula: BASE/QUOTE = (EUR/QUOTE) / (EUR/BASE)
Example: NOK/SEK = (EUR/SEK) / (EUR/NOK)
```

### YTD Definition

**Year-To-Date (YTD)** = Cumulative metrics from **January 1st** of the current year up to and including the current date.

For each currency pair and each date, we calculate:
- **ytd_avg_rate**: Average rate from Jan 1 to current date
- **ytd_min_rate**: Minimum rate YTD
- **ytd_max_rate**: Maximum rate YTD
- **ytd_first_rate**: Rate on January 1st (first trading day)
- **ytd_last_rate**: Rate on current date
- **ytd_change_pct**: Percentage change from first to last: `((last - first) / first) × 100`
- **ytd_days_count**: Number of trading days with data

### Database Schema

#### Dimension Table
- **dim_currencies** - Currency reference data

#### Fact Tables
- **fact_fx_rates_daily** - Daily exchange rates (grain: date × currency_pair)
- **fact_fx_rates_ytd** - YTD metrics (grain: date × currency_pair)

#### Audit Table
- **pipeline_execution_log** - Pipeline execution tracking

---

## 🎯 Exit Codes

All scripts return standard exit codes:
- **0**: Success
- **1**: Failure

Useful for orchestration tools (Azure Data Factory, Airflow, cron jobs).

---

## 📝 Logging

Logs are written to:
1. **Console** (stdout) - Real-time progress
2. **MySQL table** `pipeline_execution_log` - Execution history

Log format:
```
2024-12-16 14:30:15 - INFO - ✅ 10,584 records extracted in 5s
```

---

## 🔄 Re-running the Pipeline

The pipeline is **idempotent**:
- **Daily rates**: Uses `INSERT ... ON DUPLICATE KEY UPDATE` (UPSERT)
- **YTD metrics**: Deletes existing records for the date range, then inserts fresh data
- Re-running the same date range will update existing records, not duplicate them

---

## ⚠️ Troubleshooting

### Issue: MySQL Connection Error

```
❌ Error connecting to MySQL: (1045, "Access denied for user 'root'@'localhost'")
```

**Solution:** Check your `.env` file credentials:
```env
DB_USER=root
DB_PASSWORD=your_actual_password
```

### Issue: API Timeout

```
❌ Error during extraction: HTTPSConnectionPool timeout
```

**Solution:** Check your internet connection or increase timeout in `config.py`:
```python
response = requests.get(url, params=params, timeout=60)  # Increase from 30 to 60
```

### Issue: Missing temp/ Directory

**Solution:** The pipeline creates it automatically, but you can manually:
```bash
mkdir temp
```

---

## 🚀 Azure Data Factory Integration

This modular design is ready for Azure Data Factory orchestration:

1. Create 3 Python Script activities:
   - **Extract** → `scripts/extract.py`
   - **Transform** → `scripts/transform.py` (depends on Extract)
   - **Load** → `scripts/load.py` (depends on Transform)

2. Use Azure Blob Storage for temp CSV files

3. Pass parameters via command-line arguments:
   ```json
   {
     "activities": [
       {
         "name": "Extract_FX_Data",
         "type": "PythonScript",
         "script": "scripts/extract.py",
         "arguments": ["--start-date", "@{pipeline().parameters.StartDate}"]
       }
     ]
   }
   ```

See `DESIGN.md` for more details on orchestration options.

---

## 📧 Support

For questions or issues:
1. Check `DESIGN.md` for architecture decisions
2. Review `validation_queries.sql` for more query examples
3. Check `pipeline_execution_log` table for execution history

