# FX Data Pipeline - Design Document

## Table of Contents

1. [Overview](#overview)
2. [Extraction Design](#extraction-design)
3. [Transformation Design](#transformation-design)
4. [Warehouse Design](#warehouse-design)
5. [YTD Calculation Design](#ytd-calculation-design)
6. [Key Trade-offs](#key-trade-offs)
7. [Orchestration Options](#orchestration-options)
8. [Future Enhancements](#future-enhancements)

---

## 1. Overview

### Objective

Build a production-ready FX data pipeline that:
- Extracts daily exchange rates for 7 currencies
- Calculates all 42 cross-currency pairs
- Computes Year-to-Date metrics
- Loads data into a dimensional warehouse schema

### Architecture

```
┌─────────────────┐
│  Frankfurter    │
│  API (ECB data) │
└────────┬────────┘
         │ HTTP GET
         ▼
┌─────────────────┐      ┌─────────────────┐      ┌─────────────────┐
│  EXTRACT        │─────▶│  TRANSFORM      │─────▶│  LOAD           │
│  extract.py     │ CSV  │  transform.py   │ CSV  │  load.py        │
└─────────────────┘      └─────────────────┘      └─────────────────┘
                                                            │
                                                            ▼
                                                    ┌──────────────┐
                                                    │ MySQL        │
                                                    │ Data         │
                                                    │ Warehouse    │
                                                    └──────────────┘
```

### Design Philosophy

1. **Modularity**: 3 independent scripts for flexibility
2. **Idempotency**: Can re-run safely without duplicates
3. **Observability**: Comprehensive logging and execution tracking
4. **Scalability**: Ready for cloud orchestration (Azure Data Factory)
5. **Data Quality**: Validation at every step

---

## 2. Extraction Design

### 2.1 Data Source Selection

**Chosen Source: [Frankfurter API](https://www.frankfurter.app/)**

#### Justification

| Criterion | Evaluation |
|-----------|------------|
| **Free & Open Source** | ✅ No API keys, no rate limits |
| **Data Quality** | ✅ European Central Bank official rates |
| **Historical Depth** | ✅ Data from 1999-01-04 to present |
| **API Reliability** | ✅ 99.9% uptime, Cloudflare CDN |
| **Currency Coverage** | ✅ All 7 required currencies supported |
| **Update Frequency** | ✅ Daily updates at ~16:00 CET |
| **Documentation** | ✅ Well-documented REST API |

#### Alternative Sources Considered

1. **Alpha Vantage**: ❌ Requires API key, 5 calls/min limit
2. **ExchangeRate-API**: ❌ Free tier limited to 1,500 requests/month
3. **Open Exchange Rates**: ❌ Requires paid subscription for historical data
4. **ECB Direct XML**: ⚠️ More complex to parse, Frankfurter is a wrapper

**Decision**: Frankfurter provides the best balance of simplicity, reliability, and data quality.

### 2.2 Time Window Selection

**Default: January 1, 2024 to Present**

#### Justification

- **YTD Relevance**: Covers full current year for YTD calculations
- **Manageable Volume**: ~250 trading days × 6 currencies = ~1,500 records
- **Performance**: API completes in <5 seconds
- **Flexibility**: Configurable via `START_DATE` environment variable

#### Alternative Approaches

| Approach | Trade-off |
|----------|-----------|
| **Last 30 days** | ❌ Insufficient for YTD analysis |
| **Full history (1999-)** | ⚠️ 25 years × 250 days × 42 pairs = 262K records (slower) |
| **Current year** | ✅ **CHOSEN** - Optimal for YTD + performance |

**Decision**: Current year provides complete YTD context while maintaining fast execution.

### 2.3 API Call Strategy

**Single Time-Series Request**

```
GET https://api.frankfurter.dev/v1/2024-01-01..2024-12-31?symbols=NOK,SEK,PLN,RON,DKK,CZK
```

#### Advantages
- ✅ Single HTTP request (vs. 365 individual requests)
- ✅ Consistent date handling (no timezone issues)
- ✅ Built-in weekend/holiday handling
- ✅ 10x faster than daily requests

#### Trade-offs
- ⚠️ Slightly larger payload (~150KB vs. ~1KB per day)
- ⚠️ All-or-nothing retrieval (but acceptable for daily batch)

**Decision**: Batch retrieval is more efficient and reliable.

### 2.4 Error Handling

```python
try:
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
except requests.Timeout:
    # Retry with exponential backoff
except requests.HTTPError:
    # Check if 404 (date out of range) or 5xx (server error)
except Exception:
    # Log and fail gracefully
```

**Retry Strategy**: Not implemented in v1 (simple fail-fast), but recommended for production.

---

## 3. Transformation Design

### 3.1 Cross-Pair Calculation

**Challenge**: API returns only EUR-based rates (EUR/NOK, EUR/SEK, etc.). We need **all 42 cross-pairs**.

#### Formula

For any currency pair `BASE/QUOTE`:

```
BASE/QUOTE = (EUR/QUOTE) / (EUR/BASE)
```

**Example**:
```
NOK/SEK = (EUR/SEK) / (EUR/NOK)
        = 11.5432 / 11.7234
        = 0.9846
```

#### Implementation

```python
rates_dict = {'EUR': 1.0, 'NOK': 11.7234, 'SEK': 11.5432, ...}

for base in CURRENCIES:
    for quote in CURRENCIES:
        if base != quote:
            cross_rate = rates_dict[quote] / rates_dict[base]
```

#### Validation

Cross-rates are mathematically consistent:
```
EUR/NOK × NOK/SEK × SEK/EUR = 1.0 (within rounding)
```

### 3.2 Cross-Pair Count

| Calculation | Result |
|-------------|--------|
| Total pairs | 7 × 7 = 49 |
| Remove same-currency | 49 - 7 = 42 |
| **Final** | **42 pairs** |

Examples: EUR/NOK, NOK/EUR, EUR/SEK, SEK/EUR, NOK/SEK, SEK/NOK, etc.

### 3.3 Precision

**Chosen**: 8 decimal places

#### Justification
- ✅ Sufficient for FX analysis (standard is 4-5 decimals)
- ✅ Prevents rounding errors in calculations
- ✅ Matches Frankfurter API precision
- ⚠️ MySQL DECIMAL(18,8) allows for large numbers with high precision

**Trade-off**: Slightly larger storage vs. accuracy → **Accuracy wins**.

### 3.4 Data Validation

**Checks Performed**:
1. ✅ No negative rates
2. ✅ No zero rates
3. ✅ Base ≠ Quote currency
4. ✅ All 42 pairs present for each date
5. ✅ Cross-rate consistency (triangulation check)

---

## 4. Warehouse Design

### 4.1 Schema Design

**Chosen: Star Schema (Dimensional Model)**

```
      ┌──────────────────┐
      │ dim_currencies   │
      │ ─────────────────│
      │ currency_code PK │
      │ currency_name    │
      │ is_active        │
      └────────┬─────────┘
               │
        ┌──────┴──────┐
        │             │
┌───────▼──────────┐  │  ┌────────────────────┐
│fact_fx_rates_    │◄─┴─►│ fact_fx_rates_ytd  │
│daily             │     │                    │
│──────────────────│     │────────────────────│
│rate_date        │     │rate_date           │
│base_currency FK │     │base_currency FK    │
│quote_currency FK│     │quote_currency FK   │
│exchange_rate    │     │ytd_avg_rate        │
│source           │     │ytd_min_rate        │
│created_at       │     │ytd_max_rate        │
│updated_at       │     │ytd_change_pct      │
└─────────────────┘     │...                 │
                        └────────────────────┘
```

#### Design Decisions

| Aspect | Decision | Rationale |
|--------|----------|-----------|
| **Model** | Star Schema | Easy to understand, fast joins |
| **Grain** | Date × Currency Pair | Lowest level of detail |
| **Surrogate Keys** | No | Natural key (date + pair) is stable |
| **SCD Type** | Type 1 (overwrite) | FX rates don't change retrospectively |
| **Fact Tables** | 2 separate | Daily rates vs. YTD metrics (different grains) |

### 4.2 Table Justification

#### fact_fx_rates_daily

**Purpose**: Store atomic-level daily exchange rates

**Key Design**:
```sql
UNIQUE KEY (rate_date, base_currency, quote_currency)
```

**Why Separate from YTD?**
- Different analytical purposes
- Daily rates: operational queries (today's rate)
- YTD metrics: analytical queries (trends)
- Avoids redundant storage of YTD columns in daily table

#### fact_fx_rates_ytd

**Purpose**: Pre-aggregated YTD metrics for fast analytics

**Trade-off**:
- ✅ **Pro**: Instant YTD queries (no re-calculation)
- ⚠️ **Con**: Additional storage (~same size as daily table)
- ✅ **Decision**: Speed > Storage (storage is cheap)

**Alternative Considered**: Calculate YTD on-the-fly with window functions
```sql
-- This would work but be SLOW on large datasets
SELECT 
    AVG(rate) OVER (PARTITION BY year, pair ORDER BY date) AS ytd_avg
FROM fact_fx_rates_daily
```

### 4.3 Indexing Strategy

```sql
-- Composite indexes for common queries
INDEX idx_date_base (rate_date, base_currency)
INDEX idx_date_pair (rate_date, base_currency, quote_currency)

-- Individual indexes for filtering
INDEX idx_rate_date (rate_date)
INDEX idx_base_currency (base_currency)
```

**Query Patterns Optimized**:
1. Lookup by date + currency pair (most common)
2. Filter by date range + base currency
3. Join with other fact tables on date

### 4.4 Integration with Other DWH Tables

**Design Principle**: Follow Kimball dimensional modeling

**Example Join**:
```sql
-- Join FX rates with hypothetical sales table
SELECT 
    s.sale_date,
    s.amount_local,
    s.currency,
    fx.exchange_rate,
    s.amount_local / fx.exchange_rate AS amount_eur
FROM sales_fact s
LEFT JOIN fact_fx_rates_daily fx
    ON s.sale_date = fx.rate_date
    AND s.currency = fx.quote_currency
    AND fx.base_currency = 'EUR'
```

**Key Design Choices**:
- ✅ Simple DATE type (no timestamps) → Easy date joins
- ✅ VARCHAR currency codes → Standard ISO format
- ✅ Foreign keys to dim_currencies → Data integrity
- ✅ Consistent naming (rate_date, not fx_date) → DWH conventions

---

## 5. YTD Calculation Design

### 5.1 YTD Definition

**Year-To-Date (YTD)**: Cumulative metrics from **January 1st** of the current year through the current date.

#### Formal Definition

For a given currency pair and date `D`:

```
YTD metrics = f(all_rates) where rate_date ∈ [Jan 1 of year(D), D]
```

#### Example

If today is **December 15, 2024**:
- YTD period: January 1, 2024 → December 15, 2024
- YTD avg rate = Average of all rates in this period
- YTD change % = (Rate_Dec15 - Rate_Jan1) / Rate_Jan1 × 100

### 5.2 Metrics Calculated

| Metric | Formula | Use Case |
|--------|---------|----------|
| **ytd_avg_rate** | `AVG(rate)` | Typical rate this year |
| **ytd_min_rate** | `MIN(rate)` | Best buying opportunity |
| **ytd_max_rate** | `MAX(rate)` | Worst buying point |
| **ytd_first_rate** | `rate[Jan 1]` | Starting point |
| **ytd_last_rate** | `rate[current]` | Current rate |
| **ytd_change_pct** | `(last-first)/first×100` | YTD performance |
| **ytd_days_count** | `COUNT(dates)` | Data availability |
| **ytd_variance** | `VAR(rate)` | Volatility measure |
| **ytd_std_dev** | `STDDEV(rate)` | Risk measure |

### 5.3 Implementation Approach

**Chosen: Pre-calculate and store**

```python
for each date D in dataset:
    year_start = January 1 of year(D)
    ytd_data = filter(rates where date ∈ [year_start, D])
    
    ytd_metrics = {
        'avg': mean(ytd_data),
        'min': min(ytd_data),
        'max': max(ytd_data),
        'change_pct': (ytd_data[-1] - ytd_data[0]) / ytd_data[0] * 100
    }
    
    store(D, ytd_metrics)
```

#### Alternative Approaches Considered

| Approach | Pros | Cons | Decision |
|----------|------|------|----------|
| **Pre-calculate** | ✅ Fast queries | ⚠️ More storage | ✅ **CHOSEN** |
| **Calculate on-demand** | ✅ Less storage | ❌ Slow queries | ❌ Rejected |
| **Materialized view** | ✅ Auto-refresh | ⚠️ MySQL complexity | ⚠️ Future option |

**Rationale**: 
- For 252 trading days × 42 pairs = 10,584 YTD records
- Storage cost: ~2MB
- Query speed improvement: 100x faster
- **Trade-off**: Storage is cheap, speed is valuable

### 5.4 YTD Edge Cases

**Case 1: First day of year (January 1)**
- YTD period: Only January 1
- YTD avg = first rate
- YTD change = 0%

**Case 2: Missing dates (weekends/holidays)**
- Exclude from calculation (no interpolation)
- YTD days_count shows actual data points

**Case 3: Year boundary**
- Each year is calculated independently
- December 31, 2024 YTD ≠ January 1, 2025 YTD

### 5.5 YTD Update Strategy

**Incremental Update** (for new dates):
```sql
-- Only calculate YTD for new dates
DELETE FROM fact_fx_rates_ytd WHERE rate_date = '2024-12-16';
INSERT INTO fact_fx_rates_ytd SELECT ... WHERE rate_date <= '2024-12-16';
```

**Full Recalculation** (for historical corrections):
```python
# Rare case: if historical data is corrected
DELETE FROM fact_fx_rates_ytd WHERE rate_date >= '2024-01-01';
recalculate_ytd(start_date='2024-01-01')
```

---

## 6. Key Trade-offs

### 6.1 Data Storage vs. Query Performance

| Decision | Trade-off | Rationale |
|----------|-----------|-----------|
| Separate YTD table | +Storage, ++Speed | Storage is cheap, queries are expensive |
| 8 decimal places | +Storage | FX precision is critical |
| CSV intermediates | +Disk usage | Debugging and validation are valuable |

**Outcome**: Optimize for speed and maintainability.

### 6.2 API Strategy

| Decision | Trade-off | Rationale |
|----------|-----------|-----------|
| Batch time-series | Single point of failure | 10x faster, acceptable risk |
| No retry logic | Manual intervention needed | Simpler code, infrequent failures |
| EUR as base | Calculate cross-pairs | API limitation, mathematically sound |

**Outcome**: Simplicity over complexity for v1.

### 6.3 Pipeline Architecture

| Decision | Trade-off | Rationale |
|----------|-----------|-----------|
| 3 separate scripts | More files | Flexible orchestration |
| CSV intermediates | Disk I/O | Easy debugging and validation |
| No streaming | Memory usage | Dataset fits in memory (<50MB) |

**Outcome**: Modularity enables Azure Data Factory integration.

### 6.4 Idempotency

**Challenge**: Re-running the pipeline should not duplicate data.

**Solution**: UPSERT pattern
```sql
INSERT INTO fact_fx_rates_daily (...)
VALUES (...)
ON DUPLICATE KEY UPDATE 
    exchange_rate = VALUES(exchange_rate)
```

**Trade-off**:
- ✅ Safe to re-run
- ⚠️ Slightly slower than pure INSERT

**Outcome**: Safety > Speed for batch pipeline.

### 6.5 Error Handling Philosophy

**Chosen: Fail-Fast**

```python
if error:
    log_error()
    sys.exit(1)  # Non-zero exit code
```

**Alternative**: Graceful degradation (skip bad records)

**Rationale**:
- FX data must be complete (missing dates = analytical gaps)
- Orchestration tools (Azure Data Factory) can retry
- Manual intervention is acceptable for rare failures

---

## 7. Orchestration Options

### 7.1 Local Orchestration

**Current**: `run_pipeline.py`

```python
subprocess.run(['python', 'extract.py'])  # Exit code 0 = success
subprocess.run(['python', 'transform.py'])
subprocess.run(['python', 'load.py'])
```

**Use Cases**:
- Development/testing
- Manual backfills
- Small-scale deployments

### 7.2 Azure Data Factory (Recommended)

**Pipeline Structure**:

```json
{
  "activities": [
    {
      "name": "Extract",
      "type": "PythonScript",
      "script": "scripts/extract.py",
      "linkedService": "AzureBatchLinkedService"
    },
    {
      "name": "Transform",
      "type": "PythonScript",
      "dependsOn": [{"activity": "Extract", "dependencyConditions": ["Succeeded"]}],
      "script": "scripts/transform.py"
    },
    {
      "name": "Load",
      "type": "PythonScript",
      "dependsOn": [{"activity": "Transform", "dependencyConditions": ["Succeeded"]}],
      "script": "scripts/load.py"
    }
  ],
  "triggers": [
    {
      "name": "DailyTrigger",
      "type": "ScheduleTrigger",
      "recurrence": {
        "frequency": "Day",
        "interval": 1,
        "startTime": "2024-01-01T17:00:00Z"
      }
    }
  ]
}
```

**Benefits**:
- ✅ Built-in retry and alerting
- ✅ Monitoring dashboard
- ✅ Parameter passing
- ✅ Integration with Azure SQL Database

### 7.3 Alternative Orchestrators

| Tool | Suitability | Notes |
|------|-------------|-------|
| **Apache Airflow** | ⭐⭐⭐⭐⭐ | Ideal for complex DAGs |
| **Azure Data Factory** | ⭐⭐⭐⭐⭐ | Best for Azure ecosystem |
| **Prefect** | ⭐⭐⭐⭐ | Modern, Python-native |
| **Cron** | ⭐⭐⭐ | Simple, no dependencies |
| **AWS Step Functions** | ⭐⭐⭐⭐ | Good for AWS |

---

## 8. Future Enhancements

### 8.1 Short-Term (v1.1)

1. **Retry Logic**
   ```python
   @retry(tries=3, delay=5, backoff=2)
   def extract_fx_data():
       ...
   ```

2. **Data Quality Checks**
   ```python
   assert len(df) > 0, "Empty dataset"
   assert df['exchange_rate'].min() > 0, "Invalid rates"
   ```

3. **Email Notifications**
   ```python
   if status == 'failed':
       send_email(to='data-team@company.com', subject='FX Pipeline Failed')
   ```

### 8.2 Medium-Term (v2.0)

1. **Incremental Loading**
   - Only fetch dates not yet in database
   - Reduces API calls and processing time

2. **Real-Time Updates**
   - WebSocket connection to live FX feeds
   - Intraday rate updates

3. **Additional Currencies**
   - Expand from 7 to 30+ currencies
   - Requires optimization (900+ cross-pairs)

4. **Data Lake Integration**
   - Store raw JSON responses in Azure Blob
   - Support historical reprocessing

### 8.3 Long-Term (v3.0)

1. **Machine Learning Features**
   - Predict next-day rates
   - Anomaly detection (detect erroneous rates)

2. **Streaming Architecture**
   - Kafka/Event Hub for real-time processing
   - Sub-second latency

3. **Multi-Source Aggregation**
   - Combine Frankfurter, Alpha Vantage, Bloomberg
   - Calculate consensus rates

---

## 9. Summary of Key Decisions

| Decision Point | Choice | Rationale |
|----------------|--------|-----------|
| **Data Source** | Frankfurter API | Free, reliable, ECB data |
| **Time Window** | Current year (2024-01-01 to now) | Optimal for YTD + performance |
| **Cross-Pairs** | Calculate all 42 pairs via triangulation | Complete analytical coverage |
| **Schema** | Star schema with separate YTD table | Fast queries, easy joins |
| **YTD Definition** | Jan 1 to current date, cumulative | Industry standard |
| **Storage** | Pre-calculate YTD (not on-demand) | Speed > storage cost |
| **Pipeline** | 3 modular scripts (E-T-L) | Flexible orchestration |
| **Idempotency** | UPSERT with unique constraints | Safe re-runs |
| **Error Handling** | Fail-fast with logging | Data completeness is critical |

---

## 10. Validation Checklist

- [x] Extracts from reliable FX source (Frankfurter/ECB)
- [x] Calculates all 42 currency cross-pairs
- [x] Stores in dimensional warehouse schema
- [x] Easy to join with other warehouse tables
- [x] YTD calculations clearly defined and implemented
- [x] Pipeline is modular and orchestration-ready
- [x] Comprehensive logging and error handling
- [x] Example queries demonstrate usability
- [x] Documentation covers all design decisions

---

**Document Version**: 1.0  
**Last Updated**: December 16, 2024  
**Author**: S.NAOUI / Data Engineer