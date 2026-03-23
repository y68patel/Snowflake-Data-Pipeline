# Snowflake Data Pipeline — Marketing Analytics

End-to-end pipeline for automated ingestion, transformation, and reporting of digital ad performance data.

---

## Architecture

```
S3 Bucket
    │  (ObjectCreated event → SQS notification)
    ▼
Snowpipe (AUTO_INGEST)          ← 06_snowpipe.sql
    │  continuous micro-batch load
    ▼
RAW tables                      ← 02_raw_tables.sql
    │
    ▼
Streams (CDC)                   ← 07_streams.sql
    │
    ▼  (daily Task chain at 02:00 UTC)
STAGING tables  — MERGE upsert  ← 08_tasks.sql
    │
    ▼
ANALYTICS tables — aggregated   ← 08_tasks.sql
    │
    ▼
Power BI Dashboards             ← powerbi/
```

---

## Data Layers

| Layer | Schema | Purpose |
|-------|--------|---------|
| Raw | `MARKETING_ANALYTICS.RAW` | Landing zone — data as ingested from S3 |
| Staging | `MARKETING_ANALYTICS.STAGING` | Cleansed, keyed, deduplicated with audit columns |
| Analytics | `MARKETING_ANALYTICS.ANALYTICS` | Aggregated, reporting-ready facts |

---

## Repository Structure

```
Snowflake-Data-Pipeline/
├── 01_setup.sql                  — Database & schema creation
├── 02_raw_tables.sql             — RAW layer table definitions
├── 03_staging_tables.sql         — STAGING layer tables (PKs, audit cols)
├── 04_analytics_tables.sql       — ANALYTICS layer table definitions
├── 05_file_formats_stages.sql    — CSV file format, S3 storage integration & stages
├── 06_snowpipe.sql               — Snowpipe (AUTO_INGEST) definitions + SQS setup guide
├── 07_streams.sql                — CDC streams on all RAW tables
├── 08_tasks.sql                  — Scheduled task chain + RESUME statements
├── 09_analytics_queries.sql      — Sample reporting queries
├── 10_sample_data.sql            — Dev/QA test inserts (do not run in production)
└── powerbi/
    ├── data_model.md             — Connection setup, relationships, recommended visuals
    ├── queries/
    │   ├── 00_connection.pq      — Shared Snowflake connection parameters
    │   ├── 01_fact_campaign_performance_daily.pq
    │   ├── 02_dim_campaign.pq
    │   ├── 03_dim_ad_group.pq
    │   ├── 04_dim_ad.pq
    │   └── 05_dim_date.pq        — Generated date dimension (no Snowflake table needed)
    └── dax_measures.dax          — All DAX measures (KPIs, ratios, time intelligence, ranking)
```

---

## Snowpipe — Automated Ingestion

Each RAW table has a dedicated Snowpipe with `AUTO_INGEST = TRUE`. Files dropped into the corresponding S3 prefix are loaded within seconds via SQS event notifications.

| Pipe | S3 Prefix | Target Table |
|------|-----------|--------------|
| `dim_campaign_pipe` | `marketing-data/dim_campaign/` | `RAW.DIM_CAMPAIGN` |
| `dim_ad_group_pipe` | `marketing-data/dim_ad_group/` | `RAW.DIM_AD_GROUP` |
| `dim_ad_pipe` | `marketing-data/dim_ad/` | `RAW.DIM_AD` |
| `fact_ad_performance_pipe` | `marketing-data/fact_ad_performance/` | `RAW.FACT_AD_PERFORMANCE_DAILY` |

**Post-deployment**: run `SHOW PIPES IN SCHEMA MARKETING_ANALYTICS.RAW;`, copy each pipe's `notification_channel` (SQS ARN), and configure it as an S3 event notification on the bucket.

---

## Task Chain

Runs daily at **02:00 UTC**. Child tasks must be resumed before the root task.

```
LOAD_DIM_CAMPAIGN_TASK              ← root, scheduled
  └── LOAD_DIM_AD_GROUP_TASK
        └── LOAD_DIM_AD_TASK
              └── LOAD_FACT_AD_PERFORMANCE_DAILY_TASK
                    └── LOAD_ANALYTICS_FACT_CAMPAIGN_DAILY_TASK
```

Each task uses a `MERGE` (upsert) pattern driven by CDC streams, so only changed rows are processed.

---

## Power BI Integration

See [`powerbi/data_model.md`](powerbi/data_model.md) for full setup instructions.

**Key decisions:**
- `FACT_CAMPAIGN_PERFORMANCE_DAILY` → **DirectQuery** (always current, no import lag)
- All dimension tables → **Import** (small, fast for slicers)
- `DimDate` → generated in Power Query, no Snowflake table required
- Scheduled refresh in Power BI Service: **06:00 UTC** (after the Task chain completes)

**DAX measures included:** Total Impressions, Clicks, Conversions, Spend · CTR, CVR, CPC, CPA, CPM · MoM %, YTD · 7-day rolling averages · Campaign spend rank

---

## Deployment Order

```
01_setup.sql
02_raw_tables.sql
03_staging_tables.sql
04_analytics_tables.sql
05_file_formats_stages.sql    ← update ARN and S3 bucket path first
06_snowpipe.sql               ← then configure SQS notifications in AWS
07_streams.sql
08_tasks.sql
```

`09_analytics_queries.sql` and `10_sample_data.sql` are for ad-hoc / dev use only.
