-- ============================================================
-- 04_analytics_tables.sql
-- ANALYTICS layer tables — aggregated, reporting-ready
-- ============================================================

USE SCHEMA MARKETING_ANALYTICS.ANALYTICS;

CREATE OR REPLACE TABLE ANALYTICS.FACT_CAMPAIGN_PERFORMANCE_DAILY (
    date              DATE          NOT NULL,
    campaign_id       INTEGER       NOT NULL,
    campaign_name     STRING,
    channel           STRING,
    device            STRING,
    country           STRING,
    city              STRING,
    impressions       INTEGER,
    clicks            INTEGER,
    conversions       INTEGER,
    total_spend       NUMBER(12,2),
    CONSTRAINT pk_fact_campaign_perf PRIMARY KEY (date, campaign_id, device, country, city)
);
