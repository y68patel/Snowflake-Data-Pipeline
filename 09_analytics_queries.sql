-- ============================================================
-- 09_analytics_queries.sql
-- Sample reporting queries against the ANALYTICS layer
-- ============================================================

USE SCHEMA MARKETING_ANALYTICS.ANALYTICS;

-- Campaign-level performance summary
SELECT
    campaign_name,
    SUM(impressions)  AS impressions,
    SUM(clicks)       AS clicks,
    SUM(conversions)  AS conversions,
    SUM(total_spend)  AS spend
FROM ANALYTICS.FACT_CAMPAIGN_PERFORMANCE_DAILY
GROUP BY campaign_name
ORDER BY spend DESC;


-- Channel-level performance summary
SELECT
    channel,
    SUM(impressions)  AS impressions,
    SUM(clicks)       AS clicks,
    SUM(total_spend)  AS spend
FROM ANALYTICS.FACT_CAMPAIGN_PERFORMANCE_DAILY
GROUP BY channel
ORDER BY spend DESC;


-- Daily clicks and conversions trend
SELECT
    date,
    SUM(clicks)       AS clicks,
    SUM(conversions)  AS conversions
FROM ANALYTICS.FACT_CAMPAIGN_PERFORMANCE_DAILY
GROUP BY date
ORDER BY date;
