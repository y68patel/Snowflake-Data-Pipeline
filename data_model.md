# Power BI Data Model

## Connection Setup

1. Open Power BI Desktop → **Get Data → Snowflake**
2. Enter:
   - **Server**: `your_account.snowflakecomputing.com`
   - **Warehouse**: `COMPUTE_WH`
3. Select **DirectQuery** for `FACT_CAMPAIGN_PERFORMANCE_DAILY`
   Select **Import** for all dimension tables and `DimDate`
4. Authenticate with your Snowflake credentials (or SSO)

---

## Table Load Modes

| Table | Source | Mode | Reason |
|-------|--------|------|--------|
| `FACT_CAMPAIGN_PERFORMANCE_DAILY` | `ANALYTICS` schema | DirectQuery | Large, refreshed daily by Task pipeline |
| `DIM_CAMPAIGN` | `STAGING` schema | Import | Small, rarely changes |
| `DIM_AD_GROUP` | `STAGING` schema | Import | Small, rarely changes |
| `DIM_AD` | `STAGING` schema | Import | Small, rarely changes |
| `DimDate` | Power Query generated | Import | No Snowflake table needed |

---

## Relationships

```
DimDate[Date]  ──────────────────────────────►  FACT[date]
DIM_CAMPAIGN[campaign_id]  ──────────────────►  FACT[campaign_id]
DIM_CAMPAIGN[campaign_id]  ──►  DIM_AD_GROUP[campaign_id]
DIM_AD_GROUP[ad_group_id]  ──►  DIM_AD[ad_group_id]
```

All relationships: **single direction**, **many-to-one** from FACT to DIM.

---

## Recommended Pages / Visuals

### Page 1 — Executive Summary
| Visual | Fields |
|--------|--------|
| KPI Cards | Total Impressions, Total Clicks, Total Conversions, Total Spend |
| KPI Cards | CTR, CVR, CPC, CPA |
| Line chart | Date (X) · Total Clicks, Total Conversions (Y) |
| Bar chart | campaign_name (X) · Total Spend (Y) — sorted desc |

### Page 2 — Channel & Device Breakdown
| Visual | Fields |
|--------|--------|
| Donut chart | channel · Total Spend |
| Clustered bar | device · Impressions, Clicks |
| Matrix | channel (rows) · MonthName (cols) · Total Spend (values) |

### Page 3 — Geographic Performance
| Visual | Fields |
|--------|--------|
| Map / Filled map | country, city · Total Conversions |
| Table | country, city, Total Spend, CPA — sorted by CPA |

### Page 4 — Trend Analysis
| Visual | Fields |
|--------|--------|
| Line chart (dual axis) | Date · Total Spend (bar) + CTR (line) |
| Line chart | Date · 7-Day Avg Clicks, 7-Day Avg Conversions |
| Card | Total Spend MoM % |

---

## Slicers (apply to all pages)
- `DimDate[Year]`
- `DimDate[MonthName]`
- `FACT_CAMPAIGN_PERFORMANCE_DAILY[channel]`
- `FACT_CAMPAIGN_PERFORMANCE_DAILY[device]`
- `FACT_CAMPAIGN_PERFORMANCE_DAILY[country]`

---

## Scheduled Refresh

1. Publish the report to **Power BI Service**
2. Go to the dataset → **Settings → Scheduled refresh**
3. Set frequency to **Daily** (recommend 06:00 UTC — after the Snowflake Task chain completes at 02:00 UTC)
4. Configure the **Snowflake gateway** or use the cloud connection if your Snowflake account is reachable from Power BI Service
