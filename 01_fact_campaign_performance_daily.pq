// ============================================================
// 01_fact_campaign_performance_daily.pq
// Main fact table — campaign performance aggregated by day
// Source: MARKETING_ANALYTICS.ANALYTICS.FACT_CAMPAIGN_PERFORMANCE_DAILY
// Load mode: DirectQuery (large, continuously updated)
// ============================================================

let
    Source = Snowflake.Databases(
        "your_account.snowflakecomputing.com",
        "COMPUTE_WH",
        [Database = "MARKETING_ANALYTICS"]
    ),
    Database    = Source{[Name = "MARKETING_ANALYTICS"]}[Data],
    Analytics   = Database{[Schema = "ANALYTICS"]}[Data],
    FactTable   = Analytics{[Name = "FACT_CAMPAIGN_PERFORMANCE_DAILY"]}[Data],

    // Enforce expected types
    TypedTable  = Table.TransformColumnTypes(FactTable, {
        {"date",          type date},
        {"campaign_id",   Int64.Type},
        {"campaign_name", type text},
        {"channel",       type text},
        {"device",        type text},
        {"country",       type text},
        {"city",          type text},
        {"impressions",   Int64.Type},
        {"clicks",        Int64.Type},
        {"conversions",   Int64.Type},
        {"total_spend",   Currency.Type}
    })
in
    TypedTable
