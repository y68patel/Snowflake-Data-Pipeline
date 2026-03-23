// ============================================================
// 00_connection.pq
// Shared Snowflake connection parameters
// Use this as a reference when building each query.
// In Power BI Desktop, set these as parameters:
//   SnowflakeServer   → your_account.snowflakecomputing.com
//   SnowflakeWarehouse → COMPUTE_WH
// ============================================================

let
    SnowflakeServer    = "your_account.snowflakecomputing.com",
    SnowflakeWarehouse = "COMPUTE_WH",
    SnowflakeDatabase  = "MARKETING_ANALYTICS",

    Source = Snowflake.Databases(
        SnowflakeServer,
        SnowflakeWarehouse,
        [
            Database              = SnowflakeDatabase,
            // Use DirectQuery for large fact tables; Import for small dims
            CreateNavigationProperties = true
        ]
    )
in
    Source
