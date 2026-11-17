-- Bronze
CREATE OR REFRESH STREAMING TABLE
    bikesales.bronze.business_parter_bronze_dp AS
SELECT *
FROM STREAM read_files(
    '/Volumes/bikesales/sales-md/dbv/BusiessPartner/',
    format => 'csv'
);


-- Silver
CREATE OR REFRESH STREAMING TABLE
    bikesales.silver.business_parter_silver_dp AS
SELECT *
FROM STREAM bikesales.bronze.business_parter_bronze_dp;

-- Gold
CREATE OR REFRESH MATERIALIZED VIEW
    bikesales.gold.business_parter_gold_dp AS
SELECT
    count(*) AS total_orders
FROM bikesales.silver.business_parter_silver_dp;