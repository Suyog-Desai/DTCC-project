-- Part 4: analytical queries on the Iceberg table trips_clean.
--
-- Written for Snowflake, where the table is taxi_sf.lake.trips_clean (an Iceberg table that
-- reads the same files Spark wrote). The SQL is plain ANSI, so run_queries.py runs the
-- identical text in Spark after swapping the table name. Same query, same answer.
--
-- Each query starts with a "-- name:" line; run_queries.py uses it as the CSV file name.
-- Keep semicolons out of the comments inside a query, the script splits on them.
--
-- Assumptions (also in the README):
--   * revenue = total_amount. That is what the passenger paid, so it INCLUDES the tip,
--     taxes and surcharges. It is not the driver's or the company's income.
--   * Zone 'N/A' and 'Outside of NYC' are not real places, so they are not ranked.
--   * No extra filtering: dq_checks.py already flags 98 rows with a fare, total or distance
--     above 1000 as a WARN, and the queries use the table as it is.


-- name: q1_top_pickup_zones_by_revenue
-- Top 10 pickup zones by revenue.
SELECT
    pickup_zone,
    pickup_borough,
    COUNT(*)                     AS trips,
    ROUND(SUM(total_amount), 2)  AS total_revenue,
    ROUND(AVG(total_amount), 2)  AS avg_revenue_per_trip
FROM taxi_sf.lake.trips_clean
WHERE pickup_zone IS NOT NULL
  AND pickup_zone NOT IN ('N/A', 'Outside of NYC')
GROUP BY pickup_zone, pickup_borough
ORDER BY total_revenue DESC
LIMIT 10;


-- name: q2_demand_by_hour
-- Trips per hour of the day (0-23) by pickup time, and each hour's share of all trips.
SELECT
    HOUR(tpep_pickup_datetime)                          AS pickup_hour,
    COUNT(*)                                            AS trips,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2)  AS pct_of_trips
FROM taxi_sf.lake.trips_clean
GROUP BY HOUR(tpep_pickup_datetime)
ORDER BY pickup_hour;


-- name: q3_avg_tip_pct_by_payment_type
-- Tip as a percent of the fare, by how the passenger paid.
-- avg_tip_pct is the average of each trip's own tip_pct. A trip with a tiny fare can have a
-- huge percentage and drag that average up, so two sturdier numbers sit next to it:
-- median_tip_pct, and tip_pct_of_total_fare (all tips divided by all fares).
-- Cash caveat: the taxi meter only records card tips. Cash tips are paid by hand and show up
-- as 0 in this data, so the low cash number says "not recorded", not "tips less".
SELECT
    payment_type,
    CASE payment_type
        WHEN 0 THEN 'Flex fare'
        WHEN 1 THEN 'Credit card'
        WHEN 2 THEN 'Cash'
        WHEN 3 THEN 'No charge'
        WHEN 4 THEN 'Dispute'
        WHEN 5 THEN 'Unknown'
        WHEN 6 THEN 'Voided trip'
        ELSE 'Other'
    END                                                              AS payment_name,
    COUNT(*)                                                         AS trips,
    ROUND(AVG(tip_pct), 2)                                           AS avg_tip_pct,
    ROUND(MEDIAN(tip_pct), 2)                                        AS median_tip_pct,
    ROUND(SUM(tip_amount) * 100.0 / NULLIF(SUM(fare_amount), 0), 2)  AS tip_pct_of_total_fare
FROM taxi_sf.lake.trips_clean
GROUP BY payment_type
ORDER BY trips DESC;
