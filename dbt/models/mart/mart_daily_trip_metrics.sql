-- models/mart/mart_daily_trip_metrics.sql
{{
  config(
    materialized = 'table',
    partition_by = {
      "field": "trip_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by = ["day_type", "day_of_week"]
  )
}}

WITH daily_stats AS (
    SELECT
        DATE(started_at) AS trip_date,
        COUNT(*) AS total_trips,
        COUNT(DISTINCT start_station_id) AS unique_start_stations,
        COUNT(DISTINCT end_station_id) AS unique_end_stations,
        AVG(trip_duration_sec) AS avg_trip_duration_sec,
        SUM(trip_duration_sec) AS total_trip_duration_sec,

        -- User type breakdown  
        COUNTIF(user_type = 'member') AS member_trips,
        COUNTIF(user_type = 'casual') AS casual_trips,
        
        -- Calculate percentages
        ROUND(COUNTIF(user_type = 'member') / COUNT(*) * 100, 2) AS member_percentage,
        ROUND(COUNTIF(user_type = 'casual') / COUNT(*) * 100, 2) AS casual_percentage,

        -- Time of day breakdown
        COUNTIF(time_of_day = 'Morning Rush') AS morning_rush_trips,
        COUNTIF(time_of_day = 'Evening Rush') AS evening_rush_trips,
        COUNTIF(time_of_day = 'Daytime') AS daytime_trips,
        COUNTIF(time_of_day = 'Nighttime') AS nighttime_trips,

        -- Day of the week (1 = Sunday, 7 = Saturday)
        MIN(day_of_week) AS day_of_week
    FROM {{ ref('stg_citibike_trips') }}
    GROUP BY trip_date
),

prev_day_stats AS (
    SELECT
        *,
        LAG(total_trips) OVER (ORDER BY trip_date) AS prev_day_trips,
        LAG(avg_trip_duration_sec) OVER (ORDER BY trip_date) AS prev_day_avg_duration
    FROM daily_stats
)

SELECT
    trip_date,
    total_trips,
    unique_start_stations,
    unique_end_stations,
    avg_trip_duration_sec,
    total_trip_duration_sec,
    
    -- User type metrics
    member_trips,
    casual_trips,
    member_percentage,
    casual_percentage,
    
    -- Time of day metrics
    morning_rush_trips,
    evening_rush_trips,
    daytime_trips,
    nighttime_trips,
    
    -- Time comparisons
    prev_day_trips,
    prev_day_avg_duration,
    
    -- Calculate day-over-day changes
    SAFE_DIVIDE((total_trips - prev_day_trips), prev_day_trips) * 100 AS trip_count_pct_change,
    SAFE_DIVIDE((avg_trip_duration_sec - prev_day_avg_duration), prev_day_avg_duration) * 100 AS duration_pct_change,

    -- Day information
    day_of_week,
    CASE 
        WHEN day_of_week IN (1, 7) THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type
FROM prev_day_stats
