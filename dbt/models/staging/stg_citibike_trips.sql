
{{ config(
    materialized='view',
    partition_by={
        "field": "started_at",
        "data_type": "timestamp",
        "granularity": "month"
    },
    cluster_by=["start_station_id", "end_station_id"]
) }}

WITH source_data AS (
    SELECT * FROM {{ source('citibike', 'citibike_combined') }}
),

renamed AS (
    SELECT
        -- Primary identifiers
        ride_id,
        rideable_type,
        
        -- Timestamps 
        started_at,
        ended_at,
        
        -- Station information (maintain clustering columns from raw table)
        start_station_name,
        start_station_id,
        end_station_name,
        end_station_id,
        
        -- Coordinates
        start_lat,
        start_lng,
        end_lat,
        end_lng,
        
        -- Member type
        member_casual AS user_type,
        
        -- Trip duration
        TIMESTAMP_DIFF(ended_at, started_at, SECOND) AS trip_duration_sec,
        
        -- Time dimensions
        EXTRACT(YEAR FROM started_at) AS year,
        EXTRACT(MONTH FROM started_at) AS month,
        EXTRACT(DAY FROM started_at) AS day,
        EXTRACT(HOUR FROM started_at) AS hour,
        EXTRACT(DAYOFWEEK FROM started_at) AS day_of_week,
        
        -- Time of day categorization
        CASE 
            WHEN EXTRACT(HOUR FROM started_at) BETWEEN 7 AND 10 THEN 'Morning Rush'
            WHEN EXTRACT(HOUR FROM started_at) BETWEEN 16 AND 19 THEN 'Evening Rush'
            WHEN EXTRACT(HOUR FROM started_at) BETWEEN 6 AND 22 THEN 'Daytime'
            ELSE 'Nighttime'
        END AS time_of_day
        
    FROM source_data
)

SELECT * FROM renamed
