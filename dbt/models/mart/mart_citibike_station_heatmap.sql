{{
  config(
    materialized = 'table',
    cluster_by = ["station_id"]
  )
}}

-- Highly aggregated station metrics, removing time dimensions
WITH station_metrics AS (
  SELECT
    -- Station dimensions
    COALESCE(start_station_id, end_station_id) AS station_id,
    COALESCE(start_station_name, end_station_name) AS station_name,
    
    -- Coordinates for mapping - use an average in case there are slight variations
    AVG(COALESCE(start_lat, end_lat)) AS latitude,
    AVG(COALESCE(start_lng, end_lng)) AS longitude,
    
    -- Total counts only
    COUNT(DISTINCT CASE WHEN start_station_id IS NOT NULL THEN ride_id END) AS trips_starting,
    COUNT(DISTINCT CASE WHEN end_station_id IS NOT NULL THEN ride_id END) AS trips_ending,
    
    -- Net flow (positive means more trips start than end here)
    COUNT(DISTINCT CASE WHEN start_station_id IS NOT NULL THEN ride_id END) - 
    COUNT(DISTINCT CASE WHEN end_station_id IS NOT NULL THEN ride_id END) AS net_flow,
    
    -- Total across both origins and destinations
    COUNT(*) AS total_activity
    
  FROM {{ ref('stg_citibike_trips') }}
  GROUP BY 
    COALESCE(start_station_id, end_station_id),
    COALESCE(start_station_name, end_station_name)
)

SELECT * FROM station_metrics