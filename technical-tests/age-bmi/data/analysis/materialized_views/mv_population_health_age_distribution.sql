-- Refresh materialized view for population health age distribution
REFRESH MATERIALIZED VIEW mv_population_health_age_distribution;

-- Query the materialized view
SELECT * 
FROM mv_population_health_age_distribution
ORDER BY age_range;
