-- Create materialized view: patient count by age range for population health
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_population_health_age_distribution AS
SELECT 
    ar.label AS age_range,
    COUNT(p.patient_id) AS patient_count
FROM patient p
JOIN age_range ar
  ON (EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM p.dob))
     BETWEEN ar.min_age AND ar.max_age
WHERE ar.use_case = 'population_health'
GROUP BY ar.label
ORDER BY ar.min_age;

