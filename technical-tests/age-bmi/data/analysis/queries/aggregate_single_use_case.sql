-- Aggregate patients by age range for a single use case
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
