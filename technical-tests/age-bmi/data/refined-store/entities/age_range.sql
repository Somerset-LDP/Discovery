-- Seed age bucket table
INSERT INTO age_range (id, use_case, min_age, max_age, label) VALUES
(1, 'population_health', 0, 5, '0–5'),
(2, 'population_health', 6, 17, '6–17'),
(3, 'population_health', 18, 64, '18–64'),
(4, 'population_health', 65, 200, '65+'),
(5, 'paediatrics', 0, 1, '0–1'),
(6, 'paediatrics', 2, 4, '2–4'),
(7, 'paediatrics', 5, 17, '5–17');

