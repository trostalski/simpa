CREATE TABLE IF NOT EXISTS similarities_20230517102646_scaled AS
with min_max_val AS (
    SELECT
        min(demographics_similarity) AS demographics_similarity_min,
        max(demographics_similarity) AS demographics_similarity_max,
        min(diagnoses_similarity) AS diagnoses_similarity_min,
        max(diagnoses_similarity) AS diagnoses_similarity_max,
        min(labevents_similarity) AS labevents_similarity_min,
        max(labevents_similarity) AS labevents_similarity_max,
        min(vitalsigns_similarity) AS vitalsigns_similarity_min,
        max(vitalsigns_similarity) AS vitalsigns_similarity_max,
        min(inputevents_similarity) AS inputevents_similarity_min,
        max(inputevents_similarity) AS inputevents_similarity_max,
        min(labevents_first24h_similarity) AS labevents_first24h_similarity_min,
        max(labevents_first24h_similarity) AS labevents_first24h_similarity_max,
        min(vitalsigns_first24h_similarity) AS vitalsigns_first24h_similarity_min,
        max(vitalsigns_first24h_similarity) AS vitalsigns_first24h_similarity_max,
        min(prescriptions_similarity) AS prescriptions_similarity_min,
        max(prescriptions_similarity) AS prescriptions_similarity_max
    FROM
        similarities_20230517102646
)
SELECT
    hadm_id_a,
    hadm_id_b,
(demographics_similarity -(
            SELECT
                demographics_similarity_min
            FROM
                min_max_val)) /((
        SELECT
            demographics_similarity_max
        FROM min_max_val) -(
        SELECT
            demographics_similarity_min
        FROM min_max_val)) AS demographics_similarity_scaled,
(diagnoses_similarity -(
            SELECT
                diagnoses_similarity_min
            FROM
                min_max_val)) /((
        SELECT
            diagnoses_similarity_max
        FROM min_max_val) -(
        SELECT
            diagnoses_similarity_min
        FROM min_max_val)) AS diagnoses_similarity_scaled,
(labevents_similarity -(
            SELECT
                labevents_similarity_min
            FROM
                min_max_val)) /((
        SELECT
            labevents_similarity_max
        FROM min_max_val) -(
        SELECT
            labevents_similarity_min
        FROM min_max_val)) AS labevents_similarity_scaled,
(labevents_first24h_similarity -(
            SELECT
                labevents_first24h_similarity_min
            FROM
                min_max_val)) /((
        SELECT
            labevents_first24h_similarity_max
        FROM min_max_val) -(
        SELECT
            labevents_first24h_similarity_min
        FROM min_max_val)) AS labevents_first24h_similarity_scaled,
(vitalsigns_similarity -(
            SELECT
                vitalsigns_similarity_min
            FROM
                min_max_val)) /((
        SELECT
            vitalsigns_similarity_max
        FROM min_max_val) -(
        SELECT
            vitalsigns_similarity_min
        FROM min_max_val)) AS vitalsigns_similarity_scaled,
(vitalsigns_first24h_similarity -(
            SELECT
                vitalsigns_first24h_similarity_min
            FROM
                min_max_val)) /((
        SELECT
            vitalsigns_first24h_similarity_max
        FROM min_max_val) -(
        SELECT
            vitalsigns_first24h_similarity_min
        FROM min_max_val)) AS vitalsigns_first24h_similarity_scaled,
(inputevents_similarity -(
            SELECT
                inputevents_similarity_min
            FROM
                min_max_val)) /((
        SELECT
            inputevents_similarity_max
        FROM min_max_val) -(
        SELECT
            inputevents_similarity_min
        FROM min_max_val)) AS inputevents_similarity_scaled,
(prescriptions_similarity -(
            SELECT
                prescriptions_similarity_min
            FROM
                min_max_val)) /((
        SELECT
            prescriptions_similarity_max
        FROM min_max_val) -(
        SELECT
            prescriptions_similarity_min
        FROM min_max_val)) AS prescriptions_similarity_scaled
FROM
    similarities_20230517102646
