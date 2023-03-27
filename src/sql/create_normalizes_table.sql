CREATE TABLE IF NOT EXISTS similarities_20230326115023_scaled AS with min_max_val as(
    select
        min(demographics_similarity) AS demographics_similarity_min,
        max(demographics_similarity) AS demographics_similarity_max,
        min(diagnoses_similarity) AS diagnoses_similarity_min,
        max(diagnoses_similarity) AS diagnoses_similarity_max,
        min(labevents_similarity) AS labevents_similarity_min,
        max(labevents_similarity) AS labevents_similarity_max,
        min(vitalsigns_similarity) AS vitalsigns_similarity_min,
        max(vitalsigns_similarity) AS vitalsigns_similarity_max,
        min(inputevents_similarity) AS inputevents_similarity_min,
        max(inputevents_similarity) AS inputevents_similarity_max
    from
        similarities_20230326115023
)
select
    hadm_id_a,
    hadm_id_b,
    (
        demographics_similarity - (
            select
                demographics_similarity_min
            from
                min_max_val
        )
    ) /(
        (
            select
                demographics_similarity_max
            from
                min_max_val
        ) - (
            select
                demographics_similarity_min
            from
                min_max_val
        )
    ) as demographics_similarity_scaled,
    (
        diagnoses_similarity - (
            select
                diagnoses_similarity_min
            from
                min_max_val
        )
    ) /(
        (
            select
                diagnoses_similarity_max
            from
                min_max_val
        ) - (
            select
                diagnoses_similarity_min
            from
                min_max_val
        )
    ) as diagnoses_similarity_scaled,
    (
        labevents_similarity - (
            select
                labevents_similarity_min
            from
                min_max_val
        )
    ) /(
        (
            select
                labevents_similarity_max
            from
                min_max_val
        ) - (
            select
                labevents_similarity_min
            from
                min_max_val
        )
    ) as labevents_similarity_scaled,
    (
        vitalsigns_similarity - (
            select
                vitalsigns_similarity_min
            from
                min_max_val
        )
    ) /(
        (
            select
                vitalsigns_similarity_max
            from
                min_max_val
        ) - (
            select
                vitalsigns_similarity_min
            from
                min_max_val
        )
    ) as vitalsigns_similarity_scaled,
    (
        inputevents_similarity - (
            select
                inputevents_similarity_min
            from
                min_max_val
        )
    ) /(
        (
            select
                inputevents_similarity_max
            from
                min_max_val
        kj) - (
            select
                inputevents_similarity_min
            from
                min_max_val
        )
    ) as inputevents_similarity_scaled
from
    similarities_20230326115023