table: similarities_20230517102646_scaled

query:
SELECT DISTINCT 
    sta.hadm_id 
FROM 
    mimiciv_derived.sepsis3 sep, 
    mimiciv_icu.icustays sta, 
    mimiciv_derived.age a, 
    mimiciv_hosp.patients p 
WHERE 
    sep.stay_id = sta.stay_id 
    AND sta.hadm_id = a.hadm_id 
    AND p.subject_id = sta.subject_id 
    AND a.age >= 18
    AND a.age <= 65 
    AND p.gender = 'F';