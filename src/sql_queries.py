random_hadm_ids = (
    "SELECT hadm_id FROM mimiciv_hosp.admissions ORDER BY RANDOM() LIMIT %s ;"
)

get_demographics = """
SELECT 
    p.subject_id, a.hadm_id, da.age, p.gender, a.race
FROM 
    mimiciv_hosp.patients p, mimiciv_hosp.admissions a, mimiciv_derived.age da
WHERE
    a.hadm_id = ANY( %s ) 
    AND a.subject_id = p.subject_id
    AND da.hadm_id = a.hadm_id;
"""


get_mean_vitalsigns = """
SELECT
    i.subject_id, i.hadm_id, AVG(v.heart_rate), AVG(v.sbp_ni), AVG(v.dbp_ni), 
    AVG(v.mbp_ni), AVG(v.resp_rate), AVG(v.temperature), AVG(v.spo2), AVG(v.glucose)
FROM 
    mimiciv_derived.vitalsign v, mimiciv_icu.icustays i
WHERE 
    i.hadm_id = ANY( %s ) 
    AND v.stay_id = i.stay_id
GROUP BY 
    i.hadm_id, i.subject_id;
"""

get_mean_labevents = """
SELECT 
    le.itemid, le.subject_id, le.hadm_id, AVG(le.valuenum) as valuenum, le.valueuom,
    ls.mean_value, ls.std_dev
FROM 
    mimiciv_hosp.labevents le, labevent_statistics ls
WHERE 
    le.hadm_id = ANY( %s ) 
    AND le.itemid = ls.itemid
GROUP BY 
    le.itemid, le.hadm_id, le.subject_id, le.valueuom, ls.mean_value, ls.std_dev;
"""

get_icd_diagnoses = (
    "SELECT * FROM mimiciv_hosp.diagnoses_icd WHERE hadm_id = ANY( %s );"
)


get_inputevents = """
SELECT 
    subject_id, hadm_id, itemid, amount, amountuom, ordercategoryname
FROM 
    mimiciv_icu.inputevents
WHERE 
    hadm_id = ANY( %s );
"""

all_scaled_similarity_values = """
SELECT 
    demographics_similarity_scaled, vitalsigns_similarity_scaled, labevents_similarity_scaled, diagnoses_similarity_scaled, inputevents_similarity_scaled
FROM similarities_20230326115023_scaled ;
"""

vitalsign_mean_std = """
SELECT 
    mean_value, std_dev
FROM 
    vitalsign_statistics
WHERE 
    vitalsign_name = %s ;
"""

labevent_mean_std = """
SELECT 
    mean_value, std_dev
FROM 
    labevent_statistics
WHERE 
    itemid = %s ;
"""

endpoints_for_hadm_id = """
SELECT 
    los_icu, los_hospital
FROM 
    mimiciv_derived.icustay_detail
WHERE 
    hadm_id = %s ;
"""

sepsis_cohort = """
SELECT 
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
    AND a.age >= %s
    AND a.age <= %s 
    AND p.gender = %s
LIMIT %s ;
"""

create_similarity_table = """
CREATE TABLE IF NOT EXISTS %s (
    hadm_id_a integer,
    hadm_id_b integer,
    demographics_similarity float,
    diagnoses_similarity float,
    labevents_similarity float,
    vitalsigns_similarity float,
    inputevents_similarity float,
    PRIMARY KEY (hadm_id_a, hadm_id_b)
);

CREATE INDEX IF NOT EXISTS similarity_hadm_id_a ON similarities (hadm_id_a);

CREATE INDEX IF NOT EXISTS similarity_hadm_id_b ON similarities (hadm_id_b);
"""

insert_similarity_values = """
INSERT INTO 
    %s (hadm_id_a, hadm_id_b, demographics_similarity, diagnoses_similarity, labevents_similarity, vitalsigns_similarity, inputevents_similarity, cohort_name)
VALUES 
   ( %s , %s , %s , %s , %s , %s , %s , %s );
"""
