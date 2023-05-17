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

get_mean_vitalsigns_first_24h_icu = """
SELECT
    i.subject_id, i.hadm_id, AVG(v.heart_rate), AVG(v.sbp_ni), AVG(v.dbp_ni), 
    AVG(v.mbp_ni), AVG(v.resp_rate), AVG(v.temperature), AVG(v.spo2), AVG(v.glucose)
FROM 
    mimiciv_derived.vitalsign v, mimiciv_icu.icustays i, mimiciv_icu.icustays ie, mimiciv_derived.icustay_detail id
WHERE 
    i.hadm_id = ANY( %s ) 
    AND v.stay_id = i.stay_id
    AND i.hadm_id = ie.hadm_id
    AND i.hadm_id = id.hadm_id
    AND v.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR)
    AND v.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
    AND id.first_icu_stay = true
GROUP BY 
    i.hadm_id, i.subject_id;
"""

get_mean_labevents = """
SELECT 
    le.itemid, le.subject_id, le.hadm_id, AVG(le.valuenum) as valuenum, le.valueuom,
    ls.mean_value, ls.std_dev, li.label, le.ref_range_lower, le.ref_range_upper
FROM 
    mimiciv_hosp.labevents le, labevent_statistics ls, mimiciv_hosp.d_labitems li
WHERE 
    le.hadm_id = ANY( %s ) 
    AND le.itemid = ls.itemid
    AND le.itemid = li.itemid
GROUP BY 
    le.itemid, le.hadm_id, le.subject_id, le.valueuom, ls.mean_value, ls.std_dev, li.label, le.ref_range_lower, le.ref_range_upper;
"""

get_mean_labevents_first_24h_icu = """
SELECT 
    le.itemid, le.subject_id, le.hadm_id, AVG(le.valuenum) as valuenum, le.valueuom,
    ls.mean_value, ls.std_dev, li.label, le.ref_range_lower, le.ref_range_upper
FROM 
    mimiciv_hosp.labevents le, labevent_statistics ls, mimiciv_hosp.d_labitems li, mimiciv_icu.icustays ie, mimiciv_derived.icustay_detail id 
WHERE 
    le.hadm_id = ANY( %s ) 
    AND le.itemid = ls.itemid
    AND le.itemid = li.itemid
    AND le.hadm_id = ie.hadm_id
    AND le.hadm_id = id.hadm_id
    AND le.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR)
    AND le.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
    AND id.first_icu_stay = 'true'
GROUP BY 
    le.itemid, le.hadm_id, le.subject_id, le.valueuom, ls.mean_value, ls.std_dev, li.label, le.ref_range_lower, le.ref_range_upper;
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

get_inputevents_first_24h_icu = """
SELECT 
    ip.subject_id, ip.hadm_id, ip.itemid, ip.amount, ip.amountuom, ip.ordercategoryname
FROM 
    mimiciv_icu.inputevents ip,
    mimiciv_icu.icustays ie,
    mimiciv_derived.icustay_detail id
WHERE 
    ip.hadm_id = ANY( %s )
    AND ie.hadm_id = ip.hadm_id
    AND ie.hadm_id = id.hadm_id
    AND ip.starttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR)
    AND ip.starttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
    AND id.first_icu_stay = 'true';
"""

get_prescriptions = """
SELECT 
    pr.subject_id, pr.hadm_id, pr.pharmacy_id, pr.drug, pr.gsn, gi.id as value
FROM 
    mimiciv_hosp.prescriptions pr,
    mimiviv.gsn_ids gi
WHERE 
    hadm_id = ANY( %s ) AND
    pr.gsn = gi.gsn;
"""

get_prescriptions_icu = """
SELECT 
    pr.subject_id, pr.hadm_id, pr.pharmacy_id, pr.drug, pr.gsn, gi.id as value
FROM 
    mimiciv_hosp.prescriptions pr,
    mimiciv_icu.icustays ie,
    mimiciv_derived.icustay_detail id,
    gsn_ids gi
WHERE 
    pr.hadm_id = ANY( %s )
    AND pr.gsn = gi.gsn
    AND ie.hadm_id = pr.hadm_id
    AND ie.hadm_id = id.hadm_id
    AND pr.starttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR)
    AND pr.starttime <= DATETIME_ADD(ie.outtime, INTERVAL '6' HOUR)
    AND id.first_icu_stay = 'true';
"""


get_prescriptions_first_24h_icu = """
SELECT 
    pr.subject_id, pr.hadm_id, pr.pharmacy_id, pr.drug, pr.gsn, gi.id as value
FROM 
    mimiciv_hosp.prescriptions pr,
    mimiciv_icu.icustays ie,
    mimiciv_derived.icustay_detail id,
    gsn_ids gi
WHERE 
    pr.hadm_id = ANY( %s )
    AND pr.gsn = gi.gsn
    AND ie.hadm_id = pr.hadm_id
    AND ie.hadm_id = id.hadm_id
    AND pr.starttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR)
    AND pr.starttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
    AND id.first_icu_stay = 'true';
"""

all_scaled_similarity_values = """
SELECT 
    hadm_id_a, hadm_id_b, demographics_similarity_scaled, vitalsigns_similarity_scaled, labevents_similarity_scaled, diagnoses_similarity_scaled,
    inputevents_similarity_scaled, labevents_first24h_similarity_scaled, vitalsigns_first24h_similarity_scaled, prescriptions_similarity_scaled
FROM similarities_20230516144951_scaled
ORDER BY hadm_id_a, hadm_id_b DESC;
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
    id.los_icu,
    id.los_hospital,
    CASE WHEN id.dod <= DATETIME_ADD(ie.outtime, INTERVAL '6' HOUR) THEN 1 ELSE 0 END AS icu_mortality,
    CASE WHEN id.dod <= DATETIME_ADD(ad.dischtime, INTERVAL '6' HOUR) THEN 1 ELSE 0 END AS hosp_mortality,
    CASE WHEN id.dod <= DATETIME_ADD(ad.dischtime, INTERVAL '30' DAY) THEN 1 ELSE 0 END AS thirty_day_mortality,
    CASE WHEN id.dod <= DATETIME_ADD(ad.dischtime, INTERVAL '1' YEAR) THEN 1 ELSE 0 END AS one_year_mortality
FROM 
    mimiciv_derived.icustay_detail id,
    mimiciv_icu.icustays ie,
    mimiciv_hosp.admissions ad
WHERE 
    id.hadm_id = ie.hadm_id AND
    id.hadm_id = ad.hadm_id AND
    id.hadm_id = %s ;
"""

endpoints_for_hadm_ids = """
SELECT 
    id.hadm_id,
    id.los_icu,
    id.los_hospital,
    CASE WHEN id.dod <= DATETIME_ADD(ie.outtime, INTERVAL '6' HOUR) THEN 1 ELSE 0 END AS icu_mortality,
    CASE WHEN id.dod <= DATETIME_ADD(ad.dischtime, INTERVAL '6' HOUR) THEN 1 ELSE 0 END AS hosp_mortality,
    CASE WHEN id.dod <= DATETIME_ADD(ad.dischtime, INTERVAL '30' DAY) THEN 1 ELSE 0 END AS thirty_day_mortality,
    CASE WHEN id.dod <= DATETIME_ADD(ad.dischtime, INTERVAL '1' YEAR) THEN 1 ELSE 0 END AS one_year_mortality
FROM 
    mimiciv_derived.icustay_detail id,
    mimiciv_icu.icustays ie,
    mimiciv_hosp.admissions ad
WHERE 
    id.hadm_id = ie.hadm_id AND
    id.hadm_id = ad.hadm_id AND
    id.first_icu_stay = true AND
    id.hadm_id = ANY( %s )
GROUP BY
    id.hadm_id, id.los_icu, id.los_hospital, icu_mortality, hosp_mortality, thirty_day_mortality, one_year_mortality;
"""

sepsis_cohort = """
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
    AND a.age >= %s
    AND a.age <= %s 
    AND p.gender = 'F'
LIMIT %s ;
"""

kdigo_cohort = """
    SELECT DISTINCT sta.hadm_id
    FROM mimiciv_derived.kdigo_stages kdigo, mimiciv_icu.icustays sta
    WHERE kdigo.stay_id = sta.stay_id 
    AND kdigo.aki_stage > 2
    AND EXISTS (SELECT 1 FROM mimiciv_hosp.labevents lab WHERE lab.hadm_id = sta.hadm_id)
    ORDER BY sta.hadm_id
    LIMIT 400;
"""

icp_cohort = """
    SELECT DISTINCT sta.hadm_id
    FROM mimiciv_derived.icp icp, mimiciv_icu.icustays sta
    WHERE icp.stay_id = sta.stay_id 
    AND icp.icp > 25
    AND EXISTS (SELECT 1 FROM mimiciv_hosp.labevents lab WHERE lab.hadm_id = sta.hadm_id)
    ORDER BY sta.hadm_id
    LIMIT 400;
"""

meld_cohort = """
    SELECT DISTINCT sta.hadm_id
    FROM mimiciv_derived.meld meld, mimiciv_icu.icustays sta
    WHERE meld.stay_id = sta.stay_id
    AND EXISTS (SELECT 1 FROM mimiciv_hosp.labevents lab WHERE lab.hadm_id = sta.hadm_id)
    AND meld.meld > 32
    ORDER BY sta.hadm_id
    LIMIT 400;
"""

cardiac_query = """
    SELECT DISTINCT card.hadm_id
    FROM mimiciv_derived.cardiac_marker card, mimiciv_icu.icustays sta, mimiciv_derived.meld meld
    WHERE card.hadm_id IS NOT NULL
    AND card.ntprobnp > 1000
    AND EXISTS (SELECT 1 FROM mimiciv_hosp.labevents lab WHERE lab.hadm_id = card.hadm_id)
    AND card.hadm_id = sta.hadm_id
    AND meld.stay_id = sta.stay_id
    AND meld.meld < 20
    ORDER BY card.hadm_id
    LIMIT 400;
"""

cardiac_query_2 = """
    SELECT DISTINCT card.hadm_id
    FROM mimiciv_derived.cardiac_marker card, mimiciv_icu.icustays sta
    WHERE card.hadm_id IS NOT NULL
    AND card.ntprobnp > 10000
    AND EXISTS (SELECT 1 FROM mimiciv_hosp.labevents lab WHERE lab.hadm_id = card.hadm_id)
    AND card.hadm_id = sta.hadm_id
    ORDER BY card.hadm_id
    LIMIT 400;
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

labevent_by_id_for_hadm_ids = """
SELECT
    le.itemid, le.subject_id, le.hadm_id, AVG(le.valuenum) as valuenum, le.valueuom, ls.mean_value, ls.std_dev
FROM
    mimiciv_hosp.labevents le, labevent_statistics ls
WHERE
    le.itemid = ls.itemid
    AND
    le.hadm_id = ANY( %s )
    AND
    le.itemid = %s 
GROUP BY 
    le.itemid, le.hadm_id, le.subject_id, le.valueuom, ls.mean_value, ls.std_dev;
"""

search_labevents_by_label = """
SELECT le.itemid, li.label
FROM mimiciv_hosp.labevents le, mimiciv_hosp.d_labitems li
WHERE le.itemid = li.itemid AND li.label like '% %s %'
group by le.itemid, li.label;
"""

labevent_label = """
SELECT label
FROM mimiciv_hosp.d_labitems
WHERE itemid = %s;
"""

