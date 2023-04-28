DROP TABLE IF EXISTS vitalsign_statistics;

CREATE TABLE vitalsign_statistics(
    vitalsign_name varchar(50),
    mean_value float,
    std_dev float,
    mean_first_24h float,
    stddev_first_24h float
);

INSERT INTO vitalsign_statistics(vitalsign_name, mean_value, std_dev, mean_first_24h, stddev_first_24h)
SELECT
    'v.heart_rate' AS vitalsign_name,
    AVG(v.heart_rate) AS mean_value,
    STDDEV(v.heart_rate) AS std_dev,
    AVG(
        CASE WHEN v.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
            AND v.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR) THEN
            v.heart_rate
        ELSE
            NULL
        END) AS mean_first_24h,
    STDDEV(
        CASE WHEN v.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
            AND v.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR) THEN
            v.heart_rate
        ELSE
            NULL
        END) AS stddev_first_24h
FROM
    mimiciv_derived.vitalsign v,
    mimiciv_icu.icustays ie
WHERE
    v.stay_id = ie.stay_id
    AND v.heart_rate IS NOT NULL;

INSERT INTO vitalsign_statistics(vitalsign_name, mean_value, std_dev, mean_first_24h, stddev_first_24h)
SELECT
    'v.sbp' AS vitalsign_name,
    AVG(v.sbp) AS mean_value,
    STDDEV(v.sbp) AS std_dev,
    AVG(
        CASE WHEN v.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
            AND v.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR) THEN
            v.sbp
        ELSE
            NULL
        END) AS mean_first_24h,
    STDDEV(
        CASE WHEN v.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
            AND v.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR) THEN
            v.sbp
        ELSE
            NULL
        END) AS stddev_first_24h
FROM
    mimiciv_derived.vitalsign v,
    mimiciv_icu.icustays ie
WHERE
    v.stay_id = ie.stay_id
    AND v.sbp IS NOT NULL;

INSERT INTO vitalsign_statistics(vitalsign_name, mean_value, std_dev, mean_first_24h, stddev_first_24h)
SELECT
    'v.dbp' AS vitalsign_name,
    AVG(v.dbp) AS mean_value,
    STDDEV(v.dbp) AS std_dev,
    AVG(
        CASE WHEN v.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
            AND v.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR) THEN
            v.dbp
        ELSE
            NULL
        END) AS mean_first_24h,
    STDDEV(
        CASE WHEN v.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
            AND v.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR) THEN
            v.dbp
        ELSE
            NULL
        END) AS stddev_first_24h
FROM
    mimiciv_derived.vitalsign v,
    mimiciv_icu.icustays ie
WHERE
    v.stay_id = ie.stay_id
    AND v.dbp IS NOT NULL;

INSERT INTO vitalsign_statistics(vitalsign_name, mean_value, std_dev, mean_first_24h, stddev_first_24h)
SELECT
    'v.mbp' AS vitalsign_name,
    AVG(v.mbp) AS mean_value,
    STDDEV(v.mbp) AS std_dev,
    AVG(
        CASE WHEN v.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
            AND v.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR) THEN
            v.mbp
        ELSE
            NULL
        END) AS mean_first_24h,
    STDDEV(
        CASE WHEN v.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
            AND v.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR) THEN
            v.mbp
        ELSE
            NULL
        END) AS stddev_first_24h
FROM
    mimiciv_derived.vitalsign v,
    mimiciv_icu.icustays ie
WHERE
    v.mbp = ie.stay_id
    AND v.mbp IS NOT NULL;

INSERT INTO vitalsign_statistics(vitalsign_name, mean_value, std_dev, mean_first_24h, stddev_first_24h)
SELECT
    'v.sbp_ni' AS vitalsign_name,
    AVG(v.sbp_ni) AS mean_value,
    STDDEV(v.sbp_ni) AS std_dev,
    AVG(
        CASE WHEN v.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
            AND v.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR) THEN
            v.sbp_ni
        ELSE
            NULL
        END) AS mean_first_24h,
    STDDEV(
        CASE WHEN v.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
            AND v.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR) THEN
            v.sbp_ni
        ELSE
            NULL
        END) AS stddev_first_24h
FROM
    mimiciv_derived.vitalsign v,
    mimiciv_icu.icustays ie
WHERE
    v.stay_id = ie.stay_id
    AND v.sbp_ni IS NOT NULL;

INSERT INTO vitalsign_statistics(vitalsign_name, mean_value, std_dev, mean_first_24h, stddev_first_24h)
SELECT
    'v.spo2' AS vitalsign_name,
    AVG(v.spo2) AS mean_value,
    STDDEV(v.spo2) AS std_dev,
    AVG(
        CASE WHEN v.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
            AND v.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR) THEN
            v.spo2
        ELSE
            NULL
        END) AS mean_first_24h,
    STDDEV(
        CASE WHEN v.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
            AND v.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR) THEN
            v.spo2
        ELSE
            NULL
        END) AS stddev_first_24h
FROM
    mimiciv_derived.vitalsign v,
    mimiciv_icu.icustays ie
WHERE
    v.stay_id = ie.stay_id
    AND v.spo2 IS NOT NULL;

INSERT INTO vitalsign_statistics(vitalsign_name, mean_value, std_dev, mean_first_24h, stddev_first_24h)
SELECT
    'v.dbp_ni' AS vitalsign_name,
    AVG(v.dbp_ni) AS mean_value,
    STDDEV(v.dbp_ni) AS std_dev,
    AVG(
        CASE WHEN v.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
            AND v.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR) THEN
            v.dbp_ni
        ELSE
            NULL
        END) AS mean_first_24h,
    STDDEV(
        CASE WHEN v.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
            AND v.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR) THEN
            v.dbp_ni
        ELSE
            NULL
        END) AS stddev_first_24h
FROM
    mimiciv_derived.vitalsign v,
    mimiciv_icu.icustays ie
WHERE
    v.stay_id = ie.stay_id
    AND v.dbp_ni IS NOT NULL;

INSERT INTO vitalsign_statistics(vitalsign_name, mean_value, std_dev, mean_first_24h, stddev_first_24h)
SELECT
    'v.mbp_ni' AS vitalsign_name,
    AVG(v.mbp_ni) AS mean_value,
    STDDEV(v.mbp_ni) AS std_dev,
    AVG(
        CASE WHEN v.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
            AND v.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR) THEN
            v.mbp_ni
        ELSE
            NULL
        END) AS mean_first_24h,
    STDDEV(
        CASE WHEN v.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
            AND v.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR) THEN
            v.mbp_ni
        ELSE
            NULL
        END) AS stddev_first_24h
FROM
    mimiciv_derived.vitalsign v,
    mimiciv_icu.icustays ie
WHERE
    v.stay_id = ie.stay_id
    AND v.mbp_ni IS NOT NULL;

INSERT INTO vitalsign_statistics(vitalsign_name, mean_value, std_dev, mean_first_24h, stddev_first_24h)
SELECT
    'v.resp_rate' AS vitalsign_name,
    AVG(v.resp_rate) AS mean_value,
    STDDEV(v.resp_rate) AS std_dev,
    AVG(
        CASE WHEN v.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
            AND v.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR) THEN
            v.resp_rate
        ELSE
            NULL
        END) AS mean_first_24h,
    STDDEV(
        CASE WHEN v.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
            AND v.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR) THEN
            v.resp_rate
        ELSE
            NULL
        END) AS stddev_first_24h
FROM
    mimiciv_derived.vitalsign v,
    mimiciv_icu.icustays ie
WHERE
    v.stay_id = ie.stay_id
    AND v.resp_rate IS NOT NULL;

INSERT INTO vitalsign_statistics(vitalsign_name, mean_value, std_dev, mean_first_24h, stddev_first_24h)
SELECT
    'v.temperature' AS vitalsign_name,
    AVG(v.temperature) AS mean_value,
    STDDEV(v.temperature) AS std_dev,
    AVG(
        CASE WHEN v.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
            AND v.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR) THEN
            v.temperature
        ELSE
            NULL
        END) AS mean_first_24h,
    STDDEV(
        CASE WHEN v.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
            AND v.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR) THEN
            v.temperature
        ELSE
            NULL
        END) AS stddev_first_24h
FROM
    mimiciv_derived.vitalsign v,
    mimiciv_icu.icustays ie
WHERE
    v.stay_id = ie.stay_id
    AND v.temperature IS NOT NULL;

INSERT INTO vitalsign_statistics(vitalsign_name, mean_value, std_dev, mean_first_24h, stddev_first_24h)
SELECT
    'v.glucose' AS vitalsign_name,
    AVG(v.glucose) AS mean_value,
    STDDEV(v.glucose) AS std_dev,
    AVG(
        CASE WHEN v.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
            AND v.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR) THEN
            v.glucose
        ELSE
            NULL
        END) AS mean_first_24h,
    STDDEV(
        CASE WHEN v.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY)
            AND v.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR) THEN
            v.glucose
        ELSE
            NULL
        END) AS stddev_first_24h
FROM
    mimiciv_derived.vitalsign v,
    mimiciv_icu.icustays ie
WHERE
    v.stay_id = ie.stay_id
    AND v.glucose IS NOT NULL;

