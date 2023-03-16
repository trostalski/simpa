DROP TABLE IF EXISTS vitalsign_statistics;

CREATE TABLE vitalsign_statistics (
    vitalsign_name varchar(50),
    mean_value float,
    std_dev float
);

INSERT INTO vitalsign_statistics (vitalsign_name, mean_value, std_dev)
SELECT
    'heart_rate' AS vitalsign_name,
    AVG(heart_rate) AS mean_value,
    STDDEV(heart_rate) AS std_dev
FROM
    mimiciv_derived.vitalsign;

INSERT INTO vitalsign_statistics (vitalsign_name, mean_value, std_dev)
SELECT
    'sbp' AS vitalsign_name,
    AVG(sbp) AS mean_value,
    STDDEV(heart_rate) AS std_dev
FROM
    mimiciv_derived.vitalsign;

INSERT INTO vitalsign_statistics (vitalsign_name, mean_value, std_dev)
SELECT
    'dbp' AS vitalsign_name,
    AVG(dbp) AS mean_value,
    STDDEV(heart_rate) AS std_dev
FROM
    mimiciv_derived.vitalsign;

INSERT INTO vitalsign_statistics (vitalsign_name, mean_value, std_dev)
SELECT
    'mbp' AS vitalsign_name,
    AVG(mbp) AS mean_value,
    STDDEV(heart_rate) AS std_dev
FROM
    mimiciv_derived.vitalsign;

INSERT INTO vitalsign_statistics (vitalsign_name, mean_value, std_dev)
SELECT
    'sbp_ni' AS vitalsign_name,
    AVG(sbp_ni) AS mean_value,
    STDDEV(sbp_ni) AS std_dev
FROM
    mimiciv_derived.vitalsign;

INSERT INTO vitalsign_statistics (vitalsign_name, mean_value, std_dev)
SELECT
    'dbp_ni' AS vitalsign_name,
    AVG(dbp_ni) AS mean_value,
    STDDEV(dbp_ni) AS std_dev
FROM
    mimiciv_derived.vitalsign;

INSERT INTO vitalsign_statistics (vitalsign_name, mean_value, std_dev)
SELECT
    'mbp_ni' AS vitalsign_name,
    AVG(mbp_ni) AS mean_value,
    STDDEV(mbp_ni) AS std_dev
FROM
    mimiciv_derived.vitalsign;

INSERT INTO vitalsign_statistics (vitalsign_name, mean_value, std_dev)
SELECT
    'resp_rate' AS vitalsign_name,
    AVG(resp_rate) AS mean_value,
    STDDEV(resp_rate) AS std_dev
FROM
    mimiciv_derived.vitalsign;

INSERT INTO vitalsign_statistics (vitalsign_name, mean_value, std_dev)
SELECT
    'temperature' AS vitalsign_name,
    AVG(temperature) AS mean_value,
    STDDEV(temperature) AS std_dev
FROM
    mimiciv_derived.vitalsign;

INSERT INTO vitalsign_statistics (vitalsign_name, mean_value, std_dev)
SELECT
    'spo2' AS vitalsign_name,
    AVG(spo2) AS mean_value,
    STDDEV(spo2) AS std_dev
FROM
    mimiciv_derived.vitalsign;

INSERT INTO vitalsign_statistics (vitalsign_name, mean_value, std_dev)
SELECT
    'glucose' AS vitalsign_name,
    AVG(glucose) AS mean_value,
    STDDEV(glucose) AS std_dev
FROM
    mimiciv_derived.vitalsign
WHERE
    glucose < 2000;

