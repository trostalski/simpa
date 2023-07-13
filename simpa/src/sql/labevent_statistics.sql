-- This script creates a table of the mean and standard deviation of the labevent for each itemid
-- Create a table of the labevent distributions for each itemid
DROP TABLE IF EXISTS labevent_statistics;

CREATE TABLE labevent_statistics(
    itemid integer,
    mean_value float,
    std_dev float,
    mean_first_24h float,
    stddev_first_24h float,
    mean_max_value float,
    stddev_max_value float,
    mean_min_value float,
    stddev_min_value float
);

INSERT INTO labevent_statistics(itemid, mean_value, std_dev, mean_first_24h, stddev_first_24h)
SELECT
    le.itemid,
    AVG(valuenum) AS mean_value,
    STDDEV(valuenum) AS std_dev,
    AVG(
        CASE WHEN le.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY) THEN
            valuenum
        ELSE
            NULL
        END) AS mean_first_24h,
    STDDEV(
        CASE WHEN le.charttime <= DATETIME_ADD(ie.intime, INTERVAL '1' DAY) THEN
            valuenum
        ELSE
            NULL
        END) AS stddev_first_24h
FROM
    mimiciv_hosp.labevents le,
    mimiciv_icu.icustays ie
WHERE
    le.hadm_id = ie.hadm_id
    AND le.charttime >= DATETIME_SUB(ie.intime, INTERVAL '6' HOUR)
    AND valuenum IS NOT NULL
GROUP BY
    le.itemid;

