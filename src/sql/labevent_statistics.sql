-- This script creates a table of the mean and standard deviation of the labevent for each itemid
-- Create a table of the labevent distributions for each itemid
DROP TABLE IF EXISTS labevent_statistics;
CREATE TABLE labevent_statistics (
    itemid INTEGER,
    mean_value FLOAT,
    std_dev FLOAT
);
INSERT INTO labevent_statistics (itemid, mean_value, std_dev)
SELECT itemid,
    AVG(valuenum) AS mean_value,
    STDDEV(valuenum) AS std_dev
FROM mimiciv_hosp.labevents
WHERE valuenum IS NOT NULL
GROUP BY itemid;