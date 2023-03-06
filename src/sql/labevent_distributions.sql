-- This script creates a table of the mean and standard deviation of the labevent for each itemid

-- Create a table of the labevent distributions for each itemid
CREATE TABLE IF NOT EXISTS labevent_distributions (
    itemid INTEGER,
    mean FLOAT,
    std FLOAT
);

INSERT INTO labevent_distributions (itemid, mean, std)
SELECT itemid, AVG(valuenum) AS mean, STDDEV(valuenum) AS std
FROM mimiciv_hosp.labevents
WHERE valuenum IS NOT NULL
GROUP BY itemid;