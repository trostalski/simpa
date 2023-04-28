CREATE TABLE IF NOT EXISTS similarities (
    hadm_id_a integer,
    hadm_id_b integer,
    demographics_similarity float,
    icd_diagnoses_similarity float,
    labevents_similarity float,
    vitalsigns_similarity float,
    inputevents_similarity float,
    cohort_name varchar(255),
    PRIMARY KEY (hadm_id_a, hadm_id_b)
);

CREATE INDEX IF NOT EXISTS similarity_hadm_id_a ON similarities (hadm_id_a);

CREATE INDEX IF NOT EXISTS similarity_hadm_id_b ON similarities (hadm_id_b);

