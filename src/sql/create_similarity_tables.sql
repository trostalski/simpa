CREATE TABLE IF NOT EXISTS demographics_similarity (
    hadm_id_a INTEGER,
    hadm_id_b INTEGER,
    raw_similarity_value FLOAT,
    scaled_similarity_value FLOAT,
    cohort_name VARCHAR(255),
    PRIMARY KEY (hadm_id_a, hadm_id_b)
);
CREATE INDEX IF NOT EXISTS demographics_similarity_hadm_id_a ON demographics_similarity (hadm_id_a);
CREATE INDEX IF NOT EXISTS demographics_similarity_hadm_id_b ON demographics_similarity (hadm_id_b);
CREATE TABLE IF NOT EXISTS icd_diagnoses_similarity (
    hadm_id_a INTEGER,
    hadm_id_b INteger,
    raw_similarity_value FLOAT,
    scaled_similarity_value FLOAT,
    cohort_name VARCHAR(255),
    PRIMARY KEY (hadm_id_a, hadm_id_b)
);
CREATE INDEX IF NOT EXISTS icd_diagnoses_similarity_hadm_id_a ON icd_diagnoses_similarity (hadm_id_a);
CREATE INDEX IF NOT EXISTS icd_diagnoses_similarity_hadm_id_b ON icd_diagnoses_similarity (hadm_id_b);
CREATE TABLE IF NOT EXISTS labevents_similarity (
    hadm_id_a INTEGER,
    hadm_id_b INTEGER,
    raw_similarity_value FLOAT,
    scaled_similarity_value FLOAT,
    cohort_name VARCHAR(255),
    PRIMARY KEY (hadm_id_a, hadm_id_b)
);
CREATE INDEX IF NOT EXISTS labevents_similarity_hadm_id_a ON labevents_similarity (hadm_id_a);
CREATE INDEX IF NOT EXISTS labevents_similarity_hadm_id_b ON labevents_similarity (hadm_id_b);
CREATE TABLE IF NOT EXISTS vitalsigns_similarity (
    hadm_id_a INTEGER,
    hadm_id_b INTEGER,
    raw_similarity_value FLOAT,
    scaled_similarity_value FLOAT,
    cohort_name VARCHAR(255),
    PRIMARY KEY (hadm_id_a, hadm_id_b)
);
CREATE INDEX IF NOT EXISTS vitalsigns_similarity_hadm_id_a ON vitalsigns_similarity (hadm_id_a);
CREATE INDEX IF NOT EXISTS vitalsigns_similarity_hadm_id_b ON vitalsigns_similarity (hadm_id_b);
CREATE TABLE IF NOT EXISTS inputevents_similarity (
    hadm_id_a INTEGER,
    hadm_id_b INTEGER,
    raw_similarity_value FLOAT,
    scaled_similarity_value FLOAT,
    cohort_name VARCHAR(255),
    PRIMARY KEY (hadm_id_a, hadm_id_b)
);
CREATE INDEX IF NOT EXISTS inputevents_similarity_hadm_id_a ON inputevents_similarity (hadm_id_a);
CREATE INDEX IF NOT EXISTS inputevents_similarity_hadm_id_b ON inputevents_similarity (hadm_id_b);