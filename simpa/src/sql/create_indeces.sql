CREATE INDEX IF NOT EXISTS idx_patients_subject_id ON mimiciv_hosp.patients (subject_id);

CREATE INDEX IF NOT EXISTS idx_admissions_subject_id ON mimiciv_hosp.admissions (subject_id);

CREATE INDEX IF NOT EXISTS idx_diagnoses_icd_hadm_id ON mimiciv_hosp.diagnoses_icd (hadm_id);

CREATE INDEX IF NOT EXISTS idx_labevents_hadm_id ON mimiciv_hosp.labevents (hadm_id);

CREATE INDEX IF NOT EXISTS idx_labevents_itemid ON mimiciv_hosp.labevents (itemid);

CREATE INDEX IF NOT EXISTS idx_labevent_statistics_itemid ON labevent_statistics (itemid);

CREATE INDEX IF NOT EXISTS idx_vitalsigns_stay_id ON mimiciv_derived.vitalsign (stay_id);

CREATE INDEX IF NOT EXISTS idx_vitalsigns_statistics_name ON vitalsign_statistics (vitalsign_name);

CREATE INDEX IF NOT EXISTS idx_vitalsigns_hadm_id ON mimiciv_derived.vitalsign (hadm_id);

CREATE INDEX IF NOT EXISTS idx_inputevents_hadm_id ON mimiciv_icu.inputevents (hadm_id);

CREATE INDEX IF NOT EXISTS idx_icustays_hadm_id ON mimiciv_icu.icustays (hadm_id);

CREATE INDEX IF NOT EXISTS idx_icustays_stay_id ON mimiciv_icu.icustays (stay_id);

CREATE INDEX IF NOT EXISTS idx_sepsis3_stay_id ON mimiciv_derived.sepsis3 (stay_id);
