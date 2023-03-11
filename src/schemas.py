from typing import Optional
from pydantic import BaseModel
from datetime import datetime


class Demographics(BaseModel):
    subject_id: int
    age: Optional[int]
    gender: Optional[str]
    ethnicity: Optional[str]
    height: Optional[float]


class LabEvent(BaseModel):
    labevent_id: int
    item_id: int
    subject_id: int
    hadm_id: Optional[int]
    specimen_id: Optional[int]
    charttime: Optional[datetime]
    value: Optional[str]
    valuenum: Optional[float]
    valueuom: Optional[str]
    ref_range_lower: Optional[float]
    ref_range_upper: Optional[float]
    flag: Optional[str]
    priority: Optional[str]
    comments: Optional[str]
    label: Optional[str]
    fluid: Optional[str]
    category: Optional[str]


class ICDDiagnosis(BaseModel):
    subject_id: int
    hadm_id: int
    seq_num: int
    code: str
    version: str
    tfidf_score: Optional[float] = None


class Proband(BaseModel):
    subject_id: int
    hadm_id: int
    los_icu: Optional[float]
    los_hosp: Optional[float]


class SimilarityEncounter(BaseModel):
    subject_id: int
    hadm_id: int
    diagnoses: list[ICDDiagnosis]
    demographics: Demographics
    labevents: list[LabEvent]


class Pharmacy(BaseModel):
    subject_id: int
    hadm_id: int
    pharmacy_id: int
    medication: str
    route: str
