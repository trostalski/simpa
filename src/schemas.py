from typing import Optional, Union
from pydantic import BaseModel, validator
from datetime import datetime


class DistributionCategory(BaseModel):
    id: Union[int, str]
    value: Optional[float]


class Demographics(BaseModel):
    subject_id: int
    age: Optional[int]
    gender: Optional[str]
    ethnicity: Optional[str]
    height: Optional[float]


class LabEvent(DistributionCategory):
    item_id: int
    subject_id: int
    hadm_id: Optional[int]
    valueuom: Optional[str]


class Vitalsign(DistributionCategory):
    subject_id: int
    hadm_id: int
    name: str


class InputEvent(BaseModel):
    subject_id: int
    hadm_id: int
    item_id: int
    amount: Optional[float]
    amountuom: Optional[str]
    ordercategoryname: Optional[str]

    @validator("amount")
    def round_amount(cls, v):
        return round(v, 2)


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
    demographics: Demographics
    diagnoses: list[ICDDiagnosis]
    labevents: list[LabEvent]
    vitalsigns: list[Vitalsign]
    inputevents: list[InputEvent]


class Pharmacy(BaseModel):
    subject_id: int
    hadm_id: int
    pharmacy_id: int
    medication: str
    route: str
