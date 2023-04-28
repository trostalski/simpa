from typing import Optional, Union, Any
from pydantic import BaseModel, validator


class DistributionCategory(BaseModel):
    id: Union[int, str]
    value: Optional[float]
    id_mean: Optional[float]
    id_std_dev: Optional[float]
    abnormal: Optional[bool] = False


class BinaryCategory(BaseModel):
    value: Any


class Demographics(BaseModel):
    subject_id: int
    hadm_id: int
    age: Optional[int]
    gender: Optional[str]
    ethnicity: Optional[str]
    height: Optional[float]


class LabEvent(DistributionCategory):
    itemid: int
    subject_id: int
    hadm_id: Optional[int]
    valueuom: Optional[str]
    valuenum: Optional[float]
    label: Optional[str]


class Vitalsign(DistributionCategory):
    subject_id: int
    hadm_id: int
    name: str


class InputEvent(BinaryCategory):
    subject_id: int
    hadm_id: int
    itemid: int
    amount: Optional[float]
    amountuom: Optional[str]
    ordercategoryname: Optional[str]

    @validator("amount")
    def round_amount(cls, v):
        return round(v, 2)


class Prescription(BinaryCategory):
    subject_id: int
    hadm_id: int
    pharmacy_id: int
    drug: str
    gsn: str


class ICDDiagnosis(BaseModel):
    subject_id: int
    hadm_id: int
    seq_num: int
    icd_code: str
    icd_version: str
    tfidf_score: Optional[float] = 1.0


class Proband(BaseModel):
    hadm_id: int
    subject_id: Optional[int]
    los_icu: Optional[float]
    los_hosp: Optional[float]


class SimilarityEncounter(BaseModel):
    hadm_id: int
    demographics: Optional[Demographics] = None
    diagnoses: Optional[list[ICDDiagnosis]] = None
    labevents: Optional[list[LabEvent]] = None
    vitalsigns: Optional[list[Vitalsign]] = None
    inputevents: Optional[list[InputEvent]] = None

    def __eq__(self, other: Any) -> bool:
        return self.hadm_id == other.hadm_id


class Pharmacy(BaseModel):
    subject_id: int
    hadm_id: int
    pharmacy_id: int
    medication: str
    route: str


class SimilarityNode(BaseModel):
    code: str
    weight: Optional[float] = 1.0

    class Config:
        validate_assignment = True

    @validator("weight")
    def set_weight(cls, v):
        return v or 1.0
