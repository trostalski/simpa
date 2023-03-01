import os
from typing import Optional, Union
import requests
from pydantic import BaseModel
from base_comparator import BaseComparator
from dotenv import load_dotenv

load_dotenv()


class ICDDiagnosis(BaseModel):
    subject_id: int
    hadm_id: int
    seq_num: int
    code: str
    version: str
    tfidf_score: Optional[float] = None


class ICDComparator(BaseComparator):
    def __init__(self):
        pass

    def compare(
        self,
        diagnoses_a: Union[list[ICDDiagnosis], ICDDiagnosis],
        diagnoses_b: Union[list[ICDDiagnosis], ICDDiagnosis],
    ) -> float:
        if isinstance(diagnoses_a, list) or isinstance(diagnoses_b, list):
            return self._compare_set(diagnoses_a, diagnoses_b)
        else:
            return self._compare_pair(diagnoses_a, diagnoses_b)

    def _compare_pair(
        self,
        diagnoses_a: ICDDiagnosis,
        diagnoses_b: ICDDiagnosis,
        cs_metric: str = None,
        ic_metric: str = None,
    ) -> float:
        terminology_base = os.getenv("TERMINOLOGY_SERVER")

        if not terminology_base:
            raise ValueError("No terminology server found.")

        code_a = diagnoses_a.code
        code_b = diagnoses_b.code

        if not code_a or not code_b:
            raise ValueError("No code found.")

        code_similarity_endpoint = "/icd10/similarity/pair"

        response = requests.post(
            terminology_base + code_similarity_endpoint,
            json={
                "node_a": {"code": code_a, "weight": diagnoses_a.tfidf_score},
                "node_b": {"code": code_b, "weight": diagnoses_b.tfidf_score},
            },
        )
        response.raise_for_status()
        similarity = response.json()["similarity"]
        return similarity

    def _compare_set(
        self,
        diagnoses_set_a: list[ICDDiagnosis],
        diagnoses_set_b: list[ICDDiagnosis],
    ) -> Union[float, bool]:
        nodes_a = []
        nodes_b = []

        for diagnosis in diagnoses_set_a:
            if diagnosis.code is None:
                continue
            nodes_a.append({"code": diagnosis.code, "weight": diagnosis.tfidf_score})
        for diagnosis in diagnoses_set_b:
            if diagnosis.code is None:
                continue
            nodes_b.append({"code": diagnosis.code, "weight": diagnosis.tfidf_score})

        terminology_base = os.getenv("TERMINOLOGY_SERVER")
        set_similarity_endpoint = "/icd10/similarity/set"

        response = requests.post(
            terminology_base + set_similarity_endpoint,
            json={
                "nodes_a": nodes_a,
                "nodes_b": nodes_b,
            },
        )

        response.raise_for_status()
        similarity = response.json()["similarity"]
        return similarity
