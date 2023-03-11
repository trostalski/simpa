from typing import Optional
from pydantic import BaseModel
import math

from db import PostgresDB
from icd_diagnoses import ICDComparator, ICDDiagnosis
from labevents import LabEvent, LabEventComparator
from demographics import Demographics, DemographicsComparator
from schemas import Proband, SimilarityEncounter


def scale_to_range(input_list: list[int], range_start: int, range_end: int):
    min_value = min(input_list)
    max_value = max(input_list)
    return [
        ((value - min_value) / (max_value - min_value)) * (range_end - range_start)
        + range_start
        for value in input_list
    ]


def get_count_of_encounters_with_diagnosis(
    input_code: str, all_encounters: tuple[SimilarityEncounter]
) -> int:
    counter = 0
    for encounter in all_encounters:
        if input_code in [d.code for d in encounter.diagnoses]:
            counter += 1
    return counter


class Cohort:
    participants: list[Proband]

    def __init__(
        self,
        db: PostgresDB,
        participants: Optional[list[Proband]] = None,
    ):
        self.participants = participants if participants else []
        self.db = db
        if len(self.participants) > 0:
            self._get_endpoints()

    @classmethod
    def from_query(cls, query: str, db: PostgresDB):
        probands = []
        db_result = db.execute_query(query)
        for subject_id, hadm_id in db_result:
            probands.append(Proband(subject_id=subject_id, hadm_id=hadm_id))
        cohort = cls(participants=probands, db=db)
        return cohort

    def initialize_data(self, with_tfidf_diagnoses: bool = False):
        """Initialize data for the cohort."""
        self.demographics = self.db.get_patient_demographics(self.subject_ids)
        self.diagnoses = self.db.get_icd_diagnoses(self.hadm_ids)
        self.labevents = self.db.get_labevents(self.hadm_ids)
        self.similarity_encounters = self._create_similarity_encounters()
        if with_tfidf_diagnoses:
            self.encounter_with_code_cache = {}
            self._get_tfidf_scores_for_encounters()

    def _get_endpoints(self):
        for participant in self.participants:
            (
                participant.los_icu,
                participant.los_hosp,
            ) = self.db.get_endpoints_for_hadm_id(participant.hadm_id)

    def _get_tfidf_scores_for_encounters(self) -> dict:
        for encounter in self.similarity_encounters:
            for diagnosis in encounter.diagnoses:
                diagnosis.tfidf_score = self._calculate_tfidf_score(
                    input_diagnosis=diagnosis, encounter=encounter
                )

    def _create_similarity_encounters(self):
        result = []
        for patient in self.participants:
            diagnoses = [d for d in self.diagnoses if d.hadm_id == patient.hadm_id]
            lab = [l for l in self.labevents if l.hadm_id == patient.hadm_id]
            for d in self.demographics:
                if d.subject_id == patient.subject_id:
                    demo = d
            result.append(
                SimilarityEncounter(
                    subject_id=patient.subject_id,
                    hadm_id=patient.hadm_id,
                    diagnoses=diagnoses,
                    demographics=demo,
                    labevents=lab,
                )
            )
        return result

    def compare_encounters(
        self,
        demographics_weight: float = 0.2,
        diagnoses_weight: float = 0.4,
        labevents_weight: float = 0.4,
        aggregate_method: str = None,
        normalize_categories: bool = True,
    ):
        result = []
        result_cache = {}
        comparator = EncounterComparator(db=self.db)
        for encounter_a in self.similarity_encounters:
            for encounter_b in self.similarity_encounters:
                if (
                    tuple(sorted([encounter_a.hadm_id, encounter_b.hadm_id]))
                    in result_cache
                ):
                    result.append(
                        {
                            "encounter_a": encounter_a.hadm_id,
                            "encounter_b": encounter_b.hadm_id,
                            "similarity": result_cache[
                                tuple(
                                    sorted([encounter_a.hadm_id, encounter_b.hadm_id])
                                )
                            ],
                        }
                    )
                else:
                    similarity = comparator.compare(
                        encounter_a,
                        encounter_b,
                        demographics_weight=demographics_weight,
                        diagnoses_weight=diagnoses_weight,
                        labevents_weight=labevents_weight,
                        aggregate_method=aggregate_method
                        if not normalize_categories
                        else None,
                    )
                    result.append(
                        {
                            "encounter_a": encounter_a.hadm_id,
                            "encounter_b": encounter_b.hadm_id,
                            "similarity": similarity,
                        }
                    )
                    result_cache[
                        tuple(sorted([encounter_a.hadm_id, encounter_b.hadm_id]))
                    ] = similarity
            print(f"Finished encounter {encounter_a.hadm_id}")

        if normalize_categories:
            print("Normalizing categories by sclaing to range 0..1")
            result = self._normalize_categories(result)
            if aggregate_method == "mean":
                for r in result:
                    if r["encounter_a"] == r["encounter_b"]:
                        r["similarity"] = 1
                        continue
                    similarities = r["similarity"]
                    r["similarity"] = (
                        demographics_weight * similarities["demographics_sim"]
                        + diagnoses_weight * similarities["diagnoses_sim"]
                        + labevents_weight * similarities["labevents_sim"]
                    )
            elif aggregate_method == "rmse":
                for r in result:
                    if r["encounter_a"] == r["encounter_b"]:
                        r["similarity"] = 1
                        continue
                    similarities = r["similarity"]
                    r["similarity"] = math.sqrt(
                        (demographics_weight * similarities["demographics_sim"] ** 2)
                        + diagnoses_weight * (similarities["diagnoses_sim"] ** 2)
                        + labevents_weight * (similarities["labevents_sim"] ** 2)
                    )
        return result

    def _normalize_categories(self, result: list[dict]) -> list[dict]:
        demographics_sims = [
            r["similarity"]["demographics_sim"]
            for r in result
            if r["encounter_a"] != r["encounter_b"]
        ]
        diagnoses_sims = [
            r["similarity"]["diagnoses_sim"]
            for r in result
            if r["encounter_a"] != r["encounter_b"]
        ]
        labevents_sims = [
            r["similarity"]["labevents_sim"]
            for r in result
            if r["encounter_a"] != r["encounter_b"]
        ]
        normalized_demographics_sims = scale_to_range(
            input_list=demographics_sims, range_start=0, range_end=1
        )
        normalized_diagnoses_sims = scale_to_range(
            input_list=diagnoses_sims, range_start=0, range_end=1
        )
        normalized_labevents_sims = scale_to_range(
            input_list=labevents_sims, range_start=0, range_end=1
        )

        for i, r in enumerate(
            [r for r in result if r["encounter_a"] != r["encounter_b"]]
        ):
            r["similarity"]["demographics_sim"] = normalized_demographics_sims[i]
            r["similarity"]["diagnoses_sim"] = normalized_diagnoses_sims[i]
            r["similarity"]["labevents_sim"] = normalized_labevents_sims[i]
        return result

    def _calculate_tfidf_score(
        self,
        input_diagnosis: ICDDiagnosis,
        encounter: SimilarityEncounter,
    ) -> dict:
        encounter_diagnoses = [d for d in encounter.diagnoses]
        encounter_diagnoses_count = len(encounter_diagnoses)
        encounter_code_count = len(
            [d for d in encounter_diagnoses if d.code == input_diagnosis.code]
        )
        tf = encounter_code_count / encounter_diagnoses_count

        total_encounters_count = len(self.similarity_encounters)

        if input_diagnosis.code in self.encounter_with_code_cache:
            encounter_with_code_count = self.encounter_with_code_cache[
                input_diagnosis.code
            ]
        else:
            encounter_with_code_count = get_count_of_encounters_with_diagnosis(
                input_code=input_diagnosis.code,
                all_encounters=self.similarity_encounters,
            )
            self.encounter_with_code_cache[
                input_diagnosis.code
            ] = encounter_diagnoses_count

        idf = math.log(total_encounters_count / encounter_with_code_count)
        return tf * idf

    def get_sepsis_cohort(self, size: int = 100):
        query = """
            SELECT sep.subject_id, sep.stay_id, sta.hadm_id
            FROM mimiciv_derived.sepsis3 sep, mimiciv_icu.icustays sta
            WHERE sep.stay_id = sta.stay_id limit %s;
        """
        db_result = self.db.execute_query(query, (size,))
        for subject_id, stay_id, hadm_id in db_result:
            self.participants.append(
                Proband(subject_id=subject_id, stay_id=stay_id, hadm_id=hadm_id)
            )
        self._get_endpoints()

    @property
    def hadm_ids(self):
        return [p.hadm_id for p in self.participants]

    @property
    def subject_ids(self):
        return [p.subject_id for p in self.participants]


class EncounterComparator:
    def __init__(
        self,
        db: PostgresDB = None,
        scale_labevents_by_distribution: bool = True,
    ):
        self.scale_by_distribution = scale_labevents_by_distribution
        self.db = db

    def compare(
        self,
        encounter_a: SimilarityEncounter,
        encounter_b: SimilarityEncounter,
        demographics_weight: float = 0.2,
        diagnoses_weight: float = 0.4,
        labevents_weight: float = 0.4,
        aggregate_method: str = None,
    ) -> dict:
        demographics_sim = self._compare_demographics(
            demographics_a=encounter_a.demographics,
            demographics_b=encounter_b.demographics,
        )
        diagnoses_sim = self._compare_diagnoses(
            diagnoses_a=encounter_a.diagnoses,
            diagnoses_b=encounter_b.diagnoses,
        )
        labevents_sim = self._compare_labevents(
            labevents_a=encounter_a.labevents, labevents_b=encounter_b.labevents
        )

        # print(f"Demographics: {demographics_sim}")
        # print(f"Diagnoses: {diagnoses_sim}")
        # print(f"Labevents: {labevents_sim}")
        # print("-----------------------------\n")

        if aggregate_method is None:
            result = {
                "demographics_sim": demographics_sim,
                "diagnoses_sim": diagnoses_sim,
                "labevents_sim": labevents_sim,
            }
        elif aggregate_method == "mean":
            result = (
                demographics_sim * demographics_weight
                + diagnoses_sim * diagnoses_weight
                + labevents_sim * labevents_weight
            )
        elif aggregate_method == "rmse":
            result = math.sqrt(
                (demographics_sim**2 * demographics_weight)
                + (diagnoses_sim**2 * diagnoses_weight)
                + (labevents_sim**2 * labevents_weight)
            )
        else:
            raise ValueError(f"Unknown aggregate method {aggregate_method}")
        return result

    def _compare_demographics(
        self, demographics_a: Demographics, demographics_b: Demographics
    ) -> float:
        comparator = DemographicsComparator()
        return comparator.compare(demographics_a, demographics_b)

    def _compare_diagnoses(
        self,
        diagnoses_a: list[ICDDiagnosis],
        diagnoses_b: list[ICDDiagnosis],
    ) -> float:
        comparator = ICDComparator()
        return comparator.compare(diagnoses_a=diagnoses_a, diagnoses_b=diagnoses_b)

    def _compare_labevents(
        self, labevents_a: list[LabEvent], labevents_b: list[LabEvent]
    ) -> float:
        comparator = LabEventComparator(db=self.db)
        return comparator.compare(
            labevents_a, labevents_b, scale_by_distribution=self.scale_by_distribution
        )
