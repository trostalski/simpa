from typing import Optional
import math

from helper import scale_to_range
from db import PostgresDB
from icd_diagnoses import ICDComparator, ICDDiagnosis
from labevents import LabEventComparator
from demographics import DemographicsComparator
from vitalsigns import VitalsignComparator
from inputevents import InputEventComparator
from base_comparators import BaseComparator
from schemas import (
    Proband,
    SimilarityEncounter,
    LabEvent,
    Vitalsign,
    Demographics,
    InputEvent,
)


def get_count_of_encounters_with_diagnosis(
    input_code: str, all_encounters: tuple[SimilarityEncounter]
) -> int:
    counter = 0
    for encounter in all_encounters:
        if input_code in [d.code for d in encounter.diagnoses]:
            counter += 1
    return counter


class Cohort:
    def __init__(
        self,
        db: PostgresDB,
        participants: Optional[list[Proband]] = None,
    ):
        self.participants = participants if participants else []
        self.db = db
        if len(self.participants) > 0:
            self._get_endpoints_for_participants()

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
        self.labevents = self.db.get_mean_labevents(self.hadm_ids)
        self.vitalsigns = self.db.get_mean_vitalsigns(self.hadm_ids)
        self.inputevents = self.db.get_inputevents(self.hadm_ids)
        self.similarity_encounters = self._create_similarity_encounters()
        if with_tfidf_diagnoses:
            self.encounter_with_code_cache = {}
            self._get_tfidf_scores_for_encounters()

    def compare_encounters(
        self,
        demographics_weight: float = 0.1,
        diagnoses_weight: float = 0.2,
        labevents_weight: float = 0.4,
        vitalsigns_weight: float = 0.2,
        inputevents_weight: float = 0.1,
        aggregate_method: str = None,
        normalize_categories: bool = True,
        scale_by_distribution: bool = True,
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
                        vitalsigns_weight=vitalsigns_weight,
                        inputevents_weight=inputevents_weight,
                        scale_by_distribution=scale_by_distribution,
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
            print("Normalizing categories by scaling to range 0..1")
            try:
                result = self._normalize_categories(result)
            except Exception as e:
                print("Failed to normalize categories")
                print("result: ", result)
                raise e
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
                        + vitalsigns_weight * similarities["vitalsigns_sim"]
                        + inputevents_weight * similarities["inputevents_sim"]
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
                        + vitalsigns_weight * (similarities["vitalsigns_sim"] ** 2)
                        + inputevents_weight * (similarities["inputevents_sim"] ** 2)
                    )
        return result

    def _create_similarity_encounters(self):
        result = []
        for patient in self.participants:
            diagnoses = [d for d in self.diagnoses if d.hadm_id == patient.hadm_id]
            lab = [l for l in self.labevents if l.hadm_id == patient.hadm_id]
            vitalsigns = [v for v in self.vitalsigns if v.hadm_id == patient.hadm_id]
            inputevents = [i for i in self.inputevents if i.hadm_id == patient.hadm_id]
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
                    vitalsigns=vitalsigns,
                    inputevents=inputevents,
                )
            )
        return result

    def _get_endpoints_for_participants(self):
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
        vitalsigns_sims = [
            r["similarity"]["vitalsigns_sim"]
            for r in result
            if r["encounter_a"] != r["encounter_b"]
        ]
        inputevents_sim = [
            r["similarity"]["inputevents_sim"]
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
        normalized_vitalsigns_sims = scale_to_range(
            input_list=vitalsigns_sims, range_start=0, range_end=1
        )
        normalized_inputevents_sims = scale_to_range(
            input_list=inputevents_sim, range_start=0, range_end=1
        )

        for i, r in enumerate(
            [r for r in result if r["encounter_a"] != r["encounter_b"]]
        ):
            r["similarity"]["demographics_sim"] = normalized_demographics_sims[i]
            r["similarity"]["diagnoses_sim"] = normalized_diagnoses_sims[i]
            r["similarity"]["labevents_sim"] = normalized_labevents_sims[i]
            r["similarity"]["vitalsigns_sim"] = normalized_vitalsigns_sims[i]
            r["similarity"]["inputevents_sim"] = normalized_inputevents_sims[i]
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
        self._get_endpoints_for_participants()

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
    ):
        self.db = db

    def compare(
        self,
        encounter_a: SimilarityEncounter,
        encounter_b: SimilarityEncounter,
        demographics_weight: float = 0.1,
        diagnoses_weight: float = 0.2,
        labevents_weight: float = 0.4,
        vitalsigns_weight: float = 0.2,
        inputevents_weight: float = 0.1,
        aggregate_method: str = None,
        scale_by_distribution: bool = True,
    ) -> dict:
        demographics_sim = self._compare_demographics(
            demographics_a=encounter_a.demographics,
            demographics_b=encounter_b.demographics,
        )
        # print("compared demographics")
        diagnoses_sim = self._compare_diagnoses(
            diagnoses_a=encounter_a.diagnoses,
            diagnoses_b=encounter_b.diagnoses,
        )
        # print("compared diagnoses")
        labevents_sim = self._compare_labevents(
            labevents_a=encounter_a.labevents,
            labevents_b=encounter_b.labevents,
            scale_by_distribution=scale_by_distribution,
        )
        # print("compared labevents")
        vitalsign_sim = self._compare_vitalsigns(
            vitalsigns_a=encounter_a.vitalsigns,
            vitalsigns_b=encounter_b.vitalsigns,
            scale_by_distribution=scale_by_distribution,
        )
        # print("compared vitalsigns")
        inputevents_sim = self._compare_inputevents(
            inputevents_a=encounter_a.inputevents, inputevents_b=encounter_b.inputevents
        )
        # print("compared inputevents")

        # print(f"Demographics: {demographics_sim}")
        # print(f"Diagnoses: {diagnoses_sim}")
        # print(f"Labevents: {labevents_sim}")
        # print("-----------------------------\n")

        if aggregate_method is None:
            result = {
                "demographics_sim": demographics_sim,
                "diagnoses_sim": diagnoses_sim,
                "labevents_sim": labevents_sim,
                "vitalsigns_sim": vitalsign_sim,
                "inputevents_sim": inputevents_sim,
            }
        elif aggregate_method == "mean":
            result = (
                demographics_sim * demographics_weight
                + diagnoses_sim * diagnoses_weight
                + labevents_sim * labevents_weight
                + vitalsign_sim * vitalsigns_weight
                + inputevents_sim * inputevents_weight
            )
        elif aggregate_method == "rmse":
            result = math.sqrt(
                (demographics_sim**2 * demographics_weight)
                + (diagnoses_sim**2 * diagnoses_weight)
                + (labevents_sim**2 * labevents_weight)
                + (vitalsign_sim**2 * vitalsigns_weight)
                + (inputevents_sim**2 * inputevents_weight)
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
        self,
        labevents_a: list[LabEvent],
        labevents_b: list[LabEvent],
        scale_by_distribution: bool,
    ) -> float:
        comparator = LabEventComparator(db=self.db)
        return comparator.compare(
            labevents_a, labevents_b, scale_by_distribution=scale_by_distribution
        )

    def _compare_vitalsigns(
        self,
        vitalsigns_a: list[Vitalsign],
        vitalsigns_b: list[Vitalsign],
        scale_by_distribution: bool,
    ) -> float:
        comparator = VitalsignComparator(db=self.db)
        return comparator.compare(
            vitalsigns_a, vitalsigns_b, scale_by_distribution=scale_by_distribution
        )

    def _compare_inputevents(
        self, inputevents_a: list[InputEvent], inputevents_b: list[InputEvent]
    ) -> float:
        comparator = InputEventComparator(db=self.db)
        return comparator.compare(inputevents_a, inputevents_b)
