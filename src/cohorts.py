from functools import cache
from pydantic import BaseModel
import math

from db import PostgresDB
from icd_diagnoses import ICDComparator, ICDDiagnosis
from labevents import LabEvent, LabEventComparator, LabEventDistributions
from demographics import Demographics, DemographicsComparator


class Proband(BaseModel):
    subject_id: int
    hadm_id: int


class SimilarityEncounter(BaseModel):
    subject_id: int
    hadm_id: int
    diagnoses: list[ICDDiagnosis]
    demographics: Demographics
    labevents: list[LabEvent]


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

    def __init__(self, participants: list[Proband], db: PostgresDB):
        self.participants = participants
        self.db = db

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
        self.labevent_distribution = LabEventDistributions(self.labevents)
        if with_tfidf_diagnoses:
            self.encounter_with_code_cache = {}
            self._get_tfidf_scores_for_encounters()

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

    def compare_encounters(self):
        result = []
        comparator = EncounterComparator(self.labevent_distribution)
        for encounter_a in self.similarity_encounters:
            for encounter_b in self.similarity_encounters:
                similarity = comparator.compare(encounter_a, encounter_b)
                if encounter_a.hadm_id == encounter_b.hadm_id:
                    continue
                result.append(
                    {
                        "encounter_a": encounter_a.hadm_id,
                        "encounter_b": encounter_b.hadm_id,
                        "similarity": similarity,
                    }
                )
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

    @property
    def hadm_ids(self):
        return [p.hadm_id for p in self.participants]

    @property
    def subject_ids(self):
        return [p.subject_id for p in self.participants]


class EncounterComparator:
    def __init__(
        self,
        labevent_distributions: LabEventDistributions = None,
        scale_labevents_by_distribution: bool = True,
    ):
        self.labevent_distributions = labevent_distributions
        if self.labevent_distributions is None:
            print("No labevent distributions provided. Can not compare labevents.")
        self.scale_by_distribution = scale_labevents_by_distribution

    def compare(
        self,
        encounter_a: SimilarityEncounter,
        encounter_b: SimilarityEncounter,
        demographics_weight: float = 0.2,
        diagnoses_weight: float = 0.4,
        labevents_weight: float = 0.4,
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
        result = (
            demographics_sim * demographics_weight
            + diagnoses_sim * diagnoses_weight
            + labevents_sim * labevents_weight
        )
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
        comparator = LabEventComparator(self.labevent_distributions)
        return comparator.compare(
            labevents_a, labevents_b, scale_by_distribution=self.scale_by_distribution
        )
