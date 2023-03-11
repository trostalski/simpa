from typing import Union
import statistics

from schemas import Demographics


class DemographicsComparator:
    def __init__(self):
        pass

    def compare(
        self,
        demographics_a: Union[list[Demographics], Demographics],
        demographics_b: Union[list[Demographics], Demographics],
    ) -> float:
        age_similarity = self._compare_age(demographics_a, demographics_b)
        gender_similarity = self._compare_gender(demographics_a, demographics_b)
        return statistics.mean([age_similarity, gender_similarity])

    def _compare_age(
        self, demographics_a: Demographics, demographics_b: Demographics
    ) -> float:
        return 1 - abs(demographics_a.age - demographics_b.age) / max(
            demographics_a.age, demographics_b.age
        )

    def _compare_height(
        self, demographics_a: Demographics, demographics_b: Demographics
    ) -> float:
        return 1 - abs(demographics_a.height - demographics_b.height) / max(
            demographics_a.height, demographics_b.height
        )

    def _compare_gender(
        self, demographics_a: Demographics, demographics_b: Demographics
    ) -> float:
        return int(demographics_a.gender == demographics_b.gender)

    def _compare_ethnicity(
        self, demographics_a: Demographics, demographics_b: Demographics
    ) -> float:
        return int(demographics_a.ethnicity == demographics_b.ethnicity)
