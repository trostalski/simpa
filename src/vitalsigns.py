import statistics
from typing import Union
import statistics
from scipy.stats import norm

from base_comparator import BaseComparator
from db import PostgresDB
from schemas import Vitalsign
from labevents import value_is_valid


class VitalsignComparator(BaseComparator):
    def __init__(self, db: PostgresDB):
        self.db = db

    def compare(
        self,
        vitalsign_a: Union[list[Vitalsign], Vitalsign],
        vitalsign_b: Union[list[Vitalsign], Vitalsign],
        scale_by_distribution: bool = False,
    ):
        result = None
        if isinstance(vitalsign_a, list) or isinstance(vitalsign_b, list):
            result = self._compare_set(
                vitalsign_set_a=vitalsign_a,
                vitalsign_set_b=vitalsign_b,
                scale_by_distribution=scale_by_distribution,
            )
        else:
            result = self._compare_pair(
                vitalsign_a=vitalsign_a,
                vitalsign_b=vitalsign_b,
                scale_by_distribution=scale_by_distribution,
            )
        return result

    def _calculate_numeric_similarity(
        self,
        vitalsign_a: Vitalsign,
        vitalsign_b: Vitalsign,
        scale_by_distribution: bool = False,
    ):
        mean, std = self.db.get_vitalsign_mean_std_for_name(vitalsign_a.name)

        if mean is None or std is None:
            return None

        value_a = float(vitalsign_a.value)
        value_b = float(vitalsign_b.value)

        z_a = norm.cdf((value_a - mean) / std)
        z_b = norm.cdf((value_b - mean) / std)

        if z_a >= z_b:
            similarity = z_b / z_a
        else:
            similarity = z_a / z_b
        if scale_by_distribution:
            mean_percentile = (z_a + z_b) / 2
            similarity *= 2 * abs(mean_percentile - 0.5)
        return similarity

    def _compare_pair(
        self,
        vitalsign_a: Vitalsign,
        vitalsign_b: Vitalsign,
        scale_by_distribution: bool = False,
    ) -> float:
        name_a = vitalsign_a.name
        name_b = vitalsign_b.name

        value_a = vitalsign_a.value
        value_b = vitalsign_b.value

        if not value_is_valid(value_a):
            return None
        elif not value_is_valid(value_b):
            return None

        if name_a != name_b:
            return None

        similarity = self._calculate_numeric_similarity(
            vitalsign_a=vitalsign_a,
            vitalsign_b=vitalsign_b,
            scale_by_distribution=scale_by_distribution,
        )

        return similarity

    def _compare_set(
        self,
        vitalsign_set_a: list[Vitalsign],
        vitalsign_set_b: list[Vitalsign],
        scale_by_distribution: bool = False,
        aggregation: str = "mean",
    ) -> float:
        similarities = []
        for vitalsign_a in vitalsign_set_a:
            for vitalsign_b in vitalsign_set_b:
                if vitalsign_a.name == vitalsign_b.name:
                    similarity = self._compare_pair(
                        vitalsign_a=vitalsign_a,
                        vitalsign_b=vitalsign_b,
                        scale_by_distribution=scale_by_distribution,
                    )
                    if similarity is not None:
                        similarities.append(similarity)
        if aggregation == "mean":
            if len(similarities) == 0:
                similarity = 0
            elif len(similarities) == 1:
                similarity = similarities[0]
            else:
                similarity = statistics.mean(similarities)
        return similarity
