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
                labevent_set_a=vitalsign_a,
                labevent_set_b=vitalsign_b,
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
        scale_by_percentile: bool = False,
    ):
        mean, std = self.db.get_labevent_mean_std_for_itemid(vitalsign_a.item_id)

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
        if scale_by_percentile:
            mean_percentile = (z_a + z_b) / 2
            similarity *= 2 * abs(mean_percentile - 0.5)
        return similarity

    def _compare_pair(
        self,
        vitalsign_a: Vitalsign,
        vitalsign_b: Vitalsign,
        scale_by_distribution: bool = False,
    ) -> float:
        itemid_a = vitalsign_a.item_id
        itemid_b = vitalsign_b.item_id

        value_a = vitalsign_a.value
        value_b = vitalsign_b.value

        if not value_is_valid(value_a):
            return None
        elif not value_is_valid(value_b):
            return None

        if itemid_a != itemid_b:
            return None

        similarity = self._calculate_numeric_similarity(
            vitalsign_a=vitalsign_a,
            vitalsign_b=vitalsign_b,
            scale_by_percentile=scale_by_distribution,
        )

        return similarity

    def _get_mean_labevents(self, labevents: list[Vitalsign]) -> list[Vitalsign]:
        item_ids = set([o.item_id for o in labevents])
        mean_labevents = []
        for item_id in item_ids:
            values = get_valid_values_from_labevents_for_itemid(
                labevents=labevents, item_id=item_id
            )
            if len(values) == 0:
                continue
            elif len(values) == 1:
                mean_value = values[0]
            else:
                mean_value = statistics.mean(values)
            mean_labevent = Vitalsign(
                labevent_id=0,  # dummy
                subject_id=0,  # dummy
                item_id=item_id,
                value=mean_value,
                valueuom=labevents[0].valueuom,
            )
            mean_labevents.append(mean_labevent)
        return mean_labevents

    def _compare_set(
        self,
        labevent_set_a: list[Vitalsign],
        labevent_set_b: list[Vitalsign],
        scale_by_distribution: bool = False,
        aggregation: str = "mean",
    ) -> float:
        mean_labevents_a = self._get_mean_labevents(labevent_set_a)
        mean_labevents_b = self._get_mean_labevents(labevent_set_b)
        similarities = []
        for labevent_a in mean_labevents_a:
            for labevent_b in mean_labevents_b:
                if labevent_a.item_id == labevent_b.item_id:
                    similarity = self._compare_pair(
                        vitalsign_a=labevent_a,
                        vitalsign_b=labevent_b,
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
