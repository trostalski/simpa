import statistics
from typing import Union, Optional
from datetime import datetime
from pydantic import BaseModel
import statistics
from scipy.stats import norm

from base_comparator import BaseComparator


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


def value_is_valid(value: str) -> bool:
    result = True
    if value in ["___", None]:
        result = False
    return result


def value_is_numeric(value: str) -> bool:
    result = True
    try:
        float(value)
    except ValueError:
        result = False
    return result


def get_valid_values_from_labevents_for_itemid(labevents: list[LabEvent], item_id: int):
    values = []
    for o in labevents:
        if (
            o.item_id == item_id
            and value_is_valid(o.value)
            and value_is_numeric(o.value)
        ):
            values.append(float(o.value))
    return values


class LabEventDistributions:
    def __init__(self, labevents: list[LabEvent]):
        self.labevents = labevents

    def get_mean_std_for_item_id(self, item_id: int):
        values = get_valid_values_from_labevents_for_itemid(
            labevents=self.labevents, item_id=item_id
        )
        units = [o.valueuom for o in self.labevents if o.item_id == item_id]
        # if not len(set(units)) == 1:
        #     print(f"Multiple units found for item_id: {item_id}")
        if len(values) > 0:
            mean = statistics.mean(values)
            std = statistics.stdev(values)
            return mean, std
        else:
            return None, None

    def get_value_freq_for_labevent(self, labevent: LabEvent):
        values = [o.value for o in self.labevents if o.item_id == labevent.item_id]
        count = values.count(labevent.value)
        freq = count / len(values)
        return freq


class LabEventComparator(BaseComparator):
    def __init__(self, distributions: LabEventDistributions):
        self.distributions = distributions

    def compare(
        self,
        labevents_a: Union[list[LabEvent], LabEvent],
        labevents_b: Union[list[LabEvent], LabEvent],
        scale_by_distribution: bool = False,
    ):
        if isinstance(labevents_a, list) or isinstance(labevents_b, list):
            return self._compare_set(
                labevent_set_a=labevents_a,
                labevent_set_b=labevents_b,
                scale_by_distribution=scale_by_distribution,
            )
        else:
            return self._compare_pair(
                labevent_a=labevents_a,
                labevent_b=labevents_b,
                scale_by_distribution=scale_by_distribution,
            )

    def _calculate_numeric_similarity(
        self,
        labevent_a: LabEvent,
        labevent_b: LabEvent,
        scale_by_percentile: bool = False,
    ):
        mean, std = self.distributions.get_mean_std_for_item_id(labevent_a.item_id)

        if mean is None or std is None:
            return None

        value_a = float(labevent_a.value)
        value_b = float(labevent_b.value)

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

    def _calculate_categorical_similarity(
        self,
        labevent_a: LabEvent,
        labevent_b: LabEvent,
        scale_by_freq: bool = False,
    ):
        if labevent_a.value == labevent_b.value:
            similarity = 1
        else:
            similarity = 0
        if scale_by_freq:
            value_freq = self.distributions.get_value_freq_for_labevent(labevent_a)
            similarity = similarity * (1 - value_freq)
        return similarity

    def _compare_pair(
        self,
        labevent_a: LabEvent,
        labevent_b: LabEvent,
        scale_by_distribution: bool = False,
    ) -> float:
        itemid_a = labevent_a.item_id
        itemid_b = labevent_b.item_id

        value_a = labevent_a.value
        value_b = labevent_b.value

        if not value_is_valid(value_a):
            return None
        elif not value_is_valid(value_b):
            return None
        if itemid_a != itemid_b:
            return None

        if value_is_numeric(value_a):
            similarity = self._calculate_numeric_similarity(
                labevent_a=labevent_a,
                labevent_b=labevent_b,
                scale_by_percentile=scale_by_distribution,
            )
        else:
            similarity = self._calculate_categorical_similarity(
                labevent_a=labevent_a,
                labevent_b=labevent_b,
                scale_by_freq=scale_by_distribution,
            )
        return similarity

    def _get_mean_labevents(self, labevents: list[LabEvent]) -> list[LabEvent]:
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
            mean_labevent = LabEvent(
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
        labevent_set_a: list[LabEvent],
        labevent_set_b: list[LabEvent],
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
                        labevent_a=labevent_a,
                        labevent_b=labevent_b,
                        scale_by_distribution=scale_by_distribution,
                    )
                    if similarity is not None:
                        similarities.append(similarity)
        if aggregation == "mean":
            similarity = statistics.mean(similarities)
        return similarity
