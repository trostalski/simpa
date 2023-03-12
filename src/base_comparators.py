from abc import ABC, abstractmethod
import statistics
from typing import Callable
from scipy.stats import norm

from db import PostgresDB
from schemas import DistributionCategory


class BaseComparator(ABC):
    def compare(self, a, b, *args, **kwargs):
        if isinstance(a, list) or isinstance(b, list):
            return self._compare_set(a, b, *args, **kwargs)
        else:
            return self._compare_pair(a, b, *args, **kwargs)

    @abstractmethod
    def _compare_pair(self, a, b):
        ...

    @abstractmethod
    def _compare_set(self) -> float:
        ...


def value_is_valid(value: str) -> bool:
    result = True
    if value in ["___", None]:
        result = False
    return result


class DistributionComparator(BaseComparator):
    def __init__(self, db: PostgresDB):
        self.db = db

    def compare(
        self, a: DistributionCategory, b: DistributionCategory, *args, **kwargs
    ):
        return super().compare(a, b, *args, **kwargs)

    def _compare_pair(
        self,
        a: DistributionCategory,
        b: DistributionCategory,
        get_mean_std_func: Callable,
        scale_by_distribution: bool = True,
    ):
        if a.id != b.id:
            return None

        if not value_is_valid(value_a):
            return None
        elif not value_is_valid(value_b):
            return None

        mean, std = get_mean_std_func(a)

        if mean is None or std is None:
            return None

        value_a = float(a.value)
        value_b = float(b.value)

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

    def _compare_set(
        self,
        set_a: list[DistributionCategory],
        set_b: list[DistributionCategory],
        scale_by_distribution: bool,
        aggregation: str = "mean",
    ) -> float:
        similarities = []
        for a in set_a:
            for b in set_b:
                if a.id == b.id:
                    similarity = self._compare_pair(
                        a=a,
                        b=b,
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


class OntologyComparator(BaseComparator):
    pass


class BinaryComparator(BaseComparator):
    pass
