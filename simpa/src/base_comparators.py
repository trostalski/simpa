from abc import ABC, abstractmethod
import statistics
from scipy.stats import norm

from simpa.src.schemas import DistributionCategory


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


def items_have_mean_and_std(a: DistributionCategory, b: DistributionCategory):
    return (
        a.id_mean is not None
        and a.id_std_dev is not None
        and b.id_mean is not None
        and b.id_std_dev is not None
    )


class DistributionComparator(BaseComparator):
    def __init__(self):
        pass

    def compare(
        self, a: DistributionCategory, b: DistributionCategory, *args, **kwargs
    ):
        return super().compare(a, b, *args, **kwargs)

    def _compare_pair(
        self,
        a: DistributionCategory,
        b: DistributionCategory,
        scale_by_distribution: bool = True,
    ):
        if a.hadm_id == b.hadm_id:
            return 1.0

        if a.id_mean is None or a.id_std_dev is None:
            return None
        if a.id != b.id:
            return None

        if not value_is_valid(a.value):
            return None
        elif not value_is_valid(b.value):
            return None

        if not a.abnormal and not b.abnormal:
            return None

        mean = float(a.id_mean)
        std = float(a.id_std_dev)

        if std == 0 or std is None:
            return None

        value_a = float(a.value)
        value_b = float(b.value)

        p_a = norm.cdf((value_a - mean) / std)
        p_b = norm.cdf((value_b - mean) / std)

        similarity = 1 - abs(p_a - p_b)

        if scale_by_distribution:
            mean_percentile = (p_a + p_b) / 2
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


class BinaryComparator(BaseComparator):
    def __init__(self):
        pass

    def compare(self, a, b, *args, **kwargs):
        return super().compare(a, b, *args, **kwargs)

    def _compare_pair(self, a, b):
        return int(a.value == b.value)

    def _compare_set(self, set_a, set_b):
        set_a = set([i.value for i in set_a])
        set_b = set([i.value for i in set_b])
        intersection = set_a.intersection(set_b)
        union = set_a.union(set_b)
        return len(intersection) / len(union)
