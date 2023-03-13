from typing import Union

from db import PostgresDB
from schemas import Vitalsign
from base_comparators import DistributionComparator


class VitalsignComparator(DistributionComparator):
    def __init__(self, db: PostgresDB):
        super().__init__(db=db)

    def compare(
        self,
        vitalsign_a: Union[list[Vitalsign], Vitalsign],
        vitalsign_b: Union[list[Vitalsign], Vitalsign],
        scale_by_distribution: bool = True,
    ):
        return super().compare(
            vitalsign_a,
            vitalsign_b,
            scale_by_distribution=scale_by_distribution,
        )
