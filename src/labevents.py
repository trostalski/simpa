from typing import Union

from db import PostgresDB
from schemas import LabEvent
from base_comparators import DistributionComparator


class LabEventComparator(DistributionComparator):
    def __init__(self, db: PostgresDB):
        super().__init__(db=db)

    def compare(
        self,
        labevent_a: Union[list[LabEvent], LabEvent],
        labevent_b: Union[list[LabEvent], LabEvent],
        scale_by_distribution: bool = True,
    ):
        return super().compare(
            labevent_a,
            labevent_b,
            scale_by_distribution=scale_by_distribution,
            category="labevents",
        )
