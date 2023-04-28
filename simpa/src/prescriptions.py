from typing import Union

from simpa.src.base_comparators import BinaryComparator
from simpa.src.schemas import Prescription


class PrescriptionComparator(BinaryComparator):
    def __init__(self):
        pass

    def compare(
        self,
        prescription_a: Union[list[Prescription], Prescription],
        prescription_b: Union[list[Prescription], Prescription],
    ):
        return super().compare(prescription_a, prescription_b)
