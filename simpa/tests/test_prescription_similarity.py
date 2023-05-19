import sys

import pytest

from simpa.src.schemas import Prescription
from simpa.src.prescriptions import PrescriptionComparator


@pytest.fixture
def presc_a():
    return Prescription(
        value=1,
        drug="drug",
        subject_id=1,
        hadm_id=1,
        gsn="gsn",
        pharmacy_id="pharmacy_id",
    )


def test_equal_prescription_pair(presc_a):
    comp = PrescriptionComparator()
    assert comp.compare(prescription_a=presc_a, prescription_b=presc_a) == 1.0


def test_equal_prescriptions(presc_a):
    comp = PrescriptionComparator()
    assert comp.compare(prescription_a=[presc_a], prescription_b=[presc_a]) == 1.0
