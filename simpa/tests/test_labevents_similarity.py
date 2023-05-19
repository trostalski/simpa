import sys

import pytest

from simpa.src.schemas import LabEvent
from simpa.src.labevents import LabEventComparator


@pytest.fixture
def labevent_a():
    return LabEvent(
        subject_id=1,
        hadm_id=1,
        itemid=1,
        id=1,
        id_mean=1,
        id_std_dev=1,
        value=1,
        valueuom="mmol/L",
        valuenum=1,
    )


def test_equal_labevents(labevent_a):
    comp = LabEventComparator()
    assert comp.compare(labevent_a=labevent_a, labevent_b=labevent_a) == 1.0


def test_labevents_with_different_itemid(labevent_a):
    comp = LabEventComparator()
    labevent_b = labevent_a.copy(deep=True)
    labevent_b.hadm_id = 2
    labevent_b.id = 2
    labevent_b.itemid = 2
    assert comp.compare(labevent_a=labevent_a, labevent_b=labevent_b) == None


def test_labevents_with_different_value(labevent_a):
    comp = LabEventComparator()
    labevent_b = labevent_a.copy(deep=True)
    labevent_b.hadm_id = 2
    labevent_b.value = 2
    assert comp.compare(labevent_a=labevent_a, labevent_b=labevent_b) < 1.0


def test_closer_to_mean_is_less_similar(labevent_a):
    comp = LabEventComparator()
    labevent_b = labevent_a.copy(deep=True)
    labevent_c = labevent_a.copy(deep=True)
    labevent_d = labevent_a.copy(deep=True)

    labevent_b.hadm_id = 2
    labevent_b.value = 1.5

    labevent_c.hadm_id = 3
    labevent_c.value = 3

    labevent_d.hadm_id = 4
    labevent_d.value = 2

    result_a = comp.compare(labevent_a=labevent_a, labevent_b=labevent_b)
    result_b = comp.compare(labevent_a=labevent_c, labevent_b=labevent_d)

    assert result_a < result_b
