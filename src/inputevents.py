from typing import Union

from base_comparator import BaseComparator
from db import PostgresDB
from schemas import InputEvent


class InputEventComparator(BaseComparator):
    def __init__(self, db: PostgresDB):
        self.db = db

    def compare(
        self,
        inputevent_a: Union[list[InputEvent], InputEvent],
        inputevent_b: Union[list[InputEvent], InputEvent],
    ):
        result = None
        if isinstance(inputevent_a, list) or isinstance(inputevent_b, list):
            result = self._compare_set(
                inputevent_set_a=inputevent_a,
                inputevent_set_b=inputevent_b,
            )
        else:
            result = self._compare_pair(
                inputevent_a=inputevent_a,
                inputevent_b=inputevent_b,
            )
        return result

    def _compare_pair(
        self,
        inputevent_a: InputEvent,
        inputevent_b: InputEvent,
    ) -> float:
        return int(inputevent_a.value == inputevent_b.value)

    def _compare_set(
        self,
        inputevent_set_a: list[InputEvent],
        inputevent_set_b: list[InputEvent],
    ) -> float:
        set_a = set([i.item_id for i in inputevent_set_a])
        set_b = set([i.item_id for i in inputevent_set_b])
        intersection = set_a.intersection(set_b)
        union = set_a.union(set_b)
        return len(intersection) / len(union)
