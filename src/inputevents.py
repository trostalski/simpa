from typing import Union

from base_comparators import BinaryComparator
from db import PostgresDB
from schemas import InputEvent


class InputEventComparator(BinaryComparator):
    def __init__(self):
        pass

    def compare(
        self,
        inputevent_a: Union[list[InputEvent], InputEvent],
        inputevent_b: Union[list[InputEvent], InputEvent],
    ):
        return super().compare(inputevent_a, inputevent_b)
