from simpa.src.constants import VITALSIGN_NORM_RANGE


def scale_to_range(input_list: list[int], range_start: int, range_end: int):
    min_value = min(input_list)
    max_value = max(input_list)
    print(f"min: {min_value}, max: {max_value}")
    return [
        ((value - min_value) / (max_value - min_value)) * (range_end - range_start)
        + range_start
        for value in input_list
    ]

def psycop_to_asyncpg_string(string: str) -> str:
    count = 1
    new_strings = []
    for w in string.split():
        if w == "%s":
            new_strings.append(f"${count}")
            count += 1
        else:
            new_strings.append(w)
    return " ".join(new_strings)


def batch(iterable, n=1):
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx : min(ndx + n, l)]


def normalize_value(value: float, mean: float, std: float) -> float:
    return (value - mean) / std

def labevent_is_abnormal(
    valuenum: int, lower_ref: int, upper_ref: int, mean: float, std_dev: float
) -> bool:
    if upper_ref and lower_ref and valuenum:
        is_abnormal = valuenum < lower_ref or valuenum > upper_ref
    elif mean and std_dev and valuenum:
        is_abnormal = valuenum < mean - 2 * std_dev or valuenum > mean + 2 * std_dev
    else:
        is_abnormal = True
    return is_abnormal


def vitalsign_is_abnormal(value: int, name: str) -> bool:
    if value is None:
        return True
    else:
        return (
            value < VITALSIGN_NORM_RANGE[name]["lower"]
            or value > VITALSIGN_NORM_RANGE[name]["upper"]
        )
