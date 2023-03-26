def scale_to_range(input_list: list[int], range_start: int, range_end: int):
    min_value = min(input_list)
    max_value = max(input_list)
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
        yield iterable[ndx:min(ndx + n, l)]