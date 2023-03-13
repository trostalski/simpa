def scale_to_range(input_list: list[int], range_start: int, range_end: int):
    min_value = min(input_list)
    max_value = max(input_list)
    return [
        ((value - min_value) / (max_value - min_value)) * (range_end - range_start)
        + range_start
        for value in input_list
    ]