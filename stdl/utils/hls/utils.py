from dataclasses import dataclass


def get_ext(file_path: str) -> str | None:
    chunks = file_path.split(".")
    if len(chunks) == 1:
        return None
    if chunks[-1] == "":
        raise ValueError("Invalid file path")
    return chunks[-1]


def merge_intersected_strings(str1: str, str2: str) -> str:
    for i in range(len(str1)):
        if str2.startswith(str1[i:]):
            return str1[:i] + str2
    return str1 + str2


@dataclass
class ValueWithIdx:
    idx: int
    value: str


def sub_lists_with_idx(origin: list[str], n: int) -> list[list[ValueWithIdx]]:
    copy = origin[:]
    result = []
    cnt = 0

    while len(copy) != 0:
        sub = []
        for _ in range(n):
            if not copy:
                break
            elem = copy.pop(0)
            sub.append(ValueWithIdx(idx=cnt, value=elem))
            cnt += 1
        result.append(sub)

    return result
