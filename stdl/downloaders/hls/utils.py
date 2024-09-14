from dataclasses import dataclass
from typing import List, TypeVar

T = TypeVar('T')


def get_ext(path: str) -> str:
    return path.split('.')[-1]


def merge_intersected_strings(str1: str, str2: str) -> str:
    for i in range(len(str1)):
        if str2.startswith(str1[i:]):
            return str1[:i] + str2
    return str1 + str2


@dataclass
class ValueWithIdx[T]:
    idx: int
    value: T


def sub_lists_with_idx(origin: List[T], n: int) -> List[List[ValueWithIdx[T]]]:
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
