from dataclasses import dataclass, asdict
from stdl.utils.dataclass import dataclass_from_dict


@dataclass
class Point:
    x: float
    y: float


@dataclass
class Line:
    a: Point
    b: Point


def test_dataclass():
    line = Line(Point(1, 2), Point(3, 4))
    assert line == dataclass_from_dict(Line, asdict(line))
