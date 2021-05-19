import pytest

from app.weather import coord_validator


@pytest.mark.parametrize(
    ("value", "expected_result"),
    [
        (('', ''), False),
        (('32.359833', '-86.217384a'), False),
        (('38.97034343', '-89.12880845'), True),
        (('388.9703434', '-89.12880845'), False),
        (('42.071304', ''), False),
        ((0, 0), True),
        ((-90, 180), True),
    ],
)
def test_coord_validator(value: float, expected_result: bool):
    actual_result = coord_validator(value)

    assert actual_result == expected_result
