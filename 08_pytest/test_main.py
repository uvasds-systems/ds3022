import pytest

from main import std_dev


def test_known_value():
    # Classic textbook example: mean is 5, std dev is exactly 2
    assert std_dev([2, 4, 4, 4, 5, 5, 7, 9]) == 2.0


def test_identical_values_have_zero_std_dev():
    assert std_dev([3, 3, 3, 3]) == 0.0


def test_single_value():
    assert std_dev([42]) == 0.0


def test_floats_use_approx():
    # Floating-point results should be compared with pytest.approx
    assert std_dev([1.5, 2.5, 3.5]) == pytest.approx(0.8165, rel=1e-3)


def test_empty_list_raises():
    with pytest.raises(ValueError):
        std_dev([])


@pytest.mark.parametrize(
    "values, expected",
    [
        ([1, 2, 3, 4, 5], 1.4142),
        ([10, 20], 5.0),
        ([-1, 1], 1.0),
    ],
)
def test_parametrized(values, expected):
    assert std_dev(values) == pytest.approx(expected, rel=1e-3)
