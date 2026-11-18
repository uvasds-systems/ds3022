# pytest demo

A minimal example of unit testing in Python with [pytest](https://docs.pytest.org/).

## Files

- `main.py` — defines `std_dev(values)`, which returns the population standard deviation of a list of numbers.
- `test_main.py` — pytest tests for `std_dev`.

## Setup

This project uses [pipenv](https://pipenv.pypa.io/). From the project folder:

```bash
pipenv install
```

This creates a virtual environment and installs pytest.

If you don't use pipenv, install pytest with pip instead:

```bash
pip install -r requirements.txt
```

## Running the tests

```bash
pipenv run pytest
```

Or enter the environment first, then run pytest directly:

```bash
pipenv shell
pytest
```

Useful options:

| Command | What it does |
|---|---|
| `pytest -v` | Verbose: lists each test and whether it passed |
| `pytest test_main.py::test_known_value` | Runs a single test |
| `pytest -k approx` | Runs tests whose names contain "approx" |
| `pytest -x` | Stops at the first failure |

## How pytest finds tests

pytest automatically collects:

- files named `test_*.py` or `*_test.py`
- functions inside them whose names start with `test_`

No extra registration is needed — name things this way and pytest runs them.

## What a passing run looks like

```
test_main.py::test_known_value PASSED
test_main.py::test_identical_values_have_zero_std_dev PASSED
...
============================== 8 passed in 0.03s ===============================
```

## Seeing a failure

To see what a failing test looks like, change the expected value in `test_known_value` from `2.0` to `3.0` and run `pytest` again. pytest shows the assertion that failed and the actual vs. expected values.

## pytest features used in `test_main.py`

- **Plain `assert`** — pytest uses Python's built-in `assert`; no special assertion methods required.
- **`pytest.approx`** — compares floating-point numbers within a tolerance, since floats are rarely exactly equal.
- **`pytest.raises`** — checks that code raises a specific exception.
- **`@pytest.mark.parametrize`** — runs the same test with several sets of inputs.
