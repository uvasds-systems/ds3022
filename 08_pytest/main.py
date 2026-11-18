import math


def std_dev(values):
    """Return the population standard deviation of a list of numbers."""
    if not values:
        raise ValueError("values must not be empty")

    mean = sum(values) / len(values)
    variance = sum((x - mean) ** 2 for x in values) / len(values)
    return math.sqrt(variance)


if __name__ == "__main__":
    data = [2, 4, 4, 4, 5, 5, 7, 9]
    print(f"Standard deviation of {data}: {std_dev(data)}")
