from prefect import flow, task


@task
def task_a() -> dict:
    return {"value": 8}


@task
def task_b(data: dict) -> str:
    print(data)
    return "c" if data["value"] > 10 else "d"


@task
def task_c(data: dict) -> str:
    print(data)
    return "handled by C"


@task(name="task_d", tags=["task-d"], log_prints=True, timeout_seconds=30)
def task_d(data: dict) -> str:
    print("Handled by D")
    return "handled by D"


# log_prints=True routes print() output into Prefect's logger (inherited by tasks)
@flow(name="logic-flow", log_prints=True)
def pipeline():
    data = task_a()
    route = task_b(data)

    # Branch to a different task based on task_b's result
    match route:
        case "c":
            result = task_c(data)
        case "d":
            result = task_d(data)
        case _:
            raise ValueError(f"Unknown route: {route}")

    print(f"Result: {result}")
    return result


if __name__ == "__main__":
    pipeline()
