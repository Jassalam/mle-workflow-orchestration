import time
from typing import Any, cast

from prefect import flow, task
from prefect.task_runners import ThreadPoolTaskRunner


@task
def print_values(values: list[str], start_after: float = 0.0) -> None:
    time.sleep(start_after)
    for value in values:
        time.sleep(1)
        print(value, end="\r")


@flow(
    name="My Example Flow",
    task_runner=cast(Any, ThreadPoolTaskRunner()),
)
def my_flow() -> None:
    print_values.submit(["AAAA"] * 10)
    print_values.submit(["BBBB"] * 5, start_after=0.5)


if __name__ == "__main__":
    my_flow()
