from typing import Any, cast

from prefect import flow, task


@task
def printer(obj: Any) -> None:
    print(f"Received a {type(obj)} with value {obj}")


# Note that we define the flow with type hints
@flow
def validation_flow(x: int, y: str) -> None:
    printer(x)
    printer(y)


raw_x: Any = "42"
raw_y: Any = 100

cast(Any, validation_flow)(x=raw_x, y=raw_y)
