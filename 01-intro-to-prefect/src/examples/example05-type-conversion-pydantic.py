from typing import Any, cast

from prefect import flow, task
from pydantic import BaseModel, ConfigDict


class Model(BaseModel):
    model_config = ConfigDict(coerce_numbers_to_str=True)
    a: int
    b: str
    c: float


@task
def printer(obj: Any) -> None:
    print(f"Received a {type(obj)} with value {obj}")


@flow
def model_validator(model: Model) -> None:
    printer(model)


raw_model: Any = {"a": 42, "b": 0, "c": 55}

cast(Any, model_validator)(raw_model)
