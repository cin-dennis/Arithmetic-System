from pydantic import BaseModel
from typing import List, Any


class CalculatorInput(BaseModel):
    current_value: float | None = None
    x: float
    y: float
    is_left_fixed: bool = False

class CalculatorOutput(BaseModel):
    result: float

class AggregatorInput(BaseModel):
    children_result: List[Any] | None = None
    constants: List[float] | None = None