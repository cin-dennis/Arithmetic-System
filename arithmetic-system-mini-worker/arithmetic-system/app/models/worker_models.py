from pydantic import BaseModel
from typing import List

class ArithmeticInput(BaseModel):
    x: float
    y: float

class ArithmeticResult(BaseModel):
    value: float

class AggregatorInput(BaseModel):
    values: List[float]