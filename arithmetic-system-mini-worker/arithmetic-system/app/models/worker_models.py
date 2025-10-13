from pydantic import BaseModel
from typing import List

class CalculatorInput(BaseModel):
    x: float
    y: float

class CalculatorOutput(BaseModel):
    value: float

class AggregatorInput(BaseModel):
    values: List[float]