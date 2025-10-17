from mini.models import BaseModel

class NumberInput(BaseModel):
    value: float

class BinaryOperationInput(BaseModel):
    a: float
    b: float

class UnaryOperationInput(BaseModel):
    value: float

class ArrayInput(BaseModel):
    values: list[float]

class AggregateInput(BaseModel):
    values: list[float] | None = None
    children_result: list[float] | None = None
    input: str | None = None

    @property
    def numbers(self) -> list[float]:
        if self.values is not None:
            return self.values
        if self.children_result is not None:
            return self.children_result
        raise ValueError("Either 'values' or 'children_result' must be provided")

class ChordCallbackInput(BaseModel):
    children_result: list[float]
    input: str | None = None

class NumberOutput(BaseModel):
    result: float

class ArrayOutput(BaseModel):
    results: list[float]

class ChainLinkInput(BaseModel):
    result: float
    next_operand: float

