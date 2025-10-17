from mini.models import BaseModel

class NumberInput(BaseModel):
    value: int | float

class BinaryOperationInput(BaseModel):
    x: int | float
    y: int | float

class UnaryOperationInput(BaseModel):
    value: int | float

class ArrayInput(BaseModel):
    values: list[int | float]

class AggregateInput(BaseModel):
    values: list[int | float] | None = None
    children_result: list[int | float] | None = None
    input: str | None = None

    @property
    def numbers(self) -> list[int | float]:
        if self.values is not None:
            return self.values
        if self.children_result is not None:
            return self.children_result
        raise ValueError("Either 'values' or 'children_result' must be provided")

class ChordCallbackInput(BaseModel):
    children_result: list[int | float] | None = None
    input: str | None = None

class NumberOutput(BaseModel):
    result: int | float

class ArrayOutput(BaseModel):
    results: list[float]

class ChainLinkInput(BaseModel):
    is_left_fixed: bool = False
    result: int | float | None = None
    next_operand: int | float

