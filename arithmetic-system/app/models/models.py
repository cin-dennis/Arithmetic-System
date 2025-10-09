from pydantic import BaseModel, Field
from typing import Optional, Any, List
from enum import Enum


class ExpressionTypeEnum(str, Enum):
    SIMPLE = "simple"
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    HYBRID = "hybrid"

class CalculateExpressionResponse(BaseModel):
    result: float = Field(..., description="Calculation result")
    expression_type: ExpressionTypeEnum = Field(..., description="Type of processing used")
    original_expression: str = Field(..., description="Original expression provided")
    parallel_groups_count: Optional[int] = Field(None, description="Number of parallel groups processed")
    sequential_chains_count: Optional[int] = Field(None, description="Number of sequential chains processed")

class ErrorResponse(BaseModel):
    error: str = Field(..., description="Error message")
    detail: Optional[str] = Field(None, description="Detailed error information")