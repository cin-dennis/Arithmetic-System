from fastapi import APIRouter, Query, HTTPException
from ..core.evaluator import evaluate_expression

router = APIRouter()

@router.get("/evaluate")
def evaluate(expression: str = Query(..., description="Arithmetic expression to evaluate")):
    try:
        result = evaluate_expression(expression)
        return {"result": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
