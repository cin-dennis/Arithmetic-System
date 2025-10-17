from fastapi import APIRouter, Query, HTTPException
import logging
from ..services.orchestrator import WorkflowOrchestrator
from ..models.models import CalculateExpressionResponse

router = APIRouter()
logger = logging.getLogger(__name__)
orchestrator = WorkflowOrchestrator()

@router.get("/calculate", response_model=CalculateExpressionResponse)
async def evaluate(expression: str = Query(..., description="Arithmetic expression to evaluate")):
    try:
        logger.info(f"Received expression to evaluate: {expression}")
        result = await orchestrator.calculate(expression)
        logger.info(f"Calculation result: {result}")
        return CalculateExpressionResponse(**result)
    except Exception as e:
        logger.error(f"Exception occurred: {e}")
        raise HTTPException(status_code=400, detail=str(e))

