import logging
from .expression_parser import ExpressionParser, OperationEnum
from .workflow_builder import WorkflowBuilder
from app.models.models import CalculateExpressionResponse

logger = logging.getLogger(__name__)


class WorkflowOrchestrator:
    def __init__(self):
        self.parser = ExpressionParser()
        self.builder = WorkflowBuilder()

    def calculate(self, expression: str) -> CalculateExpressionResponse:
        parsed = self.parser.parse(expression)
        workflow_async_result, workflow_str = self.builder.build(parsed)
        final_result = workflow_async_result.get(timeout=3)
        logging.info(f"Workflow String: {workflow_str}")
        logger.info(f"Final Result: {final_result}")

        return CalculateExpressionResponse(result=final_result, workflow=workflow_str)
