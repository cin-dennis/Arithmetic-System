import logging
from .expression_parser import ExpressionParser
from .workflow_builder import WorkflowBuilder
from app.models.models import CalculateExpressionResponse
from ..config import BROKER, RESULT_BACKEND
from mini.worker.result_backends import NodeResult
import asyncio

logger = logging.getLogger(__name__)


class WorkflowOrchestrator:
    def __init__(self):
        self.parser = ExpressionParser()
        self.builder = WorkflowBuilder()

    async def calculate(self, expression: str) -> CalculateExpressionResponse:
        parsed = self.parser.parse(expression)

        logger.info("Parsed expression: %s", parsed)

        workflow, workflow_str = self.builder.build(parsed)

        logger.info("Workflow is: %s", workflow_str)

        if isinstance(workflow, (int, float)):
            logger.info(f"Expression is a constant: {workflow}")
            return CalculateExpressionResponse(result=workflow, workflow=workflow_str)

        await workflow.create(RESULT_BACKEND)
        await workflow.start(BROKER)

        final_result = await self._wait_for_result(workflow.id)
        logging.info(f"Workflow String: {workflow_str}")

        return CalculateExpressionResponse(result=final_result, workflow=workflow_str)

    async def _wait_for_result(self, workflow_id: str, timeout: int = 10) -> int | float:
        logger.info(f"Waiting for result for workflow_id: {workflow_id}")

        for i in range(timeout * 2):
            result_node: NodeResult | None = await RESULT_BACKEND.get_result(workflow_id)

            if result_node:
                logger.info(f"Result found for {workflow_id}: {result_node.result}")
                final_value = result_node.result_obj
                if isinstance(final_value, dict) and 'result' in final_value:
                    return float(final_value['result'])

                return final_value

            logger.debug(f"Result for {workflow_id} not ready yet. Attempt {i + 1}/{timeout * 2}")
            await asyncio.sleep(0.1)

        raise TimeoutError(f"Workflow {workflow_id} timed out after {timeout} seconds.")
