import logging
from typing import Dict, Any
import asyncio
from ..config import BROKER, RESULT_BACKEND
from .expression_parser import ExpressionParser
from .workflow_builder import WorkflowBuilder

logger = logging.getLogger(__name__)

class WorkflowOrchestrator:
    def __init__(self):
        self.parser = ExpressionParser()
        self.builder = WorkflowBuilder()

    async def calculate(self, expression: str) -> Dict[str, Any]:
        try:
            parsed = self.parser.parse(expression)
            logger.info(f"Parsed Expression Tree: {parsed.expression_tree}")
            workflow = self.builder.build(parsed.expression_tree)
            logger.info(f"Built Workflow: {workflow}")
            if isinstance(workflow, float):
                final_result = workflow
            else:
                final_result = await self._execute_workflow(workflow)
            logger.info(f"Final Result: {final_result}")

            return {
                "result": final_result,
                "original_expression": expression,
            }
        except Exception as e:
            logger.error(f"Error while calculating '{expression}': {str(e)}", exc_info=True)
            raise ValueError(f"Cannot calculate expression: {expression}. Error: {str(e)}")

    async def _execute_workflow(self, workflow):
        await workflow.create(result_backend=RESULT_BACKEND)
        await workflow.start(broker=BROKER)

        logger.info(f"Started workflow with ID: {workflow.id}")
        logger.info("Waiting for workflow to complete...")
        result_node = await RESULT_BACKEND.get_result(workflow.id)
        logger.info(f"Result Node: {result_node}")
        while result_node is None:
            await asyncio.sleep(0.1)
            logger.info(f"Waiting for workflow {workflow.id} to complete...")
            result_node = await RESULT_BACKEND.get_result(workflow.id)

        logger.info(f"Finished workflow with ID: {workflow.id}")
        return result_node.result_obj.get('result')