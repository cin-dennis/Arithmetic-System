from celery.result import AsyncResult
from typing import Dict, Any
import logging

from .add_service import add as add_task
from .mul_service import multiply as mul_task
from .div_service import divide as div_task
from .sub_service import subtract as sub_task

from .expression_parser import (
    ExpressionParser, ExpressionType, Operations
)
from .expression_analyzer import ExpressionAnalyzer
from .task_executor import TaskExecutor

logger = logging.getLogger(__name__)


class WorkflowOrchestrator:
    def __init__(self):
        self.task_map = {
            Operations.ADD: add_task,
            Operations.SUB: sub_task,
            Operations.MUL: mul_task,
            Operations.DIV: div_task
        }
        self.parser = ExpressionParser()
        self.analyzer = ExpressionAnalyzer(self.task_map)
        self.executor = TaskExecutor(self.task_map)

    def calculate(self, expression: str) -> Dict[str, Any]:
        try:
            logger.info(f"=== Starting calculation for expression: '{expression}' ===")

            logger.info("Step 1: Parsing expression...")
            parsed = self.parser.parse(expression)

            logger.info("\n\n\n\n")
            logger.info("Step 2: Analyzing expression structure...")
            parsed = self.analyzer.determine_expression_type(parsed)
            logger.info(f"Expression analysis complete - Final type: {parsed.expression_type}")

            logger.info("\n\n\n\n")
            logger.info("Step 3: Selecting execution strategy...")
            if parsed.expression_type == ExpressionType.SIMPLE:
                celery_result = self.executor.execute_simple(parsed)
            elif parsed.expression_type == ExpressionType.SEQUENTIAL:
                celery_result = self.executor.execute_sequential(parsed)
            elif parsed.expression_type == ExpressionType.PARALLEL:
                parallel_tasks = self.analyzer.extract_parallel_tasks_from_expression(parsed.expression_tree)
                logger.info(f"Parallel tasks extracted: {parallel_tasks}")
                celery_result = self.executor.execute_parallel(parsed, parallel_tasks)
            elif parsed.expression_type == ExpressionType.HYBRID:
                parallel_tasks = self.analyzer.extract_parallel_tasks_from_expression(parsed.expression_tree)
                logger.info(f"Parallel tasks extracted for hybrid: {parallel_tasks}")
                final_operation, final_operand = self.analyzer.extract_final_operation(parsed.expression_tree)
                celery_result = self.executor.execute_hybrid(parsed, parallel_tasks, final_operation, final_operand)
            else:
                raise ValueError(f"Unsupported expression type: {parsed.expression_type}")

            logger.info("\n\n\n\n")
            final_result = celery_result.get(timeout=30)

            logger.info("\n\n\n\n")
            logger.info(f"Step 4: Calculation complete! Final result: {final_result}")

            return {
                "result": final_result,
                "expression_type": parsed.expression_type.value,
                "original_expression": expression,
            }

        except Exception as e:
            logger.error(f"Error in calculate_sync for '{expression}': {str(e)}")
            raise ValueError(f"Failed to process expression: {str(e)}")

    # Expression analysis methods have been moved to ExpressionAnalyzer class
    # Task execution methods have been moved to TaskExecutor class

    # Expression analysis methods have been moved to ExpressionAnalyzer class

    def get_result(self, task_id: str) -> Dict[str, Any]:
        try:
            result = AsyncResult(task_id)

            return {
                "task_id": task_id,
                "status": result.status,
                "result": result.result if result.ready() else None,
                "error": str(result.info) if result.failed() else None
            }
        except Exception as e:
            logger.error(f"Error getting result for task {task_id}: {str(e)}")
            return {
                "task_id": task_id,
                "status": "ERROR",
                "result": None,
                "error": str(e)
            }
