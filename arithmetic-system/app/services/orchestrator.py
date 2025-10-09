from celery import chain, chord
from celery.result import AsyncResult
from typing import Dict, Any, List, Union, Tuple, Optional
import logging

from .add_service import add as add_task
from .mul_service import multiply as mul_task
from .div_service import divide as div_task
from .sub_service import subtract as sub_task
from .xsum_service import xsum as array_sum_task

from .expression_parser import (
    ExpressionParser, ParsedExpression, ExpressionType, ExpressionNode
)

logger = logging.getLogger(__name__)


class WorkflowOrchestrator:
    def __init__(self):
        self.parser = ExpressionParser()
        self.task_map = {
            'add': add_task,
            'sub': sub_task,
            'mul': mul_task,
            'div': div_task
        }

    def calculate(self, expression: str) -> Dict[str, Any]:
        try:
            parsed = self.parser.parse(expression)

            parsed = self._override_expression_type_if_needed(parsed)

            logger.info(f"Synchronous expression analysis: {expression}")
            logger.info(f"Type: {parsed.expression_type}, Parallel groups: {len(parsed.parallel_groups)}")
            logger.info(f"Sequential chains: {len(parsed.sequential_chains)}")

            if parsed.expression_type == ExpressionType.SIMPLE:
                celery_result = self._execute_simple(parsed)
            elif parsed.expression_type == ExpressionType.SEQUENTIAL:
                celery_result = self._execute_sequential(parsed)
            elif parsed.expression_type == ExpressionType.PARALLEL:
                celery_result = self._execute_parallel(parsed)
            elif parsed.expression_type == ExpressionType.HYBRID:
                celery_result = self._execute_hybrid(parsed)
            else:
                raise ValueError(f"Unsupported expression type: {parsed.expression_type}")

            logger.info(f"Waiting for calculation result for task: {celery_result.id}")
            final_result = celery_result.get(timeout=60)

            return {
                "result": final_result,
                "expression_type": parsed.expression_type.value,
                "original_expression": expression,
                "parallel_groups_count": self._get_actual_parallel_groups_count(parsed),
                "sequential_chains_count": len(parsed.sequential_chains)
            }

        except Exception as e:
            logger.error(f"Error in calculate_sync: {str(e)}")
            raise ValueError(f"Failed to process expression: {str(e)}")

    def _get_actual_parallel_groups_count(self, parsed: ParsedExpression) -> int:
        if parsed.expression_type == ExpressionType.PARALLEL:
            parallel_tasks = self._extract_parallel_tasks_from_expression(parsed.expression_tree)
            return len(parallel_tasks) if len(parallel_tasks) >= 2 else 0
        return len(parsed.parallel_groups)

    def _override_expression_type_if_needed(self, parsed: ParsedExpression) -> ParsedExpression:
        if not parsed.expression_tree:
            return parsed

        parallel_tasks = self._extract_parallel_tasks_from_expression(parsed.expression_tree)

        if (len(parallel_tasks) >= 2 and
                not self._is_purely_additive_expression(parsed.expression_tree)):
            parsed.expression_type = ExpressionType.HYBRID
            parsed.parallel_groups = [parallel_tasks]
            logger.info(
                f"Overriding expression type to HYBRID, found {len(parallel_tasks)} parallel tasks with sequential operations")

        elif (len(parallel_tasks) >= 2 and
              self._is_purely_additive_expression(parsed.expression_tree) and
              parsed.expression_tree.operation == 'add'):
            parsed.expression_type = ExpressionType.PARALLEL
            parsed.parallel_groups = [parallel_tasks]
            logger.info(f"Overriding expression type to PARALLEL, found {len(parallel_tasks)} parallel tasks")

        return parsed

    def _is_purely_additive_expression(self, tree: Union[ExpressionNode, float]) -> bool:
        if not isinstance(tree, ExpressionNode):
            return True

        if tree.operation != 'add':
            return False

        left_additive = True
        right_additive = True

        if isinstance(tree.left, ExpressionNode):
            left_additive = tree.left.operation == 'add' or isinstance(tree.left.left, (int, float))

        if isinstance(tree.right, ExpressionNode):
            right_additive = (isinstance(tree.right.left, (int, float)) and
                              isinstance(tree.right.right, (int, float)))

        return left_additive and right_additive

    def _execute_hybrid(self, parsed: ParsedExpression) -> AsyncResult:
        logger.info(
            f"Executing hybrid workflow with {len(parsed.parallel_groups)} parallel groups and {len(parsed.sequential_chains)} sequential chains")

        parallel_tasks = self._extract_parallel_tasks_from_expression(parsed.expression_tree)

        if len(parallel_tasks) >= 2:
            final_operation, final_operand = self._extract_final_operation(parsed.expression_tree)

            if final_operation and final_operand is not None:
                logger.info(
                    f"Hybrid pattern detected: {len(parallel_tasks)} parallel tasks followed by {final_operation} {final_operand}")

                parallel_chord = chord(parallel_tasks, array_sum_task.s())
                final_task = self.task_map[final_operation]

                workflow = chain(parallel_chord, final_task.s(final_operand))
                return workflow.apply_async()
            else:
                workflow = chord(parallel_tasks, array_sum_task.s())
                return workflow.apply_async()

        return self._execute_sequential(parsed)

    def _extract_final_operation(self, tree: Union[ExpressionNode, float]) -> Tuple[Optional[str], Optional[float]]:
        if not isinstance(tree, ExpressionNode):
            return None, None

        if tree.operation != 'add':
            if isinstance(tree.right, (int, float)):
                return tree.operation, float(tree.right)
            elif isinstance(tree.left, (int, float)):
                return tree.operation, float(tree.left)

        return None, None

    def _execute_advanced_parallel(self, parsed: ParsedExpression) -> AsyncResult:
        parallel_tasks = self._create_advanced_parallel_tasks(parsed)

        if len(parallel_tasks) < 2:
            return self._execute_sequential(parsed)

        logger.info(f"Executing advanced parallel chord with {len(parallel_tasks)} tasks")
        workflow = chord(parallel_tasks, array_sum_task.s())
        return workflow.apply_async()

    def _create_advanced_parallel_tasks(self, parsed: ParsedExpression) -> List:
        tasks = []

        if parsed.expression_tree and isinstance(parsed.expression_tree, ExpressionNode):
            tasks.extend(self._extract_parallel_tasks_from_tree(parsed.expression_tree))

        if not tasks and parsed.parallel_groups:
            for group in parsed.parallel_groups:
                for op in group:
                    if (isinstance(op.operand1, (int, float)) and
                            isinstance(op.operand2, (int, float)) and
                            op.can_parallelize):
                        task_func = self.task_map.get(op.operation)
                        if task_func:
                            tasks.append(task_func.s(op.operand1, op.operand2))

        return tasks

    def _extract_parallel_tasks_from_tree(self, tree: ExpressionNode) -> List:
        tasks = []

        if tree.is_parallel_group:
            # This node can be parallelized
            left_tasks = self._get_leaf_operations(tree.left)
            right_tasks = self._get_leaf_operations(tree.right)

            tasks.extend(left_tasks)
            tasks.extend(right_tasks)
        else:
            # Check children for parallel opportunities
            if isinstance(tree.left, ExpressionNode):
                tasks.extend(self._extract_parallel_tasks_from_tree(tree.left))
            if isinstance(tree.right, ExpressionNode):
                tasks.extend(self._extract_parallel_tasks_from_tree(tree.right))

        return tasks

    def _get_leaf_operations(self, node: Union[ExpressionNode, float]) -> List:
        tasks = []

        if isinstance(node, ExpressionNode):
            # If this is a simple operation with numeric operands
            if (isinstance(node.left, (int, float)) and
                    isinstance(node.right, (int, float))):
                task_func = self.task_map.get(node.operation)
                if task_func:
                    tasks.append(task_func.s(node.left, node.right))
            else:
                # Recursively get tasks from children
                tasks.extend(self._get_leaf_operations(node.left))
                tasks.extend(self._get_leaf_operations(node.right))

        return tasks

    def _flatten_expression_tree(self, tree: Union[ExpressionNode, float]) -> List[Dict[str, Any]]:
        if isinstance(tree, (int, float)):
            return []

        if not isinstance(tree, ExpressionNode):
            return []

        operations = []

        # Process left subtree first
        if isinstance(tree.left, ExpressionNode):
            left_ops = self._flatten_expression_tree(tree.left)
            operations.extend(left_ops)

        # Process right subtree
        if isinstance(tree.right, ExpressionNode):
            right_ops = self._flatten_expression_tree(tree.right)
            operations.extend(right_ops)

        # Add current operation
        if isinstance(tree.left, (int, float)) and isinstance(tree.right, (int, float)):
            # Both operands are numbers - this is a leaf operation
            operations.append({
                'operation': tree.operation,
                'operand1': tree.left,
                'operand2': tree.right
            })
        elif isinstance(tree.left, ExpressionNode) and isinstance(tree.right, (int, float)):
            # Left is complex, right is number - use previous result as left operand
            operations.append({
                'operation': tree.operation,
                'operand1': 'previous_result',
                'operand2': tree.right
            })
        elif isinstance(tree.left, (int, float)) and isinstance(tree.right, ExpressionNode):
            # Left is number, right is complex - this is unusual but handle it
            operations.append({
                'operation': tree.operation,
                'operand1': tree.left,
                'operand2': 'previous_result'
            })
        else:
            # Both are complex - this should be handled by the recursive calls above
            # Just add the operation with previous result logic
            operations.append({
                'operation': tree.operation,
                'operand1': 'previous_result',
                'operand2': 'previous_result'
            })

        return operations

    def _execute_simple(self, parsed: ParsedExpression) -> AsyncResult:
        if not parsed.operations:
            raise ValueError("No operations found in simple expression")

        op = parsed.operations[0]
        task_func = self.task_map.get(op.operation)

        if not task_func:
            raise ValueError(f"Unsupported operation: {op.operation}")

        logger.info(f"Executing simple: {op.operation}({op.operand1}, {op.operand2})")
        return task_func.delay(op.operand1, op.operand2)

    def _execute_sequential(self, parsed: ParsedExpression) -> AsyncResult:
        if not parsed.expression_tree:
            return self._execute_simple(parsed)

        # Convert expression tree to sequential operations
        sequential_ops = self._flatten_expression_tree(parsed.expression_tree)

        if len(sequential_ops) < 2:
            return self._execute_simple(parsed)

        # Build chain of operations
        tasks = []

        # First operation - should have two concrete operands
        first_op = sequential_ops[0]
        first_task = self.task_map[first_op['operation']]
        tasks.append(first_task.s(first_op['operand1'], first_op['operand2']))

        # Subsequent operations (use previous result)
        for op in sequential_ops[1:]:
            task_func = self.task_map[op['operation']]
            # In a chain, the previous result is automatically passed as the first argument
            # Only pass the second operand if it's not 'previous_result'
            if op['operand2'] != 'previous_result':
                tasks.append(task_func.s(op['operand2']))
            else:
                # This means we need both operands from previous results - not supported in simple chain
                # Fall back to using the operand1 if it's numeric
                if isinstance(op.get('operand1'), (int, float)):
                    tasks.append(task_func.s(op['operand1']))
                else:
                    # Complex case - might need different handling
                    tasks.append(task_func.s(0))  # Fallback

        logger.info(f"Executing sequential chain with {len(tasks)} operations")
        workflow = chain(*tasks)
        return workflow.apply_async()

    def _execute_parallel(self, parsed: ParsedExpression) -> AsyncResult:
        parallel_tasks = self._extract_parallel_tasks_from_expression(parsed.expression_tree)

        if len(parallel_tasks) < 2:
            return self._execute_sequential(parsed)

        logger.info(f"Executing parallel chord with {len(parallel_tasks)} tasks")
        workflow = chord(parallel_tasks, array_sum_task.s())
        return workflow.apply_async()

    def _extract_parallel_tasks_from_expression(self, tree: Union[ExpressionNode, float]) -> List:
        if not isinstance(tree, ExpressionNode):
            return []

        tasks = []

        if tree.operation == 'add':
            left_tasks = []
            right_tasks = []

            # Check left operand
            if isinstance(tree.left, ExpressionNode):
                if isinstance(tree.left.left, (int, float)) and isinstance(tree.left.right, (int, float)):
                    # This is a leaf operation like (2 + 2)
                    task_func = self.task_map.get(tree.left.operation)
                    if task_func:
                        left_tasks.append(task_func.s(tree.left.left, tree.left.right))
                else:
                    # Recursively extract from left subtree
                    left_tasks.extend(self._extract_parallel_tasks_from_expression(tree.left))

            # Check right operand
            if isinstance(tree.right, ExpressionNode):
                if isinstance(tree.right.left, (int, float)) and isinstance(tree.right.right, (int, float)):
                    # This is a leaf operation like (5 + 5)
                    task_func = self.task_map.get(tree.right.operation)
                    if task_func:
                        right_tasks.append(task_func.s(tree.right.left, tree.right.right))
                else:
                    # Recursively extract from right subtree
                    right_tasks.extend(self._extract_parallel_tasks_from_expression(tree.right))

            # Combine all tasks found
            tasks.extend(left_tasks)
            tasks.extend(right_tasks)

        # If no parallel tasks found at this level, check children
        if not tasks:
            if isinstance(tree.left, ExpressionNode):
                tasks.extend(self._extract_parallel_tasks_from_expression(tree.left))
            if isinstance(tree.right, ExpressionNode):
                tasks.extend(self._extract_parallel_tasks_from_expression(tree.right))

        return tasks

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