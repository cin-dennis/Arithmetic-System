import json
from typing import Union, List
from .expression_parser import ExpressionNode, OperationEnum
import logging
from mini.worker.workers.canvas import Node, Chain, Chord
from ..models.worker_models import ArithmeticInput, AggregatorInput

logger = logging.getLogger(__name__)

OPERATION_TOPIC_MAP = {
    OperationEnum.ADD: "add_tasks",
    OperationEnum.SUB: "sub_tasks",
    OperationEnum.MUL: "mul_tasks",
    OperationEnum.DIV: "div_tasks",
}
AGGREGATOR_TOPIC_MAP = {
    OperationEnum.ADD: "xsum_tasks",
    OperationEnum.MUL: "xprod_tasks",
}
COMBINER_TOPIC = "combiner_tasks"

class WorkflowBuilder:
    def __init__(self, task_map):
        self.task_map = task_map

    def build(self, expression_tree: Union[ExpressionNode, float]) -> Union[Node, Chain, Chord, float]:
        return self._build_recursive(expression_tree)

    def _build_recursive(self, node: Union[ExpressionNode, float]) -> Union[Node, Chain, Chord, float]:
        if isinstance(node, (int, float)):
            return float(node)

        if not isinstance(node, ExpressionNode):
            raise TypeError(f"Invalid node type: {type(node)}")

        is_left_constant = isinstance(node.left, (int, float))
        is_right_constant = isinstance(node.right, (int, float))
        if is_left_constant and is_right_constant:
            op_task = self.task_map[node.operation]
            return op_task.s(node.left, node.right)

        if node.operation.is_commutative and not is_left_constant and not is_right_constant:
            return self._build_flat_workflow(node)
        else:
            left_workflow = self._build_recursive(node.left)
            right_workflow = self._build_recursive(node.right)

            left_is_value = isinstance(left_workflow, float)
            right_is_value = isinstance(right_workflow, float)

            if left_is_value and right_is_value:
                task_input = ArithmeticInput(x=left_workflow, y=right_workflow)
                return Node(
                    topic=OPERATION_TOPIC_MAP[node.operation],
                    input=task_input.model_dump_json()
                )
            if not left_is_value and not right_is_value:
                aggregator_topic = AGGREGATOR_TOPIC_MAP.get(node.operation)
                if not aggregator_topic:
                    raise NotImplementedError(
                        f"Combining two complex workflows with non-commutative op '{node.operation}' is not supported.")

                return Chord(
                    nodes=[left_workflow, right_workflow],
                    callback=Node(topic=aggregator_topic)
                )
            else:
                main_workflow = left_workflow if not left_is_value else right_workflow
                fixed_value = right_workflow if left_is_value else left_workflow
                is_left_fixed = right_is_value

                combiner_config = {
                    "fixed_operand": fixed_value,
                    "is_left_fixed": is_left_fixed,
                    "operation_name": node.operation.value,
                }

                return Chord(
                    nodes=[main_workflow],
                    callback=Node(
                        topic=COMBINER_TOPIC,
                        input=json.dumps(combiner_config)
                    )
                )

    def _collect_operands(self, node, operation: OperationEnum):
        operands = []
        if isinstance(node, ExpressionNode) and node.operation == operation:
            operands.extend(self._collect_operands(node.left, operation))
            operands.extend(self._collect_operands(node.right, operation))
        else:
            operands.append(node)
        return operands

    def _build_flat_workflow(self, node: ExpressionNode) -> Chord:
        all_operands = self._collect_operands(node, node.operation)
        child_workflows = [self._build_recursive(op) for op in all_operands]

        tasks = [wf for wf in child_workflows if not isinstance(wf, float)]
        constants = [wf for wf in child_workflows if isinstance(wf, float)]

        aggregator_topic = AGGREGATOR_TOPIC_MAP.get(node.operation)
        if not aggregator_topic:
            raise ValueError(f"No aggregator for commutative operation: {node.operation}")

        aggregator_input = AggregatorInput(values=constants).model_dump_json()

        if not tasks:
            return Node(
                topic=aggregator_topic,
                input=aggregator_input
            )

        return Chord(
            nodes=tasks,
            callback=Node(
                topic=aggregator_topic,
                input=aggregator_input
            )
        )
