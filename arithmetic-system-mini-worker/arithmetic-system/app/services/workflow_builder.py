import json
from typing import Union
from .expression_parser import ExpressionNode, OperationEnum
import logging
from mini.worker.workers.canvas import Node, Chain, Chord
from ..models.worker_models import ArithmeticInput

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
COMBINER_TOPIC = "chord_combiner_tasks"

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
            if not node.operation.is_commutative:
                raise NotImplementedError("Combining two complex workflows with a non-commutative operator is not yet supported.")

            aggregator_topic = AGGREGATOR_TOPIC_MAP.get(node.operation)
            if not aggregator_topic:
                raise ValueError(f"No aggregator available for commutative operation: {node.operation}")

            return Chord(
                nodes=[left_workflow, right_workflow],
                callback=Node(topic=aggregator_topic)
            )

        else:
            if right_is_value:
                main_workflow = left_workflow
                fixed_value = right_workflow
                is_left_fixed = False
            else:
                main_workflow = right_workflow
                fixed_value = left_workflow
                is_left_fixed = True

            combiner_config = {
                "fixed_operand": fixed_value,
                "is_left_fixed": is_left_fixed,
                "operation_name": node.operation.value,
            }

            callback_node = Node(
                topic=COMBINER_TOPIC,
                input=json.dumps(combiner_config)
            )

            return Chord(
                nodes=[main_workflow],
                callback=callback_node
            )
