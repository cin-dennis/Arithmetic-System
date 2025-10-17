from typing import Union, List
from .expression_parser import ExpressionNode
import logging
from mini.worker.workers.canvas import Node, Chain, Chord
from ..models.worker_models import BinaryOperationInput, AggregateInput, ChordCallbackInput, ChainLinkInput
from ..constants.constants import (
    OperationEnum,
    OPERATION_TOPIC_MAP,
    AGGREGATOR_TOPIC_MAP
)

logger = logging.getLogger(__name__)


class WorkflowBuilder:
    def build(
            self,
            expression_tree:
            Union[ExpressionNode, float]
    ) -> Chain | Chord | float:
        logger.info(f"Building {expression_tree}")
        return self._build_recursive(expression_tree)

    def _build_recursive(
            self,
            node: Union[ExpressionNode,
            float]
    ) -> Node | Chain | Chord | float:
        if isinstance(node, (int, float)):
            return float(node)

        if not isinstance(node, ExpressionNode):
            raise TypeError(f"Invalid node type: {type(node)}")

        is_left_constant = isinstance(node.left, (int, float))
        is_right_constant = isinstance(node.right, (int, float))

        # Both are constants
        if is_left_constant and is_right_constant:
            task_input = BinaryOperationInput(x=node.left, y=node.right)
            op_node = Node(
                topic=OPERATION_TOPIC_MAP[node.operation],
                input=task_input.model_dump_json()
            )
            return Chain(nodes=[op_node])

        # Both are same operation and commutative
        if node.operation.is_commutative and (
            not is_left_constant or not is_right_constant
        ):
            return self._build_flat_workflow(node)

        left_workflow = self._build_recursive(node.left)
        right_workflow = self._build_recursive(node.right)

        # Left is OperationNode, Right is constant
        if not is_left_constant and is_right_constant:
            op_input = ChainLinkInput(next_operand=right_workflow, is_left_fixed=False)
            op_node = Node(
                topic=OPERATION_TOPIC_MAP[node.operation],
                input=op_input.model_dump_json()
            )
            return Chain(nodes=[left_workflow, op_node])

        # Left is constant, Right is OperationNode
        if is_left_constant and not is_right_constant:
            op_input = ChainLinkInput(next_operand=left_workflow, is_left_fixed=True)
            op_node = Node(
                topic=OPERATION_TOPIC_MAP[node.operation],
                input=op_input.model_dump_json()
            )
            return Chain(nodes=[right_workflow, op_node])

        # Both are OperationNodes
        callback_node = Node(topic=OPERATION_TOPIC_MAP[node.operation])
        return Chord(
            nodes=[left_workflow, right_workflow],
            callback=callback_node
        )

    def _flatten_commutative_operands(
            self, node, operation: OperationEnum
    ) -> list[ExpressionNode | float | int]:
        sub_commutative_expression: list[ExpressionNode | float | int] = []

        if node is None:
            return sub_commutative_expression

        if not isinstance(node, ExpressionNode) or node.operation != operation:
            sub_commutative_expression.append(node)
            return sub_commutative_expression

        sub_commutative_expression.extend(
            self._flatten_commutative_operands(node.left, operation)
        )
        sub_commutative_expression.extend(
            self._flatten_commutative_operands(node.right, operation)
        )

        return sub_commutative_expression

    def _build_flat_workflow(self, node: ExpressionNode) -> Node | Chain | Chord | float:
        flatten_commutative_nodes = self._flatten_commutative_operands(
            node, node.operation
        )
        child_workflows = [
            self._build_recursive(sub_node) for sub_node in flatten_commutative_nodes
        ]

        tasks, constants = self._split_tasks_and_constants(child_workflows)
        identity = 0.0 if node.operation == OperationEnum.ADD else 1.0

        if not tasks:
            return self._handle_no_tasks(node, constants, identity)
        if len(tasks) == 1:
            return self._handle_single_task(node, tasks, constants)
        return self._handle_multiple_tasks(node, tasks, constants)

    def _split_tasks_and_constants(
            self, workflows: list[Node | int | float]
    ) -> tuple[list[Node], list[int | float]]:
        tasks = [w for w in workflows if isinstance(w, Node)]
        constants = [w for w in workflows if isinstance(w, (int, float))]
        return tasks, constants

    def _handle_no_tasks(
        self,
        node: ExpressionNode, constants, identity
    ) -> Node | float:
        num_constants = len(constants)
        if num_constants == 0:
            return identity
        if num_constants == 1:
            return constants[0]
        if num_constants == 2:
            task_input = BinaryOperationInput(x=node.left, y=node.right)
            return Node(
                topic=OPERATION_TOPIC_MAP[node.operation],
                input=task_input.model_dump_json(),
            )

        aggregate_input = AggregateInput(values=constants)
        return Node(
            topic=AGGREGATOR_TOPIC_MAP[node.operation],
            input=aggregate_input.model_dump_json(),
        )

    def _handle_single_task(
        self,
        node: ExpressionNode,
        tasks, constants
    ) -> Node | Chain | Chord | float:
        task = tasks[0]
        num_constants = len(constants)

        if not constants:
            return task
        if num_constants == 1:
            task_input = ChainLinkInput(next_operand=constants[0], is_left_fixed=False)
            op_task = Node(
                topic=OPERATION_TOPIC_MAP[node.operation],
                input=task_input.model_dump_json(),
            )
            return Chain(nodes=[task, op_task])

        aggregate_input = AggregateInput(values=constants)
        aggregate_task = Node(
            topic=AGGREGATOR_TOPIC_MAP[node.operation],
            input=aggregate_input.model_dump_json(),
        )
        tasks.append(aggregate_task)
        return Chord(nodes=tasks, callback=aggregate_task)

    def _handle_multiple_tasks(
        self,
        node: ExpressionNode,
        tasks, constants
    ) -> Chord:
        num_constants = len(constants)

        if num_constants == 1:
            const = constants[0]
            task_input = ChainLinkInput(next_operand=const, is_left_fixed=False)
            op_task = Node(
                topic=OPERATION_TOPIC_MAP[node.operation],
                input=task_input.model_dump_json(),
            )

            aggregate_input = ChordCallbackInput()
            aggregate_task = Node(
                topic=AGGREGATOR_TOPIC_MAP[node.operation],
                input=aggregate_input.model_dump_json(),
            )
            chain = Chain(nodes=[aggregate_task, op_task])
            return Chord(nodes=tasks, callback=chain)

        if num_constants > 1:
            aggregate_input = AggregateInput(values=constants)
            aggregate_task = Node(
                topic=AGGREGATOR_TOPIC_MAP[node.operation],
                input=aggregate_input.model_dump_json(),
            )
            tasks.append(aggregate_task)

        aggregate_input = ChordCallbackInput()
        aggregate_task = Node(
            topic=AGGREGATOR_TOPIC_MAP[node.operation],
            input=aggregate_input.model_dump_json(),
        )
        return Chord(nodes=tasks, callback=aggregate_task)