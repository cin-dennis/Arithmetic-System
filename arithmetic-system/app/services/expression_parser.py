import re
import ast
from typing import Dict, List, Tuple, Union, Any, Optional
from dataclasses import dataclass
from enum import Enum

class ExpressionType(Enum):
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    HYBRID = "hybrid"
    SIMPLE = "simple"

@dataclass
class Operation:
    operation: str
    operand1: Union[float, str, 'ExpressionNode']
    operand2: Union[float, str, 'ExpressionNode']
    can_parallelize: bool = False


@dataclass
class ExpressionNode:
    operation: str
    left: Union[float, 'ExpressionNode']
    right: Union[float, 'ExpressionNode']
    is_parallel_group: bool = False
    level: int = 0


@dataclass
class ParsedExpression:
    expression_type: ExpressionType
    operations: List[Operation]
    expression_tree: Optional[ExpressionNode]
    parallel_groups: List[List[Operation]]
    sequential_chains: List[List[Operation]]
    original_expression: str


class ExpressionParser:

    OPERATORS = {
        '+': 'add',
        '-': 'sub',
        '*': 'mul',
        '/': 'div'
    }

    # Operator precedence for determining evaluation order
    PRECEDENCE = {
        '+': 1, '-': 1,
        '*': 2, '/': 2
    }

    def __init__(self):
        self.operations = []
        self.parallel_groups = []
        self.sequential_chains = []

    def parse(self, expression: str) -> ParsedExpression:
        try:
            # Flow: Clean -> Parse AST -> Build Tree -> Analyze

            clean_expr = self._clean_expression(expression)

            tree = ast.parse(clean_expr, mode='eval')

            expr_tree = self._build_expression_tree(tree.body, level=0)

            parallel_groups = self._find_parallel_groups(expr_tree)

            sequential_chains = self._find_sequential_chains(expr_tree)

            operations = self._extract_operations(expr_tree)

            expr_type = self._determine_expression_type(operations, parallel_groups, sequential_chains)

            return ParsedExpression(
                expression_type=expr_type,
                operations=operations,
                expression_tree=expr_tree,
                parallel_groups=parallel_groups,
                sequential_chains=sequential_chains,
                original_expression=expression
            )

        except Exception as e:
            raise ValueError(f"Invalid expression: {expression}. Error: {str(e)}")

    def _build_expression_tree(self, node, level: int = 0) -> Union[ExpressionNode, float]:
        if isinstance(node, ast.BinOp):
            op_symbol = self._get_operator_symbol(node.op)
            if op_symbol not in self.OPERATORS:
                raise ValueError(f"Unsupported operator: {op_symbol}")

            left = self._build_expression_tree(node.left, level + 1)
            right = self._build_expression_tree(node.right, level + 1)

            # Detect if this can be parallelized
            is_parallel = self._can_parallelize(left, right, op_symbol)

            return ExpressionNode(
                operation=self.OPERATORS[op_symbol],
                left=left,
                right=right,
                is_parallel_group=is_parallel,
                level=level
            )
        elif isinstance(node, ast.Constant):
            return float(node.value)
        else:
            raise ValueError(f"Unsupported node type: {type(node)}")

    def _can_parallelize(self, left, right, operation: str) -> bool:
        # If both sides are complex expressions and operation is addition
        if (isinstance(left, ExpressionNode) and isinstance(right, ExpressionNode)
                and operation == 'add'):
            return True

        # If one side is complex and the other is simple, still can parallelize
        if ((isinstance(left, ExpressionNode) and isinstance(right, (int, float))) or
                (isinstance(right, ExpressionNode) and isinstance(left, (int, float)))):
            return operation == 'add'

        return False

    def _find_parallel_groups(self, tree: Union[ExpressionNode, float]) -> List[List[Operation]]:
        parallel_groups = []

        if isinstance(tree, ExpressionNode):
            if tree.is_parallel_group:
                left_ops = self._extract_operations_from_subtree(tree.left)
                right_ops = self._extract_operations_from_subtree(tree.right)

                if left_ops and right_ops:
                    parallel_groups.append(left_ops + right_ops)

            # Recursively check children
            if isinstance(tree.left, ExpressionNode):
                parallel_groups.extend(self._find_parallel_groups(tree.left))
            if isinstance(tree.right, ExpressionNode):
                parallel_groups.extend(self._find_parallel_groups(tree.right))

        return parallel_groups

    def _find_sequential_chains(self, tree: Union[ExpressionNode, float]) -> List[List[Operation]]:
        """Find chains of operations that must run sequentially"""
        sequential_chains = []

        if isinstance(tree, ExpressionNode):
            if not tree.is_parallel_group:
                # This represents a sequential operation
                chain = []

                # Build chain from left to right
                if isinstance(tree.left, ExpressionNode):
                    left_chains = self._find_sequential_chains(tree.left)
                    if left_chains:
                        chain.extend(left_chains[0])  # Take the main chain

                # Add current operation
                chain.append(Operation(
                    operation=tree.operation,
                    operand1=tree.left if isinstance(tree.left, (int, float)) else "prev_result",
                    operand2=tree.right if isinstance(tree.right, (int, float)) else "prev_result",
                    can_parallelize=tree.is_parallel_group
                ))

                if isinstance(tree.right, ExpressionNode):
                    right_chains = self._find_sequential_chains(tree.right)
                    sequential_chains.extend(right_chains)

                if chain:
                    sequential_chains.append(chain)

        return sequential_chains

    def _extract_operations_from_subtree(self, subtree: Union[ExpressionNode, float]) -> List[Operation]:
        operations = []

        if isinstance(subtree, ExpressionNode):
            # Add current operation
            operations.append(Operation(
                operation=subtree.operation,
                operand1=subtree.left if isinstance(subtree.left, (int, float)) else "complex",
                operand2=subtree.right if isinstance(subtree.right, (int, float)) else "complex",
                can_parallelize=subtree.is_parallel_group
            ))

            # Recursively extract from children
            if isinstance(subtree.left, ExpressionNode):
                operations.extend(self._extract_operations_from_subtree(subtree.left))
            if isinstance(subtree.right, ExpressionNode):
                operations.extend(self._extract_operations_from_subtree(subtree.right))

        return operations

    def _extract_operations(self, tree: Union[ExpressionNode, float]) -> List[Operation]:
        """Extract all operations from the expression tree"""
        return self._extract_operations_from_subtree(tree)

    def _determine_expression_type(self, operations: List[Operation],
                                   parallel_groups: List[List[Operation]],
                                   sequential_chains: List[List[Operation]]) -> ExpressionType:
        """Determine the overall expression type based on analysis"""

        # Simple expression
        if len(operations) <= 1:
            return ExpressionType.SIMPLE

        # Hybrid: has both parallel and sequential components
        if parallel_groups and sequential_chains:
            return ExpressionType.HYBRID

        # Purely parallel
        if parallel_groups and not sequential_chains:
            return ExpressionType.PARALLEL

        # Purely sequential
        if sequential_chains and not parallel_groups:
            return ExpressionType.SEQUENTIAL

        # Default fallback
        return ExpressionType.SEQUENTIAL

    def _clean_expression(self, expression: str) -> str:
        # Remove whitespace
        clean = re.sub(r'\s+', '', expression)

        # Validate characters (numbers, operators, parentheses, decimal points)
        if not re.match(r'^[0-9+\-*/().]+$', clean):
            raise ValueError("Expression contains invalid characters")

        return clean

    def _get_operator_symbol(self, op) -> str:
        op_map = {
            ast.Add: '+',
            ast.Sub: '-',
            ast.Mult: '*',
            ast.Div: '/'
        }
        return op_map.get(type(op), '?')


def create_simple_expression(a: float, operator: str, b: float) -> str:
    """Create a simple expression: a + b"""
    return f"{a} {operator} {b}"


def create_sequential_expression(operations: List[Tuple[float, str, float]]) -> str:
    """Create a sequential expression: ((a + b) - c) * d"""
    if not operations:
        return ""

    expr = f"({operations[0][0]} {operations[0][1]} {operations[0][2]})"

    for op in operations[1:]:
        expr = f"({expr} {op[1]} {op[2]})"

    return expr


def create_parallel_expression(groups: List[Tuple[float, str, float]]) -> str:
    """Create a parallel expression: (a + b) + (c - d) + (e * f)"""
    if not groups:
        return ""

    group_exprs = [f"({g[0]} {g[1]} {g[2]})" for g in groups]
    return " + ".join(group_exprs)