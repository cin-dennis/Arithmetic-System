import ast
from celery import chord
from ..celery import app

OPERATOR_TASKS = {
    ast.Add: ('add', 'add_tasks'),
    ast.Sub: ('subtract', 'sub_tasks'),
    ast.Mult: ('multiply', 'mul_tasks'),
    ast.Div: ('divide', 'div_tasks'),
}

def build_task(op_node, left, right):
    op_type = type(op_node)
    if op_type not in OPERATOR_TASKS:
        raise ValueError(f"Unsupported operator: {op_node}")
    task_name, queue = OPERATOR_TASKS[op_type]
    return app.signature(task_name, args=(left, right), queue=queue)


def flatten_parallel_add(node):
    if not isinstance(node, ast.BinOp) or not isinstance(node.op, ast.Add):
        return [node]
    return flatten_parallel_add(node.left) + flatten_parallel_add(node.right)


def can_parallelize(node):
    return (
        isinstance(node, ast.BinOp)
        and isinstance(node.op, ast.Add)
        and (
            isinstance(node.left, ast.BinOp)
            or isinstance(node.right, ast.BinOp)
        )
    )

def evaluate_node(node):
    if isinstance(node, ast.Constant):
        print(f"🔢 Constant: {node.value}")
        return node.value

    elif isinstance(node, ast.BinOp):
        if can_parallelize(node):
            sub_nodes = flatten_parallel_add(node)
            parallel_tasks = []

            for sub_expr in sub_nodes:
                if isinstance(sub_expr, ast.BinOp):
                    left = evaluate_node(sub_expr.left)
                    right = evaluate_node(sub_expr.right)
                    task = build_task(sub_expr.op, left, right)
                elif isinstance(sub_expr, ast.Constant):
                    task = app.signature('add', args=(sub_expr.value, 0), queue='add_tasks')
                else:
                    raise TypeError(f"Unexpected node type in parallel execution: {type(sub_expr)}")

                parallel_tasks.append(task)
            return chord(parallel_tasks)(app.signature('xsum', queue='add_tasks'))

        left = evaluate_node(node.left)
        right = evaluate_node(node.right)
        op_type = type(node.op)

        if op_type not in OPERATOR_TASKS:
            raise ValueError(f"Unsupported operator: {node.op}")
        task_name, queue = OPERATOR_TASKS[op_type]

        # CASE 1️⃣: both constants → just create a signature
        if isinstance(left, (int, float)) and isinstance(right, (int, float)):
            print(f"🧮 Both operands are constants: {left} {node.op} {right}")
            return app.signature(task_name, args=(left, right), queue=queue)

        # CASE 2️⃣: left is a chain / signature → pass right as argument
        if not isinstance(left, (int, float)):
            print(f"🔗 Chaining left operand: {left}")
            return left | app.signature(task_name, args=(right,), queue=queue)

        # CASE 3️⃣: right is a chain / signature → pass left as argument
        if not isinstance(right, (int, float)):
            print(f"🔗 Chaining right operand: {right}")
            return right | app.signature(task_name, args=(left,), queue=queue)

        # Fallback (shouldn't happen)
        print("🔗 Creating task for operation (fallback)")
        return app.signature(task_name, args=(left, right), queue=queue)
    else:
        raise ValueError(f"Unsupported AST node: {type(node)}")


def evaluate_expression(expr: str) -> float:
    print(f"\n🧾 Evaluating: {expr}")
    tree = ast.parse(expr, mode='eval')
    print(f"🌳 AST: {ast.dump(tree)}")

    graph = evaluate_node(tree.body)

    print(f"🔗 Graph: {graph}")

    if hasattr(graph, "apply_async"):
        print("🚀 Executing Celery graph...")
        async_result = graph.apply_async()
        result_value = async_result.get()

        # 🚀 Case 2: It's an AsyncResult directly
    elif hasattr(graph, "get"):
        print("🚀 Getting value from AsyncResult...")
        result_value = graph.get()

        # 🚀 Case 3: Already a computed constant
    elif isinstance(graph, (int, float)):
        result_value = graph

    else:
        raise TypeError(f"Unexpected return type from evaluate_node: {type(graph)}")

    print(f"✅ Final float result: {result_value}")

    print(f"✅ Result: {result_value}")
    return result_value
