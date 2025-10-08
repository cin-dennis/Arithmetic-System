import re
from celery import Celery

app = Celery(
    'add_service',
    broker='pyamqp://guest@rabbitmq//',
    backend='redis://redis:6379/0'
)

def evaluate_expression(expression: str) -> float:
    try:
        print("Evaluating expression:", expression)
        tokens = tokenize(expression)
        print("Tokens:", tokens)

        postfix = infix_to_postfix(tokens)
        print("Postfix:", postfix)

        result = evaluate_postfix(postfix)
        print("Result:", result)

        return result
    except Exception:
        print("Failed to evaluate expression:", expression)
        raise ValueError("Invalid expression")


TASKS = {
    '+': {'name': 'add', 'queue': 'add_tasks'},
    '-': {'name': 'subtract', 'queue': 'sub_tasks'},
    '*': {'name': 'multiply', 'queue': 'mul_tasks'},
    '/': {'name': 'divide', 'queue': 'div_tasks'},
}

PRECEDENCE = {'+': 1, '-': 1, '*': 2, '/': 2}

def tokenize(expr: str):
    expr = expr.replace(' ', '')
    return re.findall(r'\d+\.?\d*|[+\-*/()]', expr)

def infix_to_postfix(tokens):
    output, stack = [], []
    for token in tokens:
        if re.match(r'\d+\.?\d*', token):
            output.append(float(token))
        elif token in PRECEDENCE:
            while stack and stack[-1] in PRECEDENCE and PRECEDENCE[stack[-1]] >= PRECEDENCE[token]:
                output.append(stack.pop())
            stack.append(token)
        elif token == '(':
            stack.append(token)
        elif token == ')':
            while stack and stack[-1] != '(':
                output.append(stack.pop())
            stack.pop()
    while stack:
        output.append(stack.pop())
    return output

def evaluate_postfix(postfix):
    stack = []
    for token in postfix:
        if isinstance(token, float):
            stack.append(token)
        elif token in TASKS:
            b = stack.pop()
            a = stack.pop()
            task_info = TASKS[token]
            res = app.send_task(
                task_info['name'],
                args=(a, b),
                queue=task_info['queue']
            ).get()
            stack.append(res)
    return stack[0]
