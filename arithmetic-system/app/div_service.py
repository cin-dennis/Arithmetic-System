from .celery import app

@app.task(name='divide', queue='div_tasks')
def divide(x, y):
    if y == 0:
        raise ZeroDivisionError('Division by zero is not allowed')
    return x / y
