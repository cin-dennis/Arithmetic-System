from celery import Celery

app = Celery(
    'div_service',
    broker='pyamqp://guest@localhost//',
    backend='redis://localhost:6379/0'
)

@app.task(name='divide', queue='div_tasks')
def divide(x, y):
    if y == 0:
        raise ZeroDivisionError('Division by zero is not allowed')
    return x / y
