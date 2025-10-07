from celery import Celery

app = Celery(
    'div_service',
    broker='pyamqp://guest@rabbitmq//',
    backend='redis://redis:6379/0'
)

@app.task(name='divide', queue='div_tasks')
def divide(x, y):
    if y == 0:
        raise ZeroDivisionError('Division by zero is not allowed')
    return x / y
