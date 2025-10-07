from celery import Celery

app = Celery(
    'mul_service',
    broker='pyamqp://guest@rabbitmq//',
    backend='redis://redis:6379/0'
)

@app.task(name='multiply', queue='mul_tasks')
def multiply(x, y):
    return x * y
