from celery import Celery

app = Celery(
    'add_service',
    broker='pyamqp://guest@rabbitmq//',
    backend='redis://redis:6379/0'
)

@app.task(name='add', queue='add_tasks')
def add(x, y):
    return x + y