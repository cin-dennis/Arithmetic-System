from celery import Celery

app = Celery(
    'sub_service',
    broker='pyamqp://guest@rabbitmq//',
    backend='redis://redis:6379/0'
)

@app.task(name='subtract', queue='sub_tasks')
def subtract(x, y):
    return x - y
