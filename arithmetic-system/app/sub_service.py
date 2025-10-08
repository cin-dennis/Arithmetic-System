from .celery import app

@app.task(name='subtract', queue='sub_tasks')
def subtract(x, y):
    return x - y
