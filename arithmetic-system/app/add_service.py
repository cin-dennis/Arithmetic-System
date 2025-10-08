from .celery import app

@app.task(name='add', queue='add_tasks')
def add(x, y):
    return x + y