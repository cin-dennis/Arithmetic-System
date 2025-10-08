from .celery import app

@app.task(name='multiply', queue='mul_tasks')
def multiply(x, y):
    return x * y
