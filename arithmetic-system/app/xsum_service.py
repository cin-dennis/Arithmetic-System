from .celery import app

@app.task(name='xsum', queue='add_tasks')
def xsum(numbers):
    if not isinstance(numbers, (list, tuple)):
        raise ValueError(f"xsum expects a list/tuple, got: {type(numbers)}")
    total = sum(numbers)
    return total
