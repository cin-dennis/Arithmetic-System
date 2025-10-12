from ..celery import app
import logging
from celery import Task

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.task(name='add', queue='add_tasks')
def add(x, y):
    try:
        result = x + y
        return result
    except Exception as exc:
        logger.error(f"Error in add_task: {exc}")
        raise