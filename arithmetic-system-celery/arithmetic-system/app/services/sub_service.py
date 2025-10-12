from ..celery import app
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.task(name='subtract', queue='sub_tasks')
def subtract(x, y):
    try:
        result = x - y
        return result
    except Exception as exc:
        logger.error(f"Error in subtract_task: {exc}")
        raise

