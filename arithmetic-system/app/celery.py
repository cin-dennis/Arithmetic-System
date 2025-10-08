from celery import Celery

app = Celery(
    'arithmetic_system',
    broker='pyamqp://guest@rabbitmq//',
    backend='redis://redis:6379/0',
    # List all modules where tasks are defined so the worker can find them
    include=[
        'app.add_service',
        'app.sub_service',
        'app.mul_service',
        'app.div_service',
        'app.xsum_service'
    ]
)