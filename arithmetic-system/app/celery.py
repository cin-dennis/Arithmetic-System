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

app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_time_limit=30 * 60,  # 30 minutes
    task_soft_time_limit=25 * 60,  # 25 minutes
    worker_prefetch_multiplier=1,
    worker_max_tasks_per_child=1000,
)