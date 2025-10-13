from mini.worker.brokers.rabbitmq import RabbitMQBroker
from mini.worker.result_backends.redis import RedisBackend

BROKER = RabbitMQBroker("amqp://guest:guest@rabbitmq:5672/")
RESULT_BACKEND = RedisBackend("redis://redis:6379/0")