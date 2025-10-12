import asyncio

from mini.worker.workers import Worker
from mini.worker.brokers.rabbitmq import RabbitMQBroker
from mini.worker.result_backends.redis import RedisBackend
from ..models.worker_models import ArithmeticInput, ArithmeticResult

class SubWorker(Worker[ArithmeticInput, ArithmeticResult]):
    Input = ArithmeticInput
    Output = ArithmeticResult

    async def process(self, input_obj: ArithmeticInput) -> ArithmeticResult:
        result = input_obj.x - input_obj.y
        # -------------------------
        return ArithmeticResult(value=result)

async def main():
    broker = RabbitMQBroker("amqp://guest:guest@rabbitmq:5672/")
    result_backend = RedisBackend("redis://redis:6379/0")

    sub_worker = SubWorker(broker, "sub_tasks", result_backend)
    await sub_worker.arun()

if __name__ == "__main__":
    asyncio.run(main())