import asyncio
from math import prod

from mini.worker.workers import Worker
from mini.worker.brokers.rabbitmq import RabbitMQBroker
from mini.worker.result_backends.redis import RedisBackend
from ..models.worker_models import AggregatorInput, ArithmeticResult

class XProdWorker(Worker[AggregatorInput, ArithmeticResult]):
    Input = AggregatorInput
    Output = ArithmeticResult

    async def process(self, input_obj: AggregatorInput) -> ArithmeticResult:
        result = float(prod(input_obj.values))
        return ArithmeticResult(value=result)

async def main():
    broker = RabbitMQBroker("amqp://guest:guest@rabbitmq:5672/")
    result_backend = RedisBackend("redis://redis:6379/0")

    xprod_worker = XProdWorker(broker, "xprod_tasks", result_backend)
    await xprod_worker.arun()


if __name__ == "__main__":
    asyncio.run(main())