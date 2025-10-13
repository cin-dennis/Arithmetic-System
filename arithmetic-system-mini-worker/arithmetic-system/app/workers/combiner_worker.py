import asyncio

from mini.worker.workers import Worker
from mini.worker.brokers.rabbitmq import RabbitMQBroker
from mini.worker.result_backends.redis import RedisBackend
from pydantic import BaseModel

class CombinerInput(BaseModel):
    previous_result: float

class CombinerOutput(BaseModel):
    value: float

class CombinerWorker(Worker[BaseModel, CombinerOutput]):
    Input = BaseModel
    Output = CombinerOutput

    async def process(self, input_obj: BaseModel) -> CombinerOutput:
        pass


async def main():
    broker = RabbitMQBroker("amqp://guest:guest@rabbitmq:5672/")
    result_backend = RedisBackend("redis://redis:6379/0")

    worker = CombinerWorker(broker, "combiner_tasks", result_backend)
    await worker.arun()


if __name__ == "__main__":
    asyncio.run(main())
