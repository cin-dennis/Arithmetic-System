import asyncio
import json

from mini.worker.workers import Worker
from mini.worker.brokers.rabbitmq import RabbitMQBroker
from mini.worker.result_backends.redis import RedisBackend
from pydantic import BaseModel

class CombinerInput(BaseModel):
    previous_result: float

class CombinerConfig(BaseModel):
    fixed_operand: float
    is_left_fixed: bool
    operation_name: str

class CombinerOutput(BaseModel):
    value: float

OPS = {
    "add": lambda x, y: x + y,
    "sub": lambda x, y: x - y,
    "mul": lambda x, y: x * y,
    "div": lambda x, y: x / y,
}

class CombinerWorker(Worker[BaseModel, CombinerOutput]):
    Input = BaseModel
    Output = CombinerOutput

    async def process(self, input_obj: BaseModel) -> CombinerOutput:
        data = json.loads(input_obj.model_dump_json())
        prev_result_data = json.loads(data['body'])

        message_config = json.loads(data['input'])
        config = CombinerConfig(**message_config)

        prev_value = float(prev_result_data['value'])

        op_func = OPS[config.operation_name]

        if config.is_left_fixed:
            result = op_func(config.fixed_operand, prev_value)
        else:
            result = op_func(prev_value, config.fixed_operand)

        return CombinerOutput(value=result)


async def main():
    broker = RabbitMQBroker("amqp://guest:guest@rabbitmq:5672/")
    result_backend = RedisBackend("redis://redis:6379/0")

    worker = CombinerWorker(broker, "combiner_tasks", result_backend)
    await worker.arun()


if __name__ == "__main__":
    asyncio.run(main())
