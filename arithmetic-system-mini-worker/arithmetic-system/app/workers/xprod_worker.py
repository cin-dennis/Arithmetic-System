import asyncio
from math import prod

from mini.worker.workers import Worker
from ..models.worker_models import AggregateInput, NumberOutput
from ..config import BROKER, RESULT_BACKEND
from ..constants.constants import XPROD_TASKS_TOPIC

class XProdWorker(Worker[AggregateInput, NumberOutput]):
    Input = AggregateInput
    Output = NumberOutput

    async def before_start(self, input_obj: AggregateInput) -> None:
        pass

    async def on_success(self, input_obj: AggregateInput, result: NumberOutput) -> None:
        pass

    async def on_failure(self, input_obj: AggregateInput, exc: Exception) -> None:
        pass

    async def process(self, input_obj: AggregateInput) -> NumberOutput:
        numbers = input_obj.numbers
        result = 1.0
        for val in numbers:
            result *= val
        print(f"XPROD: prod({numbers}) = {result}")
        return NumberOutput(result=result)

    async def sent_result(self, topic: str, input_obj: NumberOutput) -> None:
        pass

async def main():
    xprod_worker = XProdWorker(BROKER, XPROD_TASKS_TOPIC, RESULT_BACKEND)
    await xprod_worker.arun()


if __name__ == "__main__":
    asyncio.run(main())