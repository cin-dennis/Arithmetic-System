from mini.worker.workers import Worker
from ..models.worker_models import AggregateInput, NumberOutput
from ..config import BROKER, RESULT_BACKEND
import asyncio
from ..constants import XPROD_TASKS_TOPIC

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
        return NumberOutput(result=result)

    async def sent_result(self, topic: str, input_obj: NumberOutput) -> None:
        pass

if __name__ == "__main__":
    worker = XProdWorker(
        broker=BROKER,
        topic=XPROD_TASKS_TOPIC,
        result_backend=RESULT_BACKEND,
    )
    asyncio.run(worker.arun())