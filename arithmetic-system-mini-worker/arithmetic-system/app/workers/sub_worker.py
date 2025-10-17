from ..models.worker_models import BinaryOperationInput, NumberOutput
from mini.worker.workers import Worker
from ..config import BROKER, RESULT_BACKEND
import asyncio
from ..constants import SUB_TASKS_TOPIC

class SubWorker(Worker[BinaryOperationInput, NumberOutput]):
    Input = BinaryOperationInput
    Output = NumberOutput

    async def before_start(self, input_obj: BinaryOperationInput) -> None:
        pass

    async def on_success(self, input_obj: BinaryOperationInput, result: NumberOutput) -> None:
        pass

    async def on_failure(self, input_obj: BinaryOperationInput, exc: Exception) -> None:
        pass

    async def process(self, input_obj: BinaryOperationInput) -> NumberOutput:
        result = input_obj.x - input_obj.y
        print(f"SUB: {input_obj.x} - {input_obj.y} = {result}")
        return NumberOutput(result=result)

    async def sent_result(self, topic: str, input_obj: NumberOutput) -> None:
        pass

if __name__ == "__main__":
    worker = SubWorker(
        broker=BROKER,
        topic=SUB_TASKS_TOPIC,
        result_backend=RESULT_BACKEND,
    )
    asyncio.run(worker.arun())