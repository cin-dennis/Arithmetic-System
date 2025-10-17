import asyncio
from mini.worker.workers import Worker
from ..models.worker_models import BinaryOperationInput, NumberOutput
from ..config import BROKER, RESULT_BACKEND
from ..constants.constants import MUL_TASKS_TOPIC

class MulWorker(Worker[BinaryOperationInput, NumberOutput]):
    Input = BinaryOperationInput
    Output = NumberOutput

    async def before_start(self, input_obj: BinaryOperationInput) -> None:
        pass

    async def on_success(self, input_obj: BinaryOperationInput, result: NumberOutput) -> None:
        pass

    async def on_failure(self, input_obj: BinaryOperationInput, exc: Exception) -> None:
        pass

    async def process(self, input_obj: BinaryOperationInput) -> NumberOutput:
        if input_obj.result is not None:
            result = input_obj.result * input_obj.y
        else:
            result = input_obj.x * input_obj.y
        return NumberOutput(result=result)

    async def sent_result(self, topic: str, input_obj: NumberOutput) -> None:
        pass

async def main():

    mul_worker = MulWorker(BROKER, MUL_TASKS_TOPIC, RESULT_BACKEND)
    await mul_worker.arun()

if __name__ == "__main__":
    asyncio.run(main())