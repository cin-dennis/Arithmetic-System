import asyncio
from mini.worker.workers import Worker
from ..models.worker_models import BinaryOperationInput, NumberOutput
from ..config import BROKER, RESULT_BACKEND
from ..constants.constants import DIV_TASKS_TOPIC

class DivWorker(Worker[BinaryOperationInput, NumberOutput]):
    Input = BinaryOperationInput
    Output = BinaryOperationInput

    async def before_start(self, input_obj: BinaryOperationInput) -> None:
        pass

    async def on_success(self, input_obj: BinaryOperationInput, result: NumberOutput) -> None:
        pass

    async def on_failure(self, input_obj: BinaryOperationInput, exc: Exception) -> None:
        pass

    async def process(self, input_obj: BinaryOperationInput) -> NumberOutput:
        result = input_obj.a + input_obj.b
        print(f"ADD: {input_obj.a} + {input_obj.b} = {result}")
        return NumberOutput(result=result)

    async def sent_result(self, topic: str, input_obj: NumberOutput) -> None:
        pass

async def main():

    div_worker = DivWorker(BROKER, DIV_TASKS_TOPIC, RESULT_BACKEND)
    await div_worker.arun()

if __name__ == "__main__":
    asyncio.run(main())