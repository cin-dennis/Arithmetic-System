from mini.worker.workers import Worker
from ..models.worker_models import ChainLinkInput, NumberOutput
from ..config import BROKER, RESULT_BACKEND
import asyncio
from ..constants import ADD_TASKS_WRAPPER_TOPIC

class AddWrapperWorker(Worker[ChainLinkInput, NumberOutput]):
    Input = ChainLinkInput
    Output = NumberOutput

    async def process(self, input_obj: ChainLinkInput) -> NumberOutput:
        result = input_obj.result + input_obj.next_operand
        print(f"ADD_WRAPPER: {input_obj.result} + {input_obj.next_operand} = {result}")
        return NumberOutput(result=result)

    async def before_start(self, input_obj: ChainLinkInput) -> None:
        print(f"[ADD_WRAPPER] Starting: {input_obj}")

    async def on_success(self, input_obj: ChainLinkInput, result: NumberOutput) -> None:
        print(f"[ADD_WRAPPER] Success: {result.result}")

    async def on_failure(self, input_obj: ChainLinkInput, exc: Exception) -> None:
        print(f"[ADD_WRAPPER] Failed: {exc}")


if __name__ == "__main__":
    worker = AddWrapperWorker(
        broker=BROKER,
        topic=ADD_TASKS_WRAPPER_TOPIC,
        result_backend=RESULT_BACKEND,
    )
    asyncio.run(worker.arun())