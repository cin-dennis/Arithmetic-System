from mini.worker.workers import Worker
from ..models.worker_models import ChainLinkInput, NumberOutput
from ..config import BROKER, RESULT_BACKEND
import asyncio
from ..constants import SUB_TASKS_WRAPPER_TOPIC

class SubWrapperWorker(Worker[ChainLinkInput, NumberOutput]):
    Input = ChainLinkInput
    Output = NumberOutput

    async def process(self, input_obj: ChainLinkInput) -> NumberOutput:
        if not input_obj.is_left_fixed:
            result = input_obj.result - input_obj.next_operand
        else:
            result = input_obj.next_operand - input_obj.result
        print(f"SUB_WRAPPER: {input_obj.result} - {input_obj.next_operand} = {result}")
        return NumberOutput(result=result)

    async def before_start(self, input_obj: ChainLinkInput) -> None:
        print(f"[SUB_WRAPPER] Starting: {input_obj}")

    async def on_success(self, input_obj: ChainLinkInput, result: NumberOutput) -> None:
        print(f"[SUB_WRAPPER] Success: {result.result}")

    async def on_failure(self, input_obj: ChainLinkInput, exc: Exception) -> None:
        print(f"[SUB_WRAPPER] Failed: {exc}")

if __name__ == "__main__":
    worker = SubWrapperWorker(
        broker=BROKER,
        topic=SUB_TASKS_WRAPPER_TOPIC,
        result_backend=RESULT_BACKEND,
    )
    asyncio.run(worker.arun())