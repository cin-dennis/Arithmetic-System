from mini.worker.workers import Worker
from ..models.worker_models import ChainLinkInput, NumberOutput
from ..config import BROKER, RESULT_BACKEND
import asyncio
from ..constants import DIV_TASKS_WRAPPER_TOPIC

class DivWrapperWorker(Worker[ChainLinkInput, NumberOutput]):
    Input = ChainLinkInput
    Output = NumberOutput

    async def process(self, input_obj: ChainLinkInput) -> NumberOutput:
        if input_obj.is_left_fixed:
            dividend = input_obj.next_operand
            divisor = input_obj.result
        else:
            dividend = input_obj.result
            divisor = input_obj.next_operand

        if divisor == 0:
            raise ValueError("Division by zero")

        result = dividend / divisor

        return NumberOutput(result=result)

    async def before_start(self, input_obj: ChainLinkInput) -> None:
        print(f"[DIV_WRAPPER] Starting: {input_obj}")

    async def on_success(self, input_obj: ChainLinkInput, result: NumberOutput) -> None:
        print(f"[DIV_WRAPPER] Success: {result.result}")

    async def on_failure(self, input_obj: ChainLinkInput, exc: Exception) -> None:
        print(f"[DIV_WRAPPER] Failed: {exc}")

if __name__ == "__main__":
    worker = DivWrapperWorker(
        broker=BROKER,
        topic=DIV_TASKS_WRAPPER_TOPIC,
        result_backend=RESULT_BACKEND,
    )
    asyncio.run(worker.arun())