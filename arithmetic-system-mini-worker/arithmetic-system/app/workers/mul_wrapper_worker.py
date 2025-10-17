from mini.worker.workers import Worker
from ..models.worker_models import ChainLinkInput, NumberOutput
from ..config import BROKER, RESULT_BACKEND
import asyncio
from ..constants import MUL_TASKS_WRAPPER_TOPIC
import logging

logger = logging.getLogger(__name__)

class MulWrapperWorker(Worker[ChainLinkInput, NumberOutput]):
    Input = ChainLinkInput
    Output = NumberOutput

    async def process(self, input_obj: ChainLinkInput) -> NumberOutput:
        result = input_obj.result * input_obj.next_operand
        logger.info(f"MUL_WRAPPER: {input_obj.result} * {input_obj.next_operand} = {result}")
        return NumberOutput(result=result)

    async def before_start(self, input_obj: ChainLinkInput) -> None:
        logger.info(f"[MUL_WRAPPER] Starting: {input_obj}")

    async def on_success(self, input_obj: ChainLinkInput, result: NumberOutput) -> None:
        logger.info(f"[MUL_WRAPPER] Success: {result.result}")

    async def on_failure(self, input_obj: ChainLinkInput, exc: Exception) -> None:
        logger.info(f"[MUL_WRAPPER] Failed: {exc}")

if __name__ == "__main__":
    worker = MulWrapperWorker(
        broker=BROKER,
        topic=MUL_TASKS_WRAPPER_TOPIC,
        result_backend=RESULT_BACKEND,
    )
    asyncio.run(worker.arun())