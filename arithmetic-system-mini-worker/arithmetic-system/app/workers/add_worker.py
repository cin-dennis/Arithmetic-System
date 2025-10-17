from ..models.worker_models import BinaryOperationInput, NumberOutput
import logging
from mini.worker.workers import Worker
from ..config import BROKER, RESULT_BACKEND
import asyncio
from ..constants import ADD_TASKS_TOPIC

logger = logging.getLogger(__name__)

class AddWorker(Worker[BinaryOperationInput, NumberOutput]):
    Input = BinaryOperationInput
    Output = NumberOutput

    async def before_start(self, input_obj: BinaryOperationInput) -> None:
        logger.info(f"Before start: {input_obj}")

    async def on_success(self, input_obj: BinaryOperationInput, result: NumberOutput) -> None:
        logger.info(f"Task succeeded: {input_obj} -> {result}")

    async def on_failure(self, input_obj: BinaryOperationInput, exc: Exception) -> None:
        logger.error(f"Task failed: {input_obj} with exception {exc}", exc_info=True)

    async def process(self, input_obj: BinaryOperationInput) -> NumberOutput:
        logger.info(f"Task started: {input_obj}")
        result = input_obj.x + input_obj.y
        return NumberOutput(result=result)

    async def sent_result(self, topic: str, input_obj: BinaryOperationInput) -> None:
        logger.info(f"Result sent to {topic}: {input_obj}")


if __name__ == "__main__":
    worker = AddWorker(
        broker=BROKER,
        topic=ADD_TASKS_TOPIC,
        result_backend=RESULT_BACKEND,
    )
    asyncio.run(worker.arun())