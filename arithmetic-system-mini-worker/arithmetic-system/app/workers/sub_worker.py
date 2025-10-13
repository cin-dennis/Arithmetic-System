import asyncio

from mini.worker.workers import Worker
from ..models.worker_models import ArithmeticInput, ArithmeticResult
from .common import BROKER, RESULT_BACKEND

class SubWorker(Worker[ArithmeticInput, ArithmeticResult]):
    Input = ArithmeticInput
    Output = ArithmeticResult

    async def process(self, input_obj: ArithmeticInput) -> ArithmeticResult:
        result = input_obj.x - input_obj.y
        return ArithmeticResult(value=result)

async def main():

    sub_worker = SubWorker(BROKER, "sub_tasks", RESULT_BACKEND)
    await sub_worker.arun()

if __name__ == "__main__":
    asyncio.run(main())