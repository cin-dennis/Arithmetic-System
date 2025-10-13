import asyncio

from mini.worker.workers import Worker
from ..models.worker_models import ArithmeticInput, ArithmeticResult
from .common import BROKER, RESULT_BACKEND

class MulWorker(Worker[ArithmeticInput, ArithmeticResult]):
    Input = ArithmeticInput
    Output = ArithmeticResult

    async def process(self, input_obj: ArithmeticInput) -> ArithmeticResult:
        result = input_obj.x * input_obj.y
        return ArithmeticResult(value=result)

async def main():

    mul_worker = MulWorker(BROKER, "mul_tasks", RESULT_BACKEND)
    await mul_worker.arun()

if __name__ == "__main__":
    asyncio.run(main())