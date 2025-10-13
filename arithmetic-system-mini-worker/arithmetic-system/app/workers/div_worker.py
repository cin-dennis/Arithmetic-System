import asyncio

from mini.worker.workers import Worker
from ..models.worker_models import ArithmeticInput, ArithmeticResult
from .common import BROKER, RESULT_BACKEND

class DivWorker(Worker[ArithmeticInput, ArithmeticResult]):
    Input = ArithmeticInput
    Output = ArithmeticResult

    async def process(self, input_obj: ArithmeticInput) -> ArithmeticResult:
        if input_obj.y == 0:
            raise ValueError("Cannot divide by zero.")
        result = input_obj.x / input_obj.y
        return ArithmeticResult(value=result)

async def main():

    div_worker = DivWorker(BROKER, "div_tasks", RESULT_BACKEND)
    await div_worker.arun()

if __name__ == "__main__":
    asyncio.run(main())