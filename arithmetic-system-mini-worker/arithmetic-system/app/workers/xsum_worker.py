import asyncio
from math import fsum

from mini.worker.workers import Worker
from ..models.worker_models import AggregatorInput, ArithmeticResult
from .common import BROKER, RESULT_BACKEND

class XSumWorker(Worker[AggregatorInput, ArithmeticResult]):
    Input = AggregatorInput
    Output = ArithmeticResult

    async def process(self, input_obj: AggregatorInput) -> ArithmeticResult:
        result = fsum(input_obj.values)
        return ArithmeticResult(value=result)

async def main():
    xsum_worker = XSumWorker(BROKER, "xsum_tasks", RESULT_BACKEND)
    await xsum_worker.arun()

if __name__ == "__main__":
    asyncio.run(main())