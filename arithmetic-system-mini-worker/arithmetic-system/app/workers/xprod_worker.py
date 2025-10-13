import asyncio
from math import prod

from mini.worker.workers import Worker
from ..models.worker_models import AggregatorInput, ArithmeticResult
from .common import BROKER, RESULT_BACKEND

class XProdWorker(Worker[AggregatorInput, ArithmeticResult]):
    Input = AggregatorInput
    Output = ArithmeticResult

    async def process(self, input_obj: AggregatorInput) -> ArithmeticResult:
        result = float(prod(input_obj.values))
        return ArithmeticResult(value=result)

async def main():
    xprod_worker = XProdWorker(BROKER, "xprod_tasks", RESULT_BACKEND)
    await xprod_worker.arun()


if __name__ == "__main__":
    asyncio.run(main())