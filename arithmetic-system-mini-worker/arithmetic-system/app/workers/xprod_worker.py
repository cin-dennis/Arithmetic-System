import asyncio
from math import prod

from mini.worker.workers import Worker
from ..models.worker_models import AggregatorInput, CalculatorOutput
from ..config import BROKER, RESULT_BACKEND
from ..constants.constants import XPROD_TASKS_TOPIC

class XProdWorker(Worker[AggregatorInput, CalculatorOutput]):
    Input = AggregatorInput
    Output = CalculatorOutput

    async def process(self, input_obj: AggregatorInput) -> CalculatorOutput:
        result = float(prod(input_obj.values))
        return CalculatorOutput(result=result)

async def main():
    xprod_worker = XProdWorker(BROKER, XPROD_TASKS_TOPIC, RESULT_BACKEND)
    await xprod_worker.arun()


if __name__ == "__main__":
    asyncio.run(main())