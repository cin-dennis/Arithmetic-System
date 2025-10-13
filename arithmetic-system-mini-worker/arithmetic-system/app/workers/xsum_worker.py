import asyncio
from math import fsum

from mini.worker.workers import Worker
from ..models.worker_models import AggregatorInput, CalculatorOutput
from ..config import BROKER, RESULT_BACKEND
from ..constants.constants import XSUM_TASKS_TOPIC

class XSumWorker(Worker[AggregatorInput, CalculatorOutput]):
    Input = AggregatorInput
    Output = CalculatorOutput

    async def process(self, input_obj: AggregatorInput) -> CalculatorOutput:
        result = fsum(input_obj.values)
        return CalculatorOutput(result=result)

async def main():
    xsum_worker = XSumWorker(BROKER, XSUM_TASKS_TOPIC, RESULT_BACKEND)
    await xsum_worker.arun()

if __name__ == "__main__":
    asyncio.run(main())