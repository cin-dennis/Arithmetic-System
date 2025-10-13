import asyncio

from mini.worker.workers import Worker
from ..models.worker_models import CalculatorInput, CalculatorOutput
from .common import BROKER, RESULT_BACKEND

class MulWorker(Worker[CalculatorInput, CalculatorOutput]):
    Input = CalculatorInput
    Output = CalculatorOutput

    async def process(self, input_obj: CalculatorInput) -> CalculatorOutput:
        result = input_obj.x * input_obj.y
        return CalculatorOutput(value=result)

async def main():

    mul_worker = MulWorker(BROKER, "mul_tasks", RESULT_BACKEND)
    await mul_worker.arun()

if __name__ == "__main__":
    asyncio.run(main())