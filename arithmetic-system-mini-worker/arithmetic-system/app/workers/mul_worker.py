import asyncio

from mini.worker.workers import Worker
from ..models.worker_models import CalculatorInput, CalculatorOutput
from .common import BROKER, RESULT_BACKEND

class MulWorker(Worker[CalculatorInput, CalculatorOutput]):
    Input = CalculatorInput
    Output = CalculatorOutput

    async def process(self, input_obj: CalculatorInput) -> CalculatorOutput:
        if input_obj.current_value is not None:
            result = input_obj.current_value * input_obj.y
        else:
            result = input_obj.x * input_obj.y
        return CalculatorOutput(result=result)

async def main():

    mul_worker = MulWorker(BROKER, "mul_tasks", RESULT_BACKEND)
    await mul_worker.arun()

if __name__ == "__main__":
    asyncio.run(main())