import asyncio
from mini.worker.workers import Worker
from ..models.worker_models import CalculatorInput, CalculatorOutput
from .common import BROKER, RESULT_BACKEND

class AddWorker(Worker[CalculatorInput, CalculatorOutput]):
    Input = CalculatorInput
    Output = CalculatorOutput

    async def process(self, input_obj: CalculatorInput) -> CalculatorOutput:
        if input_obj.current_value is not None:
            result = input_obj.current_value + input_obj.y
        else:
            result = input_obj.x + input_obj.y
        return CalculatorOutput(result=result)

async def main():
    add_worker = AddWorker(BROKER, "add_tasks", RESULT_BACKEND)
    await add_worker.arun()

if __name__ == "__main__":
    asyncio.run(main())