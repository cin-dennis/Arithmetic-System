import asyncio

from mini.worker.workers import Worker
from ..models.worker_models import CalculatorInput, CalculatorOutput
from .common import BROKER, RESULT_BACKEND

class DivWorker(Worker[CalculatorInput, CalculatorOutput]):
    Input = CalculatorInput
    Output = CalculatorOutput

    async def process(self, input_obj: CalculatorInput) -> CalculatorOutput:
        if input_obj.current_value is not None:
            if input_obj.is_left_fixed:
                if input_obj.current_value == 0:
                    raise ValueError("Cannot divide by zero.")
                result = input_obj.x / input_obj.current_value
            else:
                if input_obj.y == 0:
                    raise ValueError("Cannot divide by zero.")
                result = input_obj.current_value / input_obj.y
        else:
            if input_obj.y == 0:
                raise ValueError("Cannot divide by zero.")
            result = input_obj.x / input_obj.y

        return CalculatorOutput(result=result)

async def main():

    div_worker = DivWorker(BROKER, "div_tasks", RESULT_BACKEND)
    await div_worker.arun()

if __name__ == "__main__":
    asyncio.run(main())