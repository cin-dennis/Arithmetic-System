import asyncio
from mini.worker.workers import Worker
from ..models.worker_models import CalculatorInput, CalculatorOutput
from ..config import BROKER, RESULT_BACKEND
from ..constants.constants import SUB_TASKS_TOPIC

class SubWorker(Worker[CalculatorInput, CalculatorOutput]):
    Input = CalculatorInput
    Output = CalculatorOutput

    async def process(self, input_obj: CalculatorInput) -> CalculatorOutput:
        if input_obj.current_value is not None:
            if input_obj.is_left_fixed:
                result = input_obj.x - input_obj.current_value
            else:
                result = input_obj.current_value - input_obj.y
        else:
            result = input_obj.x - input_obj.y

        return CalculatorOutput(result=result)

async def main():

    sub_worker = SubWorker(BROKER, SUB_TASKS_TOPIC, RESULT_BACKEND)
    await sub_worker.arun()

if __name__ == "__main__":
    asyncio.run(main())