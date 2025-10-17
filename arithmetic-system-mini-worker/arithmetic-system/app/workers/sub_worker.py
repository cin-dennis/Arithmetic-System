from ..models.worker_models import BinaryOperationInput, NumberOutput
from mini.worker.workers import Worker

class SubWorker(Worker[BinaryOperationInput, NumberOutput]):
    Input = BinaryOperationInput
    Output = NumberOutput

    async def before_start(self, input_obj: BinaryOperationInput) -> None:
        pass

    async def on_success(self, input_obj: BinaryOperationInput, result: NumberOutput) -> None:
        pass

    async def on_failure(self, input_obj: BinaryOperationInput, exc: Exception) -> None:
        pass

    async def process(self, input_obj: BinaryOperationInput) -> NumberOutput:
        result = input_obj.a - input_obj.b
        print(f"SUB: {input_obj.a} - {input_obj.b} = {result}")
        return NumberOutput(result=result)

    async def sent_result(self, topic: str, input_obj: NumberOutput) -> None:
        pass
