from mini.worker.workers import Worker
from ..models.worker_models import BinaryOperationInput, NumberOutput

class MulWorker(Worker[BinaryOperationInput, NumberOutput]):
    Input = BinaryOperationInput
    Output = NumberOutput

    async def before_start(self, input_obj: BinaryOperationInput) -> None:
        pass

    async def on_success(self, input_obj: BinaryOperationInput, result: NumberOutput) -> None:
        pass

    async def on_failure(self, input_obj: BinaryOperationInput, exc: Exception) -> None:
        pass

    async def process(self, input_obj: BinaryOperationInput) -> NumberOutput:
        if input_obj.result is not None:
            result = input_obj.result * input_obj.y
        else:
            result = input_obj.x * input_obj.y
        return NumberOutput(result=result)

    async def sent_result(self, topic: str, input_obj: NumberOutput) -> None:
        pass
