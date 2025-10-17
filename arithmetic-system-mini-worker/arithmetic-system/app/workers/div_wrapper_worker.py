from mini.worker.workers import Worker
from ..models.worker_models import ChainLinkInput, NumberOutput


class DivWrapperWorker(Worker[ChainLinkInput, NumberOutput]):
    Input = ChainLinkInput
    Output = NumberOutput

    async def process(self, input_obj: ChainLinkInput) -> NumberOutput:
        if input_obj.next_operand == 0:
            raise ValueError("Division by zero")
        result = input_obj.result / input_obj.next_operand
        print(f"DIV_WRAPPER: {input_obj.result} / {input_obj.next_operand} = {result}")
        return NumberOutput(result=result)

    async def before_start(self, input_obj: ChainLinkInput) -> None:
        print(f"[DIV_WRAPPER] Starting: {input_obj}")

    async def on_success(self, input_obj: ChainLinkInput, result: NumberOutput) -> None:
        print(f"[DIV_WRAPPER] Success: {result.result}")

    async def on_failure(self, input_obj: ChainLinkInput, exc: Exception) -> None:
        print(f"[DIV_WRAPPER] Failed: {exc}")