import asyncio
from pydantic import ValidationError
from mini.worker.models import Message
from mini.worker.exc import MalformedMessageError, NodeNotFoundError
from mini.worker.result_backends import Task
from mini.worker.workers import Worker
from pydantic import BaseModel
from .common import BROKER, RESULT_BACKEND
from ..models.worker_models import CalculatorInput, CalculatorOutput

OPERATION_TOPIC_MAP = {
    "add": "add_tasks",
    "sub": "sub_tasks",
    "mul": "mul_tasks",
    "div": "div_tasks",
}

class CombinerInput(BaseModel):
    previous_result: float
    config : "CombinerConfig"

class CombinerConfig(BaseModel):
    fixed_operand: float
    is_left_fixed: bool
    operation_name: str

class CombinerOutput(BaseModel):
    value: float

class CombinerWorker(Worker[BaseModel, CombinerOutput]):
    Input = BaseModel
    Output = BaseModel

    async def process(self, input_obj: BaseModel) -> CombinerOutput:
        pass

    async def on_message(self, message_bytes: bytes) -> None:
        """
        Custom message handler that overrides the default worker behavior.
        1. Parses the result from the previous task in the chain.
        2. Retrieves its own static configuration from the result backend.
        3. Combines them into a new ArithmeticInput.
        4. Dispatches the new task to the correct arithmetic worker queue.
        """
        try:
            # Step 1: Parse the incoming message from the previous task
            incoming_message = Message.model_validate_json(message_bytes)
            previous_result = CalculatorOutput.model_validate_json(incoming_message.body)

            node_id = incoming_message.id

            # Step 2: Get this worker's static configuration from the result backend
            node = await self.result_backend.get_node(node_id)
            if not isinstance(node, Task) or node.input is None:
                raise NodeNotFoundError(f"Combiner node config not found: {node_id}")

            config = CombinerConfig.model_validate_json(node.input)

            # Step 3: Combine dynamic result and static config into a new input
            if config.is_left_fixed:
                final_input = CalculatorInput(x=config.fixed_operand, y=previous_result.value)
            else:
                final_input = CalculatorInput(x=previous_result.value, y=config.fixed_operand)

            # Step 4: Dispatch the new task to the correct worker
            target_topic = OPERATION_TOPIC_MAP.get(config.operation_name)
            if not target_topic:
                raise ValueError(f"No topic found for operation: {config.operation_name}")

            outgoing_message = Message(id=node_id, body=final_input.model_dump_json())

            await self.broker.publish(target_topic, outgoing_message.to_bytes())

        except (ValidationError, NodeNotFoundError, KeyError, ValueError) as exc:
            raise MalformedMessageError(exc) from exc


async def main():
    worker = CombinerWorker(BROKER, "combiner_tasks", RESULT_BACKEND)
    await worker.arun()


if __name__ == "__main__":
    asyncio.run(main())
