import asyncio

from mini.worker.workers import Worker
from pydantic import BaseModel
from .common import BROKER, RESULT_BACKEND

class CombinerInput(BaseModel):
    previous_result: float

class CombinerOutput(BaseModel):
    value: float

class CombinerWorker(Worker[BaseModel, CombinerOutput]):
    Input = BaseModel
    Output = CombinerOutput

    async def process(self, input_obj: BaseModel) -> CombinerOutput:
        pass


async def main():
    worker = CombinerWorker(BROKER, "combiner_tasks", RESULT_BACKEND)
    await worker.arun()


if __name__ == "__main__":
    asyncio.run(main())
