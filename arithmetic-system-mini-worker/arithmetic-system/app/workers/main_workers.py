import asyncio
from ..config import BROKER, RESULT_BACKEND
from ..constants import (
    ADD_TASKS_TOPIC,
    SUB_TASKS_TOPIC,
    MUL_TASKS_TOPIC,
    DIV_TASKS_TOPIC,
    XSUM_TASKS_TOPIC,
    XPROD_TASKS_TOPIC,
)

from . import (
    AddWorker,
    SubWorker,
    MulWorker,
    DivWorker,
    XSumWorker,
    XProdWorker,
    AddWrapperWorker
)


async def start_all_workers():
    add_worker = AddWorker(BROKER, ADD_TASKS_TOPIC, RESULT_BACKEND)
    sub_worker = SubWorker(BROKER, SUB_TASKS_TOPIC, RESULT_BACKEND)
    mul_worker = MulWorker(BROKER, MUL_TASKS_TOPIC, RESULT_BACKEND)
    div_worker = DivWorker(BROKER, DIV_TASKS_TOPIC, RESULT_BACKEND)
    xsum_worker = XSumWorker(BROKER, XSUM_TASKS_TOPIC, RESULT_BACKEND)
    xprod_worker = XProdWorker(BROKER, XPROD_TASKS_TOPIC, RESULT_BACKEND)

    add_wrapper = AddWrapperWorker(BROKER, ADD_TASKS_TOPIC, RESULT_BACKEND)


    await asyncio.gather(
        add_worker.arun(),
        sub_worker.arun(),
        mul_worker.arun(),
        div_worker.arun(),
        xsum_worker.arun(),
        xprod_worker.arun(),
        add_wrapper.arun(),
    )


def main():
    asyncio.run(start_all_workers())


if __name__ == "__main__":
    main()