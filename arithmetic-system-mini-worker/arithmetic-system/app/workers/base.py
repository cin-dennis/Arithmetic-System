from mini.worker.workers.base import Worker as PackageWorker
from mini.worker.models import Message
from mini.worker.exc import (
    MalformedMessageError,
    NodeNotFoundError,
    OperationalError,
)
from pydantic import ValidationError
from mini.logger import get_logger
from mini.models import BaseModel

logger = get_logger(__name__)


class AppBaseWorker[T: BaseModel, RT: BaseModel](PackageWorker[T, RT]):

    async def on_message(self, message_bytes: bytes) -> None:
        await logger.ainfo("Received message from topic (using OVERRIDDEN on_message)", topic=self.topic)

        try:
            logger.info(f"Received message from topic (using OVERRIDDEN on_message)")
            message = Message.model_validate_json(message_bytes)
            input_obj = self.Input.model_validate(
                message.data
            )

        except Exception as exc:
            logger.exception("FATAL: Failed to validate or parse message body")
            raise MalformedMessageError(exc) from exc

        try:
            logger.info(f"Processing message ID: {message.id}")
            await self.before_start(input_obj=input_obj)
            result = self.Output.model_validate(
                await self.process(input_obj),
                from_attributes=True,
            )
            logger.info(f"Processed message ID: {message.id} with result: {result}")
            await self._check_next_step(message.id, result)
            logger.info(f"Processed message ID: {message.id} with result: {result}")
            await self.on_success(input_obj, result)
            logger.info(f"Successfully completed message ID: {message.id}")
        except ValidationError as err:
            await self.on_failure(input_obj, MalformedMessageError(err))
        except NodeNotFoundError as node_err:
            await self.on_failure(input_obj, node_err)
        except Exception as exc:
            logger.exception("Error processing message")
            await self.on_failure(input_obj, OperationalError(exc))