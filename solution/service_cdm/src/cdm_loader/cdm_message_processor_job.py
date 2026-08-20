from datetime import datetime
from logging import Logger
from uuid import UUID

from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository


class CdmMessageProcessor:

    def __init__(
        self,
        consumer: KafkaConsumer,
        cdm_repository: CdmRepository,
        logger: Logger,
    ) -> None:
        self._consumer = consumer
        self._cdm_repository = cdm_repository
        self._logger = logger
        self._batch_size = 100

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()

            if not msg:
                break

            self._logger.info(
                f"{datetime.utcnow()}: Message received: {msg['object_id']}"
            )

            order = msg["payload"]

            user_id = UUID(order["user"]["id"])

            for product in order["products"]:
                product_id = UUID(product["id"])

                category_id = UUID(product["category"]["id"])
                category_name = product["category"]["name"]

                product_name = product["name"]

                self._cdm_repository.user_product_counter_insert(
                    user_id=user_id,
                    product_id=product_id,
                    product_name=product_name,
                )

                self._cdm_repository.user_category_counter_insert(
                    user_id=user_id,
                    category_id=category_id,
                    category_name=category_name,
                )

            self._logger.info(
                f"{datetime.utcnow()}: CDM loaded for order {msg['object_id']}"
            )

        self._logger.info(f"{datetime.utcnow()}: FINISH")