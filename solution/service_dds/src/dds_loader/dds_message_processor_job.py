from datetime import datetime
from logging import Logger

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from dds_loader.repository.dds_repository import DdsRepository


class DdsMessageProcessor:
    def __init__(
        self,
        consumer: KafkaConsumer,
        producer: KafkaProducer,
        dds_repository: DdsRepository,
        batch_size: int,
        logger: Logger,
    ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._dds_repository = dds_repository
        self._logger = logger
        self._batch_size = batch_size

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

            load_dt = datetime.utcnow()
            load_src = "stg-service-orders"

            # Хабы
            # User
            user_id = order["user"]["id"]
            h_user_pk = self._dds_repository.h_user_insert(
                user_id=user_id,
                load_dt=load_dt,
                load_src=load_src,
            )

            # Restaurant
            restaurant_id = order["restaurant"]["id"]
            h_restaurant_pk = self._dds_repository.h_restaurant_insert(
                restaurant_id=restaurant_id,
                load_dt=load_dt,
                load_src=load_src,
            )

            # Order
            order_id = order["id"]
            order_dt = datetime.strptime(
                order["date"],
                "%Y-%m-%d %H:%M:%S",
            )

            h_order_pk = self._dds_repository.h_order_insert(
                order_id=order_id,
                order_dt=order_dt,
                load_dt=load_dt,
                load_src=load_src,
            )

            self._logger.info(
                f"{datetime.utcnow()}: Hubs loaded for order {order_id}"
            )

            # Линки

            products_for_cdm = []

            # Продукты + категории
            for product in order["products"]:
                product_id = product["id"]
                category_name = product["category"]

                h_product_pk = self._dds_repository.h_product_insert(
                    product_id=product_id,
                    load_dt=load_dt,
                    load_src=load_src,
                )

                h_category_pk = self._dds_repository.h_category_insert(
                    category_name=category_name,
                    load_dt=load_dt,
                    load_src=load_src,
                )

                # Линки продукта
                self._dds_repository.l_order_product_insert(
                    h_order_pk=h_order_pk,
                    h_product_pk=h_product_pk,
                    load_dt=load_dt,
                    load_src=load_src,
                )

                self._dds_repository.l_product_category_insert(
                    h_product_pk=h_product_pk,
                    h_category_pk=h_category_pk,
                    load_dt=load_dt,
                    load_src=load_src,
                )

                self._dds_repository.l_product_restaurant_insert(
                    h_product_pk=h_product_pk,
                    h_restaurant_pk=h_restaurant_pk,
                    load_dt=load_dt,
                    load_src=load_src,
                )

                # Сателлит продукта
                self._dds_repository.s_product_names_insert(
                    h_product_pk=h_product_pk,
                    name=product["name"],
                    load_dt=load_dt,
                    load_src=load_src,
                )

                # Данные для CDM
                products_for_cdm.append(
                    {
                        "id": str(h_product_pk),
                        "name": product["name"],
                        "category": {
                            "id": str(h_category_pk),
                            "name": product["category"],
                        },
                    }
                )

            # Линк заказа с пользователем
            self._dds_repository.l_order_user_insert(
                h_order_pk=h_order_pk,
                h_user_pk=h_user_pk,
                load_dt=load_dt,
                load_src=load_src,
            )   

            self._logger.info(
                f"{datetime.utcnow()}: Links loaded for order {order_id}"
            )

            # Сателлиты (Сателлит продукта в линках)

            # Стоимость заказа
            self._dds_repository.s_order_cost_insert(
                h_order_pk=h_order_pk,
                cost=order["cost"],
                payment=order["payment"],
                load_dt=load_dt,
                load_src=load_src,
            )

            # Статус заказов
            self._dds_repository.s_order_status_insert(
                h_order_pk=h_order_pk,
                status=order["status"],
                load_dt=load_dt,
                load_src=load_src,
            )

            # Сателлит ресторана
            self._dds_repository.s_restaurant_names_insert(
                h_restaurant_pk=h_restaurant_pk,
                name=order["restaurant"]["name"],
                load_dt=load_dt,
                load_src=load_src,
            )

            # Сателлит пользователя
            self._dds_repository.s_user_names_insert(
                h_user_pk=h_user_pk,
                username=order["user"]["name"],
                userlogin=order["user"]["login"],
                load_dt=load_dt,
                load_src=load_src,
            )

            # Сообщение для CDM
            dst_msg = {
                "object_id": msg["object_id"],
                "object_type": msg["object_type"],
                "payload": {
                    "user": {
                        "id": str(h_user_pk),
                    },
                "products": products_for_cdm,
                },
            }

            self._producer.produce(dst_msg)

            self._logger.info(
                f"{datetime.utcnow()}: Message sent for order {order_id}"
            )

        self._logger.info(f"{datetime.utcnow()}: FINISH")
