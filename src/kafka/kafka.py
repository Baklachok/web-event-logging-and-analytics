import json
import time

from aiokafka import AIOKafkaProducer
from aiokafka.errors import RequestTimedOutError, KafkaConnectionError

from src.utils.logging import get_logger

logger = get_logger("kafka")


class KafkaProducer:
    def __init__(
        self,
        bootstrap_servers,
        metrics,
        topic: str,
        acks="all",
        enable_idempotence=True,
    ):
        if enable_idempotence and acks != "all":
            raise ValueError(
                f"enable_idempotence=True requires acks='all', got acks={acks!r}"
            )

        self.metrics = metrics
        self.topic = topic  # ← фиксируем при старте
        self.producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            acks=acks,
            enable_idempotence=enable_idempotence,
            key_serializer=lambda k: str(k).encode("utf-8"),
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode(
                "utf-8"
            ),
        )

    async def start(self):
        await self.producer.start()

    async def stop(self):
        await self.producer.stop()

    async def publish(self, event: dict):
        start = time.monotonic()
        user_id = event.get("user_id")
        try:
            metadata = await self.producer.send_and_wait(
                self.topic,
                value=event,
                key=user_id,  # ← всегда в self.topic
            )
            latency = time.monotonic() - start
            self.metrics.inc_sent()
            self.metrics.add_latency(latency)
            logger.info(
                f"Сообщение записано в топик {metadata.topic}, партиция {metadata.partition}"
            )
            return metadata
        except (RequestTimedOutError, KafkaConnectionError) as e:
            logger.error(f"Ошибка репликации/сети при acks=all: {e}")
            raise
        except Exception as e:
            logger.error(f"Непредвиденная ошибка при публикации: {e}")
            raise
