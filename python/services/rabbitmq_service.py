import json
import os
import aio_pika
import logging
from typing import Optional, Dict, Any

logger = logging.getLogger("marketsentinel.rabbitmq")

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")

connection: Optional[aio_pika.RobustConnection] = None
channel: Optional[aio_pika.RobustChannel] = None
exchange: Optional[aio_pika.RobustExchange] = None

async def init_rabbitmq():
    global connection, channel, exchange
    try:
        connection = await aio_pika.connect_robust(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            login=RABBITMQ_USER,
            password=RABBITMQ_PASSWORD
        )
        channel = await connection.channel()
        exchange = await channel.declare_exchange(
            "marketsentinel.topic",
            aio_pika.ExchangeType.TOPIC,
            durable=True
        )

        # Queues & Bindings
        dlq = await channel.declare_queue("marketsentinel.dlq", durable=True)
        anomalies_q = await channel.declare_queue("marketsentinel.anomalies.queue", durable=True)
        await anomalies_q.bind(exchange, routing_key="market.anomalies.*")

        dropouts_q = await channel.declare_queue("marketsentinel.dropouts.queue", durable=True)
        await dropouts_q.bind(exchange, routing_key="market.dropouts.*")

        telemetry_q = await channel.declare_queue("marketsentinel.telemetry.queue", durable=True)
        await telemetry_q.bind(exchange, routing_key="market.telemetry.*")

        logger.info("RabbitMQ exchange 'marketsentinel.topic' declared successfully.")
    except Exception as e:
        logger.warning(f"RabbitMQ connection warning: {e}. Running in resilient fallback mode.")

async def publish_event(routing_key: str, event: Dict[str, Any]):
    global exchange
    if exchange is not None:
        try:
            message = aio_pika.Message(
                body=json.dumps(event).encode("utf-8"),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                content_type="application/json"
            )
            await exchange.publish(message, routing_key=routing_key)
        except Exception as e:
            logger.error(f"Failed to publish to RabbitMQ: {e}")

async def close_rabbitmq():
    global connection, channel
    if channel and not channel.is_closed:
        try:
            await channel.close()
        except Exception:
            pass
    if connection and not connection.is_closed:
        try:
            await connection.close()
        except Exception:
            pass
