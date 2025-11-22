import asyncio
from src.rabbit.rabbit import RabbitMQ


async def process_message(message):
    print("Received:", message.body.decode())


async def main():
    rabbit = RabbitMQ("amqp://guest:guest@rabbitmq:5672/")
    await rabbit.connect()
    await rabbit.consume(process_message)


if __name__ == "__main__":
    asyncio.run(main())
