"""
Модуль для работы с RabbitMQ
"""

import pika
import socket
from typing import Callable, Optional

from config import (
    RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_QUEUE, RABBITMQ_EXCHANGE,
    RABBITMQ_USER, RABBITMQ_PASSWORD,
    RABBITMQ_DELAYED_QUEUE, RABBITMQ_DELAYED_EXCHANGE, RABBITMQ_DLX_EXCHANGE,
    RABBITMQ_HEARTBEAT, RABBITMQ_TIMEOUT
)


class RabbitMQConnection:
    """Класс для управления подключением к RabbitMQ"""

    def __init__(self, logger, max_workers: int = 1):
        self.logger = logger
        self.max_workers = max_workers
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.adapters.blocking_connection.BlockingChannel] = None

    def check_connectivity(self) -> bool:
        """Проверка возможности подключения к RabbitMQ"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((RABBITMQ_HOST, RABBITMQ_PORT))
            sock.close()

            if result == 0:
                self.logger.info(f"Хост {RABBITMQ_HOST}:{RABBITMQ_PORT} доступен")
                return True
            else:
                self.logger.error(f"Хост {RABBITMQ_HOST}:{RABBITMQ_PORT} недоступен")
                return False

        except Exception as e:
            self.logger.error(f"Ошибка при проверке подключения: {e}")
            return False

    def connect(self) -> bool:
        """Настройка подключения к RabbitMQ"""
        try:
            self.logger.info(f"Попытка подключения к RabbitMQ...")
            self.logger.info(f"   Хост: {RABBITMQ_HOST}:{RABBITMQ_PORT}")
            self.logger.info(f"   Пользователь: {RABBITMQ_USER}")
            self.logger.info(f"   Очередь: {RABBITMQ_QUEUE}")
            self.logger.info(f"   Exchange: {RABBITMQ_EXCHANGE}")

            if not self.check_connectivity():
                return False

            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
            parameters = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                credentials=credentials,
                heartbeat=RABBITMQ_HEARTBEAT,
                blocked_connection_timeout=RABBITMQ_TIMEOUT,
                connection_attempts=3,
                retry_delay=5
            )

            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()

            # 1. Создаем DLX Exchange (для перенаправления из отложенной очереди)
            self.channel.exchange_declare(
                exchange=RABBITMQ_DLX_EXCHANGE,
                exchange_type='direct',
                durable=True
            )
            self.logger.debug(f"Создан DLX Exchange: {RABBITMQ_DLX_EXCHANGE}")

            # 2. Создаем основную очередь с привязкой к DLX
            self.channel.queue_declare(
                queue=RABBITMQ_QUEUE,
                durable=True,
                arguments={
                    'x-dead-letter-exchange': RABBITMQ_DLX_EXCHANGE,
                    'x-dead-letter-routing-key': RABBITMQ_QUEUE
                }
            )
            self.logger.debug(f"Создана очередь: {RABBITMQ_QUEUE}")

            # 3. Создаем основной exchange
            self.channel.exchange_declare(
                exchange=RABBITMQ_EXCHANGE,
                exchange_type='direct',
                durable=True
            )
            self.logger.debug(f"Создан Exchange: {RABBITMQ_EXCHANGE}")

            # 4. Привязываем основную очередь к основному exchange
            self.channel.queue_bind(
                queue=RABBITMQ_QUEUE,
                exchange=RABBITMQ_EXCHANGE,
                routing_key=RABBITMQ_QUEUE
            )
            self.logger.debug(f"Привязка: {RABBITMQ_QUEUE} -> {RABBITMQ_EXCHANGE}")

            # 5. Создаем отложенную очередь с TTL
            from config import RETRY_DELAY_MS
            self.channel.queue_declare(
                queue=RABBITMQ_DELAYED_QUEUE,
                durable=True,
                arguments={
                    'x-dead-letter-exchange': RABBITMQ_DLX_EXCHANGE,
                    'x-dead-letter-routing-key': RABBITMQ_QUEUE,
                    'x-message-ttl': RETRY_DELAY_MS
                }
            )
            self.logger.debug(f"Создана отложенная очередь: {RABBITMQ_DELAYED_QUEUE}")

            # 6. Создаем delayed exchange
            self.channel.exchange_declare(
                exchange=RABBITMQ_DELAYED_EXCHANGE,
                exchange_type='direct',
                durable=True
            )
            self.logger.debug(f"Создан Delayed Exchange: {RABBITMQ_DELAYED_EXCHANGE}")

            # 7. Привязываем delayed exchange к отложенной очереди
            self.channel.queue_bind(
                queue=RABBITMQ_DELAYED_QUEUE,
                exchange=RABBITMQ_DELAYED_EXCHANGE,
                routing_key='delayed'
            )
            self.logger.debug(f"Привязка: {RABBITMQ_DELAYED_QUEUE} -> {RABBITMQ_DELAYED_EXCHANGE}")

            # 8. Привязываем DLX exchange к основной очереди
            self.channel.queue_bind(
                queue=RABBITMQ_QUEUE,
                exchange=RABBITMQ_DLX_EXCHANGE,
                routing_key=RABBITMQ_QUEUE
            )
            self.logger.debug(f"Привязка DLX: {RABBITMQ_QUEUE} -> {RABBITMQ_DLX_EXCHANGE}")

            # Настраиваем QoS - обрабатываем по 1 сообщению за раз
            self.channel.basic_qos(prefetch_count=1)

            self.logger.info(f"Успешно подключено к RabbitMQ")
            self.logger.info(f"Прослушиваю очередь: {RABBITMQ_QUEUE}")
            self.logger.info(f"Максимум воркеров: {self.max_workers}")

            return True

        except pika.exceptions.AMQPConnectionError as e:
            self.logger.error(f"Ошибка соединения AMQP: {e}")
            return False
        except pika.exceptions.AuthenticationError as e:
            self.logger.error(f"Ошибка аутентификации: {e}")
            return False
        except Exception as e:
            self.logger.error(f"Неожиданная ошибка при подключении: {e}")
            return False

    def start_consuming(self, message_handler: Callable, stats_callback: Optional[Callable] = None):
        """Запуск прослушивания очереди"""
        if not self.channel or not self.connection:
            self.logger.error("Соединение не установлено")
            return

        try:
            self.channel.basic_consume(
                queue=RABBITMQ_QUEUE,
                on_message_callback=message_handler,
                auto_ack=False
            )

            self.logger.info("Начинаю прослушивание очереди...")
            self.channel.start_consuming()

        except Exception as e:
            self.logger.error(f"Ошибка при прослушивании очереди: {e}")
            raise

    def close(self):
        """Корректное закрытие соединения"""
        try:
            if self.channel and self.channel.is_open:
                self.channel.stop_consuming()
                self.channel.close()
            if self.connection and self.connection.is_open:
                self.connection.close()
            self.logger.info("Соединение с RabbitMQ закрыто")
        except Exception as e:
            self.logger.error(f"Ошибка при закрытии соединения: {e}")