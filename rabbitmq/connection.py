"""
Модуль для работы с RabbitMQ
"""

import pika
import socket
from typing import Callable, Optional

from config import (
    RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_QUEUE,
    RABBITMQ_USER, RABBITMQ_PASSWORD
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

            if not self.check_connectivity():
                return False

            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
            parameters = pika.ConnectionParameters(
                host=RABBITMQ_HOST,
                port=RABBITMQ_PORT,
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300,
                connection_attempts=3,
                retry_delay=5
            )

            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()

            # Объявляем основную очередь
            self.channel.queue_declare(
                queue=RABBITMQ_QUEUE,
                durable=True
            )

            # Объявляем очередь ошибок
            dlq_name = f"{RABBITMQ_QUEUE}_dlq"
            self.channel.queue_declare(
                queue=dlq_name,
                durable=True
            )
            self.logger.debug(f"Очередь ошибок: {dlq_name}")

            # Настраиваем QoS
            self.channel.basic_qos(prefetch_count=self.max_workers)

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