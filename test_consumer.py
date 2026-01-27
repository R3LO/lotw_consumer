#!/usr/bin/env python3
"""
Test consumer - Получает и выводит сообщения из очереди RabbitMQ
"""

import pika
import json
import sys
import signal
from datetime import datetime

from config import (
    RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_QUEUE,
    RABBITMQ_USER, RABBITMQ_PASSWORD
)


class TestConsumer:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.running = True
        self.message_count = 0

    def connect(self) -> bool:
        """Подключение к RabbitMQ"""
        try:
            print("\n" + "=" * 60)
            print(" ПОДКЛЮЧЕНИЕ К RABBITMQ")
            print("=" * 60)
            print(f"   Хост: {RABBITMQ_HOST}:{RABBITMQ_PORT}")
            print(f"   Пользователь: {RABBITMQ_USER}")
            print(f"   Очередь: {RABBITMQ_QUEUE}")
            print()

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

            # Объявляем очередь
            self.channel.queue_declare(
                queue=RABBITMQ_QUEUE,
                durable=True
            )

            # Настраиваем QoS
            self.channel.basic_qos(prefetch_count=1)

            print("[OK] Успешно подключено к RabbitMQ")
            print(f"[OK] Ожидаю сообщений из очереди: {RABBITMQ_QUEUE}")
            print()
            return True

        except Exception as e:
            print(f"[ERROR] Ошибка подключения: {e}")
            return False

    def on_message(self, ch, method, properties, body):
        """Обработка входящего сообщения"""
        self.message_count += 1

        print("=" * 60)
        print(f" СООБЩЕНИЕ #{self.message_count}")
        print("=" * 60)

        try:
            # Пытаемся распарсить JSON
            data = json.loads(body.decode('utf-8'))
            print("Формат: JSON")
            print()

            for key, value in data.items():
                if key == 'password':
                    # Скрываем пароль
                    if isinstance(value, str) and len(value) > 3:
                        print(f"  {key}: {value[:3]}***")
                    else:
                        print(f"  {key}: ***")
                else:
                    print(f"  {key}: {value}")

        except json.JSONDecodeError:
            print("Формат: Raw (не JSON)")
            print(f"  Тело: {body.decode('utf-8', errors='replace')}")

        print()
        print(f"  Получено в: {datetime.now().isoformat()}")
        print()

        # Сообщение автоматически подтверждено (auto_ack=True)

    def start(self):
        """Запуск прослушивания очереди"""
        if not self.connect():
            print("Не удалось подключиться к RabbitMQ")
            sys.exit(1)

        print("=" * 60)
        print(" ОЖИДАНИЕ СООБЩЕНИЙ...")
        print(" (Нажмите Ctrl+C для остановки)")
        print("=" * 60)
        print()

        try:
            self.channel.basic_consume(
                queue=RABBITMQ_QUEUE,
                on_message_callback=self.on_message,
                auto_ack=True
            )

            self.channel.start_consuming()

        except KeyboardInterrupt:
            print("\n\nОстановлено пользователем")
        except Exception as e:
            print(f"Ошибка: {e}")
        finally:
            self.stop()

    def stop(self):
        """Остановка"""
        self.running = False
        if self.channel and self.channel.is_open:
            self.channel.stop_consuming()
        if self.connection and self.connection.is_open:
            self.connection.close()

        print("\n" + "=" * 60)
        print(" СТАТИСТИКА")
        print("=" * 60)
        print(f"  Всего сообщений получено: {self.message_count}")
        print("=" * 60)


def signal_handler(signum, frame):
    """Обработчик сигналов"""
    print("\nПолучен сигнал завершения...")
    sys.exit(0)


if __name__ == "__main__":
    # Обработчик сигналов
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    consumer = TestConsumer()
    consumer.start()