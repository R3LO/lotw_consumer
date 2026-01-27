#!/usr/bin/env python3
"""
Настройка RabbitMQ очередей с Dead Letter Exchange (DLX)
Создает основную очередь и отложенную очередь для повторных попыток
"""

import pika
from config import (
    RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASSWORD,
    RABBITMQ_QUEUE, RABBITMQ_EXCHANGE,
    RABBITMQ_DELAYED_QUEUE, RABBITMQ_DELAYED_EXCHANGE, RABBITMQ_DLX_EXCHANGE,
    RETRY_DELAY_MS
)


def setup_rabbitmq():
    """Настройка очередей RabbitMQ с DLX"""

    print("=" * 60)
    print(" НАСТРОЙКА RABBITMQ ОЧЕРЕДЕЙ")
    print("=" * 60)

    try:
        # Подключение
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        parameters = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=credentials,
            connection_attempts=3,
            retry_delay=5
        )

        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        print(f"Подключено к RabbitMQ: {RABBITMQ_HOST}:{RABBITMQ_PORT}")
        print()

        # 1. Создаем DLX Exchange (для перенаправления из отложенной очереди)
        print("1. Создание DLX Exchange...")
        channel.exchange_declare(
            exchange=RABBITMQ_DLX_EXCHANGE,
            exchange_type='direct',
            durable=True
        )
        print(f"   [OK] {RABBITMQ_DLX_EXCHANGE}")

        # 2. Создаем основную очередь с привязкой к DLX
        print("2. Создание основной очереди...")
        channel.queue_declare(
            queue=RABBITMQ_QUEUE,
            durable=True,
            arguments={
                'x-dead-letter-exchange': RABBITMQ_DLX_EXCHANGE,
                'x-dead-letter-routing-key': RABBITMQ_QUEUE
            }
        )
        print(f"   [OK] {RABBITMQ_QUEUE}")

        # 3. Привязываем основную очередь к основному exchange
        print("3. Создание основного exchange...")
        channel.exchange_declare(
            exchange=RABBITMQ_EXCHANGE,
            exchange_type='direct',
            durable=True
        )
        print(f"   [OK] {RABBITMQ_EXCHANGE}")

        # Привязка очереди к exchange
        channel.queue_bind(
            queue=RABBITMQ_QUEUE,
            exchange=RABBITMQ_EXCHANGE,
            routing_key=RABBITMQ_QUEUE
        )
        print(f"   [OK] Привязка {RABBITMQ_QUEUE} -> {RABBITMQ_EXCHANGE}")

        # 4. Создаем отложенную очередь с TTL
        print("4. Создание отложенной очереди...")
        channel.queue_declare(
            queue=RABBITMQ_DELAYED_QUEUE,
            durable=True,
            arguments={
                'x-dead-letter-exchange': RABBITMQ_DLX_EXCHANGE,
                'x-dead-letter-routing-key': RABBITMQ_QUEUE,
                'x-message-ttl': RETRY_DELAY_MS  # 45 минут
            }
        )
        print(f"   [OK] {RABBITMQ_DELAYED_QUEUE} (TTL: {RETRY_DELAY_MS} мс)")

        # 5. Создаем delayed exchange для отправки сообщений в отложенную очередь
        print("5. Создание Delayed Exchange...")
        channel.exchange_declare(
            exchange=RABBITMQ_DELAYED_EXCHANGE,
            exchange_type='direct',
            durable=True
        )
        print(f"   [OK] {RABBITMQ_DELAYED_EXCHANGE}")

        # Привязка delayed exchange к отложенной очереди
        channel.queue_bind(
            queue=RABBITMQ_DELAYED_QUEUE,
            exchange=RABBITMQ_DELAYED_EXCHANGE,
            routing_key='delayed'
        )
        print(f"   [OK] Привязка {RABBITMQ_DELAYED_QUEUE} -> {RABBITMQ_DELAYED_EXCHANGE}")

        # 6. Привязка DLX exchange к основной очереди
        print("6. Привязка DLX...")
        channel.queue_bind(
            queue=RABBITMQ_QUEUE,
            exchange=RABBITMQ_DLX_EXCHANGE,
            routing_key=RABBITMQ_QUEUE
        )
        print(f"   [OK] {RABBITMQ_QUEUE} -> {RABBITMQ_DLX_EXCHANGE}")

        connection.close()

        print()
        print("=" * 60)
        print(" НАСТРОЙКА ЗАВЕРШЕНА")
        print("=" * 60)
        print()
        print("Структура очередей:")
        print(f"  {RABBITMQ_EXCHANGE} -> {RABBITMQ_QUEUE} (основная очередь)")
        print(f"  {RABBITMQ_DELAYED_EXCHANGE} -> {RABBITMQ_DELAYED_QUEUE} (отложенная, TTL={RETRY_DELAY_MS}мс)")
        print(f"  {RABBITMQ_DLX_EXCHANGE} -> {RABBITMQ_QUEUE} (возврат из отложенной)")
        print()
        print("При ошибке API:")
        print(f"  1. Сообщение отправляется в {RABBITMQ_DELAYED_EXCHANGE}")
        print(f"  2. Попадает в {RABBITMQ_DELAYED_QUEUE}")
        print(f"  3. Через {RETRY_DELAY_MS}мс перенаправляется в {RABBITMQ_QUEUE}")
        print("=" * 60)

        return True

    except Exception as e:
        print(f"[ERROR] Ошибка настройки: {e}")
        return False


def delete_queues():
    """Удаление существующих очередей"""
    print("=" * 60)
    print(" УДАЛЕНИЕ ОЧЕРЕДЕЙ")
    print("=" * 60)

    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        parameters = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=credentials
        )

        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        queues_to_delete = [RABBITMQ_QUEUE, RABBITMQ_DELAYED_QUEUE]
        exchanges_to_delete = [RABBITMQ_EXCHANGE, RABBITMQ_DELAYED_EXCHANGE, RABBITMQ_DLX_EXCHANGE]

        # Удаляем очереди
        for queue in queues_to_delete:
            try:
                channel.queue_delete(queue=queue)
                print(f"   [OK] Удалена очередь: {queue}")
            except Exception as e:
                print(f"   [WARN] Не удалось удалить очередь {queue}: {e}")

        # Удаляем exchanges
        for exchange in exchanges_to_delete:
            try:
                channel.exchange_delete(exchange=exchange)
                print(f"   [OK] Удален exchange: {exchange}")
            except Exception as e:
                print(f"   [WARN] Не удалось удалить exchange {exchange}: {e}")

        connection.close()
        print()
        print("Очереди удалены")
        return True

    except Exception as e:
        print(f"[ERROR] Ошибка: {e}")
        return False


def check_queues():
    """Проверка существующих очередей"""

    print("=" * 60)
    print(" ПРОВЕРКА ОЧЕРЕДЕЙ")
    print("=" * 60)

    try:
        credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
        parameters = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=credentials
        )

        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        queues = [RABBITMQ_QUEUE, RABBITMQ_DELAYED_QUEUE]

        for queue in queues:
            try:
                result = channel.queue_declare(queue=queue, passive=True)
                print(f"  {queue}: {result.method.message_count} сообщений")
            except Exception:
                print(f"  {queue}: не существует")

        connection.close()

    except Exception as e:
        print(f"[ERROR] Ошибка проверки: {e}")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        if sys.argv[1] == '--delete':
            delete_queues()
        elif sys.argv[1] == '--check':
            check_queues()
        else:
            print("Использование:")
            print("  python setup_rabbitmq.py          - создать очереди")
            print("  python setup_rabbitmq.py --delete - удалить очереди")
            print("  python setup_rabbitmq.py --check  - проверить очереди")
    else:
        setup_rabbitmq()