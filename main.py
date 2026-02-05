#!/usr/bin/env python3
"""
Главный файл запуска LoTW Consumer
"""

import argparse
from consumer import LoTWConsumer
from config import (
    RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_QUEUE, RABBITMQ_USER,
    DB_HOST, DB_PORT, DB_NAME, DB_SCHEMA
)

def print_config():
    """Вывод конфигурации"""
    print("\n" + "="*60)
    print("⚙️  НАСТРОЙКИ КОНСЬЮМЕРА LOTW")
    print("="*60)
    print(f"Конфиг файл: config.py")
    print(f"RabbitMQ: {RABBITMQ_HOST}:{RABBITMQ_PORT}")
    print(f"Очередь: {RABBITMQ_QUEUE}")
    print(f"Пользователь: {RABBITMQ_USER}")
    print(f"База данных: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print(f"Схема: {DB_SCHEMA}")
    print("="*60)
    print("⚠️  ВАЖНО: Поле id таблицы tlog_qso требует явного UUID")
    print("✅ Consumer генерирует UUID для новых записей")
    print("="*60 + "\n")

def main():
    """Основная функция запуска"""
    parser = argparse.ArgumentParser(
        description='LoTW Sync Consumer - обработчик задач синхронизации',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  %(prog)s                    # Запуск с настройками из config.py
  %(prog)s --workers 3        # 3 параллельных процесса
  %(prog)s --test             # Тестовый режим (имитация работы)
  %(prog)s --stats            # Показать настройки и выйти

Функциональность:
  1. Получает данные из LoTW API
  2. Парсит ADIF формат
  3. Находит user_id по username в auth_user
  4. Проверяет существующие QSO в базе (время +/- 5 минут)
  5. Добавляет новые QSO или обновляет существующие
  6. Генерирует UUID для новых записей
        """
    )

    parser.add_argument('--workers', type=int, default=1,
                       help='Количество параллельных процессов (по умолчанию: 1)')
    parser.add_argument('--test', action='store_true',
                       help='Тестовый режим (имитация обработки)')
    parser.add_argument('--stats', action='store_true',
                       help='Показать настройки и статистику')

    args = parser.parse_args()

    print_config()

    consumer = LoTWConsumer(
        max_workers=args.workers,
        test_mode=args.test
    )

    if args.stats:
        consumer.print_stats(detailed=True)
    else:
        consumer.start_consuming()

if __name__ == "__main__":
    main()