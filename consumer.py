"""
Основной класс консьюмера
"""

import time
import threading
from typing import Dict, Any

from utils.logger import setup_logging
from utils.signals import setup_signal_handlers
from utils.stats import Statistics
from rabbitmq.connection import RabbitMQConnection
from lotw.handler import MessageHandler
from lotw.api import LoTWAPI
from database.operations import DatabaseOperations


class LoTWConsumer:
    def __init__(self, max_workers: int = 1, test_mode: bool = False):
        """
        Инициализация консьюмера

        Args:
            max_workers: максимальное количество параллельных задач
            test_mode: режим тестирования (без реального RabbitMQ)
        """
        self.max_workers = max_workers
        self.test_mode = test_mode
        self.running = True
        self.rabbitmq = None

        # Инициализация компонентов
        self.logger = setup_logging()
        self.stats = Statistics(test_mode)
        self.db_ops = DatabaseOperations(self.logger)
        self.lotw_api = LoTWAPI(self.logger)
        self.message_handler = MessageHandler(
            logger=self.logger,
            stats=self.stats,
            db_ops=self.db_ops,
            lotw_api=self.lotw_api,
            max_retries=5  # Из конфига
        )

        # Настройка
        setup_signal_handlers(self)

    def signal_handler(self, signum, frame):
        """Обработчик сигналов остановки"""
        from utils.signals import get_signal_name
        signal_name = get_signal_name(signum)
        self.logger.info(f"Получен сигнал {signal_name}, завершаю работу...")
        self.running = False
        # Останавливаем потребление сообщений
        if hasattr(self, 'rabbitmq') and self.rabbitmq:
            self.rabbitmq.close()
        self.close_connections()

    def close_connections(self):
        """Корректное закрытие всех соединений"""
        self.logger.info("Закрываю соединения...")
        # Закрытие соединений будет в отдельных классах

    def process_test_tasks(self):
        """Обработка тестовых задач"""
        self.logger.info("Тестовый режим - обработка тестовых задач")

        test_tasks = [
            {
                'task_id': 'test_001',
                'callsign': 'UA1ABC',
                'username': 'R3LO',
                'password': 'test_pass1',
                'user_id': 1
            }
        ]

        for task in test_tasks:
            if not self.running:
                break
            self.message_handler.process_task(task)
            time.sleep(1)

        self.print_stats()

    def start_consuming(self):
        """Запуск прослушивания очереди"""
        if self.test_mode:
            self.process_test_tasks()
            return

        # Инициализация RabbitMQ
        self.rabbitmq = RabbitMQConnection(
            logger=self.logger,
            max_workers=self.max_workers
        )

        if not self.rabbitmq.connect():
            self.logger.error("Не удалось подключиться к RabbitMQ")
            return

        try:
            self.logger.info(f"LoTW Consumer запущен и готов к работе")
            self.logger.info("Ожидание задач синхронизации... (Ctrl+C для остановки)")

            # Запуск потока статистики
            def stats_timer():
                while self.running:
                    time.sleep(60)
                    if self.running:
                        self.print_stats()

            stats_thread = threading.Thread(target=stats_timer, daemon=True)
            stats_thread.start()

            # Запуск прослушивания
            self.rabbitmq.start_consuming(
                message_handler=self.message_handler.handle_delivery,
                stats_callback=self.stats.update_worker_count
            )

        except KeyboardInterrupt:
            self.logger.info("\nОстановлено пользователем")
        except Exception as e:
            self.logger.error(f"Ошибка в основном цикле: {e}")
        finally:
            self.rabbitmq.close()
            self.print_stats(detailed=True)

    def print_stats(self, detailed: bool = False):
        """Вывод статистики"""
        self.stats.print_stats(detailed=detailed)