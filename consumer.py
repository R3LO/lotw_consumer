"""
Основной класс консьюмера
"""

import time
import sys
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

        self.logger.info("[INIT] LoTW Consumer инициализирован")

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

    def print_stats(self, detailed: bool = False):
        """Вывод статистики"""
        if hasattr(self, 'stats'):
            self.stats.print_stats(detailed=detailed)

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
        self.logger.info("[START] Начинаю запуск consumer...")

        if self.test_mode:
            self.logger.info("[TEST] Запуск в тестовом режиме")
            self.process_test_tasks()
            return

        # Инициализация RabbitMQ
        self.logger.info("[RABBITMQ] Инициализация подключения...")
        self.rabbitmq = RabbitMQConnection(
            logger=self.logger,
            max_workers=self.max_workers
        )

        self.logger.info("[RABBITMQ] Подключение...")
        if not self.rabbitmq.connect():
            self.logger.error("[RABBITMQ] Не удалось подключиться к RabbitMQ")
            return

        self.logger.info("[RABBITMQ] Успешно подключено")
        self.logger.info("[READY] LoTW Consumer запущен и готов к работе")
        self.logger.info("[WAIT] Ожидание задач синхронизации... (Ctrl+C для остановки)")

        try:
            # Запуск прослушивания
            self.logger.info("[CONSUME] Запуск прослушивания очереди...")
            self.rabbitmq.start_consuming(
                message_handler=self.message_handler.handle_delivery,
                stats_callback=self.stats.update_worker_count
            )

        except KeyboardInterrupt:
            self.logger.info("\n[STOP] Остановлено пользователем")
        except Exception as e:
            self.logger.error(f"[ERROR] Ошибка в основном цикле: {e}")
            import traceback
            self.logger.error(f"[TRACE] {traceback.format_exc()}")
        finally:
            self.logger.info("[CLEANUP] Закрытие...")
            if self.rabbitmq:
                try:
                    self.rabbitmq.close()
                except Exception:
                    pass
            self.logger.info("[DONE] Consumer завершен")


def main():
    """Точка входа"""
    print("=" * 60)
    print(" LoTW Consumer")
    print("=" * 60)
    print("Запуск...")

    try:
        consumer = LoTWConsumer()
        consumer.start_consuming()
    except Exception as e:
        print(f"Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()