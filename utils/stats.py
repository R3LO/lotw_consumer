"""
Модуль для работы со статистикой
"""

from datetime import datetime
from typing import Dict, Any


class Statistics:
    """Класс для сбора и отображения статистики"""

    def __init__(self, test_mode: bool = False):
        self.stats = {
            'processed': 0,
            'failed': 0,
            'retried': 0,
            'started_at': datetime.now().isoformat(),
            'last_task': None,
            'current_workers': 0,
            'test_mode': test_mode,
            'by_callsign': {},
            'by_user': {},
            'qso_added': 0,
            'qso_updated': 0,
            'qso_skipped': 0,
            'duplicates': 0
        }

    def increment_processed(self, callsign: str, username: str):
        """Увеличивает счетчик обработанных задач"""
        self.stats['processed'] += 1

        if callsign not in self.stats['by_callsign']:
            self.stats['by_callsign'][callsign] = 0
        self.stats['by_callsign'][callsign] += 1

        if username not in self.stats['by_user']:
            self.stats['by_user'][username] = 0
        self.stats['by_user'][username] += 1

    def increment_failed(self):
        """Увеличивает счетчик неудачных задач"""
        self.stats['failed'] += 1

    def increment_retried(self):
        """Увеличивает счетчик повторенных задач"""
        self.stats['retried'] += 1

    def increment_workers(self):
        """Увеличивает счетчик активных воркеров"""
        self.stats['current_workers'] += 1

    def decrement_workers(self):
        """Уменьшает счетчик активных воркеров"""
        self.stats['current_workers'] -= 1

    def update_worker_count(self, count: int):
        """Обновляет количество воркеров"""
        self.stats['current_workers'] = count

    def update_last_task(self):
        """Обновляет время последней задачи"""
        self.stats['last_task'] = datetime.now().isoformat()

    def update_qso_stats(self, added: int = 0, updated: int = 0, skipped: int = 0, duplicates: int = 0):
        """Обновляет статистику QSO"""
        self.stats['qso_added'] += added
        self.stats['qso_updated'] += updated
        self.stats['qso_skipped'] += skipped
        self.stats['duplicates'] += duplicates

    def print_stats(self, detailed: bool = False):
        """Вывод статистики"""
        from config import RABBITMQ_QUEUE, RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER
        from config import DB_HOST, DB_PORT, DB_NAME, DB_SCHEMA

        print("\n" + "="*60)
        print("📊 СТАТИСТИКА КОНСЬЮМЕРА LOTW")
        print("="*60)
        print(f"Очередь: {RABBITMQ_QUEUE}")
        print(f"Обработано задач: {self.stats['processed']}")
        print(f"Неудачных задач: {self.stats['failed']}")
        print(f"Повторенных задач: {self.stats['retried']}")
        print(f"Активных процессов: {self.stats['current_workers']}")
        print(f"QSO добавлено: {self.stats['qso_added']}")
        print(f"QSO обновлено: {self.stats['qso_updated']}")
        print(f"QSO пропущено: {self.stats['qso_skipped']}")
        print(f"QSO дубликатов: {self.stats['duplicates']}")
        print(f"Время запуска: {self.stats['started_at'][11:19]}")
        print(f"Последняя задача: {self.stats['last_task'][11:19] if self.stats['last_task'] else 'Нет'}")

        if self.stats['by_callsign']:
            print(f"\n📈 Обработано позывных: {len(self.stats['by_callsign'])}")
            top_callsigns = sorted(self.stats['by_callsign'].items(), key=lambda x: x[1], reverse=True)[:5]
            if top_callsigns:
                print("Топ позывных:")
                for callsign, count in top_callsigns:
                    print(f"  {callsign}: {count} задач")

        if detailed:
            print(f"\n⚙️ Конфигурация:")
            print(f"  RabbitMQ: {RABBITMQ_HOST}:{RABBITMQ_PORT}")
            print(f"  Пользователь: {RABBITMQ_USER}")
            print(f"  База данных: {DB_HOST}:{DB_PORT}/{DB_NAME}")
            print(f"  Схема: {DB_SCHEMA}")
            print(f"  Режим тестирования: {'Да' if self.stats['test_mode'] else 'Нет'}")
        print("="*60)