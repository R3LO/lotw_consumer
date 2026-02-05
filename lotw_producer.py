#!/usr/bin/env python3
"""
LoTW Sync Producer - Полная версия с интеграцией извлечения позывных
Автоматически извлекает позывные из базы данных и отправляет задачи в RabbitMQ
"""

import pika
import json
import time
import psycopg2
import argparse
import logging
import sys
import os
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple

# Импортируем конфигурацию
from config import (
    RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_QUEUE, RABBITMQ_EXCHANGE, RABBITMQ_USER, RABBITMQ_PASSWORD,
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DB_SCHEMA,
    BATCH_DELAY, MAX_RETRIES, LOG_LEVEL, LOG_FILE
)


class LoTWProducer:
    def __init__(self):
        """Инициализация продюсера"""
        self.setup_logging()
        self.setup_rabbitmq()

    def setup_logging(self):
        """Настройка логирования из конфига"""
        log_level = getattr(logging, LOG_LEVEL.upper(), logging.INFO)

        # Создаем логгер
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(log_level)

        # Удаляем существующие обработчики
        self.logger.handlers.clear()

        # Форматтер
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # Файловый обработчик - исправляем путь для Windows
        try:
            # Если путь начинается с /var/log/, меняем на текущую директорию
            if LOG_FILE.startswith('/var/log/'):
                log_dir = 'logs'
                os.makedirs(log_dir, exist_ok=True)
                log_filename = os.path.join(log_dir, os.path.basename(LOG_FILE))
                self.logger.info(f"Использую локальную директорию для логов: {log_filename}")
            else:
                log_filename = LOG_FILE

            # Создаем директорию если не существует
            os.makedirs(os.path.dirname(log_filename), exist_ok=True)

            file_handler = logging.FileHandler(log_filename, encoding='utf-8')
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
            self.logger.info(f"Логирование в файл: {log_filename}")
        except Exception as e:
            self.logger.warning(f"Не удалось создать файловый логгер: {e}")

        # Консольный обработчик
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        self.logger.info(f"Логирование настроено. Уровень: {LOG_LEVEL}")

    def setup_rabbitmq(self, recreate_queue: bool = False):
        """Настройка подключения к RabbitMQ"""
        try:
            self.connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=RABBITMQ_HOST,
                    port=RABBITMQ_PORT,
                    credentials=pika.PlainCredentials(
                        RABBITMQ_USER,
                        RABBITMQ_PASSWORD
                    ),
                    heartbeat=600,
                    blocked_connection_timeout=300
                )
            )
            self.channel = self.connection.channel()

            # Если очередь существует с другими параметрами, удаляем и создаем заново
            if recreate_queue:
                try:
                    self.channel.queue_delete(queue=RABBITMQ_QUEUE)
                    self.logger.info(f"Очередь {RABBITMQ_QUEUE} удалена для пересоздания")
                except Exception as e:
                    self.logger.warning(f"Не удалось удалить очередь: {e}")

            # Пытаемся получить информацию об очереди (если существует)
            try:
                queue_info = self.channel.queue_declare(
                    queue=RABBITMQ_QUEUE,
                    passive=True  # Только проверка существования
                )
                self.logger.info(f"Очередь {RABBITMQ_QUEUE} уже существует ({queue_info.method.message_count} сообщений)")
                return  # Очередь уже существует, не изменяем параметры
            except pika.exceptions.ChannelClosedByBroker as e:
                # Очередь не существует, создаем с параметрами
                self.logger.info(f"Очередь {RABBITMQ_QUEUE} не существует, создаю...")
                pass
            except Exception as e:
                self.logger.warning(f"Ошибка при проверке очереди: {e}")

            # Создаем очередь если не существует
            self.channel.queue_declare(
                queue=RABBITMQ_QUEUE,
                durable=True,
                arguments={
                    'x-message-ttl': 86400000,  # TTL 24 часа (в миллисекундах)
                    'x-dead-letter-exchange': '',
                    'x-dead-letter-routing-key': f"{RABBITMQ_QUEUE}_dlq"
                } if not recreate_queue else {}  # Для совместимости без TTL
            )

            # Создаем DLQ (Dead Letter Queue)
            dlq_name = f"{RABBITMQ_QUEUE}_dlq"
            try:
                self.channel.queue_declare(
                    queue=dlq_name,
                    durable=True
                )
                self.logger.debug(f"Очередь ошибок создана: {dlq_name}")
            except Exception as e:
                self.logger.warning(f"Не удалось создать DLQ: {e}")

            self.logger.info(f"Подключено к RabbitMQ: {RABBITMQ_HOST}:{RABBITMQ_PORT}")
            self.logger.info(f"Очередь: {RABBITMQ_QUEUE}")

        except pika.exceptions.AMQPConnectionError as e:
            self.logger.error(f"Ошибка подключения к RabbitMQ: {e}")
            self.logger.error(f"Проверьте: хост={RABBITMQ_HOST}, порт={RABBITMQ_PORT}, пользователь={RABBITMQ_USER}")
            sys.exit(1)
        except Exception as e:
            self.logger.error(f"Неожиданная ошибка подключения: {e}")
            sys.exit(1)

    def get_db_connection(self):
        """Создает соединение с базой данных"""
        try:
            self.logger.debug(f"Подключение к БД: {DB_HOST}:{DB_PORT}/{DB_NAME}")
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            # Устанавливаем схему
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {DB_SCHEMA}")
            self.logger.debug("Подключение к БД успешно")
            return conn
        except psycopg2.OperationalError as e:
            self.logger.error(f"Ошибка подключения к БД: {e}")
            self.logger.error(f"Проверьте: хост={DB_HOST}, порт={DB_PORT}, БД={DB_NAME}, пользователь={DB_USER}")
            return None
        except Exception as e:
            self.logger.error(f"Неожиданная ошибка при подключении к БД: {e}")
            return None

    def extract_callsigns_list(self) -> List[str]:
        """
        Извлекает все позывные из базы данных my_callsigns в формате списка ["R3LO", "R3LO/1"]
        Возвращает простой список позывных без учетных данных
        """
        callsign_list = []
        conn = None

        try:
            conn = self.get_db_connection()
            if not conn:
                return []

            # Получаем все записи из таблицы с проверкой: lotw_chk_pass = TRUE
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        id,
                        callsign,
                        my_callsigns,
                        lotw_user,
                        lotw_password,
                        lotw_lastsync
                    FROM tlog_radioprofile
                    WHERE lotw_user IS NOT NULL
                    AND lotw_user != ''
                    AND lotw_password IS NOT NULL
                    AND lotw_password != ''
                    AND lotw_chk_pass = TRUE
                    ORDER BY id ASC
                """)
                rows = cur.fetchall()

                total_rows = len(rows)

                for row in rows:
                    user_id, callsign_data, my_callsigns, lotw_user, lotw_password, lotw_lastsync = row

                    # Обрабатываем основной позывной
                    if callsign_data:
                        callsign_str = self.extract_callsign_name(callsign_data)
                        if callsign_str:
                            callsign_list.append(callsign_str.upper())

                    # Обрабатываем позывные из my_callsigns
                    if my_callsigns:
                        callsigns_list = self.parse_my_callsigns(my_callsigns)
                        for callsign_item in callsigns_list:
                            callsign_name = self.extract_callsign_name(callsign_item)
                            if callsign_name:
                                callsign_list.append(callsign_name.upper())

            # Удаляем дубликаты и сортируем
            callsign_list = sorted(list(set(callsign_list)))

            self.logger.info(f"Получено {len(callsign_list)} уникальных позывных из базы данных (lotw_chk_pass = TRUE)")

            # Дополнительная информация для отладки
            if callsign_list:
                self.logger.debug(f"Пример позывных: {callsign_list[:5]}...")

                # Сохраняем в файл для проверки
                try:
                    with open('callsigns_list_debug.json', 'w', encoding='utf-8') as f:
                        json.dump(callsign_list, f, indent=2, ensure_ascii=False)
                    self.logger.debug("Сохранен отладочный файл: callsigns_list_debug.json")
                except Exception as e:
                    self.logger.debug(f"Не удалось сохранить отладочный файл: {e}")

            return callsign_list

        except Exception as e:
            self.logger.error(f"Ошибка при чтении базы данных: {e}")
            return []
        finally:
            if conn:
                conn.close()

    def extract_callsigns_with_credentials(self) -> Dict[str, Dict[str, Any]]:
        """
        Извлекает все позывные из базы данных с их логинами и паролями LOTW
        Возвращает словарь, где ключ - позывной, значение - словарь с учетными данными
        """
        callsign_dict = {}
        conn = None

        try:
            conn = self.get_db_connection()
            if not conn:
                return {}

            # Получаем все записи из таблицы с проверкой: lotw_chk_pass = TRUE
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        id,
                        callsign,
                        my_callsigns,
                        lotw_user,
                        lotw_password,
                        lotw_lastsync
                    FROM tlog_radioprofile
                    WHERE lotw_user IS NOT NULL
                    AND lotw_user != ''
                    AND lotw_password IS NOT NULL
                    AND lotw_password != ''
                    AND lotw_chk_pass = TRUE
                    ORDER BY id ASC
                """)
                rows = cur.fetchall()

                total_rows = len(rows)

                for row in rows:
                    user_id, callsign_data, my_callsigns, lotw_user, lotw_password, lotw_lastsync = row

                    # Создаем словарь с учетными данными
                    credentials = {
                        "lotw_user": lotw_user,
                        "lotw_password": lotw_password,
                        "user_id": user_id,
                        "lotw_lastsync": lotw_lastsync
                    }

                    # Обрабатываем основной позывной
                    if callsign_data:
                        callsign_str = self.extract_callsign_name(callsign_data)
                        if callsign_str:
                            callsign_dict[callsign_str.upper()] = credentials

                    # Обрабатываем позывные из my_callsigns
                    if my_callsigns:
                        callsigns_list = self.parse_my_callsigns(my_callsigns)
                        for callsign_item in callsigns_list:
                            callsign_name = self.extract_callsign_name(callsign_item)
                            if callsign_name:
                                callsign_dict[callsign_name.upper()] = credentials

            self.logger.info(f"Получено {len(callsign_dict)} позывных из базы данных (lotw_chk_pass = TRUE)")

            # Дополнительная информация для отладки
            if callsign_dict:
                self.logger.debug(f"Пример позывных: {list(callsign_dict.keys())[:5]}...")

                # Сохраняем в файл для проверки
                try:
                    with open('callsigns_debug.json', 'w', encoding='utf-8') as f:
                        json.dump(callsign_dict, f, indent=2, ensure_ascii=False, default=str)
                    self.logger.debug("Сохранен отладочный файл: callsigns_debug.json")
                except Exception as e:
                    self.logger.debug(f"Не удалось сохранить отладочный файл: {e}")

            return callsign_dict

        except Exception as e:
            self.logger.error(f"Ошибка при чтении базы данных: {e}")
            return {}
        finally:
            if conn:
                conn.close()

    def extract_callsign_name(self, callsign_data) -> str:
        """
        Извлекает имя позывного из различных форматов данных
        """
        if not callsign_data:
            return ""

        if isinstance(callsign_data, str):
            if callsign_data.strip().startswith(('{', '[')):
                try:
                    data = json.loads(callsign_data)
                    if isinstance(data, dict) and 'name' in data:
                        return data['name'].strip()
                    elif isinstance(data, list) and data:
                        return self.extract_callsign_name(data[0])
                except json.JSONDecodeError:
                    pass
            return callsign_data.strip()

        elif isinstance(callsign_data, dict):
            name = callsign_data.get('name', '')
            if name:
                return name.strip()

        return str(callsign_data).strip()

    def parse_my_callsigns(self, my_callsigns) -> List[Any]:
        """
        Парсит поле my_callsigns из различных форматов
        """
        if not my_callsigns:
            return []

        if isinstance(my_callsigns, str):
            try:
                data = json.loads(my_callsigns)
                if isinstance(data, list):
                    return data
                elif isinstance(data, dict):
                    return [data]
                else:
                    return [{"name": str(data)}]
            except json.JSONDecodeError:
                if my_callsigns.strip().startswith('['):
                    return []
                else:
                    return [{"name": my_callsigns.strip()}]

        elif isinstance(my_callsigns, list):
            return my_callsigns

        return [my_callsigns]

    def send_task(self, callsign: str, credentials: Dict[str, Any]) -> bool:
        """
        Отправляет задачу синхронизации в RabbitMQ

        Args:
            callsign: позывной
            credentials: учетные данные

        Returns:
            True если успешно, False если ошибка
        """
        task_id = f"lotw_{int(time.time())}_{callsign}"

        # Преобразуем lotw_lastsync в строку для JSON сериализации
        lotw_lastsync = credentials.get('lotw_lastsync')
        if lotw_lastsync:
            if hasattr(lotw_lastsync, 'isoformat'):
                lotw_lastsync = lotw_lastsync.isoformat()
            else:
                lotw_lastsync = str(lotw_lastsync)

        task = {
            'task_id': task_id,
            'task_type': 'lotw_sync',
            'callsign': callsign,
            'username': credentials['lotw_user'],
            'password': credentials['lotw_password'],
            'user_id': credentials['user_id'],
            'lotw_lastsync': lotw_lastsync,
            'created_at': datetime.now().date().isoformat()
        }

        for attempt in range(MAX_RETRIES):
            try:
                self.channel.basic_publish(
                    exchange=RABBITMQ_EXCHANGE,
                    routing_key=RABBITMQ_QUEUE,
                    body=json.dumps(task, ensure_ascii=False),
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # Persistent
                        content_type='application/json'
                    )
                )

                self.logger.info(f"Задача отправлена: {callsign} (user_id: {credentials['user_id']}, login: {credentials['lotw_user'][:3]}***, lastsync: {credentials.get('lotw_lastsync')})")
                return True

            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    self.logger.warning(f"Попытка {attempt + 1}/{MAX_RETRIES} не удалась для {callsign}: {e}")
                    time.sleep(2 ** attempt)  # Экспоненциальная задержка
                else:
                    self.logger.error(f"Не удалось отправить задачу для {callsign} после {MAX_RETRIES} попыток: {e}")

        return False

    def test_rabbitmq_messages(self, batch_delay: Optional[float] = None):
        """
        Тестовый режим: получить данные из БД и показать сообщения RabbitMQ без отправки
        """
        if batch_delay is None:
            batch_delay = BATCH_DELAY

        self.logger.info(f"ТЕСТОВЫЙ РЕЖИМ - Получение данных из БД и показ сообщений RabbitMQ")
        self.logger.info(f"   Задержка: {batch_delay} сек")

        # Получаем позывные в формате списка
        callsigns_list = self.extract_callsigns_list()

        if not callsigns_list:
            self.logger.warning("Не найдено позывных для тестирования")
            self.logger.info("Проверьте наличие записей в таблице tlog_radioprofile с заполненными lotw_user, lotw_password и lotw_chk_pass = TRUE")
            return None

        # Получаем полные данные
        callsigns_with_credentials = self.extract_callsigns_with_credentials()

        if not callsigns_with_credentials:
            self.logger.error("Не удалось получить учетные данные")
            return None

        total = len(callsigns_list)
        would_be_sent = 0
        not_found = 0

        self.logger.info(f"Будет получено {total} сообщений из базы данных:")
        self.logger.info(f"   Позывные: {', '.join(callsigns_list[:10])}{'...' if total > 10 else ''}")

        print("\n" + "="*80)
        print(" ТЕСТОВЫЙ РЕЖИМ - СООБЩЕНИЯ ДЛЯ RABBITMQ")
        print("="*80)
        print(f"Всего позывных в базе: {total}")
        print(f"Позывные: {callsigns_list}")
        print()

        # Показываем каждое сообщение, которое БЫЛО БЫ отправлено
        for i, callsign in enumerate(callsigns_list, 1):
            if callsign in callsigns_with_credentials:
                credentials = callsigns_with_credentials[callsign]
                would_be_sent += 1

                # Формируем сообщение как оно было бы отправлено
                message = {
                    "action": "lotw_sync",
                    "callsign": callsign,
                    "lotw_user": credentials["lotw_user"],
                    "lotw_password": credentials["lotw_password"],
                    "user_id": credentials["user_id"],
                    "timestamp": datetime.now().isoformat(),
                    "test_mode": True  # Отметка что это тестовый режим
                }

                print(f"СООБЩЕНИЕ #{i}:")
                print(f"  Позывной: {callsign}")
                print(f"  LoTW пользователь: {credentials['lotw_user']}")
                print(f"  Пользователь ID: {credentials['user_id']}")
                print(f"  JSON для RabbitMQ:")
                print(f"  {json.dumps(message, indent=4, ensure_ascii=False)}")
                print(f"  -> БЫЛО БЫ ОТПРАВЛЕНО в очередь: {RABBITMQ_QUEUE}")
                print()

            else:
                not_found += 1
                self.logger.warning(f"Позывной {callsign} не найден в базе данных с учетными данными")
                print(f"СООБЩЕНИЕ #{i}:")
                print(f"  Позывной: {callsign}")
                print(f"  ❌ НЕТ УЧЕТНЫХ ДАННЫХ - НЕ БУДЕТ ОТПРАВЛЕНО")
                print()

            # Имитация задержки
            if i < total:
                time.sleep(min(batch_delay, 0.1))  # Уменьшенная задержка для теста

        print("="*80)
        print(" СТАТИСТИКА ТЕСТОВОГО РЕЖИМА")
        print("="*80)
        print(f"Всего позывных в базе: {total}")
        print(f"Будет отправлено: {would_be_sent}")
        print(f"Не найдено учетных данных: {not_found}")
        print(f"Формат данных: {callsigns_list}")
        print(f"Время завершения: {datetime.now().isoformat()}")
        print("="*80)
        print("OK ТЕСТ ЗАВЕРШЕН - Никаких сообщений не отправлено в RabbitMQ")
        print("="*80)

        # Возвращаем статистику
        return {
            'total': total,
            'would_be_sent': would_be_sent,
            'not_found': not_found,
            'callsigns_list': callsigns_list,
            'test_mode': True,
            'timestamp': datetime.now().isoformat()
        }

    def sync_all_callsigns(self, batch_delay: Optional[float] = None):
        """Синхронизирует все позывные из базы данных (новый метод с форматом списка)"""
        if batch_delay is None:
            batch_delay = BATCH_DELAY

        self.logger.info(f"Начало синхронизации всех позывных")
        self.logger.info(f"   Задержка: {batch_delay} сек")

        # Получаем позывные в формате списка
        callsigns_list = self.extract_callsigns_list()

        if not callsigns_list:
            self.logger.warning("Не найдено позывных для синхронизации")
            self.logger.info("Проверьте наличие записей в таблице tlog_radioprofile с заполненными lotw_user, lotw_password и lotw_chk_pass = TRUE")
            return None

        # Получаем полные данные для отправки в RabbitMQ
        callsigns_with_credentials = self.extract_callsigns_with_credentials()

        if not callsigns_with_credentials:
            self.logger.error("Не удалось получить учетные данные для отправки задач")
            return None

        total = len(callsigns_list)
        success = 0
        failed = 0
        not_found = 0

        self.logger.info(f"Начинаю отправку {total} задач...")
        self.logger.info(f"   Позывные: {', '.join(callsigns_list[:10])}{'...' if total > 10 else ''}")

        # Отправляем задачи из списка
        for i, callsign in enumerate(callsigns_list, 1):
            # Логируем прогресс каждые 5 задач
            if i % 5 == 0 or i == total:
                self.logger.info(f"Прогресс: {i}/{total} ({i/total*100:.1f}%)")

            # Проверяем наличие учетных данных для этого позывного
            if callsign in callsigns_with_credentials:
                credentials = callsigns_with_credentials[callsign]
                # Отправляем задачу
                if self.send_task(callsign, credentials):
                    success += 1
                else:
                    failed += 1
            else:
                not_found += 1
                self.logger.warning(f"Позывной {callsign} не найден в базе данных с учетными данными")

            # Задержка между отправками
            if i < total:
                time.sleep(batch_delay)

        # Статистика
        self.logger.info(f"Синхронизация завершена")
        self.logger.info(f"   Успешно: {success}, Ошибок: {failed}, Не найдено: {not_found}, Всего: {total}")

        # Возвращаем статистику
        return {
            'total': total,
            'success': success,
            'failed': failed,
            'not_found': not_found,
            'callsigns_list': callsigns_list,
            'timestamp': datetime.now().isoformat()
        }

    def sync_callsigns_from_list(self, batch_delay: Optional[float] = None):
        """
        Синхронизирует все позывные из базы данных в формате списка ["R3LO", "R3LO/1"]
        Новый метод с упрощенным форматом данных
        """
        if batch_delay is None:
            batch_delay = BATCH_DELAY

        self.logger.info(f"Начало синхронизации позывных из списка")
        self.logger.info(f"   Задержка: {batch_delay} сек")

        # Получаем позывные в формате списка
        callsigns_list = self.extract_callsigns_list()

        if not callsigns_list:
            self.logger.warning("Не найдено позывных для синхронизации")
            self.logger.info("Проверьте наличие записей в таблице tlog_radioprofile с заполненными lotw_user, lotw_password и lotw_chk_pass = TRUE")
            return None

        # Получаем полные данные для отправки в RabbitMQ
        callsigns_with_credentials = self.extract_callsigns_with_credentials()

        if not callsigns_with_credentials:
            self.logger.error("Не удалось получить учетные данные для отправки задач")
            return None

        total = len(callsigns_list)
        success = 0
        failed = 0
        not_found = 0

        self.logger.info(f"Начинаю отправку {total} задач из списка...")
        self.logger.info(f"   Позывные: {', '.join(callsigns_list[:10])}{'...' if total > 10 else ''}")

        # Отправляем задачи из списка
        for i, callsign in enumerate(callsigns_list, 1):
            # Логируем прогресс каждые 5 задач
            if i % 5 == 0 or i == total:
                self.logger.info(f"Прогресс: {i}/{total} ({i/total*100:.1f}%)")

            # Проверяем наличие учетных данных для этого позывного
            if callsign in callsigns_with_credentials:
                credentials = callsigns_with_credentials[callsign]
                # Отправляем задачу
                if self.send_task(callsign, credentials):
                    success += 1
                else:
                    failed += 1
            else:
                not_found += 1
                self.logger.warning(f"Позывной {callsign} не найден в базе данных с учетными данными")

            # Задержка между отправками
            if i < total:
                time.sleep(batch_delay)

        # Статистика
        self.logger.info(f"Синхронизация завершена")
        self.logger.info(f"   Успешно: {success}, Ошибок: {failed}, Не найдено: {not_found}, Всего: {total}")

        # Возвращаем статистику
        return {
            'total': total,
            'success': success,
            'failed': failed,
            'not_found': not_found,
            'callsigns_list': callsigns_list,
            'timestamp': datetime.now().isoformat()
        }

    def sync_specific_callsigns(self, callsigns_list: List[str], batch_delay: Optional[float] = None):
        """Синхронизирует только указанные позывные"""
        if batch_delay is None:
            batch_delay = BATCH_DELAY

        self.logger.info(f"Синхронизация указанных позывных: {', '.join(callsigns_list)}")

        # Получаем все позывные с учетными данными
        all_callsigns = self.extract_callsigns_with_credentials()

        success = 0
        failed = 0
        not_found = 0

        for i, callsign in enumerate(callsigns_list, 1):
            callsign_upper = callsign.upper()
            if callsign_upper in all_callsigns:
                if self.send_task(callsign_upper, all_callsigns[callsign_upper]):
                    success += 1
                    self.logger.info(f"Задача для {callsign_upper} отправлена")
                else:
                    failed += 1
                    self.logger.error(f"Ошибка отправки для {callsign_upper}")
            else:
                not_found += 1
                self.logger.warning(f"Позывной {callsign_upper} не найден в базе данных")
                self.logger.debug(f"   Доступные позывные: {list(all_callsigns.keys())}")

            # Задержка между отправками
            if i < len(callsigns_list):
                time.sleep(batch_delay)

        self.logger.info(f"Завершено")
        self.logger.info(f"   Успешно: {success}, Ошибок: {failed}, Не найдено: {not_found}")

    def get_callsigns_list_only(self) -> List[str]:
        """
        Получает только список позывных из базы данных my_callsigns
        Возвращает простой список ["R3LO", "R3LO/1"]
        """
        return self.extract_callsigns_list()

    def check_queue_status(self) -> Tuple[Optional[int], Optional[int]]:
        """Проверяет статус очереди"""
        try:
            queue_info = self.channel.queue_declare(
                queue=RABBITMQ_QUEUE,
                passive=True
            )

            dlq_name = f"{RABBITMQ_QUEUE}_dlq"
            try:
                dlq_info = self.channel.queue_declare(
                    queue=dlq_name,
                    passive=True
                )
                dlq_messages = dlq_info.method.message_count
            except:
                dlq_messages = 0

            messages = queue_info.method.message_count

            self.logger.info(f"Статус очередей:")
            self.logger.info(f"   {RABBITMQ_QUEUE}: {messages} сообщений")
            self.logger.info(f"   {dlq_name}: {dlq_messages} сообщений")

            return messages, dlq_messages

        except Exception as e:
            self.logger.error(f"❌ Ошибка проверки очереди: {e}")
            return None, None

    def test_db_connection(self):
        """Тестирует подключение к БД и показывает найденные позывные"""
        self.logger.info("Тестирование подключения к базе данных...")

        # Получаем позывные в новом формате списка
        callsigns_list = self.extract_callsigns_list()

        # Получаем позывные с учетными данными для сравнения
        callsigns_with_credentials = self.extract_callsigns_with_credentials()

        if not callsigns_list:
            self.logger.error("Не найдено позывных в базе данных")
            self.logger.info("Проверьте:")
            self.logger.info("1. Таблица tlog_radioprofile существует")
            self.logger.info("2. Поля lotw_user и lotw_password заполнены")
            self.logger.info("3. Поле lotw_chk_pass = TRUE")
            return False

        self.logger.info(f"Найдено {len(callsigns_list)} уникальных позывных в формате списка:")

        # Демонстрируем новый формат
        self.logger.info(f"   Формат списка: {callsigns_list}")

        if callsigns_with_credentials:
            self.logger.info(f"Учетные данные найдены для {len(callsigns_with_credentials)} позывных:")

            # Показываем первые 10 позывных с логинами
            for i, (callsign, credentials) in enumerate(list(callsigns_with_credentials.items())[:10], 1):
                self.logger.info(f"   {i}. {callsign} - Логин: {credentials['lotw_user']}")

            if len(callsigns_with_credentials) > 10:
                self.logger.info(f"   ... и еще {len(callsigns_with_credentials) - 10} позывных")

        # Сохраняем список в файл для проверки
        try:
            with open('callsigns_list_output.json', 'w', encoding='utf-8') as f:
                json.dump(callsigns_list, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Список позывных сохранен в файл: callsigns_list_output.json")
        except Exception as e:
            self.logger.debug(f"Не удалось сохранить файл: {e}")

        return True

    def close(self):
        """Корректно закрывает соединения"""
        try:
            if hasattr(self, 'connection') and self.connection and self.connection.is_open:
                self.connection.close()
                self.logger.info("Соединение с RabbitMQ закрыто")
        except Exception as e:
            self.logger.error(f"Ошибка при закрытии соединения: {e}")


def main():
    """Основная функция"""
    parser = argparse.ArgumentParser(
        description='LoTW Sync Producer - отправляет задачи синхронизации в RabbitMQ',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:
  %(prog)s --all                   # Синхронизировать все позывные (формат списка)
  %(prog)s --all --dry-run        # Тест: показать сообщения без отправки в RabbitMQ
  %(prog)s --all --dry-run --stats # Тест со статистикой
  %(prog)s --callsigns UA1ABC,UA1XYZ  # Синхронизировать указанные позывные
  %(prog)s --status               # Проверить статус очереди
  %(prog)s --recreate             # Пересоздать очередь (при ошибке параметров)
  %(prog)s --test-db              # Тестировать подключение к БД
  %(prog)s --test                 # Тестовый режим (только подключение)
        """
    )

    parser.add_argument('--all', action='store_true', help='Синхронизировать все позывные (формат списка ["R3LO", "R3LO/1"])')
    parser.add_argument('--dry-run', action='store_true', help='Тестовый режим: показать сообщения RabbitMQ без отправки')
    parser.add_argument('--callsigns', type=str, help='Синхронизировать указанные позывные (через запятую)')
    parser.add_argument('--status', action='store_true', help='Проверить статус очереди')
    parser.add_argument('--delay', type=float, help=f'Задержка между отправками (сек, по умолчанию: {BATCH_DELAY})')
    parser.add_argument('--stats', action='store_true', help='Показать статистику')
    parser.add_argument('--recreate', action='store_true', help='Пересоздать очередь (при ошибке параметров)')
    parser.add_argument('--test-db', action='store_true', help='Тестировать подключение к БД')
    parser.add_argument('--test', action='store_true', help='Тестовый режим (только проверка подключения)')

    args = parser.parse_args()

    producer = None

    try:
        print("\n" + "="*60)
        print(" НАСТРОЙКИ ПРОДЮСЕРА")
        print("="*60)
        print(f"Конфиг файл: config.py")
        print(f"RabbitMQ: {RABBITMQ_HOST}:{RABBITMQ_PORT}")
        print(f"Очередь: {RABBITMQ_QUEUE}")
        print(f"База данных: {DB_HOST}:{DB_PORT}/{DB_NAME}")
        print(f"Схема: {DB_SCHEMA}")
        print(f"Задержка: {BATCH_DELAY} сек")
        print(f"Макс. попыток: {MAX_RETRIES}")
        print("="*60 + "\n")

        producer = LoTWProducer()

        if args.test:
            # Тестовый режим - только проверка подключений
            producer.logger.info("ТЕСТОВЫЙ РЕЖИМ")
            producer.logger.info("Подключения успешны")
            return

        if args.test_db:
            # Тестирование подключения к БД
            producer.test_db_connection()
            return

        if args.recreate:
            # Пересоздание очереди
            producer.logger.info("Пересоздание очереди...")
            producer.close()
            producer.setup_rabbitmq(recreate_queue=True)
            producer.logger.info("Очередь пересоздана")
            return

        if args.status:
            # Проверка статуса
            messages, dlq_messages = producer.check_queue_status()
            if messages is not None and dlq_messages is not None:
                print(f"\nСТАТУС ОЧЕРЕДЕЙ")
                print(f"Основная очередь ({RABBITMQ_QUEUE}): {messages} сообщений")
                print(f"Очередь ошибок ({RABBITMQ_QUEUE}_dlq): {dlq_messages} сообщений")

        elif args.callsigns:
            # Синхронизация указанных позывных
            callsigns = [c.strip() for c in args.callsigns.split(',')]
            delay = args.delay if args.delay is not None else BATCH_DELAY
            producer.sync_specific_callsigns(callsigns, batch_delay=delay)

        elif args.all:
            # Полная синхронизация (формат списка)
            delay = args.delay if args.delay is not None else BATCH_DELAY

            if args.dry_run:
                # Тестовый режим - показать сообщения без отправки
                stats = producer.test_rabbitmq_messages(batch_delay=delay)

                if args.stats and stats:
                    print(f"\nСТАТИСТИКА ТЕСТОВОГО РЕЖИМА")
                    print(f"Всего позывных: {stats['total']}")
                    print(f"Будет отправлено: {stats['would_be_sent']}")
                    print(f"Не найдено учетных данных: {stats['not_found']}")
                    print(f"Формат списка: {stats.get('callsigns_list', [])}")
                    print(f"Время завершения: {stats['timestamp']}")
            else:
                # Обычная синхронизация с отправкой в RabbitMQ
                stats = producer.sync_all_callsigns(batch_delay=delay)

                if args.stats and stats:
                    print(f"\nСТАТИСТИКА СИНХРОНИЗАЦИИ (формат списка)")
                    print(f"Всего позывных: {stats['total']}")
                    print(f"Успешно отправлено: {stats['success']}")
                    print(f"Ошибок отправки: {stats['failed']}")
                    print(f"Не найдено учетных данных: {stats.get('not_found', 0)}")
                    print(f"Формат списка: {stats.get('callsigns_list', [])}")
                    print(f"Время завершения: {stats['timestamp']}")

        else:
            # По умолчанию показываем справку
            parser.print_help()

    except KeyboardInterrupt:
        if producer:
            producer.logger.info("Прервано пользователем")
    except Exception as e:
        if producer:
            producer.logger.error(f"Критическая ошибка: {e}")
        else:
            print(f"Критическая ошибка: {e}")
        sys.exit(1)
    finally:
        if producer:
            producer.close()


if __name__ == "__main__":
    main()