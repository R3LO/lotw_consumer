"""
Модуль для обработки сообщений RabbitMQ
"""

import json
import pika
from typing import Dict, Any
from datetime import datetime, timezone

from config import (
    MAX_RETRIES, RABBITMQ_QUEUE, RABBITMQ_EXCHANGE,
    RABBITMQ_DELAYED_QUEUE, RABBITMQ_DELAYED_EXCHANGE,
    RABBITMQ_DLX_EXCHANGE, RETRY_DELAY_MS
)


class MessageHandler:
    """Класс для обработки сообщений RabbitMQ"""

    def __init__(self, logger, stats, db_ops, lotw_api, max_retries: int = MAX_RETRIES):
        """
        Args:
            logger: Логгер
            stats: Статистика
            db_ops: Операции с БД
            lotw_api: LoTW API
            max_retries: Максимальное количество попыток
        """
        self.logger = logger
        self.stats = stats
        self.db_ops = db_ops
        self.lotw_api = lotw_api
        self.max_retries = max_retries
        self.retry_delay_ms = RETRY_DELAY_MS

    def process_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Обработка задачи синхронизации"""
        task_id = task.get('task_id', 'unknown')
        callsign = task.get('callsign', 'unknown')
        username = task.get('username', 'unknown')
        password = task.get('password', 'unknown')
        user_id = task.get('user_id', 'unknown')

        self.logger.info(f"Начинаю обработку задачи {task_id}")
        self.logger.info(f"   Позывной: {callsign}")
        self.logger.info(f"   Пользователь: {username}")

        try:
            # Если user_id неизвестен, ищем его
            if user_id == 'unknown' or not user_id:
                found_user_id = self.db_ops.get_user_id_by_username(username)
                if found_user_id:
                    user_id = found_user_id
                    self.logger.info(f"Найден user_id={user_id} для username={username}")
                else:
                    self.logger.error(f"Не найден user_id для username={username}")
                    return {
                        'success': False,
                        'callsign': callsign,
                        'error': f'Не найден user_id для username={username}',
                        'message': 'Пользователь не найден в базе данных'
                    }

            # Получаем данные из LoTW (передаем lotw_lastsync для запроса только новых QSO)
            lotw_lastsync = task.get('lotw_lastsync')
            lotw_result = self.lotw_api.get_lotw_data(callsign, username, password, lotw_lastsync)

            if not lotw_result['success']:
                self.logger.error(f"Ошибка получения данных из LoTW: {lotw_result.get('error')}")
                return lotw_result

            # ДИАГНОСТИКА: проверяем что пришло от API
            qso_data_from_api = lotw_result.get('qso_data', [])
            self.logger.debug(f"🔍 Обработчик: от API получено {len(qso_data_from_api)} QSO")

            # Показываем первые несколько QSO для диагностики
            for i, qso in enumerate(qso_data_from_api[:5]):
                self.logger.debug(f"🔍 Обработчик QSO #{i+1}: CALL={qso.get('CALL')}, BAND={qso.get('BAND')}, MODE={qso.get('MODE')}")
                self.logger.debug(f"🔍 Обработчик QSO #{i+1}: QSO_DATE={qso.get('QSO_DATE')}, TIME_ON={qso.get('TIME_ON')}")

                # Проверяем APP_LOTW_RXQSL
                app_rxqsl = qso.get('APP_LOTW_RXQSL')
                if app_rxqsl:
                    self.logger.debug(f"🔍 Обработчик QSO #{i+1}: APP_LOTW_RXQSL={app_rxqsl}")
                else:
                    self.logger.debug(f"🔍 Обработчик QSO #{i+1}: APP_LOTW_RXQSL=ОТСУТСТВУЕТ")

            if len(qso_data_from_api) > 5:
                self.logger.debug(f"🔍 ... и еще {len(qso_data_from_api) - 5} QSO")

            # Обрабатываем данные
            result = self.db_ops.process_qso_batch(
                lotw_result['qso_data'],
                callsign,  # Используем callsign из задачи как my_callsign
                user_id
            )

            if result.get('success'):
                self.stats.increment_processed(callsign, username)
                self.stats.update_qso_stats(
                    added=result.get('qso_added', 0),
                    updated=result.get('qso_updated', 0),
                    skipped=result.get('qso_skipped', 0)
                )

                # Обновляем lotw_lastsync
                created_at_str = task.get('created_at', '')
                if created_at_str:
                    # Парсим строку в datetime объект
                    if 'T' in created_at_str:
                        lotw_datetime = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                        # Конвертируем в naive datetime для консистентности
                        if lotw_datetime.tzinfo is not None:
                            lotw_datetime = lotw_datetime.replace(tzinfo=None)
                        self.logger.debug(f"🔍 Parsed ISO datetime: {lotw_datetime}")
                    else:
                        # Попытка парсинга строки в различных форматах
                        lotw_datetime = None

                        # Пробуем формат YYYY-MM-DD HH:MM:SS
                        try:
                            lotw_datetime = datetime.strptime(created_at_str, '%Y-%m-%d %H:%M:%S')
                            self.logger.debug(f"🔍 Parsed string datetime (full): {lotw_datetime}")
                        except ValueError:
                            # Пробуем формат YYYY-MM-DD (только дата)
                            try:
                                lotw_datetime = datetime.strptime(created_at_str, '%Y-%m-%d')
                                # Добавляем время 00:00:00 для консистентности
                                lotw_datetime = lotw_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
                                self.logger.debug(f"🔍 Parsed string datetime (date only): {lotw_datetime}")
                            except ValueError:
                                # Если не получается, используем текущее время
                                lotw_datetime = datetime.now(timezone.utc)
                                self.logger.warning(f"⚠️ Failed to parse datetime string '{created_at_str}', using current time: {lotw_datetime}")
                else:
                    lotw_datetime = datetime.now(timezone.utc)
                    self.logger.debug(f"🔍 Using current datetime: {lotw_datetime}")

                self.logger.debug(f"🔍 Calling update_lotw_lastsync with: {lotw_datetime} (type: {type(lotw_datetime)})")
                self.db_ops.update_lotw_lastsync(user_id, lotw_datetime)

                self.logger.debug(f"Задача {task_id} успешно обработана")
                self.logger.debug(f"   QSO: добавлено {result.get('qso_added', 0)}, обновлено {result.get('qso_updated', 0)}")
            else:
                self.stats.increment_failed()
                self.logger.error(f"Задача {task_id} завершилась с ошибкой")
                if 'error' in result:
                    self.logger.error(f"   Ошибка: {result['error']}")

            return result

        except Exception as e:
            self.logger.error(f"Критическая ошибка при обработке задачи {task_id}: {e}")
            self.stats.increment_failed()
            return {
                'success': False,
                'error': str(e),
                'message': 'Критическая ошибка обработки'
            }

    def handle_delivery(self, ch, method, properties, body):
        """
        Обработчик доставки сообщений RabbitMQ
        """
        task = None
        self.stats.increment_workers()

        def channel_is_open():
            """Проверка что канал открыт"""
            try:
                return ch.is_open
            except Exception:
                return False

        def safe_ack():
            """Безопасный ack"""
            try:
                if channel_is_open():
                    ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception:
                pass

        def safe_nack(requeue=False):
            """Безопасный nack"""
            try:
                if channel_is_open():
                    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=requeue)
            except Exception:
                pass

        def safe_publish(exchange, routing_key, body, properties):
            """Безопасный publish"""
            try:
                if channel_is_open():
                    ch.basic_publish(exchange=exchange, routing_key=routing_key, body=body, properties=properties)
                else:
                    self.logger.warning(f"Канал закрыт, не могу опубликовать сообщение")
            except Exception as e:
                self.logger.error(f"Ошибка публикации: {e}")

        try:
            task = json.loads(body.decode('utf-8'))
            task_id = task.get('task_id', 'unknown')

            result = self.process_task(task)

            if result.get('success'):
                safe_ack()
                self.stats.update_last_task()
                self.logger.info(f"Задача {task_id} завершена")
            else:
                retry_count = task.get('retry_count', 0) + 1

                if retry_count <= self.max_retries:
                    task['retry_count'] = retry_count
                    task['last_retry'] = datetime.now(timezone.utc).isoformat()
                    task['last_error'] = result.get('error', 'Unknown error')

                    # Фиксированная задержка 45 минут (2700000 мс)
                    delay_ms = self.retry_delay_ms
                    delay_minutes = delay_ms // 60000

                    # Публикуем в отложенную очередь (через DLX)
                    safe_publish(
                        exchange=RABBITMQ_DELAYED_EXCHANGE,
                        routing_key='delayed',
                        body=json.dumps(task, ensure_ascii=False),
                        properties=pika.BasicProperties(
                            delivery_mode=2,
                            content_type='application/json',
                            expiration=str(delay_ms)  # TTL сообщения в мс
                        )
                    )
                    safe_ack()
                    self.stats.increment_retried()
                    self.logger.warning(f"Задача {task_id} будет повторена через {delay_minutes} мин (попытка {retry_count}/{self.max_retries})")
                else:
                    safe_nack(requeue=False)
                    self.logger.error(f"Задача {task_id} перемещена в DLQ после {self.max_retries} попыток")

        except json.JSONDecodeError as e:
            self.logger.error(f"Ошибка декодирования JSON: {e}")
            safe_nack(requeue=False)
            self.stats.increment_failed()
        except Exception as e:
            self.logger.error(f"Непредвиденная ошибка: {e}")
            if task:
                task_id = task.get('task_id', 'unknown')
                self.logger.error(f"Задача вызвавшая ошибку: {task_id}")
            safe_nack(requeue=False)
            self.stats.increment_failed()
        finally:
            self.stats.decrement_workers()