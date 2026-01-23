"""
Операции с базой данных
"""

import uuid
import psycopg2
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from psycopg2.extras import RealDictCursor

from database.connection import DatabaseConnection
from lotw.normalizer import DataNormalizer


class DatabaseOperations:
    """Класс для операций с базой данных"""

    def __init__(self, logger):
        self.logger = logger
        self.db_conn = DatabaseConnection(logger)
        self.normalizer = DataNormalizer(logger)

    def get_user_id_by_username(self, username: str) -> Optional[int]:
        """Ищет user_id по username в таблице auth_user"""
        conn = self.db_conn.get_connection()
        if not conn:
            return None

        try:
            with conn.cursor() as cur:
                query = "SELECT id FROM auth_user WHERE username = %s"
                cur.execute(query, (username,))
                result = cur.fetchone()

                if result:
                    self.logger.debug(f"Найден user_id={result[0]} для username={username}")
                    return result[0]
                else:
                    self.logger.warning(f"⚠️ Не найден user_id для username={username}")
                    return None
        except Exception as e:
            self.logger.error(f"❌ Ошибка при поиске user_id: {e}")
            return None
        finally:
            conn.close()

    def find_existing_qso(self, qso_data: Dict[str, str], user_id: int) -> Optional[Dict[str, Any]]:
        """
        Ищет существующую QSO в базе данных.
        """
        callsign = qso_data.get('CALL', '').upper()
        my_callsign = qso_data.get('STATION_CALLSIGN', '') or 'R3LO'
        date_str = self.normalizer.normalize_date(qso_data.get('QSO_DATE', ''))
        time_str = self.normalizer.normalize_time(qso_data.get('TIME_ON', ''))
        band = self.normalizer.normalize_band(qso_data.get('BAND', ''))
        mode = self.normalizer.get_mode(qso_data)

        if not all([callsign, my_callsign, date_str, time_str, band, mode]):
            self.logger.debug(f"⚠️ Недостаточно данных для поиска QSO")
            return None

        conn = self.db_conn.get_connection()
        if not conn:
            return None

        try:
            qso_time = datetime.strptime(time_str, '%H:%M:%S').time()

            # Верхняя и нижняя границы времени
            time_lower = (datetime.combine(datetime.today(), qso_time) - timedelta(minutes=10)).time()
            time_upper = (datetime.combine(datetime.today(), qso_time) + timedelta(minutes=10)).time()

            with self.db_conn.get_cursor(conn) as cur:
                # Поиск по точному совпадению
                query = """
                    SELECT id, callsign, my_callsign, date, time, band, mode
                    FROM tlog_qso
                    WHERE user_id = %s
                    AND my_callsign = %s
                    AND callsign = %s
                    AND date = %s
                    AND time >= %s
                    AND time <= %s
                    AND band = %s
                    AND mode = %s
                """

                params = [
                    user_id, my_callsign, callsign, date_str,
                    time_lower.strftime('%H:%M:%S'), time_upper.strftime('%H:%M:%S'),
                    band, mode
                ]

                cur.execute(query, params)
                result = cur.fetchone()

                if result:
                    self.logger.debug(f"✅ Найдена существующая QSO: ID={result['id']}")
                    return result

                # Расширенный поиск
                query2 = """
                    SELECT id, callsign, my_callsign, date, time, band, mode
                    FROM tlog_qso
                    WHERE user_id = %s
                    AND my_callsign = %s
                    AND callsign = %s
                    AND date = %s
                    AND band = %s
                    AND mode = %s
                    ORDER BY ABS(EXTRACT(EPOCH FROM (time - %s::time))) ASC
                    LIMIT 1
                """

                params2 = [
                    user_id, my_callsign, callsign, date_str, band, mode, time_str
                ]

                cur.execute(query2, params2)
                result2 = cur.fetchone()

                if result2:
                    self.logger.debug(f"✅ Найдена близкая QSO: ID={result2['id']}")
                    return result2

                return None

        except Exception as e:
            self.logger.error(f"❌ Ошибка при поиске QSO: {e}")
            import traceback
            self.logger.debug(f"🔍 Детали ошибки:\n{traceback.format_exc()}")
            return None
        finally:
            conn.close()

    def insert_qso(self, qso_data: Dict[str, str], username: str, user_id: int) -> bool:
        """Вставляет новую QSO в базу данных с UUID"""
        conn = self.db_conn.get_connection()
        if not conn:
            return False

        try:
            record_id = str(uuid.uuid4())
            callsign = qso_data.get('CALL', '').upper()

            # Подготовка данных через нормализатор
            normalized_data = self.normalizer.prepare_qso_data(qso_data, username)

            self.logger.debug(f"📝 Вставляем QSO: {callsign} {normalized_data['date']} {normalized_data['time']}")
            self.logger.debug(f"📝 UUID: {record_id}")

            with conn.cursor() as cur:
                query = """
                    INSERT INTO tlog_qso (
                        id, callsign, my_callsign, band, frequency, mode,
                        date, time, prop_mode, sat_name, lotw, paper_qsl, r150s,
                        gridsquare, my_gridsquare, rst_sent, rst_rcvd,
                        ru_region, cqz, ituz, user_id, continent, dxcc, adif_upload_id,
                        created_at, updated_at
                    ) VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                """

                params = [
                    record_id,
                    callsign, username,
                    normalized_data['band'], normalized_data['frequency'], normalized_data['mode'],
                    normalized_data['date'], normalized_data['time'],
                    normalized_data['prop_mode'], normalized_data['sat_name'],
                    normalized_data['lotw'], 'N', normalized_data['r150s'],
                    normalized_data['gridsquare'], normalized_data['my_gridsquare'],
                    normalized_data['rst_sent'], normalized_data['rst_rcvd'],
                    normalized_data['ru_region'], normalized_data['cqz'], normalized_data['ituz'],
                    user_id, normalized_data['continent'], normalized_data['dxcc'], None
                ]

                cur.execute(query, params)
                conn.commit()

                self.logger.debug(f"✅ Добавлена новая QSO: {callsign} (UUID: {record_id})")
                return True

        except psycopg2.errors.UniqueViolation:
            conn.rollback()
            self.logger.warning(f"⚠️ Обнаружен дубликат QSO (уникальное ограничение)")
            return False
        except Exception as e:
            conn.rollback()
            self.logger.error(f"❌ Ошибка при добавлении QSO: {e}")
            return False
        finally:
            conn.close()

    def update_qso(self, qso_id: str, qso_data: Dict[str, str]) -> bool:
        """Обновляет существующую QSO в базе данных"""
        conn = self.db_conn.get_connection()
        if not conn:
            return False

        try:
            # Подготовка данных через нормализатор
            normalized_data = self.normalizer.prepare_qso_data(qso_data)

            with conn.cursor() as cur:
                query = """
                    UPDATE tlog_qso SET
                        band = %s,
                        frequency = %s,
                        mode = %s,
                        prop_mode = %s,
                        sat_name = %s,
                        lotw = %s,
                        r150s = %s,
                        gridsquare = %s,
                        my_gridsquare = %s,
                        rst_sent = %s,
                        rst_rcvd = %s,
                        ru_region = %s,
                        cqz = %s,
                        ituz = %s,
                        continent = %s,
                        dxcc = %s,
                        updated_at = NOW()
                    WHERE id = %s::uuid
                """

                params = [
                    normalized_data['band'], normalized_data['frequency'], normalized_data['mode'],
                    normalized_data['prop_mode'], normalized_data['sat_name'], normalized_data['lotw'],
                    normalized_data['r150s'], normalized_data['gridsquare'], normalized_data['my_gridsquare'],
                    normalized_data['rst_sent'], normalized_data['rst_rcvd'], normalized_data['ru_region'],
                    normalized_data['cqz'], normalized_data['ituz'], normalized_data['continent'],
                    normalized_data['dxcc'], qso_id
                ]

                cur.execute(query, params)
                conn.commit()

                self.logger.debug(f"✅ Обновлена QSO ID={qso_id}")
                return True

        except Exception as e:
            conn.rollback()
            self.logger.error(f"❌ Ошибка при обновлении QSO ID={qso_id}: {e}")
            return False
        finally:
            conn.close()

    def process_qso_batch(self, qso_data_list: List[Dict[str, str]], username: str, user_id: int) -> Dict[str, Any]:
        """Обрабатывает пакет QSO"""
        conn = self.db_conn.get_connection()
        if not conn:
            return {
                'success': False,
                'error': 'Не удалось подключиться к базе данных'
            }

        try:
            added = 0
            updated = 0
            skipped = 0
            errors = 0
            duplicates = 0

            self.logger.info(f"🔄 Обработка {len(qso_data_list)} QSO (user_id={user_id})")

            for i, qso_data in enumerate(qso_data_list, 1):
                # Проверка обязательных полей
                if not all([qso_data.get('CALL'), qso_data.get('QSO_DATE'),
                           qso_data.get('TIME_ON'), qso_data.get('BAND')]):
                    self.logger.warning(f"⚠️ QSO #{i} пропущена: отсутствуют обязательные поля")
                    skipped += 1
                    continue

                # Добавляем my_callsign в данные QSO для поиска
                qso_data['STATION_CALLSIGN'] = username

                # Ищем существующую QSO
                existing_qso = self.find_existing_qso(qso_data, user_id)

                if existing_qso:
                    # Обновляем существующую QSO
                    if self.update_qso(existing_qso['id'], qso_data):
                        updated += 1
                    else:
                        errors += 1
                else:
                    # Добавляем новую QSO
                    if self.insert_qso(qso_data, username, user_id):
                        added += 1
                    else:
                        errors += 1

                # Логируем прогресс
                if i % 10 == 0 or i == len(qso_data_list):
                    self.logger.info(f"📊 Прогресс: {i}/{len(qso_data_list)} QSO обработано")

            result = {
                'success': True,
                'user_id': user_id,
                'username': username,
                'total_qso': len(qso_data_list),
                'qso_added': added,
                'qso_updated': updated,
                'qso_skipped': skipped,
                'errors': errors,
                'duplicates': duplicates,
                'message': f'Обработано {len(qso_data_list)} QSO'
            }

            self.logger.info(f"✅ Обработка завершена: добавлено {added}, обновлено {updated}")

            return result

        except Exception as e:
            self.logger.error(f"❌ Критическая ошибка при обработке данных: {e}")
            return {
                'success': False,
                'error': str(e),
                'message': 'Критическая ошибка при обработке данных'
            }
        finally:
            conn.close()