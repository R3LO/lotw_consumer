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
        # Инициализируем функции lookup для DXCC и R150
        from r150s_lookup import get_dxcc_info as get_r150_info
        from cty_lookup import get_dxcc_from_cty
        self._get_r150_info = get_r150_info
        self._get_dxcc_from_cty = get_dxcc_from_cty

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
        my_callsign = qso_data.get('STATION_CALLSIGN', '')
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

    def insert_qso(self, qso_data: Dict[str, str], my_callsign: str, user_id: int) -> bool:
        """Вставляет новую QSO в базу данных с UUID"""
        conn = self.db_conn.get_connection()
        if not conn:
            return False

        try:
            record_id = str(uuid.uuid4())
            callsign = qso_data.get('CALL', '').upper()

            # Подготовка данных через нормализатор
            normalized_data = self.normalizer.prepare_qso_data(qso_data, my_callsign)

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
                    callsign, my_callsign,
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

    def process_qso_batch(self, qso_data_list: List[Dict[str, str]], my_callsign: str, user_id: int) -> Dict[str, Any]:
        """Обрабатывает пакет QSO с batch-запросами"""
        conn = self.db_conn.get_connection()
        if not conn:
            return {
                'success': False,
                'error': 'Не удалось подключиться к базе данных'
            }

        try:
            self.logger.info(f"🔄 Обработка {len(qso_data_list)} QSO (user_id={user_id})")

            # Нормализуем все данные заранее
            normalized_list = []
            skipped = 0

            for qso_data in qso_data_list:
                # Проверка обязательных полей
                if not all([qso_data.get('CALL'), qso_data.get('QSO_DATE'),
                           qso_data.get('TIME_ON'), qso_data.get('BAND')]):
                    skipped += 1
                    continue

                normalized = self.normalizer.prepare_qso_data(qso_data, my_callsign)
                normalized_list.append(normalized)

            if not normalized_list:
                return {
                    'success': True,
                    'user_id': user_id,
                    'my_callsign': my_callsign,
                    'total_qso': len(qso_data_list),
                    'qso_added': 0,
                    'qso_updated': 0,
                    'qso_skipped': skipped,
                    'message': 'Нет данных для обработки'
                }

            # Batch поиск существующих QSO
            existing_qsos = self._find_existing_batch(normalized_list, user_id, conn)

            # Создаем множество ключей существующих записей для исключения из insert
            existing_keys = set()
            for q in existing_qsos:
                key = (q['callsign'], str(q['date']), q['band'], q['mode'], str(q['time'])[:5])
                existing_keys.add(key)

            # Разделяем на новые и существующие (исключаем те, что уже есть в БД)
            new_qsos = []
            update_qsos = []

            for q in normalized_list:
                # Проверяем, есть ли точное совпадение времени (±0 сек)
                found = False
                for ex in existing_qsos:
                    if (q['callsign'] == ex['callsign'] and
                        str(q['date']) == str(ex['date']) and
                        q['band'] == ex['band'] and
                        q['mode'] == ex['mode'] and
                        q['time'][:5] == str(ex['time'])[:5]):
                        update_qsos.append(q)
                        found = True
                        break

                if not found:
                    new_qsos.append(q)

            added = 0
            updated = 0

            # Batch insert новых
            if new_qsos:
                added = self._batch_insert(new_qsos, user_id, conn)

            # Batch update существующих
            if update_qsos:
                updated = self._batch_update(update_qsos, existing_qsos, conn)

            self.logger.info(f"✅ Обработка завершена: добавлено {added}, обновлено {updated}")

            return {
                'success': True,
                'user_id': user_id,
                'my_callsign': my_callsign,
                'total_qso': len(qso_data_list),
                'qso_added': added,
                'qso_updated': updated,
                'qso_skipped': skipped,
                'message': f'Обработано {len(qso_data_list)} QSO'
            }

        except Exception as e:
            self.logger.error(f"❌ Критическая ошибка при обработке данных: {e}")
            return {
                'success': False,
                'error': str(e),
                'message': 'Критическая ошибка при обработке данных'
            }
        finally:
            conn.close()

    def _find_existing_batch(self, normalized_list: List[Dict], user_id: int, conn) -> List[Dict]:
        """Batch поиск существующих QSO с погрешностью времени ±5 минут"""
        if not normalized_list:
            return []

        try:
            with conn.cursor() as cur:
                # Формируем VALUES для поиска по callsign, date, band, mode (без time)
                values = []
                params = [user_id]
                for q in normalized_list:
                    values.append("(%s, %s::date, %s, %s)")
                    params.extend([q['callsign'], q['date'], q['band'], q['mode']])

                query = f"""
                    SELECT id, callsign, date::text, band, mode, time::text
                    FROM tlog_qso
                    WHERE user_id = %s
                    AND (callsign, date, band, mode) IN (VALUES {', '.join(values)})
                """

                cur.execute(query, params)
                rows = cur.fetchall()

                # Преобразуем результат в список словарей
                existing_qsos = []
                for row in rows:
                    existing_qsos.append({
                        'id': row[0],
                        'callsign': row[1],
                        'date': row[2],
                        'band': row[3],
                        'mode': row[4],
                        'time': row[5]
                    })

                # Фильтруем по времени с погрешностью ±5 минут (300 секунд)
                filtered = []
                for new_q in normalized_list:
                    new_time_str = new_q['time'][:5]
                    try:
                        # Конвертируем время в секунды
                        h, m = map(int, new_time_str.split(':'))
                        new_seconds = h * 3600 + m * 60
                    except Exception:
                        continue

                    for existing in existing_qsos:
                        # Проверяем совпадение по основным полям
                        if (new_q['callsign'] == existing['callsign'] and
                            str(new_q['date']) == str(existing['date']) and
                            new_q['band'] == existing['band'] and
                            new_q['mode'] == existing['mode']):

                            # Проверяем время с погрешностью ±5 минут
                            try:
                                ex_time = existing['time'][:5]
                                h, m = map(int, ex_time.split(':'))
                                existing_seconds = h * 3600 + m * 60
                                time_diff = abs(new_seconds - existing_seconds)
                                if time_diff <= 300:  # 5 минут = 300 секунд
                                    filtered.append(existing)
                                    break
                            except Exception:
                                continue

                return filtered

        except Exception as e:
            self.logger.error(f"❌ Ошибка batch поиска: {e}")
            return []

    def _batch_insert(self, normalized_list: List[Dict], user_id: int, conn) -> int:
        """Batch вставка новых QSO с пропуском дубликатов"""
        if not normalized_list:
            return 0

        try:
            with conn.cursor() as cur:
                values = []
                params = []
                for q in normalized_list:
                    record_id = str(uuid.uuid4())
                    date_str = str(q['date']) if q['date'] else None
                    time_str = q['time'][:5] if q['time'] else None
                    values.append("(%s::uuid, %s, %s, %s, %s, %s, %s::date, %s::time, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())")
                    params.extend([
                        record_id, q['callsign'], q['my_callsign'],
                        q['band'], q['frequency'], q['mode'],
                        date_str, time_str,
                        q['prop_mode'], q['sat_name'], q['lotw'], 'N', q['r150s'],
                        q['gridsquare'], q['my_gridsquare'], q['rst_sent'], q['rst_rcvd'],
                        q['ru_region'], q['cqz'], q['ituz'], user_id,
                        q['continent'], q['dxcc'], None
                    ])

                query = f"""
                    INSERT INTO tlog_qso (
                        id, callsign, my_callsign, band, frequency, mode,
                        date, time, prop_mode, sat_name, lotw, paper_qsl, r150s,
                        gridsquare, my_gridsquare, rst_sent, rst_rcvd,
                        ru_region, cqz, ituz, user_id, continent, dxcc, adif_upload_id,
                        created_at, updated_at
                    ) VALUES {', '.join(values)}
                    ON CONFLICT ON CONSTRAINT unique_qso DO NOTHING
                """

                cur.execute(query, params)
                conn.commit()
                return cur.rowcount

        except Exception as e:
            conn.rollback()
            self.logger.error(f"❌ Ошибка batch insert: {e}")
            return 0

    def _batch_update(self, normalized_list: List[Dict], existing_qsos: List[Dict], conn) -> int:
        """Batch обновление существующих QSO данными из LoTW (время не обновляется)"""
        if not normalized_list or not existing_qsos:
            return 0

        try:
            with conn.cursor() as cur:
                updated = 0

                # Сопоставляем normalized_list с existing_qsos по ключу
                for new_q in normalized_list:
                    new_time = new_q['time'][:5]

                    for existing in existing_qsos:
                        # Проверяем совпадение по основным полям
                        if (new_q['callsign'] == existing['callsign'] and
                            str(new_q['date']) == str(existing['date']) and
                            new_q['band'] == existing['band'] and
                            new_q['mode'] == existing['mode']):

                            # Проверяем время с погрешностью ±5 минут
                            try:
                                new_seconds = int(new_time.split(':')[0]) * 3600 + int(new_time.split(':')[1]) * 60
                                ex_time = str(existing['time'])[:5]
                                existing_seconds = int(ex_time.split(':')[0]) * 3600 + int(ex_time.split(':')[1]) * 60
                                time_diff = abs(new_seconds - existing_seconds)

                                if time_diff <= 300:  # ±5 минут
                                    # Обновляем только непустые поля из LoTW
                                    updates = []
                                    values = []

                                    if new_q.get('lotw'):
                                        updates.append("lotw = %s")
                                        values.append(new_q['lotw'])
                                    if new_q.get('gridsquare'):
                                        updates.append("gridsquare = %s")
                                        values.append(new_q['gridsquare'])
                                    if new_q.get('ru_region'):
                                        updates.append("ru_region = %s")
                                        values.append(new_q['ru_region'])
                                    if new_q.get('continent'):
                                        updates.append("continent = %s")
                                        values.append(new_q['continent'])
                                    if new_q.get('cqz') is not None:
                                        updates.append("cqz = %s")
                                        values.append(new_q['cqz'])
                                    if new_q.get('ituz') is not None:
                                        updates.append("ituz = %s")
                                        values.append(new_q['ituz'])
                                    if new_q.get('prop_mode'):
                                        updates.append("prop_mode = %s")
                                        values.append(new_q['prop_mode'])
                                    if new_q.get('sat_name'):
                                        updates.append("sat_name = %s")
                                        values.append(new_q['sat_name'])

                                    # Обновляем dxcc из cty.dat
                                    dxcc = self._get_dxcc_from_cty(new_q['callsign'])
                                    if dxcc:
                                        updates.append("dxcc = %s")
                                        values.append(dxcc)

                                    # Обновляем r150s из r150cty.dat
                                    r150_info = self._get_r150_info(new_q['callsign'])
                                    if r150_info and r150_info.get('country'):
                                        updates.append("r150s = %s")
                                        values.append(r150_info['country'].upper())

                                    if updates:
                                        updates.append("updated_at = NOW()")
                                        values.append(existing['id'])

                                        query = f"""
                                            UPDATE tlog_qso
                                            SET {', '.join(updates)}
                                            WHERE id = %s::uuid
                                        """
                                        cur.execute(query, values)
                                        updated += 1
                                    break
                            except Exception:
                                continue

                conn.commit()
                return updated

        except Exception as e:
            conn.rollback()
            self.logger.error(f"❌ Ошибка batch update: {e}")
            return 0

    def update_lotw_lastsync(self, user_id: int, created_at: str = None) -> bool:
        """
        Обновляет поле lotw_lastsync в таблице tlog_radioprofile.

        Args:
            user_id: ID пользователя
            created_at: Дата синхронизации (по умолчанию текущая дата)

        Returns:
            bool: Успех операции
        """
        if created_at is None:
            from datetime import date
            created_at = date.today().isoformat()

        conn = self.db_conn.get_connection()
        if not conn:
            return False

        try:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE tlog_radioprofile
                    SET lotw_lastsync = %s
                    WHERE id = %s
                """, (created_at, user_id))

                conn.commit()

                if cur.rowcount > 0:
                    self.logger.info(f"✅ lotw_lastsync обновлен для user_id={user_id}: {created_at}")
                    return True
                else:
                    self.logger.warning(f"⚠️ Не найдена запись tlog_radioprofile для user_id={user_id}")
                    return False

        except Exception as e:
            self.logger.error(f"❌ Ошибка обновления lotw_lastsync: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()