"""
Операции с базой данных
"""

import uuid
import psycopg2
from typing import Dict, Any, Optional, List, Union
from datetime import datetime, timedelta, timezone
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
                        gridsquare, my_gridsquare, vucc_grids, iota, app_lotw_rxqsl, rst_sent, rst_rcvd,
                        ru_region, cqz, ituz, user_id, continent, dxcc, adif_upload_id,
                        created_at, updated_at
                    ) VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                              %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                """

                params = [
                    record_id,
                    callsign, my_callsign,
                    normalized_data['band'], normalized_data['frequency'], normalized_data['mode'],
                    normalized_data['date'], normalized_data['time'],
                    normalized_data['prop_mode'], normalized_data['sat_name'],
                    normalized_data['lotw'], 'N', normalized_data['r150s'],
                    normalized_data['gridsquare'], normalized_data['my_gridsquare'],
                    normalized_data['vucc_grids'], normalized_data['iota'],
                    normalized_data['app_lotw_rxqsl'],
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
                        vucc_grids = %s,
                        iota = %s,
                        app_lotw_rxqsl = %s,
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
                    normalized_data['vucc_grids'], normalized_data['iota'],
                    normalized_data['app_lotw_rxqsl'],
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
            self.logger.debug(f"🔄 Обработка {len(qso_data_list)} QSO (user_id={user_id})")

            # Проверяем структуру таблицы
            self.logger.debug("🔍 Проверяем структуру таблицы...")
            if not self.check_table_structure():
                return {
                    'success': False,
                    'error': 'Неправильная структура таблицы tlog_qso',
                    'message': 'Колонка app_lotw_rxqsl не найдена'
                }

            # Нормализуем все данные заранее
            normalized_list = []
            skipped = 0

            self.logger.debug(f"🔍 Нормализация: начинаем обработку {len(qso_data_list)} сырых данных")

            for i, qso_data in enumerate(qso_data_list):
                self.logger.debug(f"🔍 Нормализация QSO #{i+1}: CALL={qso_data.get('CALL')}, BAND={qso_data.get('BAND')}")

                # Проверка обязательных полей
                required_fields = ['CALL', 'QSO_DATE', 'TIME_ON', 'BAND']
                missing_fields = [field for field in required_fields if not qso_data.get(field)]

                if missing_fields:
                    self.logger.debug(f"🔍 Нормализация QSO #{i+1}: пропущен, отсутствуют поля: {missing_fields}")
                    skipped += 1
                    continue

                try:
                    normalized = self.normalizer.prepare_qso_data(qso_data, my_callsign)
                    self.logger.debug(f"🔍 Нормализация QSO #{i+1}: успешно нормализован")
                    self.logger.debug(f"🔍 Нормализация QSO #{i+1}: app_lotw_rxqsl={normalized.get('app_lotw_rxqsl')} (тип: {type(normalized.get('app_lotw_rxqsl'))})")
                    normalized_list.append(normalized)
                except Exception as e:
                    self.logger.error(f"❌ Нормализация QSO #{i+1}: ошибка - {e}")
                    skipped += 1
                    continue

            self.logger.info(f"🔍 Нормализация: завершено. Добавлено {len(normalized_list)}, пропущено {skipped}")

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

            # Разделяем на новые и существующие на основе app_lotw_rxqsl
            new_qsos = []
            update_qsos = []

            self.logger.info(f"🔍 Найдено {len(existing_qsos)} существующих QSO в БД")
            self.logger.info(f"🔍 Обрабатываем {len(normalized_list)} нормализованных QSO")

            for i, q in enumerate(normalized_list):
                self.logger.debug(f"🔍 QSO #{i+1}: {q['callsign']} {q['date']} {q['time']} {q['band']} {q['mode']}")

                # Ищем соответствующий существующий QSO
                matching_existing = None
                for j, ex in enumerate(existing_qsos):
                    if (q['callsign'] == ex['callsign'] and
                        str(q['date']) == str(ex['date']) and
                        q['band'] == ex['band'] and
                        q['mode'] == ex['mode']):
                        # Проверяем время с погрешностью ±5 минут (300 секунд)
                        try:
                            new_time = q['time'][:5]
                            h, m = map(int, new_time.split(':'))
                            new_seconds = h * 3600 + m * 60

                            ex_time = str(ex['time'])[:5]
                            h, m = map(int, ex_time.split(':'))
                            existing_seconds = h * 3600 + m * 60

                            time_diff = abs(new_seconds - existing_seconds)
                            self.logger.debug(f"🔍 Время сравнения: new={new_time}({new_seconds}s), existing={ex_time}({existing_seconds}s), diff={time_diff}s")

                            if time_diff <= 300:  # 5 минут = 300 секунд
                                matching_existing = ex
                                self.logger.debug(f"🔍 Найдено совпадение с существующим QSO #{j+1}")
                                break
                        except Exception as e:
                            self.logger.error(f"❌ Ошибка при сравнении времени: {e}")
                            continue

                if matching_existing:
                    # Проверяем, нужно ли обновлять на основе app_lotw_rxqsl
                    should_update = self._should_update_qso(q, matching_existing)
                    if should_update:
                        update_qsos.append(q)
                        self.logger.debug(f"🔍 QSO #{i+1} будет обновлено (app_lotw_rxqsl новее)")
                    else:
                        self.logger.debug(f"🔍 QSO #{i+1} пропущено (app_lotw_rxqsl не новее)")
                else:
                    new_qsos.append(q)
                    # Добавляем дополнительное логирование для отладки
                    self.logger.debug(f"✅ QSO #{i+1} {q['callsign']} {q['date']} {q['time']} {q['band']} {q['mode']} добавлено как НОВОЕ")

                    # Логируем детали для IC8TEM или первых нескольких QSO
                    if q['callsign'] == 'IC8TEM' or i < 3:
                        self.logger.debug(f"🔍 Детали нового QSO {q['callsign']}:")
                        self.logger.debug(f"   - callsign: {q['callsign']}")
                        self.logger.debug(f"   - my_callsign: {q['my_callsign']}")
                        self.logger.debug(f"   - date: {q['date']} (тип: {type(q['date'])})")
                        self.logger.debug(f"   - time: {q['time']} (тип: {type(q['time'])})")
                        self.logger.debug(f"   - band: {q['band']}")
                        self.logger.debug(f"   - mode: {q['mode']}")
                        self.logger.debug(f"   - app_lotw_rxqsl: {q.get('app_lotw_rxqsl')}")

            self.logger.info(f"🔍 Итого: {len(new_qsos)} новых QSO, {len(update_qsos)} для обновления")

            added = 0
            updated = 0

            # Batch insert новых
            if new_qsos:
                self.logger.debug(f"🔍 Вызываем _batch_insert для {len(new_qsos)} новых QSO")

                # Создаем курсор для проверки дубликатов
                with conn.cursor() as cur:
                    # Проверяем, нет ли уже таких QSO в базе данных
                    for q in new_qsos[:3]:  # Проверяем первые 3 для отладки
                        check_query = """
                            SELECT COUNT(*) FROM tlog_qso
                            WHERE user_id = %s AND callsign = %s AND date = %s::date AND band = %s AND mode = %s
                        """
                        cur.execute(check_query, (user_id, q['callsign'], str(q['date']), q['band'], q['mode']))
                        count = cur.fetchone()[0]
                        self.logger.debug(f"🔍 Проверка дубликатов для {q['callsign']} {q['date']} {q['band']} {q['mode']}: {count} найдено")

                added = self._batch_insert(new_qsos, user_id, conn)
            else:
                added = 0
                self.logger.info("🔍 Нет новых QSO для вставки")

            # Batch update существующих
            if update_qsos:
                self.logger.debug(f"🔍 Вызываем _batch_update для {len(update_qsos)} QSO для обновления")
                updated = self._batch_update(update_qsos, existing_qsos, conn)
            else:
                updated = 0
                self.logger.info("🔍 Нет QSO для обновления")

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
            import traceback
            self.logger.error(f"❌ Stack trace: {traceback.format_exc()}")
            return {
                'success': False,
                'error': str(e),
                'message': 'Критическая ошибка при обработке данных'
            }
        finally:
            conn.close()

    def check_table_structure(self) -> bool:
        """Проверяет структуру таблицы tlog_qso"""
        conn = self.db_conn.get_connection()
        if not conn:
            return False

        try:
            with conn.cursor() as cur:
                # Проверяем существование колонки app_lotw_rxqsl
                cur.execute("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = 'tlog_qso' AND column_name = 'app_lotw_rxqsl'
                """)
                result = cur.fetchone()
                if result:
                    self.logger.debug(f"✅ Колонка app_lotw_rxqsl существует: {result}")
                    return True
                else:
                    self.logger.error("❌ Колонка app_lotw_rxqsl НЕ существует в таблице tlog_qso!")
                    # Показываем все колонки
                    cur.execute("""
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_name = 'tlog_qso'
                        ORDER BY column_name
                    """)
                    columns = cur.fetchall()
                    self.logger.debug("🔍 Существующие колонки в tlog_qso:")
                    for col in columns:
                        self.logger.debug(f"  - {col[0]} ({col[1]})")
                    return False
        except Exception as e:
            self.logger.error(f"❌ Ошибка проверки структуры таблицы: {e}")
            return False
        finally:
            conn.close()

    def _should_update_qso(self, new_q: Dict, existing_q: Dict) -> bool:
        """
        Определяет, нужно ли обновлять существующий QSO на основе app_lotw_rxqsl.

        Возвращает True если:
        - app_lotw_rxqsl в базе данных NULL
        - app_lotw_rxqsl в новых данных новее чем в базе данных

        Args:
            new_q: Новые данные QSO из LoTW
            existing_q: Существующий QSO из базы данных

        Returns:
            bool: True если нужно обновить, False если пропустить
        """
        try:
            new_rxqsl = new_q.get('app_lotw_rxqsl')
            existing_rxqsl = existing_q.get('app_lotw_rxqsl')

            # Если в базе данных NULL → обновляем
            if existing_rxqsl is None:
                return True

            # Если в новых данных нет app_lotw_rxqsl → обновляем (на всякий случай)
            if new_rxqsl is None:
                return True

            # Логируем для отладки
            self.logger.debug(f"🔍 Сравнение app_lotw_rxqsl: new={new_rxqsl} vs existing={existing_rxqsl}")

            # Теперь оба datetime должны быть timezone-aware, можем сравнивать напрямую
            return new_rxqsl > existing_rxqsl

        except Exception as e:
            self.logger.error(f"❌ Ошибка при сравнении app_lotw_rxqsl: {e}")
            # В случае ошибки обновляем для безопасности
            return True

    def _find_existing_batch(self, normalized_list: List[Dict], user_id: int, conn) -> List[Dict]:
        """Batch поиск существующих QSO с погрешностью времени ±5 минут"""
        if not normalized_list:
            return []

        try:
            # Создаем курсор для операций поиска
            with conn.cursor() as cur:
                # Формируем VALUES для поиска по callsign, date, band, mode (без time)
                values = []
                params = [user_id]
                for q in normalized_list:
                    values.append("(%s, %s::date, %s, %s)")
                    params.extend([q['callsign'], q['date'], q['band'], q['mode']])

                query = f"""
                    SELECT id, callsign, date::text, band, mode, time::text, app_lotw_rxqsl
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
                        'time': row[5],
                        'app_lotw_rxqsl': row[6]
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
            self.logger.debug("🔍 _batch_insert: пустой список QSO")
            return 0

        try:
            self.logger.debug(f"🔍 _batch_insert: начинаем вставку {len(normalized_list)} QSO для user_id={user_id}")

            # Логируем первые несколько QSO для отладки
            for i, q in enumerate(normalized_list[:3]):
                self.logger.debug(f"🔍 QSO #{i+1} для вставки: {q['callsign']} {q['date']} {q['time']} {q['band']} {q['mode']}")
                app_rxqsl = q.get('app_lotw_rxqsl')
                if app_rxqsl:
                    self.logger.debug(f"🔍 QSO #{i+1} app_lotw_rxqsl: {app_rxqsl} (тип: {type(app_rxqsl)})")
                    if hasattr(app_rxqsl, 'isoformat'):
                        self.logger.debug(f"🔍 QSO #{i+1} app_lotw_rxqsl ISO: {app_rxqsl.isoformat()}")
                else:
                    self.logger.debug(f"🔍 QSO #{i+1} app_lotw_rxqsl: None")

            if len(normalized_list) > 3:
                self.logger.debug(f"🔍 ... и еще {len(normalized_list) - 3} QSO")

            # Создаем курсор для операций
            with conn.cursor() as cur:
                # ДИАГНОСТИКА: Проверяем уникальные ограничения для первых QSO
                self.logger.debug("🔍 ДИАГНОСТИКА: проверяем уникальные ограничения...")
                for i, q in enumerate(normalized_list[:3]):
                    # Проверяем существование точно такого же QSO
                    check_query = """
                        SELECT id, callsign, date, band, mode, time
                        FROM tlog_qso
                        WHERE user_id = %s AND callsign = %s AND date = %s::date AND band = %s AND mode = %s
                    """
                    cur.execute(check_query, (user_id, q['callsign'], str(q['date']), q['band'], q['mode']))
                    existing = cur.fetchall()

                    if existing:
                        self.logger.debug(f"🔍 QSO #{i+1} {q['callsign']} {q['date']} {q['band']} {q['mode']}: НАЙДЕН в БД")
                        for ex in existing:
                            self.logger.debug(f"   Существующий: {ex}")
                    else:
                        self.logger.debug(f"🔍 QSO #{i+1} {q['callsign']} {q['date']} {q['band']} {q['mode']}: НЕ НАЙДЕН в БД - будет добавлен")

                values = []
                params = []
                for q in normalized_list:
                    record_id = str(uuid.uuid4())
                    date_str = str(q['date']) if q['date'] else None
                    time_str = q['time'][:5] if q['time'] else None

                    # Проверяем app_lotw_rxqsl для первого QSO
                    if len(normalized_list) <= 3:
                        app_rxqsl_val = q.get('app_lotw_rxqsl')
                        self.logger.debug(f"🔍 Подготавливаем QSO: {q['callsign']}, app_lotw_rxqsl={app_rxqsl_val} (тип: {type(app_rxqsl_val)})")
                        if app_rxqsl_val:
                            try:
                                # Проверяем, можно ли сериализовать в ISO
                                iso_format = app_rxqsl_val.isoformat()
                                self.logger.debug(f"🔍 app_lotw_rxqsl ISO формат: {iso_format}")
                            except Exception as e:
                                self.logger.error(f"❌ Ошибка сериализации app_lotw_rxqsl: {e}")

                    # Обрабатываем app_lotw_rxqsl для безопасной передачи в PostgreSQL
                    app_lotw_rxqsl_value = q.get('app_lotw_rxqsl')
                    if app_lotw_rxqsl_value and isinstance(app_lotw_rxqsl_value, datetime):
                        # Конвертируем datetime в строку ISO формата для PostgreSQL
                        app_lotw_rxqsl_value = app_lotw_rxqsl_value.isoformat()
                        self.logger.debug(f"🔍 Конвертирован app_lotw_rxqsl в строку: {app_lotw_rxqsl_value}")

                    # Подсчитываем количество полей в VALUES
                    # id, callsign, my_callsign, band, frequency, mode, date, time, prop_mode, sat_name,
                    # lotw, paper_qsl, r150s, gridsquare, my_gridsquare, vucc_grids, iota, app_lotw_rxqsl,
                    # rst_sent, rst_rcvd, ru_region, cqz, ituz, user_id, continent, dxcc, adif_upload_id
                    # = 27 полей (created_at и updated_at будут NOW() в SQL)
                    values.append("(%s::uuid, %s, %s, %s, %s, %s, %s::date, %s::time, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())")

                    # Добавляем 27 параметров (created_at и updated_at устанавливаются NOW() в SQL)
                    params.extend([
                        record_id,                          # 1. id
                        q['callsign'],                      # 2. callsign
                        q['my_callsign'],                   # 3. my_callsign
                        q['band'],                          # 4. band
                        q['frequency'],                     # 5. frequency
                        q['mode'],                          # 6. mode
                        date_str,                           # 7. date
                        time_str,                           # 8. time
                        q['prop_mode'],                     # 9. prop_mode
                        q['sat_name'],                      # 10. sat_name
                        q['lotw'],                          # 11. lotw
                        'N',                                # 12. paper_qsl
                        q['r150s'],                         # 13. r150s
                        q['gridsquare'],                    # 14. gridsquare
                        q['my_gridsquare'],                 # 15. my_gridsquare
                        q['vucc_grids'],                    # 16. vucc_grids
                        q['iota'],                          # 17. iota
                        app_lotw_rxqsl_value,               # 18. app_lotw_rxqsl
                        q['rst_sent'],                      # 19. rst_sent
                        q['rst_rcvd'],                      # 20. rst_rcvd
                        q['ru_region'],                     # 21. ru_region
                        q['cqz'],                           # 22. cqz
                        q['ituz'],                          # 23. ituz
                        user_id,                            # 24. user_id
                        q['continent'],                     # 25. continent
                        q['dxcc'],                          # 26. dxcc
                        None                                # 27. adif_upload_id
                    ])

                # Формируем запрос с правильным экранированием
                values_str = ', '.join(values)
                query = f"""
                    INSERT INTO tlog_qso (
                        id, callsign, my_callsign, band, frequency, mode,
                        date, time, prop_mode, sat_name, lotw, paper_qsl, r150s,
                        gridsquare, my_gridsquare, vucc_grids, iota, app_lotw_rxqsl, rst_sent, rst_rcvd,
                        ru_region, cqz, ituz, user_id, continent, dxcc, adif_upload_id,
                        created_at, updated_at
                    ) VALUES {values_str}
                    ON CONFLICT ON CONSTRAINT unique_qso DO NOTHING
                    RETURNING 1
                """

                self.logger.debug(f"🔍 _batch_insert: выполняем SQL запрос с {len(params)} параметрами")
                self.logger.debug(f"🔍 SQL запрос (первые 500 символов): {query[:500]}...")
                self.logger.debug(f"🔍 Параметры типы: {[type(p).__name__ for p in params[:10]]}")  # Показываем типы первых 10 параметров

                try:
                    # Проверяем и логируем проблемные параметры
                    for i, param in enumerate(params[:5]):
                        if isinstance(param, datetime):
                            self.logger.debug(f"🔍 Параметр {i}: datetime = {param}, iso = {param.isoformat()}")

                    cur.execute(query, params)
                    inserted_rows = cur.fetchall()
                    conn.commit()
                except Exception as sql_error:
                    self.logger.error(f"❌ SQL ошибка при выполнении запроса: {sql_error}")
                    self.logger.error(f"❌ SQL запрос: {query}")
                    self.logger.error(f"❌ Параметры: {params[:20]}...")  # Показываем первые 20 параметров
                    raise sql_error

                inserted_count = len(inserted_rows) if inserted_rows else 0
                self.logger.info(f"✅ _batch_insert: добавлено {inserted_count} QSO из {len(normalized_list)}")

                if inserted_count == 0 and len(normalized_list) > 0:
                    self.logger.warning(f"⚠️ _batch_insert: 0 QSO добавлено, возможно все заблокированы уникальным ограничением")

                    # Попробуем выполнить одну вставку отдельно для диагностики
                    if len(normalized_list) > 0:
                        self.logger.debug("🔍 Пробуем вставить один QSO отдельно для диагностики...")
                        test_q = normalized_list[0]
                        test_record_id = str(uuid.uuid4())
                        test_params = [
                            test_record_id, test_q['callsign'], test_q['my_callsign'],
                            test_q['band'], test_q['frequency'], test_q['mode'],
                            str(test_q['date']), test_q['time'][:5],
                            test_q['prop_mode'], test_q['sat_name'], test_q['lotw'], 'N', test_q['r150s'],
                            test_q['gridsquare'], test_q['my_gridsquare'], test_q['vucc_grids'], test_q['iota'],
                            test_q['app_lotw_rxqsl'],
                            test_q['rst_sent'], test_q['rst_rcvd'],
                            test_q['ru_region'], test_q['cqz'], test_q['ituz'], user_id,
                            test_q['continent'], test_q['dxcc'], None
                        ]

                        test_query = """
                            INSERT INTO tlog_qso (
                                id, callsign, my_callsign, band, frequency, mode,
                                date, time, prop_mode, sat_name, lotw, paper_qsl, r150s,
                                gridsquare, my_gridsquare, vucc_grids, iota, app_lotw_rxqsl, rst_sent, rst_rcvd,
                                ru_region, cqz, ituz, user_id, continent, dxcc, adif_upload_id,
                                created_at, updated_at
                            ) VALUES (%s::uuid, %s, %s, %s, %s, %s, %s::date, %s::time, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW())
                            ON CONFLICT ON CONSTRAINT unique_qso DO NOTHING
                            RETURNING 1
                        """

                        # Логируем параметры тестового запроса
                        self.logger.debug("🔍 Параметры тестовой вставки:")
                        for i, param in enumerate(test_params):
                            self.logger.debug(f"   [{i}]: {param} (тип: {type(param).__name__})")

                        try:
                            cur.execute(test_query, test_params)
                            test_result = cur.fetchall()
                            self.logger.debug(f"🔍 Тестовая вставка: {len(test_result)} записи добавлено")
                        except Exception as test_error:
                            self.logger.error(f"❌ Ошибка тестовой вставки: {test_error}")
                            self.logger.error(f"❌ Тип ошибки: {type(test_error)}")
                            # Попробуем вставить по одному параметру для диагностики
                            try:
                                self.logger.debug("🔍 Пробуем простую вставку без app_lotw_rxqsl...")
                                simple_params = test_params[:17] + [None] + test_params[18:]  # Заменяем app_lotw_rxqsl на None
                                simple_query = test_query.replace("%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s",
                                                                  "%s, %s, %s, %s, %s, %s, %s::date, %s::time, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s")
                                cur.execute(simple_query, simple_params)
                                simple_result = cur.fetchall()
                                self.logger.debug(f"✅ Простая вставка успешна: {len(simple_result)} записи добавлено")
                            except Exception as simple_error:
                                self.logger.error(f"❌ Ошибка простой вставки: {simple_error}")

            return inserted_count

        except Exception as e:
            conn.rollback()
            self.logger.error(f"❌ Ошибка batch insert: {e}")
            import traceback
            self.logger.error(f"❌ Stack trace: {traceback.format_exc()}")
            return 0

    def _batch_update(self, normalized_list: List[Dict], existing_qsos: List[Dict], conn) -> int:
        """Batch обновление существующих QSO данными из LoTW (время не обновляется)"""
        if not normalized_list or not existing_qsos:
            return 0

        try:
            updated = 0

            # Создаем курсор для операций
            with conn.cursor() as cur:

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
                                    # Обновляем все поля из LoTW (как при повторном QSO)
                                    updates = []
                                    values = []

                                    # Логируем app_lotw_rxqsl для отладки
                                    app_rxqsl_value = new_q.get('app_lotw_rxqsl')
                                    self.logger.debug(f"🔍 app_lotw_rxqsl для {new_q['callsign']} {new_q['date']} {new_q['time']}: {app_rxqsl_value} (тип: {type(app_rxqsl_value)})")

                                    # Основные поля из LoTW
                                    updates.extend([
                                        "frequency = %s",
                                        "mode = %s",
                                        "lotw = %s",
                                        "gridsquare = %s",
                                        "my_gridsquare = %s",
                                        "vucc_grids = %s",
                                        "iota = %s",
                                        "app_lotw_rxqsl = %s",
                                        "rst_sent = %s",
                                        "rst_rcvd = %s",
                                        "cqz = %s",
                                        "ituz = %s",
                                        "prop_mode = %s",
                                        "sat_name = %s"
                                    ])
                                    values.extend([
                                        new_q.get('frequency', ''),
                                        new_q.get('mode', ''),
                                        new_q.get('lotw', 'N'),
                                        new_q.get('gridsquare', ''),
                                        new_q.get('my_gridsquare', ''),
                                        new_q.get('vucc_grids', ''),
                                        new_q.get('iota', ''),
                                        app_rxqsl_value,
                                        new_q.get('rst_sent', ''),
                                        new_q.get('rst_rcvd', ''),
                                        new_q.get('cqz'),
                                        new_q.get('ituz'),
                                        new_q.get('prop_mode', ''),
                                        new_q.get('sat_name', '')
                                    ])

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

                                    # Дополнительные поля
                                    if new_q.get('ru_region'):
                                        updates.append("ru_region = %s")
                                        values.append(new_q['ru_region'])
                                    if new_q.get('continent'):
                                        updates.append("continent = %s")
                                        values.append(new_q['continent'])

                                    # Обновляем временную метку
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

    def update_lotw_lastsync(self, user_id: int, created_at: Union[str, datetime] = None) -> bool:
        """
        Обновляет поле lotw_lastsync в таблице tlog_radioprofile.

        Args:
            user_id: ID пользователя
            created_at: Дата и время синхронизации (по умолчанию текущие дата и время)
                       Может быть строкой или объектом datetime

        Returns:
            bool: Успех операции
        """
        if created_at is None:
            created_at = datetime.now(timezone.utc)

        conn = self.db_conn.get_connection()
        if not conn:
            return False

        try:
            with conn.cursor() as cur:
                # Логируем что передаем в базу
                self.logger.info(f"🔍 update_lotw_lastsync: передаем {created_at} (тип: {type(created_at)})")

                # Проверяем тип данных и логируем дополнительную информацию
                if isinstance(created_at, str):
                    self.logger.debug(f"🔍 String datetime value: '{created_at}'")
                elif isinstance(created_at, datetime):
                    self.logger.debug(f"🔍 Datetime object: {created_at}")
                    self.logger.debug(f"🔍 Datetime isoformat(): {created_at.isoformat()}")
                    self.logger.debug(f"🔍 Datetime timezone: {created_at.tzinfo}")
                    self.logger.debug(f"🔍 Datetime is timezone-aware: {created_at.tzinfo is not None}")
                else:
                    self.logger.warning(f"🔍 Unexpected type: {type(created_at)}")

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