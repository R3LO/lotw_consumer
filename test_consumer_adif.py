#!/usr/bin/env python3
"""
Test Consumer ADIF - Обрабатывает ADIF файл как реальный consumer.py

Использование:
    python test_consumer_adif.py <имя_файла.adi> <username>

Пример:
    python test_consumer_adif.py lotwreport.adi R3LO
"""

import sys
import os
import re
import uuid
import psycopg2
from datetime import datetime

from config import (
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DB_SCHEMA
)


class ADIFParser:
    """Класс для парсинга ADIF формата"""

    def __init__(self, logger=None):
        self.logger = logger

    def parse_adif_response_all_fields(self, content: str):
        """
        Парсит ADIF формат ответа от LoTW.
        """
        qso_list = []

        if '<eor>' not in content and 'QSO_DATE' not in content:
            if self.logger:
                self.logger.info("В ответе нет данных QSO")
            return qso_list

        qso_blocks = content.split('<eor>')

        for block_num, block in enumerate(qso_blocks, 1):
            if not block.strip() or '<eoh>' in block:
                continue

            if '<eoh>' in block:
                block = block.split('<eoh>')[1]

            block = re.sub(r'//.*', '', block)

            pattern = r'<(\w+)(?::(\d+))?>([^<]*)'
            qso = {}
            fields_found = []

            matches = re.findall(pattern, block)

            for field_name, length, value in matches:
                field_name = field_name.upper()

                if length and length.isdigit():
                    value = value[:int(length)].strip()
                else:
                    value = value.strip()

                if value:
                    qso[field_name] = value
                    fields_found.append(field_name)

            if qso and 'CALL' in qso:
                qso_list.append(qso)

        return qso_list


class DataNormalizer:
    """Класс для нормализации данных"""

    def __init__(self, logger=None):
        self.logger = logger
        self._init_lookup()

    def _init_lookup(self):
        """Инициализация DXCC lookup"""
        try:
            sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
            from dxcc_lookup import get_dxcc_info as get_r150_info
            from cty_lookup import get_dxcc_from_cty
            self._get_r150_info = get_r150_info
            self._get_dxcc_from_cty = get_dxcc_from_cty
        except ImportError:
            self._get_r150_info = lambda x: {'country': None, 'continent': None}
            self._get_dxcc_from_cty = lambda x: None

    def normalize_frequency(self, freq_str: str):
        """Нормализует частоту"""
        if not freq_str:
            return None

        try:
            freq_str = freq_str.strip()
            if not re.match(r'^[\d\.]+$', freq_str):
                return None

            freq_float = float(freq_str)
            if freq_float < 10:
                freq_float = freq_float * 1000

            return round(freq_float, 3)
        except (ValueError, TypeError):
            return None

    def normalize_band(self, band_str: str):
        """Нормализует название диапазона"""
        if not band_str:
            return ''

        band_str = band_str.upper().strip()

        band_mapping = {
            '160M': '160M', '80M': '80M', '40M': '40M', '30M': '30M',
            '20M': '20M', '17M': '17M', '15M': '15M', '12M': '12M',
            '10M': '10M', '6M': '6M', '2M': '2M', '70CM': '70CM',
            '23CM': '23CM', '13CM': '13CM',
        }

        if band_str in band_mapping:
            return band_str

        for key in band_mapping.keys():
            if key in band_str:
                return key

        return band_str

    def normalize_time(self, time_str: str):
        """Нормализует время"""
        if not time_str:
            return "00:00:00"

        time_str = str(time_str).strip().zfill(4)

        if len(time_str) == 4:
            return f"{time_str[:2]}:{time_str[2:4]}:00"
        elif len(time_str) == 6:
            return f"{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}"
        else:
            return "00:00:00"

    def normalize_date(self, date_str: str):
        """Нормализует дату"""
        if not date_str:
            return ""

        date_str = str(date_str).strip()
        if len(date_str) == 8:
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        return ""

    def get_mode(self, qso_data):
        """Получает режим"""
        mode = qso_data.get('MODE', '').upper()
        if mode == 'MFSK':
            submode = qso_data.get('SUBMODE', '')
            if submode:
                return submode.upper()
        return mode

    def get_lotw_status(self, qso_data):
        """Определяет статус LoTW"""
        qsl_rcvd = qso_data.get('QSL_RCVD', '').upper()
        return 'Y' if qsl_rcvd == 'Y' else 'N'

    def normalize_cqz(self, cqz_str):
        """Нормализует CQ зону"""
        if not cqz_str:
            return None

        try:
            cqz_str = str(cqz_str).strip()
            if cqz_str.isdigit():
                return int(cqz_str)
            return None
        except (ValueError, TypeError):
            return None

    def normalize_ituz(self, ituz_str):
        """Нормализует ITU зону"""
        if not ituz_str:
            return None

        try:
            ituz_str = str(ituz_str).strip()
            if ituz_str.isdigit():
                return int(ituz_str)
            return None
        except (ValueError, TypeError):
            return None

    def prepare_qso_data(self, qso_data, username=''):
        """Подготавливает все данные QSO"""
        callsign = qso_data.get('CALL', '').upper()

        # DXCC lookup
        r150_info = self._get_r150_info(callsign) if callsign else None
        if r150_info:
            r150s = r150_info['country'].upper() if r150_info['country'] else None
            continent = r150_info['continent'].upper() if r150_info['continent'] else None
        else:
            r150s = None
            continent = None

        dxcc = self._get_dxcc_from_cty(callsign) if callsign else None

        ru_region = None
        if dxcc in ('UA', 'UA2', 'UA9'):
            state = qso_data.get('STATE', '').upper()
            if state:
                ru_region = state

        return {
            'band': self.normalize_band(qso_data.get('BAND', '')),
            'frequency': self.normalize_frequency(qso_data.get('FREQ', '')),
            'mode': self.get_mode(qso_data),
            'date': self.normalize_date(qso_data.get('QSO_DATE', '')),
            'time': self.normalize_time(qso_data.get('TIME_ON', '')),
            'prop_mode': qso_data.get('PROP_MODE', ''),
            'sat_name': qso_data.get('SAT_NAME', ''),
            'lotw': self.get_lotw_status(qso_data),
            'r150s': r150s,
            'gridsquare': qso_data.get('GRIDSQUARE', ''),
            'my_gridsquare': qso_data.get('MY_GRIDSQUARE', ''),
            'rst_sent': qso_data.get('RST_SENT', ''),
            'rst_rcvd': qso_data.get('RST_RCVD', ''),
            'ru_region': ru_region,
            'cqz': self.normalize_cqz(qso_data.get('CQZ', '')),
            'ituz': self.normalize_ituz(qso_data.get('ITUZ', '')),
            'continent': continent,
            'dxcc': dxcc,
            'callsign': callsign,
            'my_callsign': username
        }


class DatabaseOperations:
    """Класс для операций с базой данных"""

    def __init__(self, logger=None):
        self.logger = logger
        self.normalizer = DataNormalizer(logger)

    def get_connection(self):
        """Подключение к БД"""
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )

        with conn.cursor() as cur:
            cur.execute(f"SET search_path TO {DB_SCHEMA}")

        return conn

    def get_user_id_by_username(self, username):
        """Ищет user_id по username"""
        conn = self.get_connection()
        if not conn:
            return None

        try:
            with conn.cursor() as cur:
                query = "SELECT id FROM auth_user WHERE username = %s"
                cur.execute(query, (username,))
                result = cur.fetchone()

                if result:
                    if self.logger:
                        self.logger.debug(f"Найден user_id={result[0]} для username={username}")
                    return result[0]
                else:
                    if self.logger:
                        self.logger.warning(f"Не найден user_id для username={username}")
                    return None
        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка при поиске user_id: {e}")
            return None
        finally:
            conn.close()

    def find_existing_qso(self, qso_data, user_id):
        """Ищет существующую QSO"""
        from datetime import datetime, timedelta

        callsign = qso_data.get('CALL', '').upper()
        my_callsign = qso_data.get('STATION_CALLSIGN', '') or qso_data.get('MY_CALLSIGN', '')
        date_str = self.normalizer.normalize_date(qso_data.get('QSO_DATE', ''))
        time_str = self.normalizer.normalize_time(qso_data.get('TIME_ON', ''))
        band = self.normalizer.normalize_band(qso_data.get('BAND', ''))
        mode = self.normalizer.get_mode(qso_data)

        if not all([callsign, my_callsign, date_str, time_str, band, mode]):
            if self.logger:
                self.logger.debug(f"Недостаточно данных для поиска QSO")
            return None

        conn = self.get_connection()
        if not conn:
            return None

        try:
            qso_time = datetime.strptime(time_str, '%H:%M:%S').time()
            time_lower = (datetime.combine(datetime.today(), qso_time) - timedelta(minutes=10)).time()
            time_upper = (datetime.combine(datetime.today(), qso_time) + timedelta(minutes=10)).time()

            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
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
                    if self.logger:
                        self.logger.debug(f"Найдена существующая QSO: ID={result[0]}")
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
                    if self.logger:
                        self.logger.debug(f"Найдена близкая QSO: ID={result2[0]}")
                    return result2

                return None

        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка при поиске QSO: {e}")
            return None
        finally:
            conn.close()

    def insert_qso(self, qso_data, username, user_id):
        """Вставляет новую QSO"""
        conn = self.get_connection()
        if not conn:
            return False

        try:
            record_id = str(uuid.uuid4())
            callsign = qso_data.get('CALL', '').upper()
            normalized_data = self.normalizer.prepare_qso_data(qso_data, username)

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

                if self.logger:
                    self.logger.debug(f"Добавлена новая QSO: {callsign} (UUID: {record_id})")
                return True

        except psycopg2.errors.UniqueViolation:
            conn.rollback()
            if self.logger:
                self.logger.warning(f"Обнаружен дубликат QSO (уникальное ограничение)")
            return False
        except Exception as e:
            conn.rollback()
            if self.logger:
                self.logger.error(f"Ошибка при добавлении QSO: {e}")
            return False
        finally:
            conn.close()

    def update_qso(self, qso_id, qso_data):
        """Обновляет существующую QSO"""
        conn = self.get_connection()
        if not conn:
            return False

        try:
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

                if self.logger:
                    self.logger.debug(f"Обновлена QSO ID={qso_id}")
                return True

        except Exception as e:
            conn.rollback()
            if self.logger:
                self.logger.error(f"Ошибка при обновлении QSO ID={qso_id}: {e}")
            return False
        finally:
            conn.close()

    def update_lotw_lastsync(self, user_id, created_at):
        """Обновляет поле lotw_lastsync"""
        conn = self.get_connection()
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
                return True

        except Exception as e:
            if self.logger:
                self.logger.error(f"Ошибка обновления lotw_lastsync: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()


class TestConsumerADIF:
    """Тестовый consumer для обработки ADIF файлов"""

    def __init__(self):
        self.parser = ADIFParser()
        self.db_ops = DatabaseOperations()

    def process_adif_file(self, filename: str, username: str):
        """Обрабатывает ADIF файл"""
        print("=" * 60)
        print(" TEST CONSUMER ADIF")
        print("=" * 60)
        print(f"   Файл: {filename}")
        print(f"   Username: {username}")
        print(f"   Время запуска: {datetime.now().isoformat()}")
        print()

        # Проверяем существование файла
        if not os.path.exists(filename):
            print(f"[ERROR] Файл не найден: {filename}")
            return

        # Читаем файл
        try:
            with open(filename, 'r', encoding='utf-8', errors='replace') as f:
                content = f.read()
        except Exception as e:
            print(f"[ERROR] Ошибка чтения файла: {e}")
            return

        # Парсим ADIF
        print("=" * 60)
        print(" ПАРСИНГ ADIF")
        print("=" * 60)

        qso_data_list = self.parser.parse_adif_response_all_fields(content)
        print(f"Найдено QSO: {len(qso_data_list)}")
        print()

        if not qso_data_list:
            print("[INFO] Нет данных QSO для обработки")
            return

        # Показываем первые 3 QSO
        print("=" * 60)
        print(" ПРИМЕРЫ QSO")
        print("=" * 60)
        for i, qso in enumerate(qso_data_list[:3], 1):
            print(f"  QSO #{i}:")
            print(f"    CALL: {qso.get('CALL', 'N/A')}")
            print(f"    DATE: {qso.get('QSO_DATE', 'N/A')}")
            print(f"    TIME: {qso.get('TIME_ON', 'N/A')}")
            print(f"    BAND: {qso.get('BAND', 'N/A')}")
            print(f"    MODE: {qso.get('MODE', 'N/A')}")
            print(f"    RST: {qso.get('RST_SENT', 'N/A')} -> {qso.get('RST_RCVD', 'N/A')}")
            print()
        print(f"  ... и еще {len(qso_data_list) - 3} QSO")
        print()

        # Ищем user_id
        print("=" * 60)
        print(" ПОИСК ПОЛЬЗОВАТЕЛЯ")
        print("=" * 60)

        user_id = self.db_ops.get_user_id_by_username(username)
        if not user_id:
            print(f"[ERROR] Не найден user_id для username={username}")
            return

        print(f"Найден user_id: {user_id}")
        print()

        # Обрабатываем QSO
        print("=" * 60)
        print(" ОБРАБОТКА QSO")
        print("=" * 60)

        added = 0
        updated = 0
        skipped = 0
        duplicates = 0
        errors = 0

        for i, qso_data in enumerate(qso_data_list, 1):
            # Проверка обязательных полей
            if not all([qso_data.get('CALL'), qso_data.get('QSO_DATE'),
                       qso_data.get('TIME_ON'), qso_data.get('BAND')]):
                print(f"  QSO #{i}: пропущена (нет обязательных полей)")
                skipped += 1
                continue

            # Добавляем my_callsign
            qso_data['STATION_CALLSIGN'] = username

            # Ищем существующую QSO
            existing_qso = self.db_ops.find_existing_qso(qso_data, user_id)

            if existing_qso:
                # Обновляем
                if self.db_ops.update_qso(existing_qso['id'], qso_data):
                    print(f"  QSO #{i}: обновлена {qso_data.get('CALL')} ({qso_data.get('QSO_DATE')})")
                    updated += 1
                else:
                    errors += 1
            else:
                # Добавляем
                if self.db_ops.insert_qso(qso_data, username, user_id):
                    print(f"  QSO #{i}: добавлена {qso_data.get('CALL')} ({qso_data.get('QSO_DATE')})")
                    added += 1
                else:
                    duplicates += 1

            # Прогресс
            if i % 10 == 0:
                print(f"  ... обработано {i}/{len(qso_data_list)}")

        print()

        # Обновляем lotw_lastsync
        print("=" * 60)
        print(" ОБНОВЛЕНИЕ lotw_lastsync")
        print("=" * 60)

        created_at = datetime.now().date().isoformat()
        if self.db_ops.update_lotw_lastsync(user_id, created_at):
            print(f"  lotw_lastsync обновлен: {created_at}")
        else:
            print(f"  [ERROR] Ошибка обновления lotw_lastsync")

        # Статистика
        print()
        print("=" * 60)
        print(" СТАТИСТИКА")
        print("=" * 60)
        print(f"  Всего QSO в файле: {len(qso_data_list)}")
        print(f"  Добавлено: {added}")
        print(f"  Обновлено: {updated}")
        print(f"  Пропущено: {skipped}")
        print(f"  Дубликаты: {duplicates}")
        print(f"  Ошибки: {errors}")
        print("=" * 60)


def main():
    """Основная функция"""
    if len(sys.argv) < 3:
        print(__doc__)
        print("\nИспользование:")
        print(f"  python {os.path.basename(sys.argv[0])} <имя_файла.adi> <username>")
        print()
        print("Примеры:")
        print(f"  python {os.path.basename(sys.argv[0])} lotwreport.adi R3LO")
        print(f"  python {os.path.basename(sys.argv[0])} data.adi UA1ABC")
        sys.exit(1)

    filename = sys.argv[1]
    username = sys.argv[2]

    consumer = TestConsumerADIF()
    consumer.process_adif_file(filename, username)


if __name__ == "__main__":
    main()