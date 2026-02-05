"""
Модуль для нормализации данных
"""

import re
from typing import Dict, Any, Optional
from datetime import datetime

from r150s_lookup import get_dxcc_info as get_r150_info
from cty_lookup import get_dxcc_from_cty


class DataNormalizer:
    """Класс для нормализации данных"""

    def __init__(self, logger):
        self.logger = logger

    def normalize_frequency(self, freq_str: str) -> Optional[float]:
        """
        Нормализует частоту из строки в число.
        """
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

    def normalize_band(self, band_str: str) -> str:
        """
        Нормализует название диапазона.
        """
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

    def normalize_time(self, time_str: str) -> str:
        """
        Нормализует время из формата LoTW (HHMM или HHMMSS) в HH:MM:SS
        """
        if not time_str:
            return "00:00:00"

        time_str = str(time_str).strip().zfill(4)

        if len(time_str) == 4:  # HHMM
            return f"{time_str[:2]}:{time_str[2:4]}:00"
        elif len(time_str) == 6:  # HHMMSS
            return f"{time_str[:2]}:{time_str[2:4]}:{time_str[4:6]}"
        else:
            return "00:00:00"

    def normalize_date(self, date_str: str) -> str:
        """
        Нормализует дату из формата LoTW (YYYYMMDD) в YYYY-MM-DD
        """
        if not date_str:
            return ""

        date_str = str(date_str).strip()
        if len(date_str) == 8:
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        return ""

    def get_mode(self, qso_data: Dict[str, str]) -> str:
        """
        Получает режим из данных QSO.
        Если MODE = 'MFSK', использует SUBMODE.
        Ограничивает длину до 10 символов для совместимости с БД.
        """
        mode = qso_data.get('MODE', '').upper()
        if mode == 'MFSK':
            submode = qso_data.get('SUBMODE', '')
            if submode:
                return submode.upper()[:10]  # Ограничиваем до 10 символов
        return mode[:10]  # Ограничиваем до 10 символов

    def get_lotw_status(self, qso_data: Dict[str, str]) -> str:
        """
        Определяет статус подтверждения LoTW.
        Возвращает 'Y' или 'N'.
        """
        qsl_rcvd = qso_data.get('QSL_RCVD', '').upper()
        return 'Y' if qsl_rcvd == 'Y' else 'N'

    def parse_lotw_rxqsl(self, rxqsl_str: str) -> Optional[datetime]:
        """
        Парсит поле APP_LOTW_RXQSL, удаляет комментарий и возвращает timezone-aware datetime.

        Args:
            rxqsl_str: Строка вида "2026-01-31 05:16:03 // QSL record matched/modified at LoTW"

        Returns:
            timezone-aware datetime объект или None если парсинг не удался
        """
        if not rxqsl_str:
            self.logger.debug(f"🔍 parse_lotw_rxqsl: пустая строка APP_LOTW_RXQSL")
            return None

        try:
            # Удаляем комментарий (часть после //)
            date_part = rxqsl_str.split('//')[0].strip()
            self.logger.debug(f"🔍 parse_lotw_rxqsl: исходная строка APP_LOTW_RXQSL='{rxqsl_str}'")
            self.logger.debug(f"🔍 parse_lotw_rxqsl: извлеченная дата='{date_part}'")

            # Парсим дату в формате "YYYY-MM-DD HH:MM:SS" и делаем timezone-aware (UTC)
            naive_dt = datetime.strptime(date_part, '%Y-%m-%d %H:%M:%S')
            # Делаем datetime timezone-aware (UTC)
            from datetime import timezone
            result = naive_dt.replace(tzinfo=timezone.utc)
            self.logger.debug(f"🔍 parse_lotw_rxqsl: результат={result} (тип: {type(result)}, tzinfo: {result.tzinfo})")
            return result
        except (ValueError, IndexError) as e:
            self.logger.error(f"❌ parse_lotw_rxqsl: ошибка парсинга APP_LOTW_RXQSL '{rxqsl_str}': {e}")
            return None

    def normalize_cqz(self, cqz_str: str) -> Optional[int]:
        """
        Нормализует значение CQ зоны.
        """
        if not cqz_str:
            return None

        try:
            cqz_str = str(cqz_str).strip()
            if cqz_str.isdigit():
                return int(cqz_str)
            return None
        except (ValueError, TypeError):
            return None

    def normalize_ituz(self, ituz_str: str) -> Optional[int]:
        """
        Нормализует значение ITU зоны.
        """
        if not ituz_str:
            return None

        try:
            ituz_str = str(ituz_str).strip()
            if ituz_str.isdigit():
                return int(ituz_str)
            return None
        except (ValueError, TypeError):
            return None

    def prepare_qso_data(self, qso_data: Dict[str, str], my_callsign: str = '') -> Dict[str, Any]:
        """Подготавливает все данные QSO для вставки/обновления"""
        callsign = qso_data.get('CALL', '').upper()
        my_callsign = my_callsign.upper()  # Сохраняем прописными

        # Логируем APP_LOTW_RXQSL для отладки
        app_rxqsl_raw = qso_data.get('APP_LOTW_RXQSL', '')
        self.logger.debug(f"🔍 prepare_qso_data: {callsign} APP_LOTW_RXQSL='{app_rxqsl_raw}'")

        # Определяем страну и континент из r150cty.dat
        r150_info = get_r150_info(callsign) if callsign else None
        if r150_info:
            r150s = r150_info['country'].upper() if r150_info['country'] else None
            continent = r150_info['continent'].upper() if r150_info['continent'] else None
        else:
            r150s = None
            continent = None

        # Определяем DXCC из поля COUNTRY в LoTW API (если есть), иначе из cty.dat
        dxcc = qso_data.get('COUNTRY', '').upper().strip()
        if not dxcc and callsign:
            dxcc = get_dxcc_from_cty(callsign) if callsign else None

        # Определяем state из STATE для любых станций
        # state заполняется всегда, если есть значение STATE
        state = None
        state_value = qso_data.get('STATE', '').upper()
        if state_value:
            state = state_value

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
            'vucc_grids': qso_data.get('VUCC_GRIDS', ''),
            'iota': qso_data.get('IOTA', ''),
            'app_lotw_rxqsl': self.parse_lotw_rxqsl(qso_data.get('APP_LOTW_RXQSL', '')),
            'rst_sent': qso_data.get('RST_SENT', ''),
            'rst_rcvd': qso_data.get('RST_RCVD', ''),
            'state': state,
            'cqz': self.normalize_cqz(qso_data.get('CQZ', '')),
            'ituz': self.normalize_ituz(qso_data.get('ITUZ', '')),
            'continent': continent,
            'dxcc': dxcc,
            'callsign': callsign,
            'my_callsign': my_callsign
        }