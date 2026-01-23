"""
Модуль для нормализации данных
"""

import re
from typing import Dict, Any, Optional

from utils.dxcc import get_dxcc_prefix


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
        """
        mode = qso_data.get('MODE', '').upper()
        if mode == 'MFSK':
            submode = qso_data.get('SUBMODE', '')
            if submode:
                return submode.upper()
        return mode

    def get_ru_region(self, qso_data: Dict[str, str]) -> Optional[str]:
        """
        Определяет регион России из поля COUNTRY.
        """
        country = qso_data.get('COUNTRY', '').upper()
        if 'ASIATIC RUSSIA' in country:
            return 'ASIATIC RUSSIA'
        elif 'EUROPEAN RUSSIA' in country:
            return 'EUROPEAN RUSSIA'
        elif 'KALININGRAD' in country:
            return 'KALININGRAD'
        return None

    def get_lotw_status(self, qso_data: Dict[str, str]) -> str:
        """
        Определяет статус подтверждения LoTW.
        Возвращает 'Y' или 'N'.
        """
        qsl_rcvd = qso_data.get('QSL_RCVD', '').upper()
        return 'Y' if qsl_rcvd == 'Y' else 'N'

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

    def prepare_qso_data(self, qso_data: Dict[str, str], username: str = '') -> Dict[str, Any]:
        """Подготавливает все данные QSO для вставки/обновления"""
        # Определяем DXCC: используем из данных или определяем по позывному
        dxcc_from_data = qso_data.get('DXCC', '')
        callsign = qso_data.get('CALL', '').upper()

        if dxcc_from_data:
            dxcc = dxcc_from_data[:10]
        elif callsign:
            dxcc = get_dxcc_prefix(callsign)
        else:
            dxcc = None

        return {
            'band': self.normalize_band(qso_data.get('BAND', '')),
            'frequency': self.normalize_frequency(qso_data.get('FREQ', '')),
            'mode': self.get_mode(qso_data),
            'date': self.normalize_date(qso_data.get('QSO_DATE', '')),
            'time': self.normalize_time(qso_data.get('TIME_ON', '')),
            'prop_mode': qso_data.get('PROP_MODE', ''),
            'sat_name': qso_data.get('SAT_NAME', ''),
            'lotw': self.get_lotw_status(qso_data),
            'r150s': qso_data.get('COUNTRY', ''),
            'gridsquare': qso_data.get('GRIDSQUARE', ''),
            'my_gridsquare': qso_data.get('MY_GRIDSQUARE', ''),
            'rst_sent': qso_data.get('RST_SENT', ''),
            'rst_rcvd': qso_data.get('RST_RCVD', ''),
            'ru_region': self.get_ru_region(qso_data),
            'cqz': self.normalize_cqz(qso_data.get('CQZ', '')),
            'ituz': self.normalize_ituz(qso_data.get('ITUZ', '')),
            'continent': qso_data.get('CONT', '')[:2] if qso_data.get('CONT') else None,
            'dxcc': dxcc,
            'callsign': callsign,
            'my_callsign': username
        }