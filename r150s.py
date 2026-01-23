import re
from dataclasses import dataclass
from typing import Dict, List, Optional

@dataclass
class DXCCEntry:
    """Класс для хранения информации о стране DXCC"""
    name: str
    cq_zone: int
    itu_zone: int
    continent: str
    lat: float
    lon: float
    timezone: float
    primary_prefix: str
    prefixes: List[str]

class DXCCDatabase:
    """База данных DXCC стран"""

    def __init__(self, filename: str = "r150cty.dat"):
        self.entries: List[DXCCEntry] = []
        self.prefix_map: Dict[str, DXCCEntry] = {}
        self._load_file(filename)

    def _load_file(self, filename: str):
        """Загружает и парсит файл r150cty.dat"""
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                lines = f.readlines()
        except FileNotFoundError:
            print(f"Файл {filename} не найден!")
            return

        current_name = None
        current_cq_zone = None
        current_itu_zone = None
        current_continent = None
        current_lat = None
        current_lon = None
        current_timezone = None
        current_primary_prefix = None
        current_prefixes = []

        i = 0
        while i < len(lines):
            line = lines[i].rstrip()

            if not line or line.startswith('#'):
                i += 1
                continue

            if ':' in line and line.count(':') >= 6:
                if current_name and current_prefixes:
                    self._add_entry(
                        current_name, current_cq_zone, current_itu_zone,
                        current_continent, current_lat, current_lon,
                        current_timezone, current_primary_prefix, current_prefixes
                    )

                parts = [p.strip() for p in line.split(':')]
                current_name = parts[0]
                current_cq_zone = int(parts[1]) if parts[1] else 0
                current_itu_zone = int(parts[2]) if parts[2] else 0
                current_continent = parts[3]
                current_lat = float(parts[4]) if parts[4] else 0.0
                current_lon = float(parts[5]) if parts[5] else 0.0
                current_timezone = float(parts[6]) if parts[6] else 0.0
                current_primary_prefix = parts[7] if len(parts) > 7 and parts[7] else ""
                current_prefixes = []

                if len(parts) > 8 and parts[8]:
                    prefixes = [p.strip() for p in parts[8].split(',') if p.strip()]
                    current_prefixes.extend(prefixes)

            elif line.startswith('    '):
                line = line.strip()
                if line:
                    if line.endswith(';'):
                        line = line[:-1]
                    prefixes = [p.strip() for p in line.split(',') if p.strip()]
                    current_prefixes.extend(prefixes)

            i += 1

        if current_name and current_prefixes:
            self._add_entry(
                current_name, current_cq_zone, current_itu_zone,
                current_continent, current_lat, current_lon,
                current_timezone, current_primary_prefix, current_prefixes
            )

        print(f"Загружено {len(self.entries)} стран DXCC")

    def _add_entry(self, name: str, cq_zone: int, itu_zone: int, continent: str,
                   lat: float, lon: float, timezone: float,
                   primary_prefix: str, prefixes: List[str]):
        """Добавляет запись о стране в базу данных"""
        entry = DXCCEntry(
            name=name,
            cq_zone=cq_zone,
            itu_zone=itu_zone,
            continent=continent,
            lat=lat,
            lon=lon,
            timezone=timezone,
            primary_prefix=primary_prefix,
            prefixes=prefixes
        )

        self.entries.append(entry)

        for prefix in prefixes:
            clean_prefix = re.sub(r'[=\[\]]', '', prefix)
            if '/' in clean_prefix:
                base_prefix = clean_prefix.split('/')[0]
                self.prefix_map[base_prefix] = entry
            self.prefix_map[clean_prefix] = entry

    def find_by_callsign(self, callsign: str) -> Optional[DXCCEntry]:
        """Находит страну DXCC по позывному"""
        callsign = callsign.upper().strip()

        for length in range(len(callsign), 0, -1):
            prefix = callsign[:length]
            if prefix in self.prefix_map:
                return self.prefix_map[prefix]

        return None

    def get_dxcc_info(self, callsign: str) -> Optional[Dict]:
        """Возвращает информацию о стране DXCC для позывного"""
        entry = self.find_by_callsign(callsign)

        if not entry:
            return None

        return {
            'callsign': callsign,
            'country': entry.name,
            'cq_zone': entry.cq_zone,
            'itu_zone': entry.itu_zone,
            'continent': entry.continent,
            'latitude': entry.lat,
            'longitude': entry.lon,
            'timezone': entry.timezone,
            'primary_prefix': entry.primary_prefix,
            'matched_prefix': self._find_matched_prefix(callsign)
        }

    def _find_matched_prefix(self, callsign: str) -> Optional[str]:
        """Находит префикс, который соответствует позывному"""
        callsign = callsign.upper().strip()

        for length in range(len(callsign), 0, -1):
            prefix = callsign[:length]
            if prefix in self.prefix_map:
                return prefix

        return None

# Глобальный экземпляр базы данных
_dxcc_db = None

def init_database(filename: str = "r150cty.dat") -> DXCCDatabase:
    """Инициализирует базу данных DXCC"""
    global _dxcc_db
    _dxcc_db = DXCCDatabase(filename)
    return _dxcc_db

def get_dxcc_info(callsign: str, filename: str = "r150cty.dat") -> Optional[Dict]:
    """
    Основная функция для получения информации о стране DXCC по позывному.

    Args:
        callsign: Позывной для поиска (например, 'UA3ACW', 'K1ABC', 'JA1XYZ')
        filename: Путь к файлу r150cty.dat (по умолчанию 'r150cty.dat')

    Returns:
        Словарь с информацией о стране или None если не найдено
    """
    global _dxcc_db

    if _dxcc_db is None:
        _dxcc_db = init_database(filename)

    return _dxcc_db.get_dxcc_info(callsign)

def print_dxcc_info(callsign: str, filename: str = "r150cty.dat"):
    """
    Выводит информацию о стране DXCC в читаемом формате.

    Args:
        callsign: Позывной для поиска
        filename: Путь к файлу r150cty.dat
    """
    info = get_dxcc_info(callsign, filename)

    if not info:
        print(f"Страна DXCC для позывного '{callsign}' не найдена")
        return

    print(f"Позывной: {info['callsign']}")
    print(f"Страна: {info['country']}")
    print(f"Континент: {info['continent']}")
    print(f"CQ зона: {info['cq_zone']}")
    print(f"ITU зона: {info['itu_zone']}")
    print(f"Широта: {info['latitude']:.2f}")
    print(f"Долгота: {info['longitude']:.2f}")
    print(f"Часовой пояс: UTC{info['timezone']:+.1f}")
    print(f"Основной префикс: {info['primary_prefix']}")
    print(f"Сопоставленный префикс: {info['matched_prefix']}")

def get_database_instance() -> Optional[DXCCDatabase]:
    """Возвращает текущий экземпляр базы данных"""
    return _dxcc_db

# Автоматическая инициализация при импорте модуля
def _auto_init():
    """Автоматическая инициализация базы данных"""
    try:
        init_database()
    except:
        pass

# Опционально: автоматически инициализировать при импорте
# _auto_init()