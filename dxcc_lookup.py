import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
import json

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
    exact_prefixes: List[str] = field(default_factory=list)


@dataclass
class DXCCException:
    """Класс для хранения исключений из r150cty.dat (позывные с = или особые префиксы)"""
    callsign_or_prefix: str  # Позывной или префикс с = или без
    cq_zone: int
    itu_zone: int
    primary_prefix: str

class DXCCDatabase:
    """База данных DXCC стран"""

    def __init__(self, filename: str = "r150cty.dat"):
        self.entries: List[DXCCEntry] = []
        self.prefix_map: Dict[str, DXCCEntry] = {}
        self.exact_prefixes: Dict[str, DXCCEntry] = {}
        self.exceptions: List[DXCCException] = []  # Список исключений
        self._load_file(filename)

    def _load_file(self, filename: str):
        """Загружает и парсит файл r150cty.dat"""
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                lines = f.readlines()
        except FileNotFoundError:
            print(f"Файл {filename} не найден!")
            print("Скачайте файл r150cty.dat с https://www.country-files.com/cty/current/r150cty.dat")
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
                    # Проверяем исключения (=позывные)
                    if '=' in parts[8]:
                        self._parse_exceptions(parts[8], DXCCEntry(
                            name=current_name,
                            cq_zone=current_cq_zone,
                            itu_zone=current_itu_zone,
                            continent=current_continent,
                            lat=current_lat,
                            lon=current_lon,
                            timezone=current_timezone,
                            primary_prefix=current_primary_prefix,
                            prefixes=[]
                        ))

                    prefixes = [p.strip() for p in parts[8].split(',') if p.strip()]
                    current_prefixes.extend(prefixes)

            elif line.startswith('    '):
                line = line.strip()
                if line:
                    if line.endswith(';'):
                        line = line[:-1]

                    # Проверяем исключения (=позывные)
                    if '=' in line:
                        self._parse_exceptions(line, DXCCEntry(
                            name=current_name,
                            cq_zone=current_cq_zone,
                            itu_zone=current_itu_zone,
                            continent=current_continent,
                            lat=current_lat,
                            lon=current_lon,
                            timezone=current_timezone,
                            primary_prefix=current_primary_prefix,
                            prefixes=[]
                        ))

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

        # Обрабатываем префиксы
        for prefix in prefixes:
            clean_prefix = re.sub(r'[=\[\]]', '', prefix)

            # Если префикс начинается с =, это точное совпадение
            if prefix.startswith('='):
                exact_prefix = clean_prefix
                self.exact_prefixes[exact_prefix] = entry
                entry.exact_prefixes.append(exact_prefix)
            # Если префикс в квадратных скобках, это тоже точное совпадение
            elif prefix.startswith('[') and prefix.endswith(']'):
                exact_prefix = clean_prefix
                self.exact_prefixes[exact_prefix] = entry
                entry.exact_prefixes.append(exact_prefix)

            # Добавляем в общую карту префиксов
            if '/' in clean_prefix:
                parts = clean_prefix.split('/')
                # Добавляем базовый префикс
                self._add_to_prefix_map(parts[0], entry)
                # Добавляем полный префикс с /
                self._add_to_prefix_map(clean_prefix, entry)
            else:
                self._add_to_prefix_map(clean_prefix, entry)

    def _add_to_prefix_map(self, prefix: str, entry: DXCCEntry):
        """Добавляет префикс в карту, если его еще нет"""
        # Удаляем начальный = если есть
        clean_prefix = prefix.lstrip('=')

        if clean_prefix and clean_prefix not in self.prefix_map:
            self.prefix_map[clean_prefix] = entry

    def _parse_exceptions(self, prefix_line: str, entry: DXCCEntry):
        """Парсит исключения из строки (=позывные)"""
        prefix_line = prefix_line.strip()
        if ';' in prefix_line:
            prefix_line = prefix_line.split(';')[0]

        parts = prefix_line.split(',')
        for part in parts:
            part = part.strip()
            if not part:
                continue

            # Проверяем на наличие = (исключение)
            if '=' in part:
                # Это исключение типа =3D2CCC
                exception_part = part
                cq_zone = entry.cq_zone
                itu_zone = entry.itu_zone

                # Извлекаем позывной (часть до '(' или сразу после =)
                clean_part = exception_part.split('(')[0].split('[')[0]
                if clean_part.startswith('='):
                    clean_part = clean_part[1:]  # Убираем =

                if clean_part:
                    exception = DXCCException(
                        callsign_or_prefix=clean_part,
                        cq_zone=cq_zone,
                        itu_zone=itu_zone,
                        primary_prefix=entry.primary_prefix
                    )
                    self.exceptions.append(exception)

    def find_by_callsign(self, callsign: str) -> Optional[DXCCEntry]:
        """Находит страну DXCC по позывному"""
        callsign = callsign.upper().strip()

        # Сначала ищем в обычных префиксах (от длинных к коротким)
        for length in range(len(callsign), 0, -1):
            prefix = callsign[:length]
            if prefix in self.prefix_map:
                entry = self.prefix_map[prefix]
                # Проверяем, есть ли исключение (=позывной) для этого конкретного позывного
                for exception in self.exceptions:
                    if callsign == exception.callsign_or_prefix:
                        return DXCCEntry(
                            name=f"Exception: {exception.callsign_or_prefix}",
                            cq_zone=exception.cq_zone,
                            itu_zone=exception.itu_zone,
                            continent=entry.continent,
                            lat=entry.lat,
                            lon=entry.lon,
                            timezone=entry.timezone,
                            primary_prefix=exception.primary_prefix,
                            prefixes=[]
                        )
                return entry

        # Если не найдено в обычной базе, проверяем точные совпадения (=позывные)
        for exact_prefix in self.exact_prefixes:
            if callsign.startswith(exact_prefix):
                return self.exact_prefixes[exact_prefix]

        return None

    def get_dxcc_info(self, callsign: str) -> Optional[Dict]:
        """Возвращает информацию о стране DXCC для позывного"""
        entry = self.find_by_callsign(callsign)

        if not entry:
            return None

        matched_prefix = self._find_matched_prefix(callsign)

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
            'matched_prefix': matched_prefix,
            'is_exact_match': matched_prefix in self.exact_prefixes if matched_prefix else False
        }

    def _find_matched_prefix(self, callsign: str) -> Optional[str]:
        """Находит префикс, который соответствует позывному"""
        callsign = callsign.upper().strip()

        # Сначала проверяем точные совпадения
        for exact_prefix in self.exact_prefixes:
            if callsign.startswith(exact_prefix):
                remaining = callsign[len(exact_prefix):]
                if not remaining or remaining[0].isdigit() or remaining[0].isalpha():
                    return exact_prefix

        # Затем обычные префиксы
        for length in range(len(callsign), 0, -1):
            prefix = callsign[:length]
            if prefix in self.prefix_map:
                return prefix

        return None

    def export_to_json(self, filename: str = "dxcc_database.json"):
        """Экспортирует базу данных в JSON файл"""
        data = {
            'entries': [
                {
                    'name': entry.name,
                    'cq_zone': entry.cq_zone,
                    'itu_zone': entry.itu_zone,
                    'continent': entry.continent,
                    'lat': entry.lat,
                    'lon': entry.lon,
                    'timezone': entry.timezone,
                    'primary_prefix': entry.primary_prefix,
                    'prefixes': entry.prefixes,
                    'exact_prefixes': entry.exact_prefixes
                }
                for entry in self.entries
            ],
            'prefix_map_keys': list(self.prefix_map.keys()),
            'exact_prefixes_keys': list(self.exact_prefixes.keys())
        }

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        print(f"База данных экспортирована в {filename}")

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
    if info.get('is_exact_match'):
        print(f"Тип совпадения: Точное")

def get_database_instance() -> Optional[DXCCDatabase]:
    """Возвращает текущий экземпляр базы данных"""
    return _dxcc_db

def export_database_to_json(filename: str = "dxcc_database.json"):
    """Экспортирует базу данных в JSON файл"""
    global _dxcc_db
    if _dxcc_db is None:
        _dxcc_db = init_database()

    _dxcc_db.export_to_json(filename)

# Автоматическая инициализация при импорте модуля
def _auto_init():
    """Автоматическая инициализация базы данных"""
    try:
        init_database()
        print("База данных DXCC инициализирована")
    except Exception as e:
        print(f"Ошибка при инициализации базы данных: {e}")

# Интерфейс командной строки
if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        callsign = sys.argv[1]
        filename = sys.argv[2] if len(sys.argv) > 2 else "r150cty.dat"

        print_dxcc_info(callsign, filename)
    else:
        print("DXCC Lookup Tool")
        print("=" * 50)
        print("Использование: python dxcc_lookup.py <позывной> [файл_базы]")
        print()
        print("Примеры:")
        print("  python dxcc_lookup.py UA3ACW")
        print("  python dxcc_lookup.py K1ABC")
        print("  python dxcc_lookup.py JA1XYZ")
        print("  python dxcc_lookup.py R2FA r150cty.dat")
        print("  python dxcc_lookup.py UB2FA")
        print("  python dxcc_lookup.py RA4F")
        print()
        print("Дополнительные команды:")
        print("  python dxcc_lookup.py --export-json   # Экспорт базы в JSON")
        print()
        print("Для получения файла r150cty.dat посетите:")
        print("https://www.country-files.com/cty/current/r150cty.dat")