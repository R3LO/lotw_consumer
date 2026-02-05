"""
Утилита для определения DXCC префикса по позывному с использованием базы cty.dat
"""

import re
import os
from dataclasses import dataclass
from typing import Dict, List, Optional
from pathlib import Path


@dataclass
class CTYEntry:
    """Класс для хранения информации о стране из cty.dat"""
    name: str
    cq_zone: int
    itu_zone: int
    continent: str
    lat: float
    lon: float
    timezone: float
    primary_prefix: str
    prefixes: List[str]


@dataclass
class CTYException:
    """Класс для хранения исключений из cty.dat (позывные с = или особые префиксы)"""
    callsign_or_prefix: str  # Позывной или префикс с = или без
    cq_zone: int
    itu_zone: int
    primary_prefix: str
    cq_zone_alt: Optional[int] = None  # Альтернативная CQ зона из скобок (38)
    itu_zone_alt: Optional[int] = None  # Альтернативная ITU зона из скобок [67]
    entry_name: Optional[str] = None  # Название страны из записи, где определено исключение
    entry_lat: Optional[float] = None  # Широта из записи
    entry_lon: Optional[float] = None  # Долгота из записи
    entry_continent: Optional[str] = None  # Континент из записи


class CTYDatabase:
    """База данных CTY (cty.dat)"""

    def __init__(self, filename: str = None):
        self.entries: List[CTYEntry] = []
        self.prefix_map: Dict[str, CTYEntry] = {}
        self.exceptions: List[CTYException] = []  # Список исключений

        if filename is None:
            # Ищем в текущей директории или в директории проекта
            import os
            current_dir = os.path.dirname(os.path.abspath(__file__))
            filename = os.path.join(current_dir, 'cty.dat')
            if not os.path.exists(filename):
                filename = os.path.join(current_dir, '..', 'cty.dat')

        if os.path.exists(filename):
            self._load_file(str(filename))
        else:
            print(f"Файл {filename} не найден!")

    def _load_file(self, filename: str):
        """Загружает и парсит файл cty.dat"""
        try:
            with open(filename, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                self._parse_cty_dat(content)
        except Exception as e:
            print(f"Ошибка загрузки cty.dat: {e}")

    def _parse_cty_dat(self, content: str):
        """Парсит файл cty.dat"""
        lines = content.strip().split('\n')
        current_entry = None

        for line in lines:
            line = line.rstrip()
            if not line:
                continue

            if line.startswith('#'):
                continue

            # Проверяем, является ли строка началом записи
            if ':' in line and line.count(':') >= 6:
                # Сохраняем предыдущую запись
                if current_entry and current_entry.prefixes:
                    self._add_entry(current_entry)

                parts = [p.strip() for p in line.split(':')]
                current_entry = CTYEntry(
                    name=parts[0],
                    cq_zone=int(parts[1]) if parts[1] else 0,
                    itu_zone=int(parts[2]) if parts[2] else 0,
                    continent=parts[3],
                    lat=float(parts[4]) if parts[4] else 0.0,
                    lon=float(parts[5]) if parts[5] else 0.0,
                    timezone=float(parts[6]) if parts[6] else 0.0,
                    primary_prefix=parts[7] if len(parts) > 7 and parts[7] else "",
                    prefixes=[]
                )

                # Парсим префиксы из той же строки (после 7-го :)
                if len(parts) > 8 and parts[8]:
                    # Проверяем исключения
                    if '=' in parts[8] or '[' in parts[8]:
                        self._parse_exceptions(parts[8], current_entry)

                    prefixes = self._parse_prefixes(parts[8])
                    current_entry.prefixes.extend(prefixes)

            elif current_entry and (line.startswith('    ') or line.startswith('\t')):
                # Строка с дополнительными префиксами
                line = line.strip()
                if line:
                    if line.endswith(';'):
                        line = line[:-1]

                    # Проверяем, есть ли исключения (= или [число])
                    has_exceptions = '=' in line or '[' in line
                    has_normal_prefixes = any(c for c in line if c.isalpha() or c.isdigit()) and not line.startswith('=')

                    if has_exceptions:
                        self._parse_exceptions(line, current_entry)

                    # Парсим обычные префиксы
                    prefixes = self._parse_prefixes(line)
                    # R0, R8, R9 должны быть UA9 (Asiatic Russia), а не European Russia
                    if current_entry.name == 'European Russia':
                        prefixes = [p for p in prefixes if not (p.startswith('R0') or p.startswith('R8') or p.startswith('R9'))]
                    current_entry.prefixes.extend(prefixes)

        # Сохраняем последнюю запись
        if current_entry and current_entry.prefixes:
            self._add_entry(current_entry)

        print(f"Загружено {len(self.entries)} стран из cty.dat")

    def _parse_prefixes(self, prefix_line: str) -> List[str]:
        """Парсит строку с префиксами"""
        prefixes = []
        prefix_line = prefix_line.strip()

        if ';' in prefix_line:
            prefix_line = prefix_line.split(';')[0]

        parts = prefix_line.split(',')
        for part in parts:
            part = part.strip()
            if not part:
                continue

            # Убираем комментарии и служебную информацию
            if '(' in part:
                part = part.split('(')[0]
            if '[' in part:
                part = part.split('[')[0]
            if '=' in part:
                continue  # Пропускаем специальные префиксы (они парсятся отдельно)

            part = part.strip()
            if part:
                prefixes.append(part)

        return prefixes

    def _parse_exceptions(self, prefix_line: str, entry: CTYEntry):
        """Парсит исключения из строки (=позывные или особые префиксы)"""
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
                # Это исключение типа =8J1RL(39)[67]
                exception_part = part
                cq_zone = entry.cq_zone
                itu_zone = entry.itu_zone
                cq_zone_alt = None
                itu_zone_alt = None

                # Ищем (число) для cq_zone_alt
                cq_match = re.search(r'\((\d+)\)', exception_part)
                if cq_match:
                    cq_zone_alt = int(cq_match.group(1))

                # Ищем [число] для itu_zone_alt
                itu_match = re.search(r'\[(\d+)\]', exception_part)
                if itu_match:
                    itu_zone_alt = int(itu_match.group(1))

                # Извлекаем позывной или префикс (часть до '(' или '[' или '=')
                clean_part = exception_part.split('(')[0].split('[')[0]
                if clean_part.startswith('='):
                    clean_part = clean_part[1:]  # Убираем =

                if clean_part:
                    exception = CTYException(
                        callsign_or_prefix=clean_part,
                        cq_zone=cq_zone,
                        itu_zone=itu_zone,
                        primary_prefix=entry.primary_prefix,
                        cq_zone_alt=cq_zone_alt,
                        itu_zone_alt=itu_zone_alt,
                        entry_name=entry.name,
                        entry_lat=entry.lat,
                        entry_lon=entry.lon,
                        entry_continent=entry.continent
                    )
                    self.exceptions.append(exception)
            else:
                # Это особый префикс без = (например, 3Y[73])
                cq_zone = entry.cq_zone
                itu_zone = entry.itu_zone
                cq_zone_alt = None
                itu_zone_alt = None

                # Ищем (число) для cq_zone_alt
                cq_match = re.search(r'\((\d+)\)', part)
                if cq_match:
                    cq_zone_alt = int(cq_match.group(1))

                # Ищем [число] для itu_zone_alt
                itu_match = re.search(r'\[(\d+)\]', part)
                if itu_match:
                    itu_zone_alt = int(itu_match.group(1))

                # Извлекаем префикс (часть до '(' или '[')
                clean_part = part.split('(')[0].split('[')[0]
                if clean_part:
                    exception = CTYException(
                        callsign_or_prefix=clean_part,
                        cq_zone=cq_zone,
                        itu_zone=itu_zone,
                        primary_prefix=entry.primary_prefix,
                        cq_zone_alt=cq_zone_alt,
                        itu_zone_alt=itu_zone_alt,
                        entry_name=entry.name,
                        entry_lat=entry.lat,
                        entry_lon=entry.lon,
                        entry_continent=entry.continent
                    )
                    self.exceptions.append(exception)

    def _add_entry(self, entry: CTYEntry):
        """Добавляет запись в базу данных"""
        self.entries.append(entry)

        for prefix in entry.prefixes:
            if prefix not in self.prefix_map:
                self.prefix_map[prefix] = entry

    def find_by_callsign(self, callsign: str) -> Optional[CTYEntry]:
        """Находит страну по позывному"""
        callsign = callsign.upper().strip()

        # Сначала проверяем исключения (=позывные) для этого конкретного позывного
        for exception in self.exceptions:
            if callsign == exception.callsign_or_prefix:
                # Используем данные из записи, где определено исключение
                # Используем альтернативные зоны, если они указаны
                final_cq_zone = exception.cq_zone_alt if exception.cq_zone_alt is not None else exception.cq_zone
                final_itu_zone = exception.itu_zone_alt if exception.itu_zone_alt is not None else exception.itu_zone

                return CTYEntry(
                    name=exception.entry_name if exception.entry_name else exception.primary_prefix,
                    cq_zone=final_cq_zone,
                    itu_zone=final_itu_zone,
                    continent=exception.entry_continent if exception.entry_continent else "",
                    lat=exception.entry_lat if exception.entry_lat is not None else 0.0,
                    lon=exception.entry_lon if exception.entry_lon is not None else 0.0,
                    timezone=0.0,
                    primary_prefix=exception.primary_prefix,
                    prefixes=[]
                )

        # Затем ищем в обычной базе (проверяем разные длины префикса)
        for length in range(len(callsign), 0, -1):
            prefix = callsign[:length]
            if prefix in self.prefix_map:
                entry = self.prefix_map[prefix]
                return entry

        # Если не найдено в обычной базе, проверяем исключения-префиксы
        for exception in self.exceptions:
            # Исключения-префиксы - это те, которые короче позывного и не совпадают полностью
            if len(exception.callsign_or_prefix) < len(callsign):
                if callsign.startswith(exception.callsign_or_prefix):
                    # Используем альтернативные зоны, если они указаны
                    final_cq_zone = exception.cq_zone_alt if exception.cq_zone_alt is not None else exception.cq_zone
                    final_itu_zone = exception.itu_zone_alt if exception.itu_zone_alt is not None else exception.itu_zone

                    return CTYEntry(
                        name=exception.entry_name if exception.entry_name else exception.primary_prefix,
                        cq_zone=final_cq_zone,
                        itu_zone=final_itu_zone,
                        continent=exception.entry_continent if exception.entry_continent else "",
                        lat=exception.entry_lat if exception.entry_lat is not None else 0.0,
                        lon=exception.entry_lon if exception.entry_lon is not None else 0.0,
                        timezone=0.0,
                        primary_prefix=exception.primary_prefix,
                        prefixes=[]
                    )

        return None

    def get_dxcc_prefix(self, callsign: str) -> Optional[str]:
        """Возвращает DXCC префикс для позывного"""
        entry = self.find_by_callsign(callsign)
        if entry:
            # Всегда возвращаем primary_prefix (UA9 для Asiatic Russia, UA для European Russia)
            return entry.primary_prefix if entry.primary_prefix else None

        return None

    def get_country_name(self, callsign: str) -> Optional[str]:
        """Возвращает название страны"""
        entry = self.find_by_callsign(callsign)
        return entry.name if entry else None


# Глобальный экземпляр
_cty_db = None

def get_cty_database() -> CTYDatabase:
    """Возвращает глобальный экземпляр базы"""
    global _cty_db
    if _cty_db is None:
        _cty_db = CTYDatabase()
    return _cty_db

def get_dxcc_from_cty(callsign: str) -> Optional[str]:
    """Возвращает DXCC префикс по позывному из cty.dat"""
    db = get_cty_database()
    return db.get_dxcc_prefix(callsign)


def get_dxcc_info(callsign: str) -> Optional[Dict]:
    """Возвращает полную информацию о стране DXCC для позывного"""
    db = get_cty_database()
    entry = db.find_by_callsign(callsign)

    if not entry:
        return None

    # Проверяем, есть ли альтернативные зоны для этого позывного
    cq_zone_alt = None
    itu_zone_alt = None
    callsign_upper = callsign.upper().strip()
    for exception in db.exceptions:
        if callsign_upper == exception.callsign_or_prefix:
            cq_zone_alt = exception.cq_zone_alt
            itu_zone_alt = exception.itu_zone_alt
            break

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
        'cq_zone_alt': cq_zone_alt,
        'itu_zone_alt': itu_zone_alt
    }


def print_dxcc_info(callsign: str):
    """Выводит информацию о стране DXCC в читаемом формате"""
    info = get_dxcc_info(callsign)

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
    if info.get('cq_zone_alt') is not None:
        print(f"Альтернативная CQ зона: {info['cq_zone_alt']}")
    if info.get('itu_zone_alt') is not None:
        print(f"Альтернативная ITU зона: {info['itu_zone_alt']}")


if __name__ == "__main__":
    db = CTYDatabase()

    test_callsigns = ['RA4FG', 'UA9ABC', 'UA2ABC', 'DL1ABC', 'K1ABC', 'UB2FGA', 'UA1ABC']
    print("\nТест DXCC из cty.dat:")
    for call in test_callsigns:
        info = get_dxcc_info(call)
        if info:
            alt_cq = f" (alt: {info['cq_zone_alt']})" if info.get('cq_zone_alt') is not None else ""
            alt_itu = f" (alt: {info['itu_zone_alt']})" if info.get('itu_zone_alt') is not None else ""
            print(f"  {call}: country={info['country']}, CQ={info['cq_zone']}{alt_cq}, ITU={info['itu_zone']}{alt_itu}, prefix={info['primary_prefix']}")