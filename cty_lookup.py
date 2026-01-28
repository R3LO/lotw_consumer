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
                # Извлекаем cq_zone и itu_zone из скобок
                cq_zone = entry.cq_zone
                itu_zone = entry.itu_zone

                # Ищем (число) для cq_zone
                cq_match = re.search(r'\((\d+)\)', exception_part)
                if cq_match:
                    cq_zone = int(cq_match.group(1))

                # Ищем [число] для itu_zone
                itu_match = re.search(r'\[(\d+)\]', exception_part)
                if itu_match:
                    itu_zone = int(itu_match.group(1))

                # Извлекаем позывной или префикс (часть до '(' или '[' или '=')
                clean_part = exception_part.split('(')[0].split('[')[0]
                if clean_part.startswith('='):
                    clean_part = clean_part[1:]  # Убираем =

                if clean_part:
                    exception = CTYException(
                        callsign_or_prefix=clean_part,
                        cq_zone=cq_zone,
                        itu_zone=itu_zone,
                        primary_prefix=entry.primary_prefix
                    )
                    self.exceptions.append(exception)
            else:
                # Это особый префикс без = (например, 3Y[73])
                cq_zone = entry.cq_zone
                itu_zone = entry.itu_zone

                # Ищем [число] для itu_zone
                itu_match = re.search(r'\[(\d+)\]', part)
                if itu_match:
                    itu_zone = int(itu_match.group(1))

                # Извлекаем префикс (часть до '(' или '[')
                clean_part = part.split('(')[0].split('[')[0]
                if clean_part:
                    exception = CTYException(
                        callsign_or_prefix=clean_part,
                        cq_zone=cq_zone,
                        itu_zone=itu_zone,
                        primary_prefix=entry.primary_prefix
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

        # Сначала ищем в обычной базе (проверяем разные длины префикса)
        for length in range(len(callsign), 0, -1):
            prefix = callsign[:length]
            if prefix in self.prefix_map:
                entry = self.prefix_map[prefix]
                # Проверяем, есть ли исключение для этого конкретного позывного (=позывной)
                has_specific_exception = False
                for exception in self.exceptions:
                    if exception.callsign_or_prefix.startswith('='):
                        exc_call = exception.callsign_or_prefix[1:]
                        if callsign == exc_call:
                            has_specific_exception = True
                            return CTYEntry(
                                name=f"Exception: {exc_call}",
                                cq_zone=exception.cq_zone,
                                itu_zone=exception.itu_zone,
                                continent="",
                                lat=0.0,
                                lon=0.0,
                                timezone=0.0,
                                primary_prefix=exception.primary_prefix,
                                prefixes=[]
                            )
                return entry

        # Если не найдено в обычной базе, проверяем исключения-префиксы (без =)
        for exception in self.exceptions:
            if not exception.callsign_or_prefix.startswith('='):
                # Проверяем совпадение по префиксу
                if callsign.startswith(exception.callsign_or_prefix):
                    return CTYEntry(
                        name=f"Exception prefix: {exception.callsign_or_prefix}",
                        cq_zone=exception.cq_zone,
                        itu_zone=exception.itu_zone,
                        continent="",
                        lat=0.0,
                        lon=0.0,
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


if __name__ == "__main__":
    db = CTYDatabase()

    test_callsigns = ['RA4FG', 'UA9ABC', 'UA2ABC', 'DL1ABC', 'K1ABC', 'UB2FGA', 'UA1ABC']
    print("\nТест DXCC из cty.dat:")
    for call in test_callsigns:
        prefix = db.get_dxcc_prefix(call)
        country = db.get_country_name(call)
        print(f"  {call}: prefix={prefix}, country={country}")