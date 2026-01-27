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


class CTYDatabase:
    """База данных CTY (cty.dat)"""

    def __init__(self, filename: str = None):
        self.entries: List[CTYEntry] = []
        self.prefix_map: Dict[str, CTYEntry] = {}

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
                    prefixes = self._parse_prefixes(parts[8])
                    current_entry.prefixes.extend(prefixes)

            elif current_entry and (line.startswith('    ') or line.startswith('\t')):
                # Строка с дополнительными префиксами
                line = line.strip()
                if line:
                    if line.endswith(';'):
                        line = line[:-1]
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
                continue  # Пропускаем специальные префиксы

            part = part.strip()
            if part:
                prefixes.append(part)

        return prefixes

    def _add_entry(self, entry: CTYEntry):
        """Добавляет запись в базу данных"""
        self.entries.append(entry)

        for prefix in entry.prefixes:
            if prefix not in self.prefix_map:
                self.prefix_map[prefix] = entry

    def find_by_callsign(self, callsign: str) -> Optional[CTYEntry]:
        """Находит страну по позывному"""
        callsign = callsign.upper().strip()

        # Пробуем разные длины префикса
        for length in range(len(callsign), 0, -1):
            prefix = callsign[:length]
            if prefix in self.prefix_map:
                return self.prefix_map[prefix]

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