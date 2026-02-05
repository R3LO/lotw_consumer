"""
Утилита для определения DXCC префикса по позывному с использованием базы r150cty.dat
"""

import re
from dataclasses import dataclass
from typing import Dict, List, Optional
from pathlib import Path


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

    def __init__(self, filename: str = None):
        self.entries: List[DXCCEntry] = []
        self.prefix_map: Dict[str, DXCCEntry] = {}
        
        if filename is None:
            base_dir = Path(__file__).parent.parent
            filename = base_dir / 'r150cty.dat'
            if not filename.exists():
                filename = base_dir / 'cty.dat'
        
        self._load_file(str(filename))

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

        matched_prefix = None
        for length in range(len(callsign), 0, -1):
            prefix = callsign[:length]
            if prefix in self.prefix_map:
                matched_prefix = prefix
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
            'matched_prefix': matched_prefix
        }

    def get_dxcc_prefix(self, callsign: str) -> Optional[str]:
        """Возвращает DXCC префикс для позывного"""
        entry = self.find_by_callsign(callsign)
        if entry:
            return entry.primary_prefix if entry.primary_prefix else (entry.prefixes[0] if entry.prefixes else None)
        return None

    def get_country_name(self, callsign: str) -> Optional[str]:
        """Возвращает название страны для позывного"""
        entry = self.find_by_callsign(callsign)
        return entry.name if entry else None


# Глобальный экземпляр базы данных
_dxcc_db = None

def get_dxcc_database() -> DXCCDatabase:
    """Возвращает глобальный экземпляр базы данных"""
    global _dxcc_db
    if _dxcc_db is None:
        _dxcc_db = DXCCDatabase()
    return _dxcc_db

def get_dxcc_prefix(callsign: str) -> Optional[str]:
    """Возвращает DXCC префикс по позывному"""
    db = get_dxcc_database()
    return db.get_dxcc_prefix(callsign)

def get_country_by_callsign(callsign: str) -> Optional[str]:
    """Возвращает название страны по позывному"""
    db = get_dxcc_database()
    return db.get_country_name(callsign)

def get_dxcc_info(callsign: str) -> Optional[Dict]:
    """Возвращает полную информацию о DXCC по позывному"""
    db = get_dxcc_database()
    return db.get_dxcc_info(callsign)


if __name__ == "__main__":
    db = DXCCDatabase()

    test_callsigns = ['RA4FG', 'UD2F', 'UA9CB', 'UB2FGA', 'R3LO', 'UA1ABC', 'DL1ABC', 'K1ABC']
    print("\nTesting DXCC lookup:")
    for call in test_callsigns:
        info = db.get_dxcc_info(call)
        if info:
            print(f"  {call}: prefix={info['primary_prefix']}, country={info['country']}")
        else:
            print(f"  {call}: NOT FOUND")
