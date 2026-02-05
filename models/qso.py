"""
Модель данных QSO
"""

from dataclasses import dataclass
from typing import Optional
from datetime import date, time


@dataclass
class QSO:
    """Модель данных QSO"""
    id: str  # UUID
    callsign: str
    my_callsign: str
    band: str
    frequency: Optional[float]
    mode: str
    date: date
    time: time
    prop_mode: str = ''
    sat_name: str = ''
    lotw: str = 'N'
    paper_qsl: str = 'N'
    r150s: str = ''
    gridsquare: str = ''
    my_gridsquare: str = ''
    vucc_grids: str = ''
    iota: str = ''
    app_lotw_rxqsl: Optional[datetime] = None
    rst_sent: str = ''
    rst_rcvd: str = ''
    state: Optional[str] = None
    cqz: Optional[int] = None
    ituz: Optional[int] = None
    user_id: Optional[int] = None
    continent: Optional[str] = None
    dxcc: Optional[str] = None
    adif_upload_id: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @classmethod
    def from_dict(cls, data: dict) -> 'QSO':
        """Создает объект QSO из словаря"""
        return cls(**data)

    def to_dict(self) -> dict:
        """Преобразует объект QSO в словарь"""
        return {
            'id': self.id,
            'callsign': self.callsign,
            'my_callsign': self.my_callsign,
            'band': self.band,
            'frequency': self.frequency,
            'mode': self.mode,
            'date': self.date.isoformat() if self.date else None,
            'time': self.time.isoformat() if self.time else None,
            'prop_mode': self.prop_mode,
            'sat_name': self.sat_name,
            'lotw': self.lotw,
            'paper_qsl': self.paper_qsl,
            'r150s': self.r150s,
            'gridsquare': self.gridsquare,
            'my_gridsquare': self.my_gridsquare,
            'vucc_grids': self.vucc_grids,
            'iota': self.iota,
            'app_lotw_rxqsl': self.app_lotw_rxqsl,
            'rst_sent': self.rst_sent,
            'rst_rcvd': self.rst_rcvd,
            'state': self.state,
            'cqz': self.cqz,
            'ituz': self.ituz,
            'user_id': self.user_id,
            'continent': self.continent,
            'dxcc': self.dxcc,
            'adif_upload_id': self.adif_upload_id,
            'created_at': self.created_at.isoformat() if self.created_at else None,
            'updated_at': self.updated_at.isoformat() if self.updated_at else None
        }