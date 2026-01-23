"""
Модуль для работы с LoTW API
"""

import requests
from typing import Dict, Any, List

from lotw.parser import ADIFParser


class LoTWAPI:
    """Класс для работы с LoTW API"""

    def __init__(self, logger):
        self.logger = logger
        self.parser = ADIFParser(logger)

    def get_lotw_data(self, callsign: str, username: str, password: str) -> Dict[str, Any]:
        """
        Получает данные из LoTW API и парсит ADIF формат.
        """
        try:
            login_url = "https://lotw.arrl.org/lotwuser/lotwreport.adi"

            params = {
                'login': username,
                'password': password,
                'qso_query': '1',
                'qso_qsl': 'yes',
                'qso_qsldetail': 'yes',
                'qso_startdate': '2025-01-01',
            }

            if callsign:
                params['callsign'] = callsign

            self.logger.info(f"📡 Запрос к LoTW API для {callsign}")

            response = requests.get(login_url, params=params, timeout=30)

            if response.status_code == 200:
                content = response.text
                self.logger.info(f"✅ Получен ответ от LoTW, длина: {len(content)} символов")

                qso_data = self.parser.parse_adif_response_all_fields(content)

                self.logger.info(f"✅ Получено {len(qso_data)} QSO для {callsign}")

                return {
                    'success': True,
                    'callsign': callsign,
                    'qso_count': len(qso_data),
                    'qso_data': qso_data,
                    'raw_data_length': len(content)
                }
            else:
                self.logger.error(f"❌ Ошибка HTTP {response.status_code} для {callsign}")
                return {
                    'success': False,
                    'callsign': callsign,
                    'error': f"HTTP {response.status_code}",
                    'message': response.text[:200] if response.text else "Пустой ответ",
                    'qso_data': []
                }

        except requests.exceptions.Timeout:
            self.logger.error(f"⏱️ Таймаут при запросе для {callsign}")
            return {
                'success': False,
                'callsign': callsign,
                'error': 'Timeout',
                'message': 'Превышено время ожидания ответа от LoTW',
                'qso_data': []
            }
        except Exception as e:
            self.logger.error(f"❌ Непредвиденная ошибка для {callsign}: {e}")
            return {
                'success': False,
                'callsign': callsign,
                'error': str(e),
                'message': 'Непредвиденная ошибка',
                'qso_data': []
            }