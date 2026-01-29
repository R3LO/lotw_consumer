"""
Модуль для парсинга ADIF формата
"""

import re
from typing import Dict, Any, List


class ADIFParser:
    """Класс для парсинга ADIF формата"""

    def __init__(self, logger):
        self.logger = logger

    def parse_adif_response_all_fields(self, content: str) -> List[Dict[str, str]]:
        """
        Парсит ADIF формат ответа от LoTW.
        """
        qso_list = []

        if '<eor>' not in content and 'QSO_DATE' not in content:
            self.logger.info("ℹ️ В ответе нет данных QSO")
            return qso_list

        # Удаляем <APP_LoTW_EOF> и всё после него перед разбором
        if '<APP_LoTW_EOF>' in content:
            content = content.split('<APP_LoTW_EOF>')[0]

        # Удаляем заголовок <eoh> и всё до него
        if '<eoh>' in content:
            content = content.split('<eoh>', 1)[1]

        qso_blocks = content.split('<eor>')

        for block_num, block in enumerate(qso_blocks, 1):
            if not block.strip() or '<eoh>' in block:
                continue

            if '<eoh>' in block:
                block = block.split('<eoh>')[1]

            block = re.sub(r'//.*', '', block)

            pattern = r'<(\w+)(?::(\d+))?>([^<]*)'
            qso = {}
            fields_found = []

            matches = re.findall(pattern, block)

            for field_name, length, value in matches:
                field_name = field_name.upper()

                if length and length.isdigit():
                    value = value[:int(length)].strip()
                else:
                    value = value.strip()

                if value:
                    qso[field_name] = value
                    fields_found.append(field_name)

            if qso and 'CALL' in qso:
                qso_list.append(qso)

        return qso_list