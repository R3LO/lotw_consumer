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
            self.logger.debug("ℹ️ В ответе нет данных QSO")
            return qso_list

        # Удаляем <APP_LoTW_EOF> и всё после него перед разбором
        if '<APP_LoTW_EOF>' in content:
            content = content.split('<APP_LoTW_EOF>')[0]

        # Удаляем заголовок <eoh> и всё до него
        if '<eoh>' in content:
            content = content.split('<eoh>', 1)[1]

        qso_blocks = content.split('<eor>')

        self.logger.debug(f"🔍 Парсер: найдено {len(qso_blocks)} блоков после разделения по <eor>")

        for block_num, block in enumerate(qso_blocks, 1):
            self.logger.debug(f"🔍 Блок #{block_num}: {len(block)} символов")

            if not block.strip():
                self.logger.debug(f"🔍 Блок #{block_num}: пустой, пропускаем")
                continue

            if '<eoh>' in block:
                self.logger.debug(f"🔍 Блок #{block_num}: содержит <eoh>, берем часть после него")
                original_block = block
                block = block.split('<eoh>')[1]
                self.logger.debug(f"🔍 Блок #{block_num}: обрезан с {len(original_block)} до {len(block)} символов")

            block = re.sub(r'//.*', '', block)

            # Паттерн для тегов с указанной длиной: захватываем ровно length символов
            pattern_with_length = r'<(\w+):(\d+)>(.{0,\2})'
            # Паттерн для тегов без длины: захватываем всё до следующего <
            pattern_without_length = r'<(\w+)>([^<]*)'

            qso = {}
            fields_found = []

            # Сначала обрабатываем теги с длиной
            matches_with_length = re.findall(pattern_with_length, block)
            # Затем теги без длины
            matches_without_length = re.findall(pattern_without_length, block)

            matches = matches_with_length + matches_without_length

            self.logger.debug(f"🔍 Блок #{block_num}: найдено {len(matches)} полей")
            for match in matches:
                # Для тегов с длиной: (field_name, length, value)
                # Для тегов без длины: (field_name, value)
                if len(match) == 3:
                    field_name, length, value = match
                    # Значение уже обрезано до нужной длины регулярным выражением
                    value = value.strip()
                else:
                    field_name, value = match
                    value = value.strip()

                field_name = field_name.upper()

                if value:
                    qso[field_name] = value
                    fields_found.append(field_name)

            self.logger.debug(f"🔍 Блок #{block_num}: CALL={qso.get('CALL', 'НЕТ')}, найдено полей: {len(fields_found)}")
            if qso.get('CALL'):
                self.logger.debug(f"🔍 Блок #{block_num}: поля {', '.join(fields_found[:10])}...")

            if qso and 'CALL' in qso:
                self.logger.debug(f"✅ Блок #{block_num}: добавлен QSO {qso['CALL']}")
                qso_list.append(qso)
            else:
                self.logger.debug(f"🔍 Блок #{block_num}: пропущен (нет CALL или пустой)")

        self.logger.info(f"🔍 Парсер: итого добавлено {len(qso_list)} QSO")
        return qso_list