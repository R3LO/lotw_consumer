"""
Модуль для настройки логирования
"""

import logging
import sys
from config import LOG_LEVEL, LOG_FILE


def setup_logging():
    """Настройка логирования из конфига"""
    log_level = getattr(logging, LOG_LEVEL.upper(), logging.INFO)

    logger = logging.getLogger(__name__)
    logger.setLevel(log_level)

    # Удаляем существующие обработчики
    logger.handlers.clear()

    # Форматтер
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # Файловый обработчик из конфига
    try:
        file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.info(f"Логирование в файл: {LOG_FILE}")
    except Exception as e:
        logger.warning(f"Не удалось настроить файловое логирование: {e}")

    # Консольный обработчик
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    logger.info(f"Логирование настроено. Уровень: {LOG_LEVEL}")
    return logger