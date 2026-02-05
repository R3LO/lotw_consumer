"""
Модуль для работы с подключением к базе данных
"""

import psycopg2
from psycopg2.extras import RealDictCursor
from typing import Optional

from config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DB_SCHEMA


class DatabaseConnection:
    """Класс для управления соединениями с БД"""

    def __init__(self, logger):
        self.logger = logger

    def get_connection(self) -> Optional[psycopg2.extensions.connection]:
        """Создает соединение с базой данных"""
        try:
            self.logger.debug(f"Подключение к БД: {DB_HOST}:{DB_PORT}/{DB_NAME}")

            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )

            # Устанавливаем схему
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {DB_SCHEMA}")

            self.logger.debug("✅ Подключение к БД успешно")
            return conn

        except psycopg2.OperationalError as e:
            self.logger.error(f"❌ Ошибка подключения к БД: {e}")
            return None
        except Exception as e:
            self.logger.error(f"❌ Неожиданная ошибка при подключении к БД: {e}")
            return None

    def get_cursor(self, conn, cursor_factory=RealDictCursor):
        """Получает курсор с указанной фабрикой"""
        return conn.cursor(cursor_factory=cursor_factory)