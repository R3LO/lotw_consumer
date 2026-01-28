# config.py
"""
Конфигурационный файл для LoTW Producer
"""

import os
from dotenv import load_dotenv

# Загружаем переменные окружения из .env файла рядом с этим файлом
dotenv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
load_dotenv(dotenv_path)

# RabbitMQ настройки
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT') or '5672')
RABBITMQ_USER = os.getenv('RABBITMQ_USER')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD')

# Таймауты RabbitMQ (в секундах)
RABBITMQ_HEARTBEAT = 600  # 10 минут
RABBITMQ_TIMEOUT = 600    # 10 минут

# Основная очередь и exchange
RABBITMQ_QUEUE = os.getenv('RABBITMQ_QUEUE')
RABBITMQ_EXCHANGE = os.getenv('RABBITMQ_EXCHANGE')

# Delayed очередь для повторных попыток (DLX)
RABBITMQ_DELAYED_QUEUE = os.getenv('RABBITMQ_DELAYED_QUEUE')
RABBITMQ_DELAYED_EXCHANGE = os.getenv('RABBITMQ_DELAYED_EXCHANGE')
RABBITMQ_DLX_EXCHANGE = os.getenv('RABBITMQ_DLX_EXCHANGE')

# Время задержки для повторных попыток (45 минут в мс)
RETRY_DELAY_MS = int(os.getenv('RETRY_DELAY_MS', '2700000'))  # 45 минут = 2700000 мс

# PostgreSQL настройки
DB_HOST = os.getenv('DB_HOST')
DB_PORT = int(os.getenv('DB_PORT') or '5432')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_SCHEMA = os.getenv('DB_SCHEMA')

# Настройки приложения
BATCH_DELAY = float(os.getenv('BATCH_DELAY') or '0.5')
MAX_RETRIES = int(os.getenv('MAX_RETRIES') or '3')
LOG_LEVEL = os.getenv('LOG_LEVEL')
LOG_FILE = os.getenv('LOG_FILE')