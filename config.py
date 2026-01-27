# config.py
"""
Конфигурационный файл для LoTW Producer
"""

import os
from dotenv import load_dotenv

# Загружаем переменные окружения из .env файла если есть
load_dotenv()

# RabbitMQ настройки
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', '192.168.31.5')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', '5672'))
RABBITMQ_QUEUE = os.getenv('RABBITMQ_QUEUE', 'lotw_sync_queue')
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'admin')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD', '603502')

# PostgreSQL настройки
DB_HOST = os.getenv('DB_HOST', '192.168.31.5')
DB_PORT = int(os.getenv('DB_PORT', '5432'))
DB_NAME = os.getenv('DB_NAME', 'tlog')
DB_USER = os.getenv('DB_USER', 'tlog')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'Labrador603502$')
DB_SCHEMA = os.getenv('DB_SCHEMA', 'public')

# Настройки приложения
BATCH_DELAY = float(os.getenv('BATCH_DELAY', '0.5'))
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FILE = os.getenv('LOG_FILE', 'log/lotw_producer.log')