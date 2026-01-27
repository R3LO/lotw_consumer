#!/usr/bin/env python3
"""
Test script - Выводит все данные, которые продюсер получает для отправки в RabbitMQ
"""

import json
import time
import psycopg2
import sys
import os
from datetime import datetime
from typing import Dict, List, Any, Optional

# Импортируем конфигурацию
from config import (
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DB_SCHEMA,
    RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_QUEUE, RABBITMQ_USER, RABBITMQ_PASSWORD,
    BATCH_DELAY
)


def get_db_connection():
    """Создает соединение с базой данных"""
    try:
        print(f"Подключение к БД: {DB_HOST}:{DB_PORT}/{DB_NAME}")
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
        print("Подключение к БД успешно\n")
        return conn
    except psycopg2.OperationalError as e:
        print(f"Ошибка подключения к БД: {e}")
        return None
    except Exception as e:
        print(f"Неожиданная ошибка при подключении к БД: {e}")
        return None


def extract_callsign_name(callsign_data) -> str:
    """Извлекает имя позывного из различных форматов данных"""
    if not callsign_data:
        return ""

    if isinstance(callsign_data, str):
        if callsign_data.strip().startswith(('{', '[')):
            try:
                data = json.loads(callsign_data)
                if isinstance(data, dict) and 'name' in data:
                    return data['name'].strip()
                elif isinstance(data, list) and data:
                    return extract_callsign_name(data[0])
            except json.JSONDecodeError:
                pass
        return callsign_data.strip()

    elif isinstance(callsign_data, dict):
        name = callsign_data.get('name', '')
        if name:
            return name.strip()

    return str(callsign_data).strip()


def parse_my_callsigns(my_callsigns) -> List[Any]:
    """Парсит поле my_callsigns из различных форматов"""
    if not my_callsigns:
        return []

    if isinstance(my_callsigns, str):
        try:
            data = json.loads(my_callsigns)
            if isinstance(data, list):
                return data
            elif isinstance(data, dict):
                return [data]
            else:
                return [{"name": str(data)}]
        except json.JSONDecodeError:
            if my_callsigns.strip().startswith('['):
                return []
            else:
                return [{"name": my_callsigns.strip()}]

    elif isinstance(my_callsigns, list):
        return my_callsigns

    return [my_callsigns]


def extract_callsigns_with_credentials(conn) -> Dict[str, Dict[str, Any]]:
    """
    Извлекает все позывные из базы данных с их логинами и паролями LOTW
    Возвращает словарь, где ключ - позывной, значение - словарь с учетными данными
    """
    callsign_dict = {}

    try:
        # Получаем все записи из таблицы с проверкой: lotw_chk_pass = TRUE
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    id,
                    callsign,
                    my_callsigns,
                    lotw_user,
                    lotw_password,
                    lotw_lastsync
                FROM tlog_radioprofile
                WHERE lotw_user IS NOT NULL
                AND lotw_user != ''
                AND lotw_password IS NOT NULL
                AND lotw_password != ''
                AND lotw_chk_pass = TRUE
                ORDER BY id ASC
            """)
            rows = cur.fetchall()

            total_rows = len(rows)

            for row in rows:
                user_id, callsign_data, my_callsigns, lotw_user, lotw_password, lotw_lastsync = row

                # Создаем словарь с учетными данными
                credentials = {
                    "lotw_user": lotw_user,
                    "lotw_password": lotw_password,
                    "user_id": user_id,
                    "lotw_lastsync": lotw_lastsync
                }

                # Обрабатываем основной позывной
                if callsign_data:
                    callsign_str = extract_callsign_name(callsign_data)
                    if callsign_str:
                        callsign_dict[callsign_str.upper()] = credentials

                # Обрабатываем позывные из my_callsigns
                if my_callsigns:
                    callsigns_list = parse_my_callsigns(my_callsigns)
                    for callsign_item in callsigns_list:
                        callsign_name = extract_callsign_name(callsign_item)
                        if callsign_name:
                            callsign_dict[callsign_name.upper()] = credentials

        print(f"Найдено {len(callsign_dict)} позывных из базы данных (lotw_chk_pass = TRUE)")

        return callsign_dict

    except Exception as e:
        print(f"Ошибка при чтении базы данных: {e}")
        return {}


def format_task_for_display(task: Dict[str, Any]) -> str:
    """Форматирует задачу для красивого вывода"""
    output = []
    output.append("=" * 60)
    output.append(f"ЗАДАЧА: {task['task_id']}")
    output.append("=" * 60)

    for key, value in task.items():
        if key in ['password']:
            # Скрываем пароль, показываем только первые 3 символа
            if isinstance(value, str) and len(value) > 3:
                output.append(f"  {key}: {value[:3]}***")
            else:
                output.append(f"  {key}: ***")
        else:
            output.append(f"  {key}: {value}")

    output.append("=" * 60)
    return "\n".join(output)


def generate_tasks(callsigns: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Генерирует задачи из позывных (как это делает продюсер)
    """
    tasks = []

    for callsign, credentials in callsigns.items():
        task_id = f"lotw_{int(time.time())}_{callsign}"

        task = {
            'task_id': task_id,
            'task_type': 'lotw_sync',
            'callsign': callsign,
            'username': credentials['lotw_user'],
            'password': credentials['lotw_password'],
            'user_id': credentials['user_id'],
            'lotw_lastsync': credentials.get('lotw_lastsync'),
            'created_at': datetime.now().date().isoformat()
        }

        tasks.append(task)

    return tasks


def main():
    """Основная функция"""
    print("\n" + "=" * 60)
    print(" ТЕСТОВЫЙ РЕЖИМ - ВЫВОД ВСЕХ ДАННЫХ ДЛЯ ОТПРАВКИ")
    print("=" * 60)
    print(f"\nКонфигурация:")
    print(f"  База данных: {DB_HOST}:{DB_PORT}/{DB_NAME}")
    print(f"  Схема: {DB_SCHEMA}")
    print(f"  RabbitMQ: {RABBITMQ_HOST}:{RABBITMQ_PORT}")
    print(f"  Очередь: {RABBITMQ_QUEUE}")
    print()

    # Подключаемся к БД
    conn = get_db_connection()
    if not conn:
        print("Не удалось подключиться к базе данных")
        sys.exit(1)

    try:
        # Извлекаем позывные из БД
        print("Извлечение позывных из базы данных...")
        callsigns = extract_callsigns_with_credentials(conn)

        if not callsigns:
            print("\nНе найдено позывных для синхронизации")
            print("Проверьте:")
            print("  1. Таблица tlog_radioprofile существует")
            print("  2. Поля lotw_user и lotw_password заполнены")
            print("  3. Поле lotw_chk_pass = TRUE")
            return

        # Генерируем задачи (как это делает продюсер)
        print("\nГенерирую задачи для отправки...\n")
        tasks = generate_tasks(callsigns)

        # Выводим каждую задачу
        print(f"ВСЕГО ЗАДАЧ: {len(tasks)}\n")

        for i, task in enumerate(tasks, 1):
            print(format_task_for_display(task))
            print()

        # Итоговая статистика
        print("=" * 60)
        print(" СТАТИСТИКА")
        print("=" * 60)
        print(f"  Всего задач: {len(tasks)}")

        # Группируем по user_id
        user_stats = {}
        for task in tasks:
            user_id = task['user_id']
            if user_id not in user_stats:
                user_stats[user_id] = {
                    'count': 0,
                    'callsigns': [],
                    'username': task['username']
                }
            user_stats[user_id]['count'] += 1
            user_stats[user_id]['callsigns'].append(task['callsign'])

        print(f"  Уникальных пользователей: {len(user_stats)}")
        print()

        for user_id, info in sorted(user_stats.items()):
            print(f"  Пользователь ID {user_id}:")
            print(f"    Логин: {info['username']}")
            print(f"    Количество позывных: {info['count']}")
            print(f"    Позывные: {', '.join(info['callsigns'])}")

        print()
        print("=" * 60)
        print(" JSON ВСЕХ ЗАДАЧ")
        print("=" * 60)

        # Выводим все задачи в формате JSON
        tasks_json = json.dumps(tasks, indent=2, ensure_ascii=False, default=str)
        print(tasks_json)

        # Сохраняем в файл
        with open('tasks_output.json', 'w', encoding='utf-8') as f:
            f.write(tasks_json)
        print(f"\n[Сохранено в файл: tasks_output.json]")

    finally:
        if conn:
            conn.close()
        print("\nПодключение к БД закрыто")


if __name__ == "__main__":
    main()