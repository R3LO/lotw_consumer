#!/usr/bin/env python3
"""
Test Consumer AddBase - Обрабатывает данные из файла (имитация очереди)
После обработки записывает lotw_lastsync в базу данных

Использование:
    python test_consumer_addbase.py <имя_файла.json>

Пример:
    python test_consumer_addbase.py tasks_output.json
"""

import json
import sys
import os
import psycopg2
from datetime import datetime


def connect_db() -> psycopg2.extensions.connection:
    """Подключение к базе данных"""
    from config import (
        DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD, DB_SCHEMA
    )

    try:
        print("=" * 60)
        print(" ПОДКЛЮЧЕНИЕ К БАЗЕ ДАННЫХ")
        print("=" * 60)
        print(f"   Хост: {DB_HOST}:{DB_PORT}/{DB_NAME}")
        print(f"   Схема: {DB_SCHEMA}")
        print()

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

        print("[OK] Успешно подключено к базе данных")
        print()
        return conn

    except Exception as e:
        print(f"[ERROR] Ошибка подключения к базе данных: {e}")
        return None


def update_lotw_lastsync(conn: psycopg2.extensions.connection, user_id: int, callsign: str, created_at: str) -> bool:
    """Обновляет поле lotw_lastsync в базе данных"""
    try:
        with conn.cursor() as cur:
            # Пытаемся обновить по user_id
            cur.execute("""
                UPDATE tlog_radioprofile
                SET lotw_lastsync = %s
                WHERE id = %s
            """, (created_at, user_id))

            if cur.rowcount == 0:
                # Если не найден по user_id, ищем по callsign
                print(f"  [WARN] Не найден user_id {user_id}, ищем по callsign...")
                cur.execute("""
                    UPDATE tlog_radioprofile
                    SET lotw_lastsync = %s
                    WHERE callsign LIKE %s
                    OR id IN (
                        SELECT id FROM tlog_radioprofile
                        WHERE my_callsigns LIKE %s
                    )
                """, (created_at, f"%{callsign}%", f"%{callsign}%"))

            conn.commit()
            return True

    except Exception as e:
        print(f"  [ERROR] Ошибка обновления БД: {e}")
        conn.rollback()
        return False


def process_file(filename: str):
    """Обрабатывает файл с данными"""
    print("=" * 60)
    print(" TEST CONSUMER ADDBASE")
    print("=" * 60)
    print(f"   Файл: {filename}")
    print(f"   Время запуска: {datetime.now().isoformat()}")
    print()

    # Проверяем существование файла
    if not os.path.exists(filename):
        print(f"[ERROR] Файл не найден: {filename}")
        return

    # Читаем файл
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            content = f.read().strip()

            # Проверяем, это массив или одиночный объект
            if content.startswith('['):
                data = json.loads(content)
                is_list = True
            else:
                data = [json.loads(content)]
                is_list = False

    except json.JSONDecodeError as e:
        print(f"[ERROR] Ошибка парсинга JSON: {e}")
        return
    except Exception as e:
        print(f"[ERROR] Ошибка чтения файла: {e}")
        return

    # Подключаемся к БД
    conn = connect_db()
    if not conn:
        print("[ERROR] Не удалось подключиться к базе данных")
        return

    processed_count = 0
    error_count = 0

    print("=" * 60)
    print(" ОБРАБОТКА ДАННЫХ")
    print("=" * 60)
    print()

    for i, item in enumerate(data, 1):
        print("=" * 60)
        print(f" ЗАПИСЬ #{i}")
        print("=" * 60)

        # Извлекаем данные
        task_id = item.get('task_id', 'N/A')
        callsign = item.get('callsign', 'N/A')
        username = item.get('username', 'N/A')
        user_id = item.get('user_id', 'N/A')
        created_at = item.get('created_at', 'N/A')
        lotw_lastsync = item.get('lotw_lastsync', 'N/A')

        print(f"  task_id: {task_id}")
        print(f"  callsign: {callsign}")
        print(f"  username: {username}")
        print(f"  user_id: {user_id}")
        print(f"  created_at: {created_at}")
        print(f"  lotw_lastsync (из очереди): {lotw_lastsync}")

        # Проверяем обязательные поля
        if not user_id or not created_at:
            print("\n  [ERROR] Отсутствуют обязательные поля: user_id или created_at")
            error_count += 1
            print()
            continue

        # Записываем lotw_lastsync в базу данных
        print("\n  [ЗАПИСЬ В БАЗУ ДАННЫХ]")
        print(f"  Обновление lotw_lastsync для user_id: {user_id}")
        print(f"  Значение: {created_at}")

        if update_lotw_lastsync(conn, user_id, callsign, created_at):
            print("  [OK] Успешно обновлено")
            processed_count += 1
        else:
            print("  [ERROR] Ошибка обновления")
            error_count += 1

        print()

    # Закрываем соединение
    conn.close()

    # Итоговая статистика
    print("=" * 60)
    print(" СТАТИСТИКА")
    print("=" * 60)
    print(f"  Всего записей в файле: {len(data)}")
    print(f"  Успешно обработано: {processed_count}")
    print(f"  Ошибок: {error_count}")
    print("=" * 60)


def main():
    """Основная функция"""
    # Проверяем аргументы
    if len(sys.argv) < 2:
        print(__doc__)
        print("\nИспользование:")
        print(f"  python {os.path.basename(sys.argv[0])} <имя_файла.json>")
        print()
        print("Примеры:")
        print(f"  python {os.path.basename(sys.argv[0])} tasks_output.json")
        print(f"  python {os.path.basename(sys.argv[0])} message.json")
        sys.exit(1)

    filename = sys.argv[1]
    process_file(filename)


if __name__ == "__main__":
    main()