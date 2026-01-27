#!/usr/bin/env python3
"""
Тестовый скрипт для реального получения данных с LoTW API.
Сохраняет полученные данные в txt файл.
"""

import requests
import sys
import os
from datetime import datetime
from typing import Optional

# Добавляем путь к модулям проекта
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def lotw_request(
    callsign: str,
    username: str,
    password: str,
    lotw_lastsync: Optional[str] = None
) -> dict:
    """
    Выполняет реальный POST запрос к LoTW API.

    Args:
        callsign: Позывной для фильтрации (qso_callsign)
        username: Логин пользователя LoTW
        password: Пароль пользователя LoTW
        lotw_lastsync: Дата последней синхронизации (YYYY-MM-DD)

    Returns:
        dict с ответом от API
    """
    # URL для LoTW ADIF запроса (GET с параметрами)
    login_url = "https://lotw.arrl.org/lotwuser/lotwreport.adi"

    # Параметры формы для POST запроса
    data = {
        'login': username,
        'password': password,
        'qso_query': '1',
        'qso_qsl': 'yes',
        'qso_qsldetail': 'yes',
        'qso_withown': 'yes',
        'qso_owncall': callsign
    }

    # Добавляем qso_qslsince если есть дата последней синхронизации
    # LoTW ожидает формат YYYYMMDD
    if lotw_lastsync:
        # Убираем дефисы если они есть (YYYY-MM-DD -> YYYYMMDD)
        lotw_lastsync_clean = lotw_lastsync.replace('-', '').replace('/', '')
        data['qso_qslsince'] = lotw_lastsync_clean

    headers = {
        'Accept': 'application/xml',
        'User-Agent': 'LoTW-Client/1.0'
    }

    # LoTW API использует GET с параметрами в URL
    response = requests.get(login_url, params=data, timeout=60)

    response.raise_for_status()

    return {
        'status_code': response.status_code,
        'text': response.text,
        'headers': dict(response.headers)
    }


def save_response_to_file(
    response: dict,
    callsign: str,
    output_dir: str = "test_results"
) -> str:
    """
    Сохраняет ответ от LoTW API в txt файл.

    Args:
        response: Ответ от API
        callsign: Использованный позывной
        output_dir: Директория для сохранения

    Returns:
        str: Путь к созданному файлу
    """
    # Создаем директорию если не существует
    os.makedirs(output_dir, exist_ok=True)

    # Формируем имя файла (очищаем от недопустимых символов)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_callsign = callsign.replace('/', '_').replace('\\', '_')
    filename = f"lotw_response_{safe_callsign}_{timestamp}.txt"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write("=" * 60 + "\n")
        f.write(f"LoTW API Response\n")
        f.write(f"Дата запроса: {datetime.now().isoformat()}\n")
        f.write(f"Позывной: {callsign}\n")
        f.write("=" * 60 + "\n\n")

        f.write("STATUS CODE:\n")
        f.write(f"{response['status_code']}\n\n")

        f.write("HEADERS:\n")
        for key, value in response['headers'].items():
            f.write(f"  {key}: {value}\n")
        f.write("\n")

        f.write("RESPONSE BODY:\n")
        f.write("-" * 40 + "\n")
        f.write(response['text'])
        f.write("\n")

    return filepath


def main():
    """
    Пример использования.
    Параметры можно задать через аргументы или переменные окружения.
    """
    import argparse

    parser = argparse.ArgumentParser(
        description='Тестовый запрос к LoTW API с сохранением в файл'
    )
    parser.add_argument('--callsign', '-c', required=True, help='Позывной (my_callsign)')
    parser.add_argument('--username', '-u', required=True, help='Логин LoTW')
    parser.add_argument('--password', '-p', required=True, help='Пароль LoTW')
    parser.add_argument(
        '--lastsync',
        '-l',
        default=None,
        help='Дата последней синхронизации (YYYYMMDD)'
    )
    parser.add_argument(
        '--output',
        '-o',
        default='test_results',
        help='Директория для сохранения результатов'
    )

    args = parser.parse_args()

    print("=" * 60)
    print("LoTW API Test")
    print("=" * 60)
    print(f"Позывной: {args.callsign}")
    print(f"Логин: {args.username}")
    print(f"Последняя синхронизация: {args.lastsync or 'не задана'}")
    print("-" * 60)

    try:
        print("Отправка запроса к LoTW API...")
        response = lotw_request(
            callsign=args.callsign,
            username=args.username,
            password=args.password,
            lotw_lastsync=args.lastsync
        )

        print(f"Получен ответ: статус {response['status_code']}")

        filepath = save_response_to_file(
            response=response,
            callsign=args.callsign,
            output_dir=args.output
        )

        print(f"Результат сохранен в: {filepath}")
        print("=" * 60)

        return 0

    except requests.exceptions.HTTPError as e:
        print(f"Ошибка HTTP: {e}")
        if e.response is not None:
            print(f"Ответ сервера: {e.response.text[:500]}")
        return 1
    except requests.exceptions.RequestException as e:
        print(f"Ошибка запроса: {e}")
        return 1
    except Exception as e:
        print(f"Неожиданная ошибка: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())