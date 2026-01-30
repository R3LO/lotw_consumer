#!/usr/bin/env python3
"""
Тестовый скрипт для демонстрации нового формата данных
Показывает работу с позывными в формате списка ["R3LO", "R3LO/1"]
"""

import sys
import os
import json
from datetime import datetime

# Добавляем текущую директорию в путь для импорта
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from lotw_producer import LoTWProducer


def test_new_format():
    """Тестирует новый формат получения данных"""
    print("\n" + "="*60)
    print(" ТЕСТ НОВОГО ФОРМАТА ДАННЫХ")
    print("="*60)
    print()

    producer = None

    try:
        producer = LoTWProducer()

        # Тестируем новый метод получения списка
        print("1. Получение позывных в формате списка...")
        callsigns_list = producer.extract_callsigns_list()

        if callsigns_list:
            print(f"✅ Получен список из {len(callsigns_list)} позывных")
            print(f"   Формат: {callsigns_list[:10]}{'...' if len(callsigns_list) > 10 else ''}")
            
            # Сохраняем в файл
            with open('test_callsigns_list.json', 'w', encoding='utf-8') as f:
                json.dump(callsigns_list, f, indent=2, ensure_ascii=False)
            print(f"   Сохранено в файл: test_callsigns_list.json")
        else:
            print("❌ Список позывных пуст")
            return

        print()

        # Сравниваем с старым методом
        print("2. Сравнение с старым форматом...")
        callsigns_dict = producer.extract_callsigns_with_credentials()
        
        if callsigns_dict:
            print(f"✅ Старый формат: {len(callsigns_dict)} позывных с учетными данными")
            print(f"   Примеры: {list(callsigns_dict.keys())[:5]}")
        else:
            print("❌ Старый формат не вернул данные")

        print()

        # Проверяем синхронизацию в новом формате
        print("3. Тестирование синхронизации (первые 3 позывных)...")
        test_callsigns = callsigns_list[:3] if len(callsigns_list) >= 3 else callsigns_list
        
        for callsign in test_callsigns:
            if callsign in callsigns_dict:
                credentials = callsigns_dict[callsign]
                print(f"   {callsign} -> логин: {credentials['lotw_user']}")
            else:
                print(f"   {callsign} -> учетные данные не найдены")

        print()
        print("="*60)
        print(" ТЕСТ ЗАВЕРШЕН УСПЕШНО")
        print("="*60)

        return True

    except Exception as e:
        print(f"❌ Ошибка тестирования: {e}")
        return False

    finally:
        if producer:
            producer.close()


def main():
    """Основная функция"""
    print("LoTW Producer - Тест нового формата данных")
    print("Формат данных: ['R3LO', 'R3LO/1']")
    
    success = test_new_format()
    
    if success:
        print("\n✅ Все тесты пройдены успешно!")
        print("\nИспользование нового формата:")
        print("  python lotw_producer.py --list-format")
        print("  python lotw_producer.py --list-format --stats")
    else:
        print("\n❌ Тесты не пройдены")
        sys.exit(1)


if __name__ == "__main__":
    main()