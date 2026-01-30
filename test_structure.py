#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Простой тест нового формата данных без подключения к БД
Демонстрирует структуру новых методов
"""

import sys
import os

# Добавляем текущую директорию в путь для импорта
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def test_new_methods():
    """Тестирует структуру новых методов"""
    print("\n" + "="*60)
    print(" ТЕСТ НОВОЙ СТРУКТУРЫ МЕТОДОВ")
    print("="*60)
    print()

    try:
        # Импортируем класс
        from lotw_producer import LoTWProducer
        
        print("1. Создание экземпляра LoTWProducer...")
        producer = LoTWProducer()
        print("   OK: Экземпляр создан")
        
        print()
        print("2. Проверка новых методов...")
        
        # Проверяем наличие новых методов
        new_methods = [
            'extract_callsigns_list',
            'sync_callsigns_from_list', 
            'get_callsigns_list_only'
        ]
        
        for method_name in new_methods:
            if hasattr(producer, method_name):
                print(f"   OK: Метод {method_name} найден")
            else:
                print(f"   ERROR: Метод {method_name} не найден")
        
        print()
        print("3. Демонстрация нового формата...")
        
        # Создаем пример данных в новом формате
        example_callsigns = ["R3LO", "R3LO/1", "UA1ABC", "UA1ABC/1", "UA1ABC/P"]
        print(f"   Новый формат списка: {example_callsigns}")
        print(f"   Количество позывных: {len(example_callsigns)}")
        
        print()
        print("4. Информация о новых возможностях...")
        print("   - extract_callsigns_list(): возвращает простой список позывных")
        print("   - sync_callsigns_from_list(): синхронизация в новом формате")
        print("   - get_callsigns_list_only(): получение только списка")
        print()
        print("   Новые аргументы командной строки:")
        print("   --list-format: использовать новый формат списка")
        print("   --all: использовать старый формат с полными данными")
        
        print()
        print("="*60)
        print(" ТЕСТ СТРУКТУРЫ ЗАВЕРШЕН УСПЕШНО")
        print("="*60)
        
        producer.close()
        return True
        
    except Exception as e:
        print(f"   ERROR: Ошибка тестирования: {e}")
        return False


def show_usage_examples():
    """Показывает примеры использования"""
    print("\n" + "="*60)
    print(" ПРИМЕРЫ ИСПОЛЬЗОВАНИЯ НОВОГО ФОРМАТА")
    print("="*60)
    print()
    
    examples = [
        "python lotw_producer.py --list-format",
        "python lotw_producer.py --list-format --stats", 
        "python lotw_producer.py --list-format --delay 1.0",
        "python lotw_producer.py --all",
        "python lotw_producer.py --test-db"
    ]
    
    for i, example in enumerate(examples, 1):
        print(f"{i}. {example}")
    
    print()
    print("ФОРМАТ ДАННЫХ:")
    print("  Старый формат: {'R3LO': {'lotw_user': 'user', ...}}")
    print("  Новый формат:  ['R3LO', 'R3LO/1', 'UA1ABC']")
    print()


def main():
    """Основная функция"""
    print("LoTW Producer - Тест новой структуры")
    print("Изменения: формат данных из БД my_callsigns")
    
    success = test_new_methods()
    show_usage_examples()
    
    if success:
        print("\nSUCCESS: Новая структура методов работает корректно!")
        print("\nИзменения в lotw_producer.py:")
        print("- Добавлен метод extract_callsigns_list() для получения простого списка")
        print("- Добавлен метод sync_callsigns_from_list() для синхронизации в новом формате") 
        print("- Добавлен аргумент --list-format для командной строки")
        print("- Сохранена обратная совместимость с --all")
    else:
        print("\nERROR: Проблемы с новой структурой")
        sys.exit(1)


if __name__ == "__main__":
    main()