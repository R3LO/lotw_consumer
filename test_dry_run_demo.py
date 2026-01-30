#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Тестовый файл для демонстрации команды lotw_producer.py --all --dry-run
Получает данные из базы данных, выводит сообщения RabbitMQ на экран,
но НЕ отправляет ничего в RabbitMQ
"""

import subprocess
import sys
import os

def main():
    """Демонстрация тестового режима"""
    print("\n" + "="*80)
    print(" ТЕСТОВЫЙ ФАЙЛ: lotw_producer.py --all --dry-run")
    print("="*80)
    print("Назначение:")
    print("  - Получить данные из базы данных my_callsigns")
    print("  - Показать сообщения для RabbitMQ на экране")  
    print("  - НЕ отправлять ничего в RabbitMQ")
    print("  - Использовать формат списка: ['R3LO', 'R3LO/1']")
    print("="*80)
    print()

    print("КОМАНДЫ ДЛЯ ТЕСТИРОВАНИЯ:")
    print()
    print("1. Базовый тест:")
    print("   python lotw_producer.py --all --dry-run")
    print()
    print("2. Тест со статистикой:")
    print("   python lotw_producer.py --all --dry-run --stats")
    print()
    print("3. Сравнение с реальной отправкой:")
    print("   python lotw_producer.py --all --dry-run  # Только показ")
    print("   python lotw_producer.py --all             # Реальная отправка")
    print()
    print("4. Справка:")
    print("   python lotw_producer.py --help")
    print()
    
    # Предлагаем запустить тест
    try:
        response = input("Запустить тестовый режим? (y/n): ").lower().strip()
        if response in ['y', 'yes', 'да', 'д']:
            print("\n" + "="*80)
            print(" ЗАПУСК ТЕСТОВОГО РЕЖИМА")
            print("="*80)
            
            # Запускаем команду
            result = subprocess.run([
                sys.executable, 'lotw_producer.py', '--all', '--dry-run'
            ], capture_output=False, text=True)
            
            if result.returncode == 0:
                print("\n✅ Тест завершен успешно!")
                print("Никаких сообщений не было отправлено в RabbitMQ")
            else:
                print(f"\n❌ Тест завершен с ошибкой (код: {result.returncode})")
                
        else:
            print("\nТест отменен пользователем")
            
    except KeyboardInterrupt:
        print("\n\nТест прерван пользователем")
    except Exception as e:
        print(f"\nОшибка при запуске теста: {e}")

    print("\n" + "="*80)
    print(" ТЕХНИЧЕСКАЯ ИНФОРМАЦИЯ")
    print("="*80)
    print("Новый функционал в lotw_producer.py:")
    print("  ✅ Метод test_rabbitmq_messages() - тестовый режим")
    print("  ✅ Аргумент --dry-run - включение тестового режима")
    print("  ✅ Показ JSON сообщений для RabbitMQ")
    print("  ✅ Статистика без реальной отправки")
    print("  ✅ Формат данных: ['R3LO', 'R3LO/1']")
    print("="*80)

if __name__ == "__main__":
    main()