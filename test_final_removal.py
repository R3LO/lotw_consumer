#!/usr/bin/env python3
"""
Финальный тест для проверки удаления команды --list-format
"""

import subprocess
import sys

def test_command_removed():
    """Тест что команда --list-format больше не работает"""
    print("Тест: Проверка удаления команды --list-format")
    
    try:
        # Пробуем запустить --list-format
        result = subprocess.run([
            sys.executable, 'lotw_producer.py', '--list-format'
        ], capture_output=True, text=True, timeout=5)
        
        if result.returncode != 0 and "unrecognized arguments: --list-format" in result.stderr:
            print("✅ Команда --list-format успешно удалена")
            return True
        else:
            print("❌ Команда --list-format все еще работает")
            print(f"Return code: {result.returncode}")
            print(f"Stderr: {result.stderr[:100]}")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Команда выполнялась слишком долго")
        return False
    except Exception as e:
        print(f"❌ Ошибка тестирования: {e}")
        return False

def test_help_updated():
    """Тест что справка обновлена"""
    print("\nТест: Проверка обновленной справки")
    
    try:
        result = subprocess.run([
            sys.executable, 'lotw_producer.py', '--help'
        ], capture_output=True, text=True, timeout=5)
        
        help_text = result.stdout + result.stderr
        
        if "--list-format" not in help_text:
            print("✅ Справка обновлена, --list-format не упоминается")
            return True
        else:
            print("❌ Справка содержит упоминания --list-format")
            return False
            
    except Exception as e:
        print(f"❌ Ошибка получения справки: {e}")
        return False

def main():
    print("ФИНАЛЬНЫЙ ТЕСТ УДАЛЕНИЯ КОМАНДЫ --list-format")
    print("="*50)
    
    test1 = test_command_removed()
    test2 = test_help_updated()
    
    print("\n" + "="*50)
    if test1 and test2:
        print("✅ ВСЕ ТЕСТЫ ПРОЙДЕНЫ")
        print("Команда --list-format успешно удалена")
        print("Теперь используйте: python lotw_producer.py --all")
    else:
        print("❌ НЕКОТОРЫЕ ТЕСТЫ НЕ ПРОЙДЕНЫ")
    
    return test1 and test2

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)