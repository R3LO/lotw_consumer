@echo off
echo Запуск LoTW Consumer...
echo Дата: %date% %time% > consumer.log
python consumer.py >> consumer.log 2>&1
echo Завершено: %date% %time% >> consumer.log