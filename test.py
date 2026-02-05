# 1. Сохраните код в файл dxcc_lookup.py
# 2. Скачайте файл r150cty.dat и положите в ту же папку
#    Скачать можно отсюда: https://www.country-files.com/contest/cty/
#    или отсюда: https://www.country-files.com/cty/current/r150cty.dat
# 3. Запустите Python скрипт:

from dxcc_lookup import print_dxcc_info

# Тестирование позывных
print_dxcc_info("R2FA")
print()
print_dxcc_info("RA1OO")
print()
print_dxcc_info("RA4F")
