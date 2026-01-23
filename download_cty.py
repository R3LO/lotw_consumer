"""
Скрипт для загрузки файла cty.dat с официального источника
"""

import urllib.request
import os
from pathlib import Path


def download_cty_dat(url: str = None, filepath: str = None):
    """
    Загружает файл cty.dat

    Args:
        url: URL для загрузки (по умолчанию - country-files.com)
        filepath: путь для сохранения файла
    """
    if url is None:
        url = "https://www.country-files.com/cty/cty.dat"

    if filepath is None:
        base_dir = Path(__file__).parent
        filepath = base_dir / "cty.dat"

    print(f"Downloading cty.dat from {url}...")

    try:
        urllib.request.urlretrieve(url, filepath)
        print(f"[OK] File saved: {filepath}")

        # Показываем первые строки
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()[:20]
            print("\nFirst lines of file:")
            print("-" * 50)
            for line in lines:
                print(line.rstrip())

        return True
    except Exception as e:
        print(f"[ERROR] Download failed: {e}")
        return False


if __name__ == "__main__":
    download_cty_dat()
