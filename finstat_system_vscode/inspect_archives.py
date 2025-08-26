#!/usr/bin/env python3
"""
Скрипт для проверки содержимого архивов
"""
import os
import sys
sys.path.append('src')

from src.archive_utils import list_archive_contents

def inspect_archives():
    input_folder = "input"
    archives = []
    
    for f in os.listdir(input_folder):
        if f.lower().endswith(('.rar', '.zip')):
            archives.append(f)
    
    if not archives:
        print("Архивы не найдены в папке input/")
        return
    
    archives.sort()
    print("=" * 60)
    print("СОДЕРЖИМОЕ АРХИВОВ")
    print("=" * 60)
    
    for archive in archives:
        print(f"\n📁 {archive}")
        print("-" * 40)
        
        archive_path = os.path.join(input_folder, archive)
        contents = list_archive_contents(archive_path)
        
        if contents:
            for file in contents:
                print(f"  📄 {file}")
        else:
            print("  ⚠️  Не удалось получить содержимое")
    
    print("\n" + "=" * 60)

if __name__ == "__main__":
    inspect_archives()