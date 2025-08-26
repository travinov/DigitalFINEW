#!/usr/bin/env python3
"""
Утилиты для работы с архивами (RAR, ZIP)
"""
import os
import tempfile
import shutil
import subprocess
from pathlib import Path
from typing import List, Optional

def _extract_rar(archive_path: str, extract_to: str) -> List[str]:
    """Извлечение RAR архива с помощью unar (macOS) или unrar (Linux)"""
    commands = [
        ['unar', '-o', extract_to, archive_path],  # macOS
        ['unrar', 'x', '-o+', archive_path, extract_to + '/']  # Linux
    ]
    
    for cmd in commands:
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            
            # Получаем список извлеченных файлов
            extracted_files = []
            for root, dirs, files in os.walk(extract_to):
                for file in files:
                    extracted_files.append(os.path.join(root, file))
            return extracted_files
            
        except subprocess.CalledProcessError as e:
            continue
        except FileNotFoundError:
            continue
    
    print(f"Не найден инструмент для RAR. Установите: brew install unar (macOS) или apt install unrar (Linux)")
    return []

def _extract_with_python_rarfile(archive_path: str, extract_to: str) -> List[str]:
    """Извлечение RAR с помощью библиотеки rarfile (если установлена)"""
    try:
        import rarfile
        
        with rarfile.RarFile(archive_path) as rf:
            rf.extractall(extract_to)
            
        # Получаем список извлеченных файлов
        extracted_files = []
        for root, dirs, files in os.walk(extract_to):
            for file in files:
                extracted_files.append(os.path.join(root, file))
        return extracted_files
        
    except ImportError:
        return []
    except Exception as e:
        print(f"Ошибка извлечения RAR {archive_path}: {e}")
        return []

def extract_archive(archive_path: str, extract_to: Optional[str] = None) -> tuple[list[str], Optional[str]]:
    """
    Извлечение архива (RAR, ZIP).
    
    Args:
        archive_path: путь к архиву
        extract_to: папка для извлечения (если None — создаётся временная)
    
    Returns:
        (dbf_files, temp_dir):
          - dbf_files: список путей к извлечённым .dbf файлам
          - temp_dir: путь к созданной временной папке (если extract_to не был задан), иначе None
    """
    if not os.path.exists(archive_path):
        print(f"Архив не найден: {archive_path}")
        return []
    
    # Создаем временную папку если не указана
    temp_dir = None
    if extract_to is None:
        temp_dir = tempfile.mkdtemp(prefix="finstat_extract_")
        extract_to = temp_dir
    else:
        os.makedirs(extract_to, exist_ok=True)
    
    archive_path = str(Path(archive_path).resolve())
    extract_to = str(Path(extract_to).resolve())
    
    extracted_files = []
    
    if archive_path.lower().endswith('.rar'):
        # Пробуем unrar командной строки
        extracted_files = _extract_rar(archive_path, extract_to)
        
        # Если не получилось, пробуем библиотеку rarfile
        if not extracted_files:
            extracted_files = _extract_with_python_rarfile(archive_path, extract_to)
            
    elif archive_path.lower().endswith('.zip'):
        import zipfile
        try:
            with zipfile.ZipFile(archive_path, 'r') as zf:
                zf.extractall(extract_to)
                for info in zf.infolist():
                    if not info.is_dir():
                        extracted_files.append(os.path.join(extract_to, info.filename))
        except Exception as e:
            print(f"Ошибка извлечения ZIP {archive_path}: {e}")
    
    # Фильтруем только .dbf файлы
    dbf_files = [f for f in extracted_files if f.lower().endswith('.dbf')]
    
    return dbf_files, temp_dir

def cleanup_temp_dir(temp_dir: str):
    """Удаление временной папки"""
    if temp_dir and os.path.exists(temp_dir):
        try:
            shutil.rmtree(temp_dir)
        except Exception as e:
            print(f"Ошибка удаления временной папки {temp_dir}: {e}")

def list_archive_contents(archive_path: str) -> List[str]:
    """Получение списка файлов в архиве без извлечения"""
    if not os.path.exists(archive_path):
        return []
    
    contents = []
    
    if archive_path.lower().endswith('.rar'):
        # Пробуем unar (macOS)
        try:
            result = subprocess.run([
                'unar', '-l', archive_path
            ], capture_output=True, text=True, check=True)
            
            # Парсим вывод unar
            lines = result.stdout.split('\n')
            for line in lines:
                if '.dbf' in line.lower() and not line.strip().startswith('Archive:'):
                    # Ищем файлы .dbf в выводе
                    if line.strip() and '.dbf' in line.lower():
                        # Извлекаем имя файла
                        parts = line.strip().split()
                        for part in parts:
                            if part.lower().endswith('.dbf'):
                                contents.append(part)
                                break
        except:
            pass
        
        # Если unar не сработал, пробуем unrar
        if not contents:
            try:
                result = subprocess.run([
                    'unrar', 'l', archive_path
                ], capture_output=True, text=True, check=True)
                
                # Парсим вывод unrar
                lines = result.stdout.split('\n')
                for line in lines:
                    if '.dbf' in line.lower():
                        # Извлекаем имя файла из строки
                        parts = line.split()
                        if parts:
                            filename = parts[-1]
                            if filename.lower().endswith('.dbf'):
                                contents.append(filename)
            except:
                pass
        
        # Если командные утилиты не сработали, пробуем с библиотекой
        if not contents:
            try:
                import rarfile
                with rarfile.RarFile(archive_path) as rf:
                    for info in rf.infolist():
                        if info.filename.lower().endswith('.dbf'):
                            contents.append(info.filename)
            except:
                pass
    
    elif archive_path.lower().endswith('.zip'):
        import zipfile
        try:
            with zipfile.ZipFile(archive_path, 'r') as zf:
                for info in zf.infolist():
                    if info.filename.lower().endswith('.dbf'):
                        contents.append(info.filename)
        except:
            pass
    
    return contents