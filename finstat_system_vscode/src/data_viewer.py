#!/usr/bin/env python3
"""
Инструмент просмотра загруженных данных в финансовой системе
"""
import argparse
import sqlite3
import pandas as pd
from .db import get_conn

def show_summary(conn):
    """Общая статистика по загруженным данным"""
    print("=" * 50)
    print("ОБЩАЯ СТАТИСТИКА")
    print("=" * 50)
    
    # Статистика по банкам
    banks = pd.read_sql_query("SELECT COUNT(*) as count FROM banks", conn)
    print(f"Банков в системе: {banks.iloc[0]['count']}")
    
    # Статистика по формам отчетности
    forms = pd.read_sql_query("SELECT COUNT(*) as count FROM forms", conn)
    print(f"Форм отчетности: {forms.iloc[0]['count']}")
    
    # Статистика по периодам
    periods = pd.read_sql_query("SELECT COUNT(DISTINCT period) as count FROM raw_values", conn)
    print(f"Периодов данных: {periods.iloc[0]['count']}")
    
    # Статистика по записям
    raw_count = pd.read_sql_query("SELECT COUNT(*) as count FROM raw_values", conn)
    print(f"Записей сырых данных: {raw_count.iloc[0]['count']}")
    
    # Статистика по индикаторам
    ind_count = pd.read_sql_query("SELECT COUNT(*) as count FROM indicator_values", conn)
    print(f"Рассчитанных индикаторов: {ind_count.iloc[0]['count']}")

def show_banks(conn):
    """Список банков"""
    print("=" * 50)
    print("БАНКИ В СИСТЕМЕ")
    print("=" * 50)
    
    df = pd.read_sql_query("""
        SELECT b.bank_id, b.bank_name, 
               COUNT(DISTINCT rv.period) as periods_count,
               COUNT(DISTINCT rv.form_code) as forms_count,
               COUNT(*) as records_count
        FROM banks b
        LEFT JOIN raw_values rv ON b.bank_id = rv.bank_id
        GROUP BY b.bank_id, b.bank_name
        ORDER BY b.bank_id
    """, conn)
    
    if df.empty:
        print("Нет данных по банкам")
        return
    
    print(df.to_string(index=False))

def show_forms(conn):
    """Список форм отчетности"""
    print("=" * 50)
    print("ФОРМЫ ОТЧЕТНОСТИ")
    print("=" * 50)
    
    df = pd.read_sql_query("""
        SELECT f.form_code, f.form_name,
               COUNT(DISTINCT rv.bank_id) as banks_count,
               COUNT(DISTINCT rv.period) as periods_count,
               COUNT(*) as records_count
        FROM forms f
        LEFT JOIN raw_values rv ON f.form_code = rv.form_code
        GROUP BY f.form_code, f.form_name
        ORDER BY f.form_code
    """, conn)
    
    if df.empty:
        print("Нет данных по формам")
        return
    
    print(df.to_string(index=False))

def show_periods(conn):
    """Список периодов"""
    print("=" * 50)
    print("ПЕРИОДЫ ДАННЫХ")
    print("=" * 50)
    
    df = pd.read_sql_query("""
        SELECT period,
               COUNT(DISTINCT bank_id) as banks_count,
               COUNT(DISTINCT form_code) as forms_count,
               COUNT(*) as records_count
        FROM raw_values
        GROUP BY period
        ORDER BY period DESC
    """, conn)
    
    if df.empty:
        print("Нет данных по периодам")
        return
    
    print(df.to_string(index=False))

def show_ingestion_log(conn):
    """Лог импорта файлов"""
    print("=" * 50)
    print("ЖУРНАЛ ИМПОРТА ФАЙЛОВ")
    print("=" * 50)
    
    df = pd.read_sql_query("""
        SELECT file_name, bank_id, form_code, period, rows_loaded, loaded_at
        FROM ingestion_log
        ORDER BY loaded_at DESC
    """, conn)
    
    if df.empty:
        print("Нет записей импорта")
        return
    
    print(df.to_string(index=False))

def show_raw_data(conn, bank_id=None, form_code=None, period=None, limit=50):
    """Просмотр сырых данных"""
    print("=" * 50)
    print("СЫРЫЕ ДАННЫЕ")
    print("=" * 50)
    
    where_conditions = []
    params = []
    
    if bank_id:
        where_conditions.append("bank_id = ?")
        params.append(bank_id)
    if form_code:
        where_conditions.append("form_code = ?")
        params.append(form_code)
    if period:
        where_conditions.append("period = ?")
        params.append(period)
    
    where_clause = " WHERE " + " AND ".join(where_conditions) if where_conditions else ""
    
    query = f"""
        SELECT bank_id, form_code, period, item_code, value
        FROM raw_values
        {where_clause}
        ORDER BY bank_id, form_code, period, item_code
        LIMIT {limit}
    """
    
    df = pd.read_sql_query(query, conn, params=params)
    
    if df.empty:
        print("Нет данных для отображения")
        return
    
    print(f"Показано {len(df)} записей (лимит: {limit})")
    if len(df) == limit:
        print("Возможно, есть еще данные. Используйте фильтры или увеличьте лимит.")
    print()
    print(df.to_string(index=False))

def show_indicators(conn, bank_id=None, period=None):
    """Просмотр рассчитанных индикаторов"""
    print("=" * 50)
    print("РАССЧИТАННЫЕ ИНДИКАТОРЫ")
    print("=" * 50)
    
    where_conditions = []
    params = []
    
    if bank_id:
        where_conditions.append("bank_id = ?")
        params.append(bank_id)
    if period:
        where_conditions.append("period = ?")
        params.append(period)
    
    where_clause = " WHERE " + " AND ".join(where_conditions) if where_conditions else ""
    
    query = f"""
        SELECT bank_id, indicator_id, period, value
        FROM indicator_values
        {where_clause}
        ORDER BY bank_id, period, indicator_id
    """
    
    df = pd.read_sql_query(query, conn, params=params)
    
    if df.empty:
        print("Нет рассчитанных индикаторов")
        return
    
    print(df.to_string(index=False))

def main():
    parser = argparse.ArgumentParser(description="Просмотр данных финансовой системы")
    parser.add_argument("command", choices=[
        "summary", "banks", "forms", "periods", "log", "raw", "indicators"
    ], help="Команда для выполнения")
    
    # Фильтры
    parser.add_argument("--bank-id", help="ID банка для фильтрации")
    parser.add_argument("--form-code", help="Код формы для фильтрации")
    parser.add_argument("--period", help="Период для фильтрации")
    parser.add_argument("--limit", type=int, default=50, help="Лимит записей (по умолчанию: 50)")
    
    args = parser.parse_args()
    
    conn = get_conn()
    
    try:
        if args.command == "summary":
            show_summary(conn)
        elif args.command == "banks":
            show_banks(conn)
        elif args.command == "forms":
            show_forms(conn)
        elif args.command == "periods":
            show_periods(conn)
        elif args.command == "log":
            show_ingestion_log(conn)
        elif args.command == "raw":
            show_raw_data(conn, args.bank_id, args.form_code, args.period, args.limit)
        elif args.command == "indicators":
            show_indicators(conn, args.bank_id, args.period)
    finally:
        conn.close()

if __name__ == "__main__":
    main()