import sqlite3, pandas as pd
from datetime import datetime
def _latest_period(conn):
    cur=conn.cursor(); cur.execute("SELECT MAX(period) FROM raw_values")
    r=cur.fetchone(); return r[0] if r and r[0] else None

def _resolve_period(conn: sqlite3.Connection, desired: str) -> str:
    """Возвращает ближайший доступный период ≤ desired. Если desired=='latest' — последний.
    Ожидается формат YYYY-MM-DD. Если точного совпадения нет — берём MAX(period) ≤ desired.
    """
    if desired == "latest" or not desired:
        return _latest_period(conn)
    cur = conn.cursor()
    # Прямое совпадение
    row = cur.execute("SELECT 1 FROM raw_values WHERE period=? LIMIT 1", (desired,)).fetchone()
    if row:
        return desired
    # Ближайший ≤ desired
    row = cur.execute("SELECT MAX(period) FROM raw_values WHERE period<=?", (desired,)).fetchone()
    if row and row[0]:
        return row[0]
    # Фолбэк: если нет периодов ≤ desired, вернуть самый ранний доступный
    row = cur.execute("SELECT MIN(period) FROM raw_values").fetchone()
    return row[0] if row and row[0] else None
def make_report(conn: sqlite3.Connection, period="latest", outfile="report.xlsx"):
    # Разрешаем произвольную дату: выбираем ближайший доступный период ≤ указанной дате
    period = _resolve_period(conn, period or "latest")
    if not period: print("Нет данных для отчета."); return
    # Генерируем имя файла с датой/временем, если используется имя по умолчанию
    if outfile == "report.xlsx" or not outfile:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_period = str(period).replace("-", "")
        outfile = f"reports/report_{safe_period}_{ts}.xlsx"
    # Создаём директорию для отчётов, если путь включает подкаталоги
    try:
        import os
        d = os.path.dirname(outfile)
        if d and not os.path.exists(d):
            os.makedirs(d, exist_ok=True)
    except Exception:
        pass
    banks=pd.read_sql_query("SELECT bank_id, COALESCE(bank_name, bank_id) as bank_name FROM banks", conn)
    ind=pd.read_sql_query("SELECT bank_id, indicator_id, value FROM indicator_values WHERE period=?", conn, params=(period,))
    if ind.empty: print("Нет индикаторов на указанный период."); return
    ind_w=ind.pivot_table(index="bank_id", columns="indicator_id", values="value", aggfunc="first").reset_index()
    algo=pd.read_sql_query("SELECT bank_id, status, details FROM algo_classifications WHERE period=?", conn, params=(period,))
    llm=pd.read_sql_query("SELECT bank_id, status, reasoning, model FROM llm_classifications WHERE period=?", conn, params=(period,))
    summary=(banks.merge(ind_w,on="bank_id",how="right")
                  .merge(algo.rename(columns={"status":"algo_status","details":"algo_details"}),on="bank_id",how="left")
                  .merge(llm.rename(columns={"status":"llm_status","reasoning":"llm_reasoning","model":"llm_model"}),on="bank_id",how="left"))
    with pd.ExcelWriter(outfile, engine="xlsxwriter") as w:
        summary.to_excel(w, sheet_name="Summary", index=False)
        ind.to_excel(w, sheet_name="Indicators_long", index=False)
        raw=pd.read_sql_query("SELECT * FROM raw_values WHERE period=?", conn, params=(period,))
        raw.to_excel(w, sheet_name="Raw_values", index=False)
        # Лист LLM: если есть классификации LLM за период, добавим и извлечём рекомендацию
        llm_df=pd.read_sql_query("SELECT bank_id, status, reasoning, model FROM llm_classifications WHERE period=?", conn, params=(period,))
        if not llm_df.empty:
            def _extract_reco(x):
                try:
                    import json
                    j=json.loads(x)
                    return j.get('recommendation')
                except Exception:
                    return None
            llm_df['recommendation']=llm_df['reasoning'].apply(_extract_reco)
            def _extract_summary(x):
                try:
                    import json
                    j=json.loads(x)
                    return j.get('summary_ru')
                except Exception:
                    return None
            llm_df['summary_ru']=llm_df['reasoning'].apply(_extract_summary)
            llm_df.to_excel(w, sheet_name="LLM", index=False)
    print(f"Готов XLS за период {period}: {outfile}")
