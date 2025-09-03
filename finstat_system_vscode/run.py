import argparse
import os
from dotenv import load_dotenv
from src.db import get_conn, init_db
from src.import_dbf import import_all_dbf
from src.indicators import calculate_indicators, calculate_indicator_changes
from src.rules_engine import classify_all
from src.llm_module import llm_analyze_all
from src.report_xls import make_report
from src.data_viewer import main as data_viewer_main

def main():
    # Подхватываем переменные окружения из .env (если есть)
    try:
        # 1) .env из текущего каталога запуска (корень проекта)
        load_dotenv()
        # 2) .env рядом с run.py (finstat_system_vscode/.env)
        local_env_path = os.path.join(os.path.dirname(__file__), ".env")
        if os.path.exists(local_env_path):
            load_dotenv(local_env_path, override=False)
    except Exception:
        pass
    parser = argparse.ArgumentParser(description="Финансовая система анализа")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init-db", help="Инициализировать БД")
    p_import = sub.add_parser("import", help="Импорт DBF из input/")
    p_import.add_argument("--all", action="store_true", help="Импортировать все новые файлы")
    sub.add_parser("calc-indicators", help="Рассчитать индикаторы")
    sub.add_parser("classify", help="Алгоритмическая классификация")
    p_llm = sub.add_parser("llm-analyze", help="LLM-анализ (кэширование промптов)")
    p_llm.add_argument("--period", default="latest", help="Дата YYYY-MM-DD или 'latest' (берется ближайший доступный период ≤ даты)")
    p_report = sub.add_parser("report", help="Сформировать XLS отчет")
    p_report.add_argument("--period", default="latest", help="Дата YYYY-MM-DD или 'latest'")
    p_report.add_argument("--outfile", default="report.xlsx", help="Имя выходного файла")
    p_view = sub.add_parser("view", help="Просмотр загруженных данных")
    p_view.add_argument("command", choices=["summary", "banks", "forms", "periods", "log", "raw", "indicators"], help="Команда просмотра")
    p_view.add_argument("--bank-id", help="ID банка для фильтрации")
    p_view.add_argument("--form-code", help="Код формы для фильтрации")
    p_view.add_argument("--period", help="Период для фильтрации")
    p_view.add_argument("--limit", type=int, default=50, help="Лимит записей")
    args = parser.parse_args()

    if args.cmd == "init-db":
        conn = get_conn(); init_db(conn); print("БД инициализирована.")
    elif args.cmd == "import":
        conn = get_conn(); import_all_dbf(conn)
    elif args.cmd == "calc-indicators":
        conn = get_conn(); calculate_indicators(conn); calculate_indicator_changes(conn)
    elif args.cmd == "classify":
        conn = get_conn(); classify_all(conn)
    elif args.cmd == "llm-analyze":
        conn = get_conn(); llm_analyze_all(conn, period=args.period)
    elif args.cmd == "report":
        conn = get_conn(); make_report(conn, period=args.period, outfile=args.outfile); print(f"Отчет сохранен: {args.outfile}")
    elif args.cmd == "view":
        import sys
        sys.argv = ["data_viewer", args.command]
        if getattr(args, "bank_id", None):
            sys.argv.extend(["--bank-id", args.bank_id])
        if getattr(args, "form_code", None):
            sys.argv.extend(["--form-code", args.form_code])
        if getattr(args, "period", None):
            sys.argv.extend(["--period", args.period])
        if getattr(args, "limit", 50) != 50:
            sys.argv.extend(["--limit", str(args.limit)])
        data_viewer_main()
    else:
        parser.print_help()

if __name__ == "__main__":
    main()
