import os
import csv
import ast
import math
import sqlite3
from typing import Optional, Dict, Any
import yaml
from datetime import date


BASE_DIR = os.path.dirname(os.path.dirname(__file__))
CFG_DIR = os.path.join(BASE_DIR, "configs")


def _load_indicators_config() -> Dict[str, str]:
    path = os.path.join(CFG_DIR, "indicators.yaml")
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    # Ожидается: { indicator_id: "FORMULA" }
    return {str(k): str(v) for k, v in data.items()}


def _load_data_dictionary() -> Dict[tuple, str]:
    """Загружает словарь соответствий (form_code, item_code) -> std_key.
    Пропускает пустые строки и комментарии (#...).
    """
    path = os.path.join(CFG_DIR, "data_dictionary.csv")
    mapping: Dict[tuple, str] = {}
    if not os.path.exists(path):
        return mapping
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        for row in reader:
            if not row or not row[0] or row[0].lstrip().startswith("#"):
                continue
            # Ожидаемые колонки: form_code, item_code, std_key, description
            if len(row) < 3:
                continue
            form_code = row[0].strip()
            item_code = row[1].strip()
            std_key = row[2].strip()
            if form_code and item_code and std_key:
                mapping[(form_code, item_code)] = std_key
    return mapping


class _SafeEvaluator(ast.NodeVisitor):
    """Безопасная оценка арифметических выражений с переменными.
    Поддержка: +, -, *, /, унарные +/-, скобки, имена переменных (A-Z,_ , цифры внутри).
    """

    def __init__(self, variables: Dict[str, float]):
        self.variables = variables

    def visit(self, node):
        if isinstance(node, ast.Expression):
            return self.visit(node.body)
        if isinstance(node, ast.BinOp):
            left = self.visit(node.left)
            right = self.visit(node.right)
            if isinstance(node.op, ast.Add):
                return left + right
            if isinstance(node.op, ast.Sub):
                return left - right
            if isinstance(node.op, ast.Mult):
                return left * right
            if isinstance(node.op, ast.Div):
                if right == 0:
                    raise ZeroDivisionError
                return left / right
            raise ValueError("Недопустимая операция")
        if isinstance(node, ast.UnaryOp):
            operand = self.visit(node.operand)
            if isinstance(node.op, ast.UAdd):
                return +operand
            if isinstance(node.op, ast.USub):
                return -operand
            raise ValueError("Недопустимая унарная операция")
        if isinstance(node, ast.Num):  # Py<3.8
            return float(node.n)
        if isinstance(node, ast.Constant):
            if isinstance(node.value, (int, float)):
                return float(node.value)
            raise ValueError("Недопустимое константное значение")
        if isinstance(node, ast.Name):
            return float(self.variables.get(node.id, 0.0))
        if isinstance(node, ast.Call):
            raise ValueError("Вызовы функций запрещены")
        raise ValueError("Недопустимое выражение")


def _eval_formula(expr: str, variables: Dict[str, float]) -> Optional[float]:
    try:
        tree = ast.parse(expr, mode="eval")
        val = _SafeEvaluator(variables).visit(tree)
        if isinstance(val, (int, float)) and (not (isinstance(val, float) and (math.isnan(val) or math.isinf(val)))):
            return float(val)
        return None
    except ZeroDivisionError:
        return None
    except Exception:
        return None


def calculate_indicators(conn: sqlite3.Connection) -> None:
    """Читает сырые данные и рассчитывает индикаторы согласно configs/indicators.yaml.
    Результат сохраняет в таблицу indicator_values (bank_id, indicator_id, period, value).
    """
    indicators = _load_indicators_config()
    mapping = _load_data_dictionary()

    cur = conn.cursor()

    # Собираем все уникальные пары (банк, период)
    pairs = cur.execute("SELECT DISTINCT bank_id, period FROM raw_values").fetchall()
    if not pairs:
        print("Нет сырых данных для расчёта индикаторов.")
        return

    total_written = 0
    for bank_id, period in pairs:
        # Получаем все строки для банка/периода
        rows = cur.execute(
            "SELECT form_code, item_code, value FROM raw_values WHERE bank_id=? AND period=?",
            (bank_id, period),
        ).fetchall()

        # Агрегируем значения по std_key
        std_values: Dict[str, float] = {}
        for form_code, item_code, value in rows:
            key = mapping.get((str(form_code), str(item_code)))
            if not key:
                continue
            std_values[key] = std_values.get(key, 0.0) + (float(value) if value is not None else 0.0)

        # Вычисляем каждую формулу
        for ind_id, expr in indicators.items():
            val = _eval_formula(expr, std_values)
            cur.execute(
                "INSERT OR REPLACE INTO indicator_values(bank_id, indicator_id, period, value) VALUES(?,?,?,?)",
                (bank_id, ind_id, period, val),
            )
            total_written += 1

    conn.commit()
    print(f"Рассчитано и сохранено значений индикаторов: {total_written}")


# Оставляем функцию QN17 для совместимости, если где-то используется напрямую
def calculate_qn17(values: Dict[str, float]) -> Optional[float]:
    denominator = values.get("R312P", 0) + values.get("R31_1P", 0) + values.get("R31_2P", 0)
    if denominator > 0:
        numerator = (values.get("R319A", 0) + values.get("R32_1A", 0) + values.get("R32_2A", 0)) - \
                    (values.get("R32_1P", 0) + values.get("R32_2P", 0))
        return (numerator / denominator) * 100
    return None


def _shift_months(period_str: str, months: int) -> Optional[str]:
    try:
        y, m, d = [int(x) for x in period_str.split("-")]
        # нормализуем на 1 число месяца
        y0, m0 = y, m
        total = y0 * 12 + (m0 - 1) - months
        if total < 0:
            return None
        y1 = total // 12
        m1 = total % 12 + 1
        return f"{y1:04d}-{m1:02d}-01"
    except Exception:
        return None


def calculate_indicator_changes(conn: sqlite3.Connection) -> None:
    """Рассчитывает % изменение за 1 и 6 месяцев для заданного набора индикаторов.
    Сохраняет как отдельные индикаторы: {BASE}_PCT_M1 и {BASE}_PCT_M6.

    Для 6 месяцев применяется гибкая логика, если нет ровно t-6 месяцев:
    - выбираем самую раннюю из доступных дат за последние 6 месяцев (но не старше 6 мес.).
    - если подходящей даты нет, значение не рассчитывается.
    """
    target_indicators = [
        "QN9", "O1", "QN15", "QN18", "QN19", "QN11", "O2", "A1", "QN13"
    ]

    cur = conn.cursor()
    # Считываем все значения интересующих индикаторов
    placeholders = ",".join(["?"] * len(target_indicators))
    rows = cur.execute(
        f"SELECT bank_id, indicator_id, period, value FROM indicator_values WHERE indicator_id IN ({placeholders})",
        target_indicators,
    ).fetchall()
    if not rows:
        return

    # Группируем: bank -> indicator -> {period: value}
    data: Dict[str, Dict[str, Dict[str, Optional[float]]]] = {}
    for bank_id, indicator_id, period, value in rows:
        data.setdefault(bank_id, {}).setdefault(indicator_id, {})[period] = value

    def months_between(prev: str, cur: str) -> Optional[int]:
        try:
            y0, m0, _ = [int(x) for x in prev.split("-")]
            y1, m1, _ = [int(x) for x in cur.split("-")]
            return (y1 * 12 + (m1 - 1)) - (y0 * 12 + (m0 - 1))
        except Exception:
            return None

    def pick_prev_within(per_map: Dict[str, Optional[float]], p: str, max_months: int) -> Optional[str]:
        """Вернуть самую раннюю доступную дату в интервале (p-max_months, p),
        т.е. максимально отстоящую назад, но не старше max_months.
        """
        candidates = []
        for cand in per_map.keys():
            if cand >= p:
                continue
            diff = months_between(cand, p)
            if diff is None:
                continue
            if 1 <= diff <= max_months:
                candidates.append((diff, cand))
        if not candidates:
            return None
        # Самая ранняя в пределах окна: максимальная разница по месяцам
        candidates.sort(reverse=True)
        return candidates[0][1]

    written = 0
    for bank_id, ind_map in data.items():
        for ind_id, per_map in ind_map.items():
            periods = sorted(per_map.keys())
            for p in periods:
                curr = per_map.get(p)
                if curr is None:
                    continue
                # 1 месяц назад
                p_m1 = _shift_months(p, 1)
                if p_m1 and p_m1 in per_map:
                    prev = per_map.get(p_m1)
                    if prev not in (None, 0):
                        chg = (curr - prev) / abs(prev) * 100.0
                        cur.execute(
                            "INSERT OR REPLACE INTO indicator_values(bank_id, indicator_id, period, value) VALUES(?,?,?,?)",
                            (bank_id, f"{ind_id}_PCT_M1", p, chg),
                        ); written += 1
                # 6 месяцев назад (гибкое окно)
                p_m6_exact = _shift_months(p, 6)
                prev_key = None
                if p_m6_exact and p_m6_exact in per_map:
                    prev_key = p_m6_exact
                else:
                    prev_key = pick_prev_within(per_map, p, 6)
                if prev_key is not None:
                    prev6 = per_map.get(prev_key)
                    if prev6 not in (None, 0):
                        chg6 = (curr - prev6) / abs(prev6) * 100.0
                        cur.execute(
                            "INSERT OR REPLACE INTO indicator_values(bank_id, indicator_id, period, value) VALUES(?,?,?,?)",
                            (bank_id, f"{ind_id}_PCT_M6", p, chg6),
                        ); written += 1

    conn.commit()
    if written:
        print(f"Рассчитаны изменения индикаторов (%%): {written}")
