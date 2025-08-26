import os, sqlite3, yaml, re, pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
CFG_DIR = os.path.join(BASE_DIR, "configs")

def _load_yaml(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def _parse_condition(cond: str):
    cond = cond.strip().lower()
    m = re.match(r"^(<=|>=|<|>)\s*([+-]?\d+(\.\d+)?)$", cond)
    if m: return ("cmp", m.group(1), float(m.group(2)))
    m = re.match(r"^between\s+([+-]?\d+(\.\d+)?),\s*([+-]?\d+(\.\d+)?)$", cond)
    if m:
        a=float(m.group(1)); b=float(m.group(3))
        return ("between", min(a,b), max(a,b))
    raise ValueError(cond)

def _check(value, rule):
    if value is None: return False
    if rule[0]=="cmp":
        op,thr=rule[1],rule[2]
        return (value<thr) if op=="<" else (value<=thr) if op=="<=" else (value>thr) if op==">" else (value>=thr)
    a,b=rule[1],rule[2]
    return a<=value<=b

def classify_all(conn: sqlite3.Connection):
    rules = _load_yaml(os.path.join(CFG_DIR, "rules.yaml")) or {}
    df = pd.read_sql_query("SELECT bank_id, indicator_id, period, value FROM indicator_values", conn)
    if df.empty:
        print("Нет индикаторов для классификации."); return

    # Разбор только наборов (AND внутри, OR между наборами)
    yellow_sets = None
    red_sets = None
    for ind, rs in rules.items():
        if ind == 'yellow_sets' and isinstance(rs, list):
            ys = []
            for s in rs:
                if isinstance(s, dict):
                    ys.append({k: _parse_condition(str(v)) for k, v in s.items()})
            yellow_sets = ys
            continue
        if ind == 'red_sets' and isinstance(rs, list):
            rs_list = []
            for s in rs:
                if isinstance(s, dict):
                    rs_list.append({k: _parse_condition(str(v)) for k, v in s.items()})
            red_sets = rs_list
            continue
        # игнорируем одиночные пороги и любые другие ключи

    cur = conn.cursor()
    results = {}; details = {}
    for (bank_id, period), grp in df.groupby(["bank_id","period"]):
        st="Green"; fired=[]; vals={r["indicator_id"]:r["value"] for _,r in grp.iterrows()}
        # 1) Сначала проверяем Red-наборы (OR между наборами, AND внутри набора)
        if st == "Green" and red_sets:
            any_red_ok = False
            for s in red_sets:
                ok = True
                for ind_key, cond in s.items():
                    v = vals.get(ind_key)
                    if v is None or not _check(v, cond):
                        ok = False; break
                if ok:
                    any_red_ok = True; break
            if any_red_ok:
                st = "Red"; fired.append("Red SET: выполнен один из наборов")

        # 2) Наборы для Yellow (OR между наборами, AND внутри набора)
        if st == "Green" and yellow_sets:
            any_set_ok = False
            for s in yellow_sets:
                ok = True
                for ind_key, cond in s.items():
                    v = vals.get(ind_key)
                    if v is None or not _check(v, cond):
                        ok = False; break
                if ok:
                    any_set_ok = True; break
            if any_set_ok:
                st = "Yellow"; fired.append("Yellow SET: выполнен один из наборов")
        # 3) Никаких одиночных правил — если ничего не сработало, остаётся Green
        results[(bank_id,period)]=st; details[(bank_id,period)]="; ".join(fired)
    for (bank_id,period),st in results.items():
        cur.execute("INSERT OR REPLACE INTO algo_classifications(bank_id,period,status,details) VALUES(?,?,?,?)",
                    (bank_id,period,st,details[(bank_id,period)]))
    conn.commit(); print(f"Классификация завершена для {len(results)} банк×период.")
