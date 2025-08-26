import os, sqlite3, yaml, re

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
CFG_PATH = os.path.join(BASE_DIR, "configs", "config.yaml")
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(DATA_DIR, "finstat.db")

def get_conn():
    os.makedirs(DATA_DIR, exist_ok=True)
    return sqlite3.connect(DB_PATH)

SCHEMA_SQL = r"""
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS banks (bank_id TEXT PRIMARY KEY, bank_name TEXT);
CREATE TABLE IF NOT EXISTS forms (form_code TEXT PRIMARY KEY, form_name TEXT);
CREATE TABLE IF NOT EXISTS raw_values (
  bank_id TEXT NOT NULL, form_code TEXT NOT NULL, period TEXT NOT NULL,
  item_code TEXT NOT NULL, value REAL,
  PRIMARY KEY (bank_id, form_code, period, item_code),
  FOREIGN KEY (bank_id) REFERENCES banks(bank_id) ON DELETE CASCADE
);
CREATE TABLE IF NOT EXISTS data_dictionary (
  form_code TEXT NOT NULL, item_code TEXT NOT NULL, std_key TEXT NOT NULL,
  description TEXT, PRIMARY KEY (form_code, item_code)
);
CREATE TABLE IF NOT EXISTS indicators (indicator_id TEXT PRIMARY KEY, name TEXT, formula TEXT, description TEXT);
CREATE TABLE IF NOT EXISTS indicator_values (
  bank_id TEXT NOT NULL, indicator_id TEXT NOT NULL, period TEXT NOT NULL, value REAL,
  PRIMARY KEY (bank_id, indicator_id, period)
);
CREATE TABLE IF NOT EXISTS algo_classifications (
  bank_id TEXT NOT NULL, period TEXT NOT NULL, status TEXT NOT NULL CHECK(status in ('Green','Yellow','Red')), details TEXT,
  PRIMARY KEY (bank_id, period)
);
CREATE TABLE IF NOT EXISTS llm_classifications (
  bank_id TEXT NOT NULL, period TEXT NOT NULL, status TEXT NOT NULL CHECK(status in ('Green','Yellow','Red')),
  reasoning TEXT, model TEXT, created_at TEXT DEFAULT (datetime('now')),
  PRIMARY KEY (bank_id, period)
);
CREATE TABLE IF NOT EXISTS ingestion_log (
  file_name TEXT PRIMARY KEY, bank_id TEXT, form_code TEXT, period TEXT, rows_loaded INTEGER,
  loaded_at TEXT DEFAULT (datetime('now'))
);
"""

def init_db(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.executescript(SCHEMA_SQL)
    conn.commit()

def load_config():
    with open(CFG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def parse_filename_generic(filename: str, pattern: str):
    m = re.match(pattern, filename, flags=re.IGNORECASE)
    if not m: return None
    gd = m.groupdict()
    raw_date = gd.get("date")
    period = None
    if raw_date:
        if len(raw_date) == 8 and raw_date.isdigit():
            period = f"{raw_date[0:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
        else:
            period = raw_date
    return gd.get("bank_id"), gd.get("form"), period
