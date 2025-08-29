import os
import json
import sqlite3
import hashlib
import yaml
from typing import Dict, List, Optional, Tuple
import pandas as pd
from openai import OpenAI
try:
    from langchain_gigachat import GigaChat
except Exception:  # пакет может быть не установлен у всех
    GigaChat = None  # type: ignore
from tqdm import tqdm
from .db import load_config


def _latest_period(conn: sqlite3.Connection) -> str:
    r = conn.cursor().execute("SELECT MAX(period) FROM raw_values").fetchone()
    return r[0] if r and r[0] else None


METRICS_BASE = [
    "A1", "QN9", "O1", "O2", "QN11", "QN15", "QN18", "QN19", "QN13"
]
METRICS_PCT = [f"{m}_PCT_M1" for m in METRICS_BASE] + [f"{m}_PCT_M6" for m in METRICS_BASE]
METRICS_SINGLE = ["QN17", "QN16"]


def _collect_series(conn: sqlite3.Connection, bank_id: str, periods: List[str]) -> Dict[str, Dict]:
    cur = conn.cursor()
    res: Dict[str, Dict] = {}
    for m in METRICS_BASE:
        rows = cur.execute(
            "SELECT period, value FROM indicator_values WHERE bank_id=? AND indicator_id=? AND period IN (%s)"
            % (",".join(["?"] * len(periods))),
            (bank_id, m, *periods),
        ).fetchall()
        res[m] = {"series": sorted([{ "p": p, "v": v } for p, v in rows], key=lambda x: x["p"]) }
    for m in METRICS_PCT + METRICS_SINGLE:
        rows = cur.execute(
            "SELECT period, value FROM indicator_values WHERE bank_id=? AND indicator_id=? AND period IN (%s)"
            % (",".join(["?"] * len(periods))),
            (bank_id, m, *periods),
        ).fetchall()
        latest = None
        if rows:
            rows.sort(key=lambda x: x[0])
            latest = rows[-1][1]
        res[m] = {"latest": latest}
    return res


def _collect_peer_percentiles(conn: sqlite3.Connection, period: str) -> Dict[str, float]:
    df = pd.read_sql_query(
        "SELECT indicator_id, value FROM indicator_values WHERE period=? AND indicator_id IN (%s)"
        % (",".join(["?"] * len(METRICS_BASE + ["QN11"]))),
        conn,
        params=(period, *METRICS_BASE, "QN11"),
    )
    out = {}
    if df.empty:
        return out
    for ind, g in df.groupby("indicator_id"):
        s = g["value"].dropna()
        if not s.empty:
            out[f"{ind}_pctl"] = float(s.rank(pct=True).iloc[-1])  # нестрого: следующий код перепишем при необходимости
    return out


def _indicator_metadata() -> Dict[str, Dict[str, str]]:
    return {
        "A1": {"name": "Денежные средства/ликвидные активы", "units": "валюта", "direction": "больше лучше", "desc": "Касса, счета в ЦБ и высоколиквидные активы."},
        "QN9": {"name": "Кредиты клиентам (net)", "units": "валюта", "direction": "зависит", "desc": "Совокупный кредитный портфель за вычетом резервов."},
        "O1": {"name": "Средства клиентов", "units": "валюта", "direction": "больше лучше", "desc": "Фондирование от клиентов; устойчивость пассивов."},
        "O2": {"name": "Средства банков", "units": "валюта", "direction": "меньше лучше", "desc": "Зависимость от межбанковского фондирования."},
        "QN11": {"name": "Проблемные кредиты (NPL)", "units": "валюта/доля", "direction": "меньше лучше", "desc": "Рост указывает на ухудшение качества активов."},
        "QN15": {"name": "Совокупный капитал", "units": "валюта", "direction": "больше лучше", "desc": "Капитальная база банка."},
        "QN18": {"name": "H1.0 — достаточность совокупного капитала", "units": "%", "direction": "больше лучше", "desc": "Регуляторный норматив достаточности капитала."},
        "QN19": {"name": "H1.2 — достаточность капитала 1-го уровня", "units": "%", "direction": "больше лучше", "desc": "Ключевой показатель качества капитала."},
        "QN13": {"name": "Совокупные активы", "units": "валюта", "direction": "зависит", "desc": "Размер баланса, база для масштабирования."},
        "QN17": {"name": "Межбанковский коэффициент", "units": "%", "direction": "меньше лучше", "desc": "Баланс межбанковских позиций; рост — повышенная зависимость от МБК."},
        "QN16": {"name": "ROAA", "units": "%", "direction": "больше лучше", "desc": "Рентабельность активов."}
    }


def _load_indicator_formulas() -> Dict[str, str]:
    base_dir = os.path.dirname(os.path.dirname(__file__))
    cfg_dir = os.path.join(base_dir, "configs")
    path = os.path.join(cfg_dir, "indicators.yaml")
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return {str(k): str(v) for k, v in data.items()}
    except Exception:
        return {}


def _load_indicator_meta() -> Dict[str, Dict]:
    base_dir = os.path.dirname(os.path.dirname(__file__))
    cfg_dir = os.path.join(base_dir, "configs")
    path = os.path.join(cfg_dir, "indicator_meta.yaml")
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        # ожидаем словарь id->поля
        normalized = {}
        for k, v in data.items():
            if isinstance(v, dict):
                normalized[str(k)] = v
        return normalized
    except Exception:
        return {}


def _direction_to_interpretation(direction: Optional[str]) -> Optional[str]:
    if not direction:
        return None
    d = direction.strip().lower()
    if "больше" in d:
        return "increase_good"
    if "меньше" in d:
        return "increase_bad"
    if "зависит" in d:
        return None
    return None


def _group_for_indicator(ind_id: str) -> Tuple[str, bool]:
    gmap = {
        "A1": ("Liquidity", True),
        "O1": ("Funding", True),
        "O2": ("Funding", True),
        "QN11": ("AssetQuality", True),
        "QN15": ("Capital", True),
        "QN18": ("Capital", True),
        "QN19": ("Capital", True),
        "QN13": ("Size", False),
        "QN17": ("Interbank", True),
        "QN16": ("Profitability", False),
    }
    return gmap.get(ind_id, ("Other", False))


def _read_text_file(path: str) -> Optional[str]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except Exception:
        return None


def _build_prompt(
    data_json: str,
    meta_json: Optional[str],
    system_prompt_text: Optional[str],
    params_json: Optional[str],
    user_prompt_text: Optional[str] = None,
) -> List[Dict[str, str]]:
    system = system_prompt_text or (
        "Ты — беспристрастный риск-аналитик межбанковского кредитования. Твоя цель — сбалансированно оценить вероятность \n"
        "возникновения различных финансовых рисков у банка на горизонте 1–3 месяцев. Используй ТОЛЬКО предоставленные данные. \n"
        "Не делай выводов о высоком риске без достаточных подтверждений несколькими независимыми показателями и устойчивой динамики. \n"
        "Если данных недостаточно — выбирай Green. Верни ЧИСТЫЙ JSON: {status, confidence, reasons[], watchlist[], recommendation, metrics_snapshot, summary_ru}. \n"
        "Определения параметров запроса (JSON):\n" + (params_json or "{}") + "\n"
    )
    # Добавим определения показателей (только название/описание), если передано
    if meta_json:
        system += "Определения показателей (JSON):\n" + meta_json + "\n"
    default_user = (
        "Данные для анализа в JSON ниже. Определи статус с учётом уровней и трендов (PCT_M1, PCT_M6). \n"
        "Интерпретация статусов: Green — нет значимых признаков риска либо данных недостаточно; \n"
        "Yellow — умеренные/локальные риски; Red — устойчивые существенные ухудшения минимум в 2–3 направлениях с подтверждённой динамикой. \n"
        "Добавь summary_ru — 2–4 предложения. Выведи строгий JSON без лишнего текста.\n\n"
    )
    user = (user_prompt_text or default_user) + data_json
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": user},
    ]


def _params_doc(full_meta: Dict[str, Dict]) -> Dict[str, Dict]:
    indicators = []
    for ind_id, meta in full_meta.items():
        indicators.append({
            "id": ind_id,
            "name": meta.get("name"),
            "description": meta.get("desc") or meta.get("description"),
            "group": meta.get("group"),
            "critical": meta.get("critical"),
            "interpretation": meta.get("interpretation"),
            "formula": meta.get("formula"),
            "thresholds": meta.get("thresholds", {}),
            "benchmarks": meta.get("benchmarks", {}),
        })
    return {
        "payload_schema": {
            "bank": {"id": "строка ИД банка", "period_latest": "YYYY-MM-01"},
            "timeseries_months": "целое число месяцев в срезе",
            "metrics": {
                "<INDICATOR_ID>": {
                    "series": "для базовых: список {p: период, v: значение}",
                    "latest": "для *_PCT_* и одиночных: последнее значение"
                }
            },
            "algo": {},
            "peers": {},
            "data_quality": {}
        },
        "indicators": indicators
    }


def _make_cache_key(model: str, messages: List[Dict[str, str]], payload: Dict) -> str:
    blob = json.dumps({"model": model, "messages": messages, "payload": payload}, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _cache_paths(logs_dir: str, bank_id: str, cache_key: str) -> Tuple[str, str]:
    req = os.path.join(logs_dir, f"{bank_id}_request_{cache_key}.json")
    resp = os.path.join(logs_dir, f"{bank_id}_response_{cache_key}.json")
    return req, resp


def _preflight_openai(client: OpenAI, model: str, timeout_sec: int) -> Tuple[bool, str]:
    # Проверка наличия ключа
    if not os.getenv("OPENAI_API_KEY"):
        return False, "ENV OPENAI_API_KEY is not set"
    # Минимальный вызов Responses API, без неподдерживаемых аргументов
    try:
        probe = client.responses.create(
            model=model,
            input="Верни JSON: {\\\"ok\\\": true}",
            reasoning={"effort": "low"},
            timeout=timeout_sec if timeout_sec and timeout_sec > 0 else 30,
        )
        text = getattr(probe, "output_text", None)
        if not text:
            return False, "No output_text from preflight"
        return True, "ok"
    except Exception as e:
        return False, f"preflight failed: {e}"


def _preflight_gigachat(gc: "GigaChat", timeout_sec: int) -> Tuple[bool, str]:
    # Проверка ENV токена и минимальный вызов
    if not os.getenv("GIGACHAT_ACCESS_TOKEN") and not os.getenv("GIGACHAT_CREDENTIALS"):
        return False, "ENV GIGACHAT_ACCESS_TOKEN (или GIGACHAT_CREDENTIALS) не задан"
    try:
        out = gc.invoke("ping")
        return True, "ok" if out else (False, "no output")[1]
    except Exception as e:
        return False, f"preflight failed: {e}"


def _extract_first_json_object(text: str) -> Optional[str]:
    if not text:
        return None
    # Попытка найти первый валидный JSON-объект по балансировке скобок
    start = text.find('{')
    while start != -1:
        depth = 0
        for i in range(start, len(text)):
            ch = text[i]
            if ch == '{':
                depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    candidate = text[start:i+1]
                    try:
                        json.loads(candidate)
                        return candidate
                    except Exception:
                        break
        start = text.find('{', start + 1)
    return None


def init_provider(provider: str, llm_cfg: Dict, model: str, timeout_sec: int):
    if provider == "openai":
        client = OpenAI()
        return client, None, model
    if provider == "gigachat":
        if GigaChat is None:
            raise RuntimeError("GigaChat не установлен: добавьте langchain-gigachat в requirements.txt")
        gc_cfg = llm_cfg.get("gigachat", {})
        gc_client = GigaChat(
            access_token=os.getenv("GIGACHAT_ACCESS_TOKEN"),
            scope=str(gc_cfg.get("scope", "GIGACHAT_API_PERS")),
            model=str(gc_cfg.get("model", "GigaChat-2-Max")),
            base_url=str(gc_cfg.get("base_url", "https://gigachat.devices.sberbank.ru/api/v1")),
            profanity_check=bool(gc_cfg.get("profanity_check", False)),
            timeout=int(gc_cfg.get("timeout_sec", timeout_sec)),
            top_p=float(gc_cfg.get("top_p", 0.3)),
            temperature=float(gc_cfg.get("temperature", 0.1)),
            max_tokens=int(gc_cfg.get("max_tokens", 2000)),
            verify_ssl_certs=bool(gc_cfg.get("verify_ssl_certs", False)),
        )
        return None, gc_client, gc_client.model
    raise ValueError(f"Неизвестный LLM провайдер: {provider}")


def preflight(provider: str, client: Optional[OpenAI], gc_client: Optional["GigaChat"], model: str, timeout_sec: int) -> Tuple[bool, str]:
    if provider == "openai" and client is not None:
        return _preflight_openai(client, model, min(timeout_sec, 30) if timeout_sec else 30)
    if provider == "gigachat" and gc_client is not None:
        return _preflight_gigachat(gc_client, min(timeout_sec, 30) if timeout_sec else 30)
    return False, "provider/client not initialized"


def build_indicator_definitions(full_meta: Dict[str, Dict]) -> Tuple[List[Dict], Optional[str]]:
    try:
        indicator_defs: List[Dict] = []
        base_name_map: Dict[str, str] = {}
        for ind_id, meta in full_meta.items():
            name = meta.get("name") or ""
            desc = meta.get("desc") or meta.get("description") or ""
            base_name_map[ind_id] = name
            indicator_defs.append({"id": ind_id, "name": name, "description": desc})
        for base_id in METRICS_BASE:
            base_nm = base_name_map.get(base_id, "")
            indicator_defs.append({
                "id": f"{base_id}_PCT_M1",
                "name": (f"Изменение {base_nm} за 1 месяц, %" if base_nm else ""),
                "description": (f"Процентное изменение значения {base_id} относительно уровня месяц назад." if base_nm else ""),
            })
            indicator_defs.append({
                "id": f"{base_id}_PCT_M6",
                "name": (f"Изменение {base_nm} за 6 месяцев, %" if base_nm else ""),
                "description": (f"Процентное изменение значения {base_id} относительно уровня шесть месяцев назад (гибкое окно)." if base_nm else ""),
            })
        meta_json = json.dumps({"indicators": indicator_defs}, ensure_ascii=False)
        return indicator_defs, meta_json
    except Exception:
        return [], None


def build_params_schema(full_meta: Dict[str, Dict]) -> str:
    return json.dumps({"payload_schema": _params_doc(full_meta)["payload_schema"]}, ensure_ascii=False)


def build_messages(data_json: str, meta_json: Optional[str], system_prompt_text: Optional[str], params_json: str, user_prompt_text: Optional[str]) -> List[Dict[str, str]]:
    return _build_prompt(data_json, meta_json, system_prompt_text, params_json, user_prompt_text)


def make_cache_key(model: str, messages: List[Dict[str, str]], payload: Dict) -> str:
    return _make_cache_key(model, messages, payload)


def _save_json(path: str, obj: dict):
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def cache_get(logs_dir: str, bank_id: str, cache_key: str) -> Tuple[str, str, Optional[Dict]]:
    req_path, resp_path = _cache_paths(logs_dir, bank_id, cache_key)
    if os.path.exists(resp_path):
        try:
            with open(resp_path, "r", encoding="utf-8") as f:
                cached = json.load(f)
            parsed = cached.get("response") or cached
            return req_path, resp_path, parsed
        except Exception:
            return req_path, resp_path, None
    return req_path, resp_path, None


def cache_put(resp_path: str, bank_id: str, logs_dir: str, parsed: Dict):
    try:
        _save_json(resp_path, {"response": parsed})
        with open(os.path.join(logs_dir, f"{bank_id}_response.json"), "w", encoding="utf-8") as f2:
            json.dump({"response": parsed}, f2, ensure_ascii=False, indent=2)
    except Exception:
        pass


def log_request(req_path: str, info: Dict):
    _save_json(req_path, info)


def call_openai(client: OpenAI, model: str, combined: str, reasoning_effort: str, timeout_sec: int) -> str:
    resp = client.responses.create(
        model=model,
        input=combined,
        reasoning={"effort": reasoning_effort},
        timeout=timeout_sec,
    )
    return resp.output_text if hasattr(resp, "output_text") else (getattr(resp, "content", None) or "")


def call_gigachat(gc_client: "GigaChat", combined: str) -> str:
    gc_resp = gc_client.invoke(combined)
    content = getattr(gc_resp, "content", None) or str(gc_resp)
    extracted = _extract_first_json_object(content)
    return extracted or content


def extract_json(text: str) -> Optional[str]:
    return _extract_first_json_object(text)


def send_with_retries(send_once, max_retries: int, backoff_seconds: int):
    attempt = 0
    last_err = None
    import time as _t
    while attempt <= max_retries:
        try:
            return send_once()
        except Exception as e:
            last_err = e
            if attempt == max_retries:
                raise last_err
            _t.sleep(backoff_seconds * (2 ** attempt))
            attempt += 1


def save_result(cur: sqlite3.Cursor, logs_dir: str, bank_id: str, model: str, period: str, resp_path: str, parsed: Dict):
    cache_put(resp_path, bank_id, logs_dir, parsed)
    status = str(parsed.get("status", "Green"))
    reasoning = json.dumps(parsed, ensure_ascii=False)
    cur.execute(
        "INSERT OR REPLACE INTO llm_classifications(bank_id,period,status,reasoning,model) VALUES(?,?,?,?,?)",
        (bank_id, period, status, reasoning, model),
    )


def select_banks(cur: sqlite3.Cursor, latest: str, only_errors: bool, bank_limit: int, max_banks: int) -> List[str]:
    banks = [r[0] for r in cur.execute("SELECT bank_id FROM banks").fetchall()]
    if only_errors:
        bad = [r[0] for r in cur.execute(
            "SELECT b.bank_id FROM banks b LEFT JOIN llm_classifications l ON (l.bank_id=b.bank_id AND l.period=?)\n"
            "WHERE l.bank_id IS NULL OR substr(l.reasoning,1,6)='error:'",
            (latest,)
        ).fetchall()]
        banks = [b for b in banks if b in set(bad)]
    try:
        env_limit = int(os.getenv("LLM_BANK_LIMIT", "0") or "0")
        if env_limit > 0:
            bank_limit = min(bank_limit or env_limit, env_limit)
    except Exception:
        pass
    if bank_limit and bank_limit > 0:
        banks = banks[:bank_limit]
    if max_banks and max_banks > 0:
        banks = banks[:max_banks]
    return banks


def analyze_one_bank(
    conn: sqlite3.Connection,
    cur: sqlite3.Cursor,
    bank_id: str,
    periods: List[str],
    latest: str,
    provider: str,
    model: str,
    client: Optional[OpenAI],
    gc_client: Optional["GigaChat"],
    full_meta: Dict[str, Dict],
    logs_dir: str,
    indicator_defs: List[Dict],
    system_prompt_text: Optional[str],
    user_prompt_text: Optional[str],
    timeout_sec: int,
    reasoning_effort: str,
    strict_cache: bool,
    dry_run: bool,
    max_retries: int,
    backoff_seconds: int,
) -> bool:
    metrics = _collect_series(conn, bank_id, periods)
    peers = {}
    algo_row = cur.execute("SELECT status, details FROM algo_classifications WHERE bank_id=? AND period= ?", (bank_id, latest)).fetchone()
    algo_payload = {"status": (algo_row[0] if algo_row else None), "details": (algo_row[1] if algo_row else None)}
    data_quality = {"std_keys_covered": len([k for k in full_meta.keys()]), "periods_available": len(periods)}
    payload = {
        "bank": {"id": bank_id, "period_latest": latest},
        "timeseries_months": len(periods),
        "metrics": metrics,
        "algo": algo_payload,
        "peers": peers,
        "data_quality": data_quality,
    }
    data_json = json.dumps(payload, ensure_ascii=False)
    params_json = build_params_schema(full_meta)
    messages = build_messages(data_json, json.dumps({"indicators": indicator_defs}, ensure_ascii=False), system_prompt_text, params_json, user_prompt_text)
    cache_key = make_cache_key(model, messages, payload)
    req_path_h, resp_path_h, cached = cache_get(logs_dir, bank_id, cache_key)
    if cached is not None:
        save_result(cur, logs_dir, bank_id, model, latest, resp_path_h, cached)
        return False
    system_text = messages[0]["content"]
    user_text = messages[1]["content"]
    combined_openai = f"<SYSTEM>\n{system_text}\n</SYSTEM>\n<USER>\n{user_text}\n</USER>"
    combined_gigachat = f"СИСТЕМА:\n{system_text}\n\nПОЛЬЗОВАТЕЛЬ:\n{user_text}"
    combined = combined_openai if provider == "openai" else combined_gigachat
    log_request(req_path_h, {
        "provider": provider,
        "mode": "responses",
        "model": model,
        "payload": payload,
        "indicator_definitions": indicator_defs,
        "prompt": {"combined": combined},
    })
    if strict_cache or dry_run:
        cur.execute(
            "INSERT OR REPLACE INTO llm_classifications(bank_id,period,status,reasoning,model) VALUES(?,?,?,?,?)",
            (bank_id, latest, "Green", "error: strict_cache_or_dry_run", model),
        )
        return False
    def _send_once():
        if provider == "openai" and client is not None:
            content = call_openai(client, model, combined, reasoning_effort, timeout_sec)
        elif provider == "gigachat" and gc_client is not None:
            content = call_gigachat(gc_client, combined)
        else:
            raise RuntimeError("LLM provider not initialized")
        parsed_local = json.loads(content)
        return parsed_local
    print(f"LLM> start bank {bank_id} (attempt 1)")
    try:
        parsed = send_with_retries(_send_once, max_retries=max_retries, backoff_seconds=backoff_seconds)
        print(f"LLM> ok bank {bank_id}")
    except Exception as e2:
        cur.execute(
            "INSERT OR REPLACE INTO llm_classifications(bank_id,period,status,reasoning,model) VALUES(?,?,?,?,?)",
            (bank_id, latest, "Green", f"error: {e2}", model),
        )
        return True
    save_result(cur, logs_dir, bank_id, model, latest, resp_path_h, parsed)
    return False
def llm_analyze_all(conn: sqlite3.Connection, months: int = 6, model: Optional[str] = None):
    latest = _latest_period(conn)
    if not latest:
        print("Нет данных для LLM-анализа."); return

    # Конфигурация LLM из YAML (если есть)
    cfg = load_config() or {}
    llm_cfg = (cfg.get("llm") or {}) if isinstance(cfg, dict) else {}
    # Провайдер и базовые настройки
    provider = str(llm_cfg.get("provider", "openai")).lower()
    mode = "responses"  # Responses API для OpenAI
    model_cfg = str(llm_cfg.get("model", "gpt-5"))
    eff = str(llm_cfg.get("reasoning_effort", "high"))
    reasoning_effort = eff if eff in ("low", "medium", "high") else "low"
    sys_prompt_file = llm_cfg.get("system_prompt_file")
    user_prompt_file = llm_cfg.get("user_prompt_file")
    bank_limit = int(llm_cfg.get("bank_limit", 0) or 0)
    max_banks = int(llm_cfg.get("max_banks", 0) or 0)
    only_errors = bool(llm_cfg.get("only_errors", False))
    dry_run = bool(llm_cfg.get("dry_run", False))
    strict_cache = bool(llm_cfg.get("strict_cache", False))
    timeout_sec = int(llm_cfg.get("timeout_sec", 120) or 120)
    max_retries = int(llm_cfg.get("max_retries", 2) or 2)
    backoff_seconds = int(llm_cfg.get("backoff_seconds", 2) or 2)
    stop_after_consecutive_errors = int(llm_cfg.get("stop_after_consecutive_errors", 10) or 10)

    # Модель из аргумента имеет приоритет
    model = model or model_cfg

    # Периоды для среза
    periods = [r[0] for r in conn.cursor().execute(
        "SELECT DISTINCT period FROM raw_values WHERE period<=? ORDER BY period DESC LIMIT ?",
        (latest, months),
    ).fetchall()]
    periods = sorted(periods)

    # Метаданные/системный промпт
    # Обогащаем метаданные индикаторов формулами, группами, критичностью и интерпретацией
    base_meta = _indicator_metadata()
    formulas = _load_indicator_formulas()
    meta_overrides = _load_indicator_meta()
    full_meta: Dict[str, Dict] = {}
    for ind_id, meta in base_meta.items():
        group, critical = _group_for_indicator(ind_id)
        m_override = meta_overrides.get(ind_id, {})
        full_meta[ind_id] = {
            **meta,
            "group": m_override.get("group", group),
            "critical": bool(m_override.get("critical", critical)),
            "interpretation": m_override.get("interpretation", _direction_to_interpretation(meta.get("direction"))),
            "formula": formulas.get(ind_id),
            "thresholds": m_override.get("thresholds", {}),
            "benchmarks": m_override.get("benchmarks", {}),
        }
    # Собираем краткие определения показателей (id, name, description) и передаём в LLM-промпт
    try:
        # Базовые определения из метаданных (если нет — оставляем пустые строки)
        indicator_defs = []
        base_name_map: Dict[str, str] = {}
        for ind_id, meta in full_meta.items():
            name = meta.get("name") or ""
            desc = meta.get("desc") or meta.get("description") or ""
            base_name_map[ind_id] = name
            indicator_defs.append({
                "id": ind_id,
                "name": name,
                "description": desc,
            })
        # Добавляем производные *_PCT_M1 и *_PCT_M6 для всех базовых метрик,
        # которые реально отправляются в LLM (METRICS_BASE)
        for base_id in METRICS_BASE:
            base_nm = base_name_map.get(base_id, "")
            indicator_defs.append({
                "id": f"{base_id}_PCT_M1",
                "name": (f"Изменение {base_nm} за 1 месяц, %" if base_nm else ""),
                "description": (f"Процентное изменение значения {base_id} относительно уровня месяц назад." if base_nm else ""),
            })
            indicator_defs.append({
                "id": f"{base_id}_PCT_M6",
                "name": (f"Изменение {base_nm} за 6 месяцев, %" if base_nm else ""),
                "description": (f"Процентное изменение значения {base_id} относительно уровня шесть месяцев назад (гибкое окно)." if base_nm else ""),
            })
        meta_json = json.dumps({"indicators": indicator_defs}, ensure_ascii=False)
    except Exception:
        indicator_defs = []
        meta_json = None
    system_prompt_text = None
    if sys_prompt_file:
        # путь относительно корня пакета
        base_dir = os.path.dirname(os.path.dirname(__file__))
        path = os.path.join(base_dir, sys_prompt_file) if not os.path.isabs(sys_prompt_file) else sys_prompt_file
        system_prompt_text = _read_text_file(path)
    user_prompt_text = None
    if user_prompt_file:
        base_dir = os.path.dirname(os.path.dirname(__file__))
        path_u = os.path.join(base_dir, user_prompt_file) if not os.path.isabs(user_prompt_file) else user_prompt_file
        user_prompt_text = _read_text_file(path_u)

    # Провайдеры
    client = None
    gc_client = None
    skip_preflight = dry_run or strict_cache
    try:
        client, gc_client, model = init_provider(provider, llm_cfg, model or model_cfg, timeout_sec)
    except Exception as e:
        print(str(e))
        return
    if not skip_preflight:
        ok_pf, why = preflight(provider, client, gc_client, model, timeout_sec)
        if not ok_pf:
            print(f"LLM preflight failed: {why}. Анализ прерван.")
            return
    cur = conn.cursor()
    banks = select_banks(cur, latest, only_errors, bank_limit, max_banks)

    print(f"LLM-анализ: период {latest}, банков: {len(banks)}, модель: {model}, режим: responses")
    logs_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "llm_logs", latest)
    os.makedirs(logs_dir, exist_ok=True)
    # Сохраняем описание параметров один раз на период
    try:
        params_doc_path = os.path.join(logs_dir, "params_doc.json")
        params_doc_obj = _params_doc(full_meta)
        if not os.path.exists(params_doc_path):
            with open(params_doc_path, "w", encoding="utf-8") as f:
                json.dump(params_doc_obj, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

    wrote = 0

    def _save(path: str, obj: dict):
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump(obj, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    # Больше не используем Assistants API: вся логика через Responses

    pbar = tqdm(banks, desc="LLM analyze", unit="bank")
    consecutive_errors = 0
    for bank_id in pbar:
        had_error = analyze_one_bank(
            conn=conn,
            cur=cur,
            bank_id=bank_id,
            periods=periods,
            latest=latest,
            provider=provider,
            model=model,
            client=client,
            gc_client=gc_client,
            full_meta=full_meta,
            logs_dir=logs_dir,
            indicator_defs=indicator_defs,
            system_prompt_text=system_prompt_text,
            user_prompt_text=user_prompt_text,
            timeout_sec=timeout_sec,
            reasoning_effort=reasoning_effort,
            strict_cache=strict_cache,
            dry_run=dry_run,
            max_retries=max_retries,
            backoff_seconds=backoff_seconds,
        )
        wrote += 1
        if had_error:
            consecutive_errors += 1
            if stop_after_consecutive_errors and consecutive_errors >= stop_after_consecutive_errors:
                print(f"Останов по лимиту ошибок: {consecutive_errors} подряд")
                break
        else:
            consecutive_errors = 0

    conn.commit(); print(f"LLM-анализ завершен: {wrote} записей.")
