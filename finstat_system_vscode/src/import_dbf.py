import os, re
from dbfread import DBF, FieldParser
from tqdm import tqdm
from .db import load_config, parse_filename_generic
from .archive_utils import extract_archive, cleanup_temp_dir, list_archive_contents

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
CFG = load_config()

class RelaxedFieldParser(FieldParser):
    """Парсер DBF, который корректно обрабатывает числа с NUL-паддингом."""
    def _clean_bytes(self, data):
        if isinstance(data, (bytes, bytearray)):
            return bytes(data).replace(b"\x00", b"").strip()
        return data

    def parseN(self, field, data):
        data = self._clean_bytes(data)
        try:
            return super().parseN(field, data)
        except Exception:
            try:
                s = data.decode('latin-1') if isinstance(data, (bytes, bytearray)) else str(data)
                s = s.replace('\x00','').replace(' ', '').replace(',', '.')
                if s == '':
                    return None
                # Пытаемся float, если точка присутствует
                return float(s) if ('.' in s) else int(s)
            except Exception:
                return None

    def parseF(self, field, data):
        data = self._clean_bytes(data)
        try:
            return super().parseF(field, data)
        except Exception:
            try:
                s = data.decode('latin-1') if isinstance(data, (bytes, bytearray)) else str(data)
                s = s.replace('\x00','').replace(' ', '').replace(',', '.')
                return float(s) if s != '' else None
            except Exception:
                return None

def _guess_field(record, preferred):
    for key in preferred:
        if key in record: return key
    lower = {k.lower(): k for k in record.keys()}
    for key in preferred:
        if key.lower() in lower: return lower[key.lower()]
    return None

def _build_meta_map(dbf_paths):
    """Строит карту признака A/P для форм 0409802/0409803 по meta-файлам в распакованном архиве."""
    meta = {}
    for p in dbf_paths:
        name = os.path.basename(p).upper()
        try:
            if name.startswith('F802META') and name.endswith('.DBF'):
                t = DBF(p, encoding='cp866', char_decode_errors='ignore')
                # ожидаем поля: FSECTION ('АКТИВЫ'/'ПАССИВЫ'), FSTR ('1','2','32.1', ...)
                for r in t:
                    sec = str(r.get('FSECTION') or '').strip().upper()
                    fstr = str(r.get('FSTR') or '').strip()
                    if not fstr:
                        continue
                    ap = 'A' if 'АКТИВ' in sec else 'P' if 'ПАССИВ' in sec else None
                    if ap:
                        meta[("0409802", fstr)] = ap
            if name.startswith('F803META') and name.endswith('.DBF'):
                t = DBF(p, encoding='cp866', char_decode_errors='ignore')
                for r in t:
                    sec = str(r.get('FSECTION') or '').strip().upper()
                    fstr = str(r.get('FSTR') or '').strip()
                    if not fstr:
                        continue
                    ap = 'A' if 'АКТИВ' in sec else 'P' if 'ПАССИВ' in sec else None
                    if ap:
                        meta[("0409803", fstr)] = ap
        except Exception:
            continue
    return meta


def import_all_dbf(conn):
    input_folder = os.path.join(BASE_DIR, CFG.get("input_folder", "input"))
    archive_folder = os.path.join(BASE_DIR, CFG.get("archive_folder", "archive"))
    os.makedirs(archive_folder, exist_ok=True)
    default_item_fields = CFG.get("default_item_fields", ["ITEM","ROW_CODE","CODE","ACODE","STR","C1"])
    default_value_fields = CFG.get("default_value_fields", ["VALUE","AMOUNT","SUM","VAL","VSEGO","C3","IITG"])
    forms_cfg = CFG.get("forms", {})
    generic_pattern = CFG.get("filename_regex")

    cur = conn.cursor()

    # Получаем все файлы (.dbf и архивы)
    all_files = []
    for f in os.listdir(input_folder):
        if f.lower().endswith((".dbf", ".rar", ".zip")):
            all_files.append(f)
    all_files.sort()

    # Обрабатываем каждый файл
    pbar = tqdm(all_files, desc="Импорт файлов")
    for fname in pbar:
        pbar.set_postfix({"файл": fname})

        if fname.lower().endswith((".rar", ".zip")):
            # Обрабатываем архив
            archive_path = os.path.join(input_folder, fname)
            dbf_files, temp_dir = extract_archive(archive_path)
            # Пытаемся извлечь полные наименования банков из справочных DBF (если есть)
            _maybe_update_bank_names(conn, dbf_files)
            meta_map = _build_meta_map(dbf_files)

            if not dbf_files:
                print(f"Не удалось извлечь DBF из {fname}")
                continue

            # Обрабатываем каждый DBF в архиве
            for dbf_path in dbf_files:
                dbf_name = os.path.basename(dbf_path)
                _process_dbf_file(conn, dbf_path, dbf_name, forms_cfg, generic_pattern,
                                 default_item_fields, default_value_fields, pbar, fname, meta_map)

            # Очищаем временную папку
            if temp_dir:
                cleanup_temp_dir(temp_dir)
            # Переносим обработанный архив в архивную папку
            try:
                os.replace(archive_path, os.path.join(archive_folder, fname))
            except Exception as e:
                print(f"Не удалось переместить архив {fname} в {archive_folder}: {e}")
        else:
            # Обрабатываем обычный DBF файл
            dbf_path = os.path.join(input_folder, fname)
            _process_dbf_file(conn, dbf_path, fname, forms_cfg, generic_pattern,
                             default_item_fields, default_value_fields, pbar, None, {})
            # Также пробуем обновить название банка, если файл содержит NAME_B
            _maybe_update_bank_names(conn, [dbf_path])
            # Переносим обработанный DBF в архивную папку
            try:
                os.replace(dbf_path, os.path.join(archive_folder, fname))
            except Exception as e:
                print(f"Не удалось переместить файл {fname} в {archive_folder}: {e}")

    print("Импорт завершен.")

def _maybe_update_bank_names(conn, dbf_paths):
    """Обновляет таблицу banks.bank_name, если во входных DBF присутствуют поля REGN и NAME_B."""
    cur = conn.cursor()
    for p in dbf_paths:
        try:
            table = DBF(p, encoding='cp866', char_decode_errors='ignore', parserclass=RelaxedFieldParser)
            # Соберём множество полей
            field_names = {f.name.upper() for f in table.fields}
            if 'REGN' in field_names and 'NAME_B' in field_names:
                for r in table:
                    regn = r.get('REGN')
                    name_b = r.get('NAME_B')
                    bank_id = str(regn) if regn is not None else None
                    bank_name = str(name_b).strip() if name_b is not None else None
                    if bank_id and bank_name:
                        cur.execute("UPDATE banks SET bank_name=? WHERE bank_id=?", (bank_name, bank_id))
                conn.commit()
        except Exception:
            # Тихо пропускаем любые ошибки на нецелевых файлах
            continue

def _process_dbf_file(conn, dbf_path, fname, forms_cfg, generic_pattern,
                     default_item_fields, default_value_fields, pbar, archive_name=None, meta_map=None):
    """Обработка одного DBF файла"""
    cur = conn.cursor()

    # Проверяем, не был ли уже импортирован (используем имя архива если есть)
    check_name = archive_name or fname
    cur.execute("SELECT 1 FROM ingestion_log WHERE file_name=?", (check_name,))
    if cur.fetchone():
        return

    bank_id = None
    form_code = None
    period = None

    # По формам (список patterns)
    for fcode, conf in forms_cfg.items():
        for pat in conf.get("filename_patterns", []):
            m = re.match(pat, fname, flags=re.IGNORECASE)
            if m:
                form_code = fcode
                gd = m.groupdict()
                if "yyyy" in gd and "mm" in gd: period = f"{gd['yyyy']}-{gd['mm']}-01"
                elif "yy" in gd and "mm" in gd: period = f"20{gd['yy']}-{gd['mm']}-01"
                elif "q" in gd and "yyyy" in gd:
                    month = int(gd["q"]) * 3
                    period = f"{gd['yyyy']}-{month:02d}-01"
                bank_id = bank_id or "UNKNOWN"
                break
        if form_code: break

    if not form_code:
        parsed = parse_filename_generic(fname, generic_pattern)
        if parsed: bank_id, form_code, period = parsed
    if not period:
        return

    cur.execute("INSERT OR IGNORE INTO banks(bank_id, bank_name) VALUES(?,?)", (bank_id or "UNKNOWN", None))
    cur.execute("INSERT OR IGNORE INTO forms(form_code, form_name) VALUES(?,?)", (form_code, None))

    # Получаем настройки кодировки
    encoding = forms_cfg.get(form_code, {}).get("encoding", "utf-8")

    try:
        table = DBF(dbf_path, encoding=encoding, char_decode_errors='ignore', parserclass=RelaxedFieldParser)
    except Exception as e:
        print(f"Ошибка чтения DBF {fname}: {e}")
        return

    item_field = forms_cfg.get(form_code, {}).get("item_field")
    value_field = forms_cfg.get(form_code, {}).get("value_field")
    bank_field = forms_cfg.get(form_code, {}).get("bank_field")
    ap_field   = forms_cfg.get(form_code, {}).get("ap_field")
    ap_map     = forms_cfg.get(form_code, {}).get("ap_map", {})
    meta_map = meta_map or {}

    # Получаем первую запись для определения полей (стримингово)
    sample = {}
    try:
        iterator = iter(table)
        first = next(iterator)
        sample = first
    except StopIteration:
        iterator = iter([])
        sample = {}
    except Exception as e:
        print(f"Ошибка чтения записей DBF {fname}: {e}")
        return

    if not item_field:  item_field  = _guess_field(sample, default_item_fields) or "ITEM"
    if not value_field: value_field = _guess_field(sample, default_value_fields) or "VALUE"

    def _to_float(val):
        try:
            if val is None or val == "":
                return None
            if isinstance(val, (int, float)):
                return float(val)
            if isinstance(val, bytes):
                # Декодируем байтовые строки с NUL-паддингом
                try:
                    s = val.decode(encoding, errors='ignore')
                except Exception:
                    s = val.decode('latin-1', errors='ignore')
                s = s.replace("\x00", "").strip().replace(" ", "").replace(",", ".")
                if not s:
                    return None
                return float(s)
            # Строка: чистим пробелы/запятые/точки
            s = str(val).replace("\x00", "").strip().replace(" ", "").replace(",", ".")
            if not s or s.lower() in ["none", "null", "n/a"]:
                return None
            return float(s)
        except Exception:
            return None

    rows=0
    # Обрабатываем первую запись, затем остальные
    for rec in ([sample] if sample else []):
        # Извлекаем bank_id из записи если есть поле банка
        current_bank_id = bank_id
        if bank_field and bank_field in rec:
            current_bank_id = str(rec.get(bank_field))
        item_code = rec.get(item_field)
        v = _to_float(rec.get(value_field))
        if v is None:
            pass
        else:
            # Актив/Пассив суффикс для item_code (только если определено ap_field)
            suffix = ""
            if ap_field and ap_field in rec:
                ap_raw = rec.get(ap_field)
                ap_key = None
                if ap_raw is not None:
                    ap_key = str(ap_raw).strip()
                ap = ap_map.get(ap_key)
                if ap == "A":
                    suffix = "A"
                elif ap == "P":
                    suffix = "P"
                elif ap == "AP":
                    suffix = ""
            # Для форм 0409802/0409803 пытаемся определить A/P из meta
            if form_code in ("0409802","0409803") and not suffix:
                ap_guess = meta_map.get((form_code, str(item_code)))
                if ap_guess in ("A","P"):
                    suffix = ap_guess

            if current_bank_id and current_bank_id != "UNKNOWN":
                cur.execute("INSERT OR IGNORE INTO banks(bank_id, bank_name) VALUES(?,?)", (current_bank_id, None))
            item_norm = str(item_code) + (suffix if suffix in ("A","P") else "")
            cur.execute("INSERT OR REPLACE INTO raw_values(bank_id,form_code,period,item_code,value) VALUES(?,?,?,?,?)",
                        (current_bank_id or "UNKNOWN", form_code, period, item_norm, v))
            rows += 1

    for rec in iterator:
        # Извлекаем bank_id из записи если есть поле банка
        current_bank_id = bank_id
        if bank_field and bank_field in rec:
            current_bank_id = str(rec.get(bank_field))

        item_code = rec.get(item_field)
        v = _to_float(rec.get(value_field))
        if v is None:
            continue

        # Актив/Пассив суффикс для item_code (только если определено ap_field)
        suffix = ""
        if ap_field and ap_field in rec:
            ap_raw = rec.get(ap_field)
            ap_key = None
            if ap_raw is not None:
                ap_key = str(ap_raw).strip()
            ap = ap_map.get(ap_key)
            if ap == "A":
                suffix = "A"
            elif ap == "P":
                suffix = "P"
            elif ap == "AP":
                # Активно-пассивный — без суффикса
                suffix = ""

        # Добавляем банк в список если его еще нет
        if current_bank_id and current_bank_id != "UNKNOWN":
            cur.execute("INSERT OR IGNORE INTO banks(bank_id, bank_name) VALUES(?,?)", (current_bank_id, None))

        # Для форм 0409802/0409803 пытаемся определить A/P из meta
        if form_code in ("0409802","0409803") and not suffix:
            ap_guess = meta_map.get((form_code, str(item_code)))
            if ap_guess in ("A","P"):
                suffix = ap_guess

        # Нормализация item_code: добавляем суффикс A/P, если найден
        item_norm = str(item_code) + (suffix if suffix in ("A","P") else "")
        # Записываем как есть; дальнейшее сопоставление делается словарем data_dictionary
        cur.execute("INSERT OR REPLACE INTO raw_values(bank_id,form_code,period,item_code,value) VALUES(?,?,?,?,?)",
                    (current_bank_id or "UNKNOWN", form_code, period, item_norm, v))
        rows += 1

    cur.execute("INSERT OR REPLACE INTO ingestion_log(file_name, bank_id, form_code, period, rows_loaded) VALUES(?,?,?,?,?)",
                (check_name, bank_id or "UNKNOWN", form_code, period, rows))
    conn.commit()
    pbar.set_postfix({"файл": check_name, "строк": rows})
