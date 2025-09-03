# Финансовая система анализа

## Назначение
Система импортирует финансовую отчетность банков (DBF/архивы), сохраняет данные в локальную SQLite, рассчитывает индикаторы и их динамику, выполняет алгоритмическую классификацию (Green/Yellow/Red), проводит LLM‑анализ и формирует XLS‑отчет с несколькими листами.

## Архитектура и модули
- `src/db.py` — инициализация БД (`data/finstat.db`), схема таблиц, загрузка конфигурации.
- `src/import_dbf.py` — импорт DBF/архивов из `input/` с автоопределением полей, кодировок и A/P суффиксов, перенос обработанных файлов в `archive/`.
- `src/indicators.py` — расчет базовых индикаторов по формулам, а также производных показателей изменения за 1 и 6 месяцев (гибкое окно).
- `src/rules_engine.py` — алгоритмическая классификация по YAML‑правилам (наборы условий AND/OR для Yellow/Red).
- `src/llm_module.py` — LLM‑анализ (OpenAI), сбор признаков, системный промпт, логирование запросов/ответов и сохранение результатов.
- `src/report_xls.py` — формирование XLS‑отчета: `Summary`, `Indicators_long`, `Raw_values`, `LLM`.
- `src/data_viewer.py` — CLI‑просмотр данных (`summary|banks|forms|periods|log|raw|indicators`).
- `src/archive_utils.py` — работа с RAR/ZIP, временные папки.
- `configs/` — конфигурации: `config.yaml`, `indicators.yaml`, `rules.yaml`, `data_dictionary.csv`.

## Схема БД (основные таблицы)
- `banks(bank_id, bank_name)`
- `forms(form_code, form_name)`
- `raw_values(bank_id, form_code, period, item_code, value)` — сырые значения (PK по всем полям).
- `indicator_values(bank_id, indicator_id, period, value)` — рассчитанные показатели.
- `algo_classifications(bank_id, period, status, details)` — результаты правил.
- `llm_classifications(bank_id, period, status, reasoning, model)` — результаты LLM.
- `ingestion_log(file_name, bank_id, form_code, period, rows_loaded)` — журнал импорта.

## Настройка путей и форм
`configs/config.yaml`:
- `input_folder: "input"`, `archive_folder: "archive"`
- Регулярные выражения и паттерны имен файлов для разных форм.
- Для 0409101 учтен признак Актив/Пассив `ap_field: "A_P"` с маппингом `ap_map`.

## Рассчитываемые показатели
Формулы заданы в `configs/indicators.yaml`. Примеры (неполный список):
- Базовые: `capital_adequacy`, `loan_to_deposit`, `npl_ratio`
- QN‑показатели: `QN1..QN20` (включая `QN17` в процентах, `QN18=H1_0`, `QN19=H1_2`)
- Усеченный баланс: `A1, A2, A3, A3_1, A3_2, A3_3, A4` и обязательства `O1, O1_1, O1_2, O1_3, O2, O3, O4`

Динамика (создаются автоматически в `indicator_values`):
- `{ID}_PCT_M1` — изменение за 1 месяц, %: `(curr − prev1) / |prev1| × 100`, если `prev1≠0`.
- `{ID}_PCT_M6` — изменение за 6 месяцев, %: используется «гибкое окно»: если ровно `p−6` нет, берется самая ранняя доступная дата в пределах последних 6 месяцев. Формула та же `(curr − prev6)/|prev6|×100`.

## Алгоритмическая классификация
`configs/rules.yaml` использует только наборы условий (одиночные пороги отключены):
- `yellow_sets`: список наборов (AND внутри набора, OR между наборами).
- `red_sets`: список наборов (AND внутри набора, OR между наборами).

Порядок проверки и приоритеты:
1) Сначала проверяются `red_sets`. Если выполнен хотя бы один набор — присваивается Red.
2) Если Red не сработал, проверяются `yellow_sets`. Если выполнен хотя бы один набор — присваивается Yellow.
3) Если ни один набор не сработал — присваивается Green.

Текущая логика в `src/rules_engine.py`:
- Сначала проверяются `red_sets` (OR по наборам, AND внутри). Если сработал любой набор → статус Red.
- Если Red не сработал, проверяются `yellow_sets` (OR по наборам, AND внутри). Если сработал любой набор → статус Yellow.
- Иначе → Green.
- Одиночные пороги отключены; учитываются только наборы.
- Приоритет реализован каскадом: Red → Yellow → Green.
3) Если ничего не сработало — `Green`.

## LLM‑анализ
Поддерживаются провайдеры:
- `openai` — Responses API (reasoning), модель по умолчанию `gpt-5`;
- `gigachat` — через `langchain_gigachat.GigaChat`.

Выбор провайдера и параметры — в `configs/config.yaml` → `llm`:
```yaml
llm:
  provider: "openai"   # openai | gigachat
  model: "gpt-5"        # для openai
  reasoning_effort: "low"
  system_prompt_file: "configs/llm_system_prompt.txt"
  user_prompt_file: "configs/llm_user_prompt.txt"
  # Параметры GigaChat
  gigachat:
    model: "GigaChat-2-Max"
    base_url: "https://sbercode.atdcode.ru/proxy/api/v1/gigachat/"
    scope: "GIGACHAT_API_PERS"
    timeout_sec: 180
    temperature: 0.1
    top_p: 0.3
    max_tokens: 2000
    profanity_check: false
    verify_ssl_certs: false
```

Ожидаемые переменные окружения:
- для OpenAI: `OPENAI_API_KEY`;
- для GigaChat: `GIGACHAT_ACCESS_TOKEN` (или `GIGACHAT_CREDENTIALS`).

Анализ выполняется для последнего периода, 6‑месячный срез метрик: A1, QN9, O1, O2, QN11, QN15, QN18, QN19, QN13, а также соответствующие PCT_M1/PCT_M6, и `QN17`, `QN16`.

### Провайдер GigaChat — настройка и быстрый старт

1) Зависимости (уже в `requirements.txt`, на всякий случай):
```bash
python3 -m pip install -r finstat_system_vscode/requirements.txt
```

2) Переключите провайдера в `configs/config.yaml`:
```yaml
llm:
  provider: "gigachat"   # openai | gigachat
  gigachat:
    model: "GigaChat-2-Max"
    base_url: "https://sbercode.atdcode.ru/proxy/api/v1/gigachat/"
    scope: "GIGACHAT_API_PERS"
    timeout_sec: 180
    temperature: 0.1
    top_p: 0.3
    max_tokens: 2000
    profanity_check: false
    verify_ssl_certs: false
```

3) Добавьте токен в `.env` (файл не коммитится):
```bash
echo 'GIGACHAT_ACCESS_TOKEN=<ВАШ_ТОКЕН>' >> finstat_system_vscode/.env
```
Примечание: токен GigaChat обычно живёт ~8 часов; при 401/403 обновите значение и повторите запуск.

4) Быстрый тест на малом числе банков:
```bash
python -m venv venv && source venv/bin/activate
pip install -r finstat_system_vscode/requirements.txt
LLM_BANK_LIMIT=1 python finstat_system_vscode/run.py llm-analyze
python finstat_system_vscode/run.py report --period latest --outfile finstat_system_vscode/reports/report_test.xlsx
```

5) Типовые проблемы:
- 401/403: истёк/неверный токен — обновите `GIGACHAT_ACCESS_TOKEN` в `finstat_system_vscode/.env`.
- Таймаут: увеличьте `llm.gigachat.timeout_sec` в `configs/config.yaml`.
- Ошибка парсинга JSON: ответ может содержать текст вокруг JSON. Модуль автоматически извлекает первый валидный JSON‑объект; при систематической проблеме уточните промпт.
- SSL/прокси: при корпоративном сертификате можно временно выставить `verify_ssl_certs: false` (понижает безопасность).

Системный промпт (сокращенно):
> Ты — беспристрастный риск‑аналитик межбанковского кредитования. Оцени риски ликвидности, фондирования, капитала и качества активов на горизонте 1–3 мес. Используй только предоставленные данные. Не делай выводов о высоком риске без подтверждений несколькими показателями и устойчивой динамики. Сезонные колебания не трактуй как ухудшение. Если данных недостаточно — выбирай Green. Верни чистый JSON со схемой: {status, confidence, reasons[], watchlist[], recommendation, metrics_snapshot, summary_ru}.

Параметры запроса/ответа логируются по каждому банку в `data/llm_logs/<latest_period>/` (файлы `*_request.json`, `*_response.json`). В таблицу `llm_classifications` пишутся `status`, `reasoning` (JSON результата), `model`.

### Новые настройки устойчивости (configs/config.yaml → llm)
```yaml
llm:
  mode: "responses"          # фиксировано
  model: "gpt-5"             # reasoning‑модель
  system_prompt_file: "configs/llm_system_prompt.txt"
  # Ограничители и устойчивость
  bank_limit: 0               # доп. ограничение числа банков сверху (0 = без лимита)
  max_banks: 0                # жёсткий потолок на прогон (0 = без)
  only_errors: false          # true — прогонять только банки без результата/с ошибкой
  dry_run: false              # true — ничего не отправлять в API (для проверки пайплайна)
  strict_cache: false         # true — не вызывать API, использовать только кэш
  timeout_sec: 120            # таймаут одного запроса
  max_retries: 2              # число повторов
  backoff_seconds: 2          # базовая задержка (экспоненциально растёт)
  stop_after_consecutive_errors: 10  # остановить прогон при N подряд ошибках
```

Рекомендации к запуску:
- Сначала `LLM_BANK_LIMIT=5 python run.py llm-analyze` (сухой прогон на малом числе банков).
- Затем при необходимости `only_errors: true` — добрать только неуспешные.
- Для экономии — `strict_cache: true` (только чтение кэша) или `dry_run: true` (без запросов).

## Процесс работы (пайплайн)
1) Инициализировать БД: `python run.py init-db`
2) Положить файлы отчетности в `input/` (поддерживаются `.dbf`, `.rar`, `.zip`).
3) Импорт: `python run.py import` (после импорта файлы перемещаются в `archive/`).
4) Расчет индикаторов: `python run.py calc-indicators` (включая PCT_M1/PCT_M6).
5) Классификация: `python run.py classify`.
6) LLM‑анализ:
   - последний период: `python run.py llm-analyze`
   - на дату (берётся ближайший доступный период ≤ даты): `python run.py llm-analyze --period 2024-06-01`

### Ограничение количества банков для анализа LLM

Есть два способа ограничить число обрабатываемых банков в одном запуске:

- Через конфиг `configs/config.yaml` → `llm.bank_limit`:
  ```yaml
  llm:
    bank_limit: 5   # 0 = без ограничения
  ```

- Через переменную окружения во время запуска (удобно для разовых прогонов):
  ```bash
  LLM_BANK_LIMIT=5 python run.py llm-analyze
  LLM_BANK_LIMIT=5 python run.py llm-analyze --period 2024-06-01
  ```

Правило при совместном использовании: берётся минимальное из `llm.bank_limit` и `LLM_BANK_LIMIT`. Если одно из них 0 (без лимита), действует другое.

Дополнительно можно прогонять только проблемные/отсутствующие записи, включив в конфиге `llm.only_errors: true`.

### Перезапись результатов LLM при повторных запусках

Поведение кэша/перезаписи регулируется флагом `llm.always_recompute` в `configs/config.yaml`:

```yaml
llm:
  # Если true (по умолчанию) — кэш игнорируется, запрос уходит в LLM заново,
  # а результат перезаписывается в БД и логи
  always_recompute: true
```

Установите `false`, если хотите использовать уже сохранённые ответы при совпадении payload+prompt (кэш‑хит).
7) Отчет XLS:
   - последний период: `python run.py report --period latest --outfile finstat_system_vscode/reports/report_latest.xlsx`
   - на дату: `python run.py report --period 2024-06-01 --outfile finstat_system_vscode/reports/report_2024-06-01.xlsx`

### Запуск полного воркфлоу одной командой

В репозитории есть скрипт `scripts/run_workflow.sh`, который выполняет полный цикл: импорт новых файлов из `input/` (если есть), пересчёт индикаторов, LLM‑анализ и формирование отчёта.

Примеры запуска:
```bash
cd finstat_system_vscode
source venv/bin/activate

# На последний период
scripts/run_workflow.sh

# На конкретную дату
scripts/run_workflow.sh --period 2024-03-01

# На дату и с лимитом 50 банков (аналог LLM_BANK_LIMIT=50)
scripts/run_workflow.sh --period 2024-03-01 --limit 50
```

Скрипт автоматически подхватывает токены из `finstat_system_vscode/.env` (например, `GIGACHAT_ACCESS_TOKEN`).
8) Просмотр данных (опционально): `python run.py view summary` и другие команды.

## Установка и запуск (How‑to)
1) Зависимости:
```
python3 -m pip install -r requirements.txt
# Для RAR на macOS: brew install unar (или apt install unrar на Linux)
```
2) Активировать среду и настроить ключи в `.env` (рекомендуется) в `finstat_system_vscode/.env`:
```
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# Ключи провайдеров (по необходимости):
OPENAI_API_KEY=sk-...
GIGACHAT_ACCESS_TOKEN=...
```
3) Импорт и пересчет (пример):
```
python run.py init-db
# Импорт всех новых файлов из input/ (обязательно с активированной средой)
python run.py import --all
# Пересчитать индикаторы и изменения
python run.py calc-indicators
python run.py classify
python run.py llm-analyze
python run.py report --period latest
```

## Кастомизация
- Добавить/изменить индикаторы: правьте `configs/indicators.yaml` и (при необходимости) сопоставления в `configs/data_dictionary.csv`.
- Пороговые правила: правьте `configs/rules.yaml` (наборы `yellow_sets`, `red_sets`, `red_and`).
- Ограничить число банков для отладки LLM: переменная окружения `LLM_BANK_LIMIT` (целое число).

## Диагностика
- Логи импорта: таблица `ingestion_log` и просмотр `python run.py view log`.
- Логи LLM промптов/ответов: `data/llm_logs/<period>/`.
- Частые причины нулевых индикаторов: отсутствие сопоставления `item_code`↔`std_key` в `data_dictionary.csv` либо коды без A/P‑суффикса.
