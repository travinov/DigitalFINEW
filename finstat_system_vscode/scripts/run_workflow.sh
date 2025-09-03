#!/usr/bin/env bash
set -euo pipefail

# Full workflow runner:
# - optional: activate venv
# - load .env
# - import new files from input/
# - calculate indicators (+ pct changes)
# - LLM analyze for a period (default: latest)
# - generate XLS report to reports/

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJ_DIR="${SCRIPT_DIR%/scripts}"
cd "$PROJ_DIR"

PERIOD="latest"
LIMIT_ENV=""

usage() {
  echo "Usage: $(basename "$0") [--period YYYY-MM-DD|latest] [--limit N]"
  echo "  --period    Target period for analysis/report (default: latest; uses closest ≤ date)"
  echo "  --limit     Limit number of banks (sets LLM_BANK_LIMIT for this run)"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --period)
      PERIOD="$2"; shift 2;;
    --limit)
      LIMIT_ENV="$2"; shift 2;;
    -h|--help)
      usage; exit 0;;
    *)
      echo "Unknown arg: $1"; usage; exit 1;;
  esac
done

# Try to activate venv if not already
if [[ -z "${VIRTUAL_ENV:-}" && -d "venv" ]]; then
  # shellcheck disable=SC1091
  source "venv/bin/activate" || true
fi

# Load .env so that provider tokens are available
if [[ -f ".env" ]]; then
  set -a; # export variables from .env
  # shellcheck disable=SC1091
  source ".env"
  set +a
fi

# Step 1: import new files if any
if find input -type f \( -name '*.dbf' -o -name '*.rar' -o -name '*.zip' \) | head -n1 >/dev/null; then
  echo "[WF] Importing new files from input/ ..."
  python run.py import --all
else
  echo "[WF] No new files in input/ — skipping import"
fi

# Step 2: calculate indicators
echo "[WF] Calculating indicators ..."
python run.py calc-indicators

# Step 3: LLM analyze
echo "[WF] Running LLM analysis for period=${PERIOD} ..."
if [[ -n "${LIMIT_ENV}" ]]; then
  LLM_BANK_LIMIT="${LIMIT_ENV}" python run.py llm-analyze --period "${PERIOD}"
else
  python run.py llm-analyze --period "${PERIOD}"
fi

# Step 4: report
OUT_FILE="reports/report_${PERIOD}.xlsx"
echo "[WF] Generating report to ${OUT_FILE} ..."
python run.py report --period "${PERIOD}" --outfile "${OUT_FILE}"

echo "[WF] Done. Report: ${OUT_FILE}"


