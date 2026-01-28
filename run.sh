#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

# Self-locating repo root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Config (anchored to repo)
VENV_DIR="${SCRIPT_DIR}/.venv"
SCRIPTS_DIR="${SCRIPT_DIR}/scripts"
LOG_DIR="${SCRIPT_DIR}/output_logs/logs"
REQUIREMENTS="${SCRIPT_DIR}/requirements.txt"
SHEETS_LIST="${SCRIPT_DIR}/sheets_to_fetch.csv"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# Weekly layout -> replaced by date-based layout
WEEK_BASE="${SCRIPT_DIR}/output_iterative"
# use date instead of ISO week: YYYY-MM-DD
WEEK_ID=$(date +"%Y-%m-%d")
WEEK_DIR="${WEEK_BASE}/${WEEK_ID}"
WEEK_DATA_DIR="${WEEK_DIR}/data/WPP Input Tables"

TOP_OUTPUT_DIRS=(
  "analysis"
  "temporal_spatial_output"
  "2d_plots"
  "3d_scatter_plots"
  "unique_processes"
  "unique_effectors"
  "common_effectors_across_systems"
  "unique_ftus"
)

# Prep
mkdir -p "${LOG_DIR}"
mkdir -p "${SCRIPTS_DIR}"
mkdir -p "${WEEK_DATA_DIR}"
mkdir -p "${WEEK_DIR}"

echo "=== RUN START ==="
echo "Script dir: ${SCRIPT_DIR}"
echo "Timestamp : ${TIMESTAMP}"
echo "Run ID    : ${WEEK_ID}"
echo "Run root  : ${WEEK_DIR}"

# Ensure venv exists
if [ ! -d "${VENV_DIR}" ]; then
  echo "Creating venv..."
  if command -v python3 >/dev/null 2>&1; then
    python3 -m venv "${VENV_DIR}"
  elif command -v python >/dev/null 2>&1; then
    python -m venv "${VENV_DIR}"
  else
    echo "ERROR: no system python available to create venv." >&2
    exit 1
  fi
fi

# Locate python in venv (Unix or Windows)
if [ -x "${VENV_DIR}/bin/python" ]; then
  PYTHON="${VENV_DIR}/bin/python"
elif [ -x "${VENV_DIR}/Scripts/python.exe" ]; then
  PYTHON="${VENV_DIR}/Scripts/python.exe"
else
  # fallback to system python
  if command -v python3 >/dev/null 2>&1; then
    PYTHON="$(command -v python3)"
  elif command -v python >/dev/null 2>&1; then
    PYTHON="$(command -v python)"
  else
    echo "ERROR: cannot find a python executable." >&2
    exit 1
  fi
fi

echo "Using python: ${PYTHON}"

# Upgrade pip + install requirements via the venv python
"$PYTHON" -m pip install --upgrade pip
if [ -f "${REQUIREMENTS}" ]; then
  "$PYTHON" -m pip install -r "${REQUIREMENTS}"
fi

# Download sheets into week data dir (if sheets file exists)
if [ -f "${SHEETS_LIST}" ]; then
  echo "Downloading sheets -> ${WEEK_DATA_DIR}"
  while IFS=, read -r fname url || [ -n "${fname:-}" ]; do
    fname="$(echo "${fname:-}" | xargs)"
    url="$(echo "${url:-}" | xargs)"
    [ -z "${fname}" ] || [ -z "${url}" ] && continue
    out="${WEEK_DATA_DIR}/${fname}"
    echo "  -> ${fname}"
    tmp="$(mktemp -u)/${fname}.${TIMESTAMP}.tmp"
    mkdir -p "$(dirname "${tmp}")"
    if curl -fsSL "${url}" -o "${tmp}"; then
      mv "${tmp}" "${out}"
    else
      echo "ERROR: failed to download ${url}" >&2
      rm -f "${tmp}" || true
      exit 1
    fi
  done < "${SHEETS_LIST}"
else
  echo "No ${SHEETS_LIST} — skipping downloads."
fi

# Create top-level output dirs under WEEK_DIR
for d in "${TOP_OUTPUT_DIRS[@]}"; do
  mkdir -p "${WEEK_DIR}/${d}"
done
echo "Created top-level output dirs under ${WEEK_DIR}"

# Run scripts — ALWAYS use the venv python to run the script file by absolute path
echo "Running scripts from ${SCRIPTS_DIR} with CWD=${WEEK_DIR} ..."
shopt -s nullglob
script_list=("${SCRIPTS_DIR}"/*.py)
shopt -u nullglob

if [ ${#script_list[@]} -eq 0 ]; then
  echo "No scripts found in ${SCRIPTS_DIR}/*.py — nothing to run."
else
  for script in "${script_list[@]}"; do
    SCRIPT_PATH="$(realpath "${script}")"
    name="$(basename "${script}")"
    logfile="${LOG_DIR}/${name%.*}_${TIMESTAMP}.log"
    echo "-> ${name} (log: ${logfile})"

    # run via "$PYTHON" so shebang/env differences don't matter
    (cd "${WEEK_DIR}" && "${PYTHON}" "${SCRIPT_PATH}") > "${logfile}" 2>&1 || echo "Script ${name} failed — see ${logfile}"

    echo "   ${name} completed"
  done
fi

# Post-run diagnostics
echo "=== RUN COMPLETE ==="
echo "Inputs  : ${WEEK_DATA_DIR}"
echo "Outputs : ${WEEK_DIR}/"
echo "Logs    : ${LOG_DIR}"

echo
echo "Files created (top 200):"
find "${WEEK_DIR}" -type f -mtime -1 -print | head -n 200 || true

echo
echo "Tail of script logs (last 200 lines of each):"
for f in "${LOG_DIR}"/*.log; do
  echo "---- ${f} ----"
  tail -n 200 "${f}" || true
done