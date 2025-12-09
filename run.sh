#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

VENV_DIR=".venv"
PYTHON="${VENV_DIR}/bin/python"
PIP="${VENV_DIR}/bin/pip"
SCRIPTS_DIR="scripts"
LOG_DIR="output_logs/logs"
REQUIREMENTS="requirements.txt"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

mkdir -p "${LOG_DIR}"

# 1) Create venv if missing
if [ ! -d "${VENV_DIR}" ]; then
  python3 -m venv "${VENV_DIR}"
fi

# 2) Install/upgrade pip and requirements
"${PIP}" install --upgrade pip
if [ -f "${REQUIREMENTS}" ]; then
  "${PIP}" install -r "${REQUIREMENTS}"
fi

# 3) Run scripts in alphanumeric order
echo "Running scripts..."
for script in $(ls "${SCRIPTS_DIR}"/*.py | sort); do
  name=$(basename "${script}")
  logfile="${LOG_DIR}/${name%.*}_${TIMESTAMP}.log"
  echo "-> ${name} (log: ${logfile})"

  if [ -x "${script}" ]; then
    "${script}" > "${logfile}" 2>&1
  else
    "${PYTHON}" "${script}" > "${logfile}" 2>&1
  fi

  echo "   ${name} completed"
done

echo "All done. Logs in ${LOG_DIR}"
