#!/usr/bin/env sh
set -eu

run_ingest() {
  echo "[worker] starting ingest run..."
  python main.py worker run \
    --input-dir "${INPUT_DIR}" \
    --archive-dir "${ARCHIVE_DIR}" \
    --pattern "${FILE_PATTERN}" \
    --db-uri "${MYSQL_URI}"
  echo "[worker] ingest run finished."
}

mkdir -p "${INPUT_DIR}" "${ARCHIVE_DIR}"

if [ "${RUN_ON_STARTUP:-true}" = "true" ]; then
  run_ingest
fi

while true; do
  sleep "${SCHEDULE_INTERVAL_SECONDS:-86400}"
  run_ingest
done
