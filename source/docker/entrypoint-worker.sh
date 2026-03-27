#!/usr/bin/env sh
set -eu

wait_for_mysql() {
  timeout_sec="${MYSQL_WAIT_TIMEOUT_SECONDS:-180}"
  interval_sec=3
  elapsed=0

  echo "[worker] waiting for mysql readiness..."
  while true; do
    if python -c "import pymysql, urllib.parse as u; p=u.urlparse('${MYSQL_URI}'); conn=pymysql.connect(host=p.hostname, port=p.port or 3306, user=u.unquote(p.username or ''), password=u.unquote(p.password or ''), database=(p.path or '/').lstrip('/'), connect_timeout=3); conn.close()" >/dev/null 2>&1; then
      echo "[worker] mysql is ready."
      break
    fi

    elapsed=$((elapsed + interval_sec))
    if [ "$elapsed" -ge "$timeout_sec" ]; then
      echo "[worker] mysql readiness timeout after ${timeout_sec}s"
      exit 1
    fi
    sleep "$interval_sec"
  done
}

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
wait_for_mysql

if [ "${RUN_ON_STARTUP:-true}" = "true" ]; then
  run_ingest
fi

while true; do
  sleep "${SCHEDULE_INTERVAL_SECONDS:-86400}"
  run_ingest
done
