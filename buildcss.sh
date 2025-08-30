#!/usr/bin/env bash
set -euo pipefail

# Paths are relative to /app (set by WORKDIR in the Dockerfile)
STYLES_DIR="/app/static/styles"
TAILWIND_BIN="${STYLES_DIR}/tailwindcss-linux-x64"
CONFIG_CSS="${STYLES_DIR}/tailwind.config.js"
INPUT_CSS="${STYLES_DIR}/input.css"
OUTPUT_CSS="${STYLES_DIR}/output.css"

# Compile Tailwind CSS
"${TAILWIND_BIN}" -c "${CONFIG_CSS}" -i "${INPUT_CSS}" -o "${OUTPUT_CSS}" --minify

# Record a UNIX-epoch build/version stamp and persist it to ~/.env
EPOCH_NOW="$(date +%s)"
ENV_FILE="/app/.env"

# Replace or create the key
if grep -qE '^CSS_BUILD_EPOCH=' "${ENV_FILE}" 2>/dev/null; then
  sed -i "s/^CSS_BUILD_EPOCH=.*/CSS_BUILD_EPOCH=${EPOCH_NOW}/" "${ENV_FILE}"
else
  printf "CSS_BUILD_EPOCH=%s\n" "${EPOCH_NOW}" >> "${ENV_FILE}"
fi

# Also export for this process tree so Gunicorn sees it without relying on dotenv
export CSS_BUILD_EPOCH="${EPOCH_NOW}"

# Pass control back to the Dockerfile
exec "$@"
