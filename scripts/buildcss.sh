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

# Pass control back to the Dockerfile
exec "$@"
