#!/bin/sh
# Node is the container's main process. `pnpm start` runs `sh -c node …`.
# pnpm 9 forwards SIGTERM only to that shell, which exits immediately and does
# not signal node, then pnpm exits too. The shutdown grace never runs.
set -eu
cd /app/apps/api
exec node --enable-source-maps dist/index.js
