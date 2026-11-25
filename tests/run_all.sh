#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "=== 1/2: Running Python Bridge E2E Tests (tests/test_bridge.py) ==="
python3 -m unittest discover -s tests -p "test_*.py" -v

echo ""
echo "=== 2/2: Running Real-Browser E2E Tests (tests/e2e_browser.mjs) ==="
node tests/e2e_browser.mjs

echo ""
echo "=== ALL E2E SUITES PASSED ==="

