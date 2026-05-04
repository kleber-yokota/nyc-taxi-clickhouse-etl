#!/usr/bin/env bash
set -euo pipefail

# Generic quality checks CI script
# Auto-discovers modules and runs coverage, radon, xenon, cohesion, vulture.
#
# Usage:
#   ./scripts/run_quality_ci.sh
#
# Thresholds (from New.md):
#   coverage ≥ 85%
#   CC (cyclomatic complexity) < 10
#   MI (maintainability index) > 70

echo "=== Quality Checks CI ==="
echo ""

# Discover modules: directories that contain a core/ subdirectory
MODULES=()
for dir in */; do
    dir="${dir%/}"
    if [ -d "${dir}/core" ] && [ ! -d "mutants/${dir}" ]; then
        MODULES+=("$dir")
    fi
done

if [ ${#MODULES[@]} -eq 0 ]; then
    echo "No modules found (directories with core/ subdir, excluding mutants/)"
    exit 1
fi

echo "Discovered modules: ${MODULES[*]}"
echo ""

FAILED_MODULES=()

for module in "${MODULES[@]}"; do
    echo "=============================="
    echo "Module: ${module}"
    echo "=============================="

    CORE_DIR="${module}core/"
    if [ ! -d "$CORE_DIR" ]; then
        echo "  WARNING: No core/ directory in ${module}, skipping"
        continue
    fi

    # 1. Coverage (≥ 85%)
    echo "  Running coverage..."
    if ! uv run python -m coverage run --include="${module}core/*.py" -m pytest "${module}tests/" -q 2>&1; then
        echo "  ERROR: pytest failed for ${module}"
        FAILED_MODULES+=("$module")
        continue
    fi

    COVERAGE_REPORT=$(uv run python -m coverage report --show-missing 2>&1)
    COVERAGE_TOTAL=$(echo "$COVERAGE_REPORT" | grep "TOTAL" | awk '{print $NF}' | tr -d '%')

    echo "  Coverage: ${COVERAGE_TOTAL}%"

    COVERAGE_BELOW=$(python3 -c "print(1 if float('${COVERAGE_TOTAL:-0}') < 85 else 0)")
    if [ "$COVERAGE_BELOW" -eq 1 ]; then
        echo "  FAIL: Coverage ${COVERAGE_TOTAL}% is below 85% threshold"
        FAILED_MODULES+=("$module")
    else
        echo "  PASS: Coverage ${COVERAGE_TOTAL}% >= 85%"
    fi

    # Clean coverage artifacts
    rm -rf .coverage .coverage.* .pytest_cache/ __pycache__/

    # 2. Radon - Cyclomatic Complexity (< 10 average)
    echo "  Running radon cc..."
    RADON_CC=$(uv run radon cc "${CORE_DIR}" -a -nb 2>&1 || true)
    RADON_CC_AVG=$(echo "$RADON_CC" | grep "Average" | head -1 | awk '{print $NF}' || echo "0")
    echo "  Radon CC average: ${RADON_CC_AVG}"

    # 3. Radon - Maintainability Index (> 70)
    echo "  Running radon mi..."
    RADON_MI=$(uv run radon mi "${CORE_DIR}" -nb 2>&1 || true)
    RADON_MI_AVG=$(echo "$RADON_MI" | grep -i "average" | head -1 | awk '{print $NF}' || echo "0")
    echo "  Radon MI average: ${RADON_MI_AVG}"

    # 4. Xenon (max-absolute B, max-modules B, max-average A)
    echo "  Running xenon..."
    if ! uv run xenon --max-absolute B --max-modules B --max-average A "${CORE_DIR}" 2>&1; then
        echo "  WARNING: Xenon gates failed for ${module}"
    else
        echo "  PASS: Xenon gates passed"
    fi

    # 5. Cohesion
    echo "  Running cohesion..."
    if ! uv run cohesion -d "${CORE_DIR}" 2>&1; then
        echo "  WARNING: Cohesion check failed for ${module}"
    else
        echo "  PASS: Cohesion check passed"
    fi

    # 6. Vulture (dead code)
    echo "  Running vulture..."
    VULTURE_OUTPUT=$(uv run vulture "${CORE_DIR}" 2>&1 || true)
    VULTURE_COUNT=$(echo "$VULTURE_OUTPUT" | wc -l | tr -d ' ')
    if [ "$VULTURE_COUNT" -gt 0 ] && [ -n "$VULTURE_OUTPUT" ]; then
        echo "  Vulture: ${VULTURE_COUNT} dead code items found"
        echo "$VULTURE_OUTPUT" | head -10
    else
        echo "  PASS: No dead code found"
    fi

   # 7. LCOM (class cohesion)
    echo "  Running LCOM analysis..."
    LCOM_OUTPUT=$(uv run python scripts/lcom.py "${CORE_DIR}" 2 2>&1 || true)
    LCOM_CLASSES=$(echo "$LCOM_OUTPUT" | grep -E "^\s+/.*\.(py):" || true)
    if [ -n "$LCOM_CLASSES" ]; then
        echo "$LCOM_CLASSES" | sed 's/^/    /'
    fi
    LCOM_FAIL=$(echo "$LCOM_OUTPUT" | grep -c "FAIL:" || true)
    if [ "$LCOM_FAIL" -gt 0 ]; then
        echo "  FAIL: Classes exceed LCOM threshold of 2"
        FAILED_MODULES+=("$module")
    else
        echo "  PASS: All classes within LCOM threshold"
    fi

    echo ""
done

echo "=============================="
echo "Summary"
echo "=============================="

if [ ${#FAILED_MODULES[@]} -eq 0 ]; then
    echo "All modules passed quality checks"
    exit 0
else
    echo "FAILED modules: ${FAILED_MODULES[*]}"
    exit 1
fi
