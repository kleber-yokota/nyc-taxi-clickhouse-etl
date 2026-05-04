#!/usr/bin/env bash
set -euo pipefail

# Generic mutation testing CI script
# Auto-discovers modules and runs mutmut on each one.
#
# Usage:
#   ./scripts/run_mutation_ci.sh [threshold]
#
# Arguments:
#   threshold  Minimum mutation score percentage (default: 85)

THRESHOLD="${1:-85}"

echo "=== Mutation Testing CI ==="
echo "Threshold: ${THRESHOLD}%"
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

    # Clean previous mutants
    rm -rf mutants/

    # Discover test files for this module
    TEST_DIR="${module}tests/"
    if [ ! -d "$TEST_DIR" ]; then
        echo "  WARNING: No tests/ directory in ${module}, skipping"
        continue
    fi

    # Find test files (test_*.py), excluding __init__.py and conftest.py
    TEST_FILES=()
    while IFS= read -r -d '' f; do
        basename_f=$(basename "$f")
        if [[ "$basename_f" == test_*.py ]]; then
            TEST_FILES+=("$f")
        fi
    done < <(find "$TEST_DIR" -name "test_*.py" -type f -print0)

    if [ ${#TEST_FILES[@]} -eq 0 ]; then
        echo "  WARNING: No test files found in ${TEST_DIR}, skipping"
        continue
    fi

    echo "  Test files: ${TEST_FILES[*]}"

    # Build pytest command
    TEST_CMD="python -m pytest ${TEST_FILES[*]} -q"

    # Run mutmut
    echo "  Running mutmut..."
    if ! mutmut run --paths-to-mutate="${module}" --runner="${TEST_CMD}" 2>&1; then
        echo "  ERROR: mutmut run failed for ${module}"
        FAILED_MODULES+=("$module")
        continue
    fi

    # Export CI stats
    mutmut export-cicd-stats

    # Calculate and check score
    SCORE_OUTPUT=$(python3 -c "
import json, sys
with open('mutmut-cicd-stats.json') as f:
    d = json.load(f)['result_counts']
killed = d['killed']
total = d['killed'] + d['success'] + d['timeout'] + d['skipped']
score = killed / total * 100 if total else 0
print(f'{score:.1f}')
print(f'  Killed: {killed}/{total}')
")

    SCORE=$(echo "$SCORE_OUTPUT" | head -1)
    DETAILS=$(echo "$SCORE_OUTPUT" | tail -n +2)

    echo "  Mutation score: ${SCORE}%"
    echo "$DETAILS"

    # Check threshold
    BELOW=$(python3 -c "print(1 if float('${SCORE}') < ${THRESHOLD} else 0)")
    if [ "$BELOW" -eq 1 ]; then
        echo "  FAIL: Score ${SCORE}% is below ${THRESHOLD}% threshold"
        FAILED_MODULES+=("$module")
    else
        echo "  PASS: Score ${SCORE}% >= ${THRESHOLD}%"
    fi
    echo ""
done

echo "=============================="
echo "Summary"
echo "=============================="

if [ ${#FAILED_MODULES[@]} -eq 0 ]; then
    echo "All modules passed mutation testing (>= ${THRESHOLD}%)"
    exit 0
else
    echo "FAILED modules: ${FAILED_MODULES[*]}"
    exit 1
fi
