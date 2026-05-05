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
ORIGINAL_PYPROJECT=""

cleanup_pyproject() {
    if [ -n "$ORIGINAL_PYPROJECT" ]; then
        echo "$ORIGINAL_PYPROJECT" > pyproject.toml
    fi
}

for module in "${MODULES[@]}"; do
    echo "=============================="
    echo "Module: ${module}"
    echo "=============================="

    # Clean previous mutants cache
    rm -rf mutants/ .mutmut-cache/

    # Discover test files for this module
    TEST_DIR="${module}/tests/"
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

    # Filter test files: only unit tests (not fuzz, e2e, properties, helpers, mutant_killing)
    UNIT_TESTS=()
    for tf in "${TEST_FILES[@]}"; do
        basename_tf=$(basename "$tf")
        case "$basename_tf" in
            test_fuzz.py|test_e2e*.py|test_properties.py|test_helpers.py|test_mutant_killing.py)
                continue
                ;;
            *)
                UNIT_TESTS+=("$tf")
                ;;
        esac
    done

    if [ ${#UNIT_TESTS[@]} -eq 0 ]; then
        echo "  WARNING: No unit test files found, skipping"
        continue
    fi

    echo "  Unit tests: ${UNIT_TESTS[*]}"
    TEST_CMD="python -m pytest ${UNIT_TESTS[*]} -q"

    # Save original pyproject.toml and create temp config for this module
    ORIGINAL_PYPROJECT=$(cat pyproject.toml)

    # Create temporary mutmut config
    cat > pyproject.toml <<PYEOF
[build-system]
requires = ["setuptools>=61.0"]
build-backend = "setuptools.build_meta"

[project]
name = "nyc-taxi-clickhouse-etl"
version = "0.1.0"
requires-python = ">=3.11"

[tool.mutmut]
paths_to_mutate = ["${module}"]
do_not_mutate = ["*/tests/*", "*/conftest.py", "*/test_*.py", "*/__init__.py"]
runner = "${TEST_CMD}"
exclude_dirs = ["__pycache__", ".venv", "mutants"]
PYEOF

    # Run mutmut with parallel execution
    # --max-children 4: run 4 mutants in parallel (4x+ speedup)
    echo "  Running mutmut (parallel)..."
    MUTMUT_OUTPUT=$(mktemp)
    if ! mutmut run --max-children 4 >"$MUTMUT_OUTPUT" 2>&1; then
        echo "  ERROR: mutmut run failed for ${module}"
        cat "$MUTMUT_OUTPUT"
        rm -f "$MUTMUT_OUTPUT"
        cleanup_pyproject
        FAILED_MODULES+=("$module")
        continue
    fi
    echo "  Done (output suppressed, see GitHub Actions log for details)"
    rm -f "$MUTMUT_OUTPUT"

    # Export CI stats (junitxml is faster than export-cicd-stats)
    mutmut junitxml > mutants/mutmut-results.xml 2>/dev/null || true
    mutmut export-cicd-stats 2>/dev/null || true

    # Restore original pyproject.toml
    cleanup_pyproject

    # Calculate and check score
    SCORE_OUTPUT=$(python3 -c "
import json, sys
with open('mutants/mutmut-cicd-stats.json') as f:
    d = json.load(f)
    killed = d['killed']
    total = d['killed'] + d['survived'] + d['timeout'] + d['skipped']
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
