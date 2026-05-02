# Python Testing Guide — pytest, hypothesis, mutmut & atheris

## Table of Contents

- [1. Setup](#1-setup)
- [2. Configuration](#2-configuration)
- [3. pytest](#3-pytest)
- [4. hypothesis](#4-hypothesis)
- [5. mutmut (Mutation Testing)](#5-mutmut-mutation-testing)
- [6. atheris (Fuzz Testing)](#6-atheris-fuzz-testing)
- [7. E2E Testing](#7-e2e-testing)
- [8. Workflow](#8-workflow)
- [9. FAQ — Errors and Solutions](#9-faq--errors-and-solutions)

---

## 1. Setup

### Create virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate        # Linux / macOS
# .venv\Scripts\activate         # Windows
```

### Install dependencies

```bash
pip install -e .[test]
```

> **CRITICAL:** The `-e` (editable) flag is **mandatory**. Without it:
> - `atheris` cannot import the package
> - `mutmut` copies the project to `mutants/` but imports fail
> - `pip install` fails with "Multiple top-level packages discovered"

### Required packages

| Package | Version | Purpose |
|---------|---------|---------|
| pytest | >=7.0 | Unit and parametrized tests |
| hypothesis | >=6.0 | Property-based testing (auto-generates inputs) |
| mutmut | >=3.0 | Mutation testing (mutates code, verifies tests catch it) |
| atheris | >=3.0 | Fuzz testing (Google's libFuzzer for Python) |
| coverage | >=7.0 | Code coverage (used by mutmut) |
| testcontainers | >=4.5 | Docker containers for E2E (MinIO) |
| responses | >=0.23 | HTTP mocking for E2E |

### Multiple packages in one project

```
project/
├── package_a/
│   └── tests/
├── package_b/
│   └── tests/
```

**Rules:**
1. `pip install -e .` installs ALL packages
2. pytest supports multiple `testpaths`
3. **mutmut runs ONE package at a time** — change config between runs
4. atheris runs standalone per package

**How to run mutmut across multiple packages:**

```bash
# Save separate configs
cp setup.cfg setup.cfg.pkg_a
cp setup.cfg setup.cfg.pkg_b

# Switch config for package_a
sed -i 's|paths_to_mutate = .*/|paths_to_mutate = package_a/|' setup.cfg.pkg_a
sed -i 's|tests_dir = .*/|tests_dir = package_a/tests/|' setup.cfg.pkg_a
cp setup.cfg.pkg_a setup.cfg && rm -rf mutants/ && python -m mutmut run

# Switch config for package_b
sed -i 's|paths_to_mutate = .*/|paths_to_mutate = package_b/|' setup.cfg.pkg_b
sed -i 's|tests_dir = .*/|tests_dir = package_b/tests/|' setup.cfg.pkg_b
cp setup.cfg.pkg_b setup.cfg && rm -rf mutants/ && python -m mutmut run
```

### Common setup errors

| Error | Cause | Solution |
|-------|-------|---------|
| `ModuleNotFoundError` | Package not installed | `pip install -e .` |
| `Multiple top-level packages discovered` | `mutants/` in project root | Add `mutants*` to `exclude` in `setup.cfg` |
| `BackendUnavailable: Cannot import 'setuptools.backends._legacy'` | Wrong build-backend in `pyproject.toml` | Use `build-backend = "setuptools.build_meta"` |
| `RuntimeError: context has already been set` | mutmut fork + hypothesis/atheris spawn conflict | Patch mutmut or use `MUTMUT_TESTING=1` |

---

## 2. Configuration

### setup.cfg

```ini
[metadata]
name = <package>
version = 0.1.0

[options]
packages = find:
python_requires = >=3.11

[options.packages.find]
exclude =
    mutants*

[mutmut]
paths_to_mutate = <package>/
do_not_mutate =
    **/__init__.py
    **/test_*.py
    **/conftest.py
    **/exceptions.py
    **/config.py
tests_dir = <package>/tests/

[tool:pytest]
testpaths = <package>/tests/
addopts = --ignore=<package>/tests/test_fuzz.py
```

### pyproject.toml

```ini
[build-system]
requires = ["setuptools>=61.0"]
build-backend = "setuptools.build_meta"

[project]
name = "<package>"
requires-python = ">=3.11"

[project.optional-dependencies]
test = [
    "pytest>=7.0",
    "hypothesis>=6.0",
    "mutmut>=3.0",
    "atheris>=3.0",
    "coverage>=7.0",
]

[tool.pytest.ini_options]
testpaths = ["<package>/tests"]
addopts = ["--ignore=<package>/tests/test_fuzz.py"]

[tool.hypothesis]
max_examples = 30
deadline = 2000
```

### Configuration rules

| File | Responsible for |
|------|-----------------|
| `setup.cfg` | mutmut config + pytest config (`[tool:pytest]`) |
| `pyproject.toml` | Build system + hypothesis config (`[tool.hypothesis]`) |

> **IMPORTANT:** mutmut reads **only** `setup.cfg`. If pytest config is only in `pyproject.toml`, mutmut hangs or runs very slowly.

### Common configuration errors

| Error | Cause | Solution |
|-------|-------|---------|
| `No interesting inputs were found` (mutmut) | `tests_dir` not set | Add `tests_dir = <package>/tests/` in `setup.cfg` |
| `WARNING: ignoring pytest config in setup.cfg` | Config duplicated | Keep pytest config in `setup.cfg` |
| `[pytest] section no longer supported` | Old `[pytest]` syntax | Change to `[tool:pytest]` |
| mutmut stuck on "Running stats" spinner | hypothesis with 200 examples per test | Reduce to `max_examples = 30` in `pyproject.toml` |
| mutmut scans wrong package | `paths_to_mutate` not switched | Verify configs before running mutmut |

---

## 3. pytest

### Commands

```bash
# All tests (except fuzz)
python -m pytest <package>/tests/ -v --ignore=<package>/tests/test_fuzz.py

# Only unit tests
python -m pytest <package>/tests/test_core.py -v

# Only property tests
python -m pytest <package>/tests/test_properties.py -v

# Verbose with full output
python -m pytest <package>/tests/ -v -s
```

### Test patterns

```python
# Basic test
def test_function_positive():
    assert func(1, 2) == 3

# Floats use pytest.approx
def test_function_with_floats():
    assert func(1.1, 2.2) == pytest.approx(3.3)

# Parametrization
@pytest.mark.parametrize("a, b, expected", [
    (1, 2, 3),
    (-1, 1, 0),
])
def test_function_parametrized(a, b, expected):
    assert func(a, b) == pytest.approx(expected, rel=1e-9)

# Exceptions with exact regex match
def test_function_raises():
    with pytest.raises(ValueError, match="^Cannot divide by zero$"):
        divide(10, 0)
```

### Common pytest errors

| Error | Cause | Solution |
|-------|-------|---------|
| `function uses no argument 'a'` | Test signature doesn't match `@parametrize` | Use `def test(a, b, expected)` instead of `def test(fixture)` |
| Float comparison fails | `1.1 + 2.2 != 3.3` | Use `pytest.approx(expected, rel=1e-6, abs=1e-6)` |
| `match` doesn't catch mutated error | Regex without anchors | Use `match="^exact message$"` |
| Imported `MagicMock` but not `patch` | Partial import | `from unittest.mock import MagicMock, patch` |

---

## 4. hypothesis

### Commands

```bash
# Property tests
python -m pytest <package>/tests/test_properties.py -v

# More examples (debug)
HYPOTHESIS_MAX_EXAMPLES=100 python -m pytest test_properties.py -v

# Treat health checks as errors
python -m pytest test_properties.py -v -W error::hypothesis.errors.FailedHealthCheck
```

### Basic strategies

```python
from hypothesis import given, settings
from hypothesis import strategies as st

# Integers without limits
integers = st.integers()

# Floats with limits (prevents overflow)
floats = st.floats(
    min_value=-1e15, max_value=1e15,
    allow_nan=False, allow_infinity=False
)

# "Safe" integers for ops with large floats
safe_integers = st.integers(min_value=-100000, max_value=100000)

# Limited binary data
binary_small = st.binary(min_size=0, max_size=500)

# Tuples (NEVER st.builds(tuple, ...))
tuple_strategy = st.tuples(
    st.sampled_from(DATA_TYPES),
    st.sampled_from(list(VALID_YEARS)),
    st.sampled_from(list(VALID_MONTHS)),
)

# Limited dictionaries
dict_strategy = st.dictionaries(
    keys=st.text(max_size=15),
    values=st.text(max_size=15),
    max_size=3
)
```

### Property test examples

```python
# Commutativity
@given(a=numeric, b=numeric)
def test_add_commutative(a, b):
    assert add(a, b) == add(b, a)

# Associativity (with tolerance)
@given(a=numeric, b=numeric, c=numeric)
def test_add_associative(a, b, c):
    left = add(add(a, b), c)
    right = add(a, add(b, c))
    assert left == pytest.approx(right, rel=1e-6, abs=1e-6)

# Identity
@given(a=numeric)
def test_add_identity(a):
    assert add(a, 0) == a

# Distributivity (use safe_integers)
@given(a=safe_integers, b=safe_integers, c=safe_integers)
def test_multiply_distributes_over_add(a, b, c):
    assert multiply(a, add(b, c)) == add(multiply(a, b), multiply(a, c))
```

### ⚠️ hypothesis errors and how to fix them

#### 1. `st.builds(tuple, ...)` crashes

```python
# WRONG
_url_valid_strategy = st.builds(
    tuple, data_type=st.sampled_from(DATA_TYPES), year=..., month=...
)
# TypeError: tuple() takes no keyword arguments

# CORRECT
_url_valid_strategy = st.tuples(
    st.sampled_from(DATA_TYPES),
    st.sampled_from(list(VALID_YEARS)),
    st.sampled_from(list(VALID_MONTHS)),
)
```

#### 2. Too much `assume()` causes `FailedHealthCheck`

```
hypothesis.errors.FailedHealthCheck: 0 inputs were generated, 50 filtered out.
```

```python
# WRONG — assume rejects 95% of inputs
@given(data_type=st.one_of(st.sampled_from(DATA_TYPES), st.text()), year=...)
def test(data_type, year):
    assume(data_type in DATA_TYPES and year in VALID_YEARS)

# CORRECT — separate into specific tests
_invalid_data_type = st.text(min_size=1, max_size=20).filter(
    lambda x: x not in DATA_TYPES
)

_invalid_year = st.integers(min_value=1900, max_value=2019) | \
               st.integers(min_value=2027, max_value=9999)

@given(data_type=_invalid_data_type, year=st.sampled_from(list(VALID_YEARS)))
def test_rejects_invalid_data_type(data_type, year):
    with pytest.raises(ValueError, match="Invalid data_type"):
        ...
```

#### 3. `b"\x00"` is falsy in Python

```python
# WRONG — b'\x00' is falsy, treated as empty
@given(data=st.binary(min_size=1))
def test_upload_valid(data):
    uploader.upload(data, "bucket", "key")  # Fails with b'\x00'

# CORRECT — min_size=2 avoids b'\x00' and b''
@given(data=st.binary(min_size=2))
def test_upload_valid(data):
    ...
```

#### 4. Tests with boto3 hang (no mock)

```python
# WRONG — connects to real S3, hangs
@given(metadata=st.dictionaries(...))
def test_upload(metadata):
    uploader.upload(data, "bucket", "key", metadata=metadata)

# CORRECT — mock boto3
@given(metadata=st.dictionaries(...))
def test_upload(metadata):
    with patch("package.s3_uploader.boto3.client") as mock_boto:
        mock_boto.return_value.put_object.return_value = {"ETag": '"test"'}
        uploader.upload(data, "bucket", "key", metadata=metadata)
```

#### 5. Non-`@given` test receives `@given` arguments

```
got an unexpected keyword argument 'timeout'
```

```python
# WRONG — simple test after one with @given
@given(timeout=st.integers(min_value=1, max_value=600))
def test_default_timeout():  # Doesn't use timeout!
    assert downloader.timeout == DEFAULT_TIMEOUT

# CORRECT — remove @given
def test_default_timeout():
    assert downloader.timeout == DEFAULT_TIMEOUT
```

#### 6. Empty strings in `or` of constructor

```python
# WRONG — empty strings are falsy: "" or default → default
@given(endpoint=st.text(min_size=0))
def test_endpoint(endpoint):
    uploader = S3Uploader(endpoint_url=endpoint)

# CORRECT — min_size=1
@given(endpoint=st.text(min_size=1))
def test_endpoint(endpoint):
    ...
```

#### 7. hypothesis too slow (>60s)

```python
# WRONG — generates data too large
@settings(max_examples=100)
@given(data=st.binary(max_size=10000))
def test(data):
    with open("output.bin", "wb") as f:
        f.write(data)  # Slow I/O!

# CORRECT — reduce max_examples and sizes
@settings(max_examples=30, deadline=None)
@given(data=st.binary(max_size=500))
def test(data):
    process_in_memory(data)
```

#### 8. Float overflow: `1.79e+308`

```python
# WRONG — st.floats() without limits generates ±1.8e+308
floats = st.floats()

# CORRECT — limit the range
floats = st.floats(min_value=-1e15, max_value=1e15, allow_nan=False, allow_infinity=False)
```

#### 9. `Cannot assign hypothesis.settings.max_examples`

```python
# WRONG — trying to set globally in conftest
settings.max_examples = 30  # AttributeError!

# CORRECT — use pyproject.toml
# [tool.hypothesis]
# max_examples = 30
```

#### 10. `unexpected keyword argument 'b'`

```python
# WRONG — @given defines 'b' but body doesn't use it
@given(a=numeric, b=numeric)
def test_self_zero(a):
    assert func(a, a) == 0

# CORRECT
@given(a=numeric)
def test_self_zero(a):
    assert func(a, a) == 0
```

### ⚠️ Never generate large files in hypothesis tests

```python
# WRONG — Hypothesis can generate huge files per example
@given(content=st.binary(min_size=0, max_size=1_048_576))
def test_write_to_file(content):
    with open("test_output.bin", "wb") as f:
        f.write(content)  # 30 examples × up to 1MB = 30MB of disk writes!

# EVEN WORSE — no size limit at all
@given(content=st.binary())
def test_write_to_file(content):
    with open("test_output.bin", "wb") as f:
        f.write(content)  # Hypothesis can generate GBs!

# WRONG — writing to disk in every example
@given(data=st.binary(max_size=10000))
def test_file_operation(data):
    with open("temp.bin", "wb") as f:
        f.write(data)  # Slow I/O, high disk usage
    with open("temp.bin", "rb") as f:
        result = process_file(f.read())

# CORRECT — process in memory
@given(content=st.binary(min_size=0, max_size=1024))
def test_in_memory(content):
    process(content)  # No disk I/O, fast

# CORRECT — use tempfile if file operations are required
import tempfile

@given(content=st.binary(min_size=0, max_size=1024))
def test_file_operation(content):
    with tempfile.NamedTemporaryFile(delete=True) as tmp:
        tmp.write(content)
        tmp.flush()
        # test file operation here
```

**Why large files are bad in hypothesis:**
- **Disk I/O is slow** — writing 10KB per example × 30 examples = 300KB of writes
- **High disk usage** — tests generating megabytes/gigabytes of temporary data
- **CI/CD failures** — disk full in CI environments
- **Test timeouts** — I/O adds seconds per example, making hypothesis timeout

**Size limits by test type:**

| Test type | Max size | Why? |
|-----------|----------|------|
| Logic tests | 1KB | Enough to test conditions |
| Edge case tests | 10KB | Edge cases like 1 byte, 100 bytes |
| Performance tests | 100KB | Only if measuring throughput |
| **Never** | **no limit** | **Can generate GBs!** |

**Checklist:**
- [ ] All `st.binary()` have `max_size` defined?
- [ ] All `st.text()` have `max_size` defined?
- [ ] No `st.binary()` without limit (can generate GBs)
- [ ] File tests use `tempfile` with `delete=True`?
- [ ] Prefer in-memory processing over disk I/O?

### Performance rules for hypothesis

| Config | Value | Why? |
|--------|-------|------|
| `max_examples` | 30 | 200 is too slow for mutmut. 30 detects property failures |
| `deadline` | 2000ms | Prevents infinite loops |
| `st.floats` | `min_value=-1e15, max_value=1e15` | Avoids float64 overflow |
| `st.binary` | `max_size=500` | Avoids I/O and memory usage |
| `st.text` | `max_size=100` | Short strings are enough for logic validation |

---

## 5. mutmut (Mutation Testing)

### Commands

```bash
# Run mutation testing (cleans and runs)
rm -rf mutants/ && python -m mutmut run

# Show saved results
python -m mutmut results

# Show diff of a specific mutant
python -m mutmut show <mutant-key>

# Apply mutant to disk
python -m mutmut apply <mutant-key>

# Reset results
rm -rf mutants/
```

### Understanding the output

```
10/10  🎉 10 🫥 0  ⏰ 0  🤔 0  🙁 0  🔇 0  🧙 0
287.01 mutations/second
```

| Symbol | Status | Meaning |
|--------|--------|---------|
| 🎉 | **Killed** | Tests detected the mutation (good!) |
| 🙁 | **Survived** | Mutant passed — tests failed to detect it (bad) |
| ⏰ | **Timeout** | Test took too long — possible infinite loop |
| 🔇 | **Ignored** | Mutant was excluded or not applicable |
| 🧙 | **Unmodified** | No mutations generated for this code |

**Score = Killed / (Killed + Survived + Timeout)**

The score represents how effective your test suite is at catching code changes. mutmut generates small mutations in the source code (e.g., `+` → `-`, `==` → `!=`, `True` → `False`) and runs your tests. If all tests pass after a mutation, that mutant "survived" — meaning your tests failed to detect the change. If tests fail, the mutant was "killed" — your tests caught it.

A high score means your tests are thorough and would catch accidental bugs. A low score means your tests have gaps.

### Minimum score: 80%

The minimum acceptable mutation score is **80%**.

| Score | Classification | Action |
|-------|----------------|--------|
| >90% | Excellent | Nothing to do |
| 80-90% | Great | Review remaining survived |
| 70-80% | Acceptable but not enough | Write tests for uncovered paths |
| <70% | Fail | Significant test gaps — stop and fix |

Why 80%? Below that threshold, there are too many mutations that slip through, meaning real bugs introduced by code changes would not be caught by your test suite.

### Exclude files from mutation

```ini
[mutmut]
do_not_mutate =
    **/__init__.py        # Only imports, no logic
    **/test_*.py          # Don't mutate tests
    **/conftest.py        # Fixtures
    **/exceptions.py      # Exception definitions
    **/config.py          # Hardcoded values
```

```python
# Exclude a single line
version = "1.0.0"  # pragma: no mutate

# Exclude entire block
def complex_algorithm():  # pragma: no mutate block
    return heavy_calculation()

# Exclude a region
# pragma: no mutate start
skip_this()
# pragma: no mutate end
```

> **IMPORTANT:** Pragma must be on the **same line** as the definition.

### ⚠️ mutmut errors and how to fix them

#### 1. `RuntimeError: context has already been set` (fork vs spawn)

```
File "mutmut/__main__.py", line 1152: set_start_method('fork')
RuntimeError: context has already been set
```

```python
# CAUSE: mutmut + hypothesis/atheris both use multiprocessing
# mutmut calls set_start_method('fork') at module level

# SOLUTION 1: Patch mutmut (temporary)
sed -i "1152s/.*/# set_start_method('fork')/" \
  /path/to/mutmut/__main__.py

# SOLUTION 2: Run mutmut in a clean container (no hypothesis import)
# SOLUTION 3: Use environment variable
MUTMUT_TESTING=1 python -m mutmut run
```

#### 2. `test_fuzz.py` imported by mutmut

```
ERROR tlc_downloader/tests/test_fuzz.py
atheris.instrument_imports()
```

```ini
# SOLUTION: ignore in pyproject.toml and setup.cfg
[tool:pytest]
addopts = --ignore=<package>/tests/test_fuzz.py
```

#### 3. mutmut hangs on e2e tests

```
# SOLUTION: ignore e2e tests
[tool:pytest]
addopts = --ignore=<package>/tests/test_e2e.py \
          --ignore=<package>/tests/test_e2e_errors.py
```

#### 4. mutmut stuck on "Running stats" spinner

```
# SOLUTION: reduce max_examples to 30 in pyproject.toml
[tool.hypothesis]
max_examples = 30
```

#### 5. `paths_to_mutate` without trailing slash

```
Could not figure out where the code to mutate is
```

```ini
# WRONG
paths_to_mutate = package

# CORRECT — with trailing slash
paths_to_mutate = package/
```

#### 6. `pip install -e .` fails with "Multiple top-level packages"

```
error: Multiple top-level packages discovered: ['mutants', 'src']
```

```ini
# SOLUTION: add mutants to exclude
[options.packages.find]
exclude =
    mutants*
```

### How to analyze survived mutants

```bash
# See which survived
python -m mutmut results

# See which tests cover a mutant
python -m mutmut tests-for-mutant <package>.<module>.<method>__mutmut_<N>

# See mutated code
grep -A 30 "def xǁ<ClassName>ǁ<method>__mutmut_<N>" mutants/<package>/<module>.py
```

### Strategy to increase score

1. **Return type tests** — `assert type(x) is bool`, `assert isinstance(x, list)`
2. **Cover all error codes** — each `error_code == "X"` needs a dedicated test
3. **Test edge cases** — exact boundary values (`MAX_FILE_SIZE`, `1 byte`, empty data)
4. **Test private methods** — `_validate_data`, `_build_extra_args`, etc.
5. **Exclude files without logic** — `config.py`, `exceptions.py`, `__init__.py`

### Expected score

| Score | Classification | Action |
|-------|----------------|--------|
| >90% | Excellent | Nothing to do |
| 80-90% | Great | Review remaining survived |
| 70-80% | Below minimum | Write tests for uncovered paths |
| <70% | Fail | Significant test gaps — must fix |

---

## 6. atheris (Fuzz Testing)

### Commands

```bash
# Run fuzzing
python <package>/tests/test_fuzz.py -runs=10000

# With existing corpus
python <package>/tests/test_fuzz.py corpus_dir/ -runs=10000

# With coverage
python -m coverage run <package>/tests/test_fuzz.py -runs=10000
python -m coverage html
```

### Basic structure

```python
import sys
import atheris

# IMPORTS MUST BE INSIDE instrument_imports — CRITICAL
with atheris.instrument_imports(include=['<package>']):
    from <package>.core import func

@atheris.instrument_func
def TestOneInput(data):
    fdp = atheris.FuzzedDataProvider(data)

    try:
        x = fdp.ConsumeIntInRange(-1000000, 1000000)
        func(x)
    except (ZeroDivisionError, OverflowError, ValueError):
        return  # Catches exceptions, doesn't crash

    try:
        y = fdp.ConsumeRegularFloat()
        func(y)
    except (ZeroDivisionError, OverflowError, ValueError):
        return

atheris.Setup(sys.argv, TestOneInput)
atheris.Fuzz()
```

### FuzzedDataProvider API (v3)

| Method | Generates |
|--------|-----------|
| `ConsumeIntInRange(min, max)` | Random integer |
| `ConsumeRegularFloat()` | Float without NaN/Inf |
| `ConsumeProbability()` | Float in [0, 1] |
| `ConsumeFloatList(count)` | List of floats |
| `ConsumeString(count)` | Random string |
| `ConsumeBool()` | True or False |
| `ConsumeBytes(size)` | Random bytes |

### ⚠️ atheris errors and how to fix them

#### 1. API changed in v3 — old methods don't exist

```
AttributeError: 'FuzzedDataProvider' object has no attribute 'Choose'
AttributeError: 'FuzzedDataProvider' object has no attribute 'ConsumeBytesOfMinimumSize'
TypeError: ConsumeUnicodeNoSurrogates(): incompatible function arguments
```

```python
# WRONG (old example)
url = fdp.ConsumeUnicodeNoSurrogates(10, 200)  # 2 args — doesn't exist in v3
data = fdp.Choose(list_of_values)
header = fdp.ConsumeBytesOfMinimumSize(10)

# CORRECT (v3)
url = fdp.ConsumeUnicodeNoSurrogates(100)  # 1 arg only
data = list_of_values[fdp.ConsumeIntInRange(0, len(list_of_values) - 1)]
header = fdp.ConsumeBytes(10)
```

#### 2. `ModuleNotFoundError: No module named '<package>'`

```
# SOLUTION: pip install -e .  (ALWAYS run before atheris)
```

#### 3. `No interesting inputs were found`

```
# SOLUTION 1: Ensure proper instrumentation
with atheris.instrument_imports(include=['<package>']):
    from <package>.core import func

# SOLUTION 2: Decorate the test function
@atheris.instrument_func
def TestOneInput(data):
    ...

# SOLUTION 3: Check include is correct (don't instrument boto3, urllib3)
```

#### 4. Tests too slow (< 10 exec/s)

```
# SOLUTION 1: Remove file I/O inside TestOneInput
# WRONG
def TestOneInput(data):
    with open("debug.txt", "a") as f:
        f.write(str(data))  # Slow!

# CORRECT — only in-memory processing
def TestOneInput(data):
    fdp = atheris.FuzzedDataProvider(data)
    x = fdp.ConsumeIntInRange(0, 100)

# SOLUTION 2: Remove network calls (requests, boto3)
# WRONG
def TestOneInput(data):
    response = requests.get("http://example.com")  # Hangs!

# SOLUTION 3: Use include=['<package>'] to avoid instrumenting libraries
```

#### 5. Crashes — `Uncaught Python exception`

```
==123== ERROR: fuzzer crashed with signal 11
Uncaught Python exception: RuntimeError: Boom
```

```
# SOLUTION: This is a BUG found by fuzzing!
# 1. Save the crash input: cp corpus/fuzzing/crash-* crash_inputs/
# 2. Reproduce: TestOneInput(open("crash_inputs/input_000042").read())
# 3. Fix the bug in the code
```

### Metrics to evaluate fuzzing

| Metric | Excellent | Good | Attention | Poor |
|--------|-----------|------|-----------|------|
| Exec/s | >100k | 10k-100k | 1k-10k | <1k |
| Coverage | >90% | 70-90% | 50-70% | <50% |
| Corpus | >500 files | 100-500 | <100 | <10 |
| Crashes | 0 | 1-2 (fixed) | >5 | Constant |

### Saturation curve

**Healthy curve:**
```
Cov: 17 |                                    ■
        |                           ■
Cov: 16 |                    ■
        |          ■
Cov: 15 |   ■
Cov: 14 |
Cov: 13 |■
        +----------------------------
         0s   20s   40s   60s
```

**Unhealthy curve (problem):**
```
Cov: 17 |
Cov: 16 |
Cov: 15 |
Cov: 14 |
Cov: 13 |■──────────────────────────
        +----------------------------
         0s   20s   40s   60s
```

---

## 7. E2E Testing

### Structure

```bash
# Run E2E tests (requires Docker)
python -m pytest <package>/tests/test_e2e.py \
                 <package>/tests/test_e2e_errors.py -v
```

### Mock boto3 correctly

```python
# WRONG — mock doesn't return the configured object
with patch("package.s3_uploader.boto3.client") as mock_boto:
    mock_instance = MagicMock()
    mock_instance.put_object.side_effect = error
    # boto3.client() returns mock_boto(), not mock_instance!

# CORRECT — set return_value
with patch("package.s3_uploader.boto3.client") as mock_boto:
    mock_instance = MagicMock()
    mock_boto.return_value = mock_instance  # ← CRITICAL
```

### VCR vs responses

| | VCR | responses |
|---|-----|-----------|
| Binary data | ❌ Bug with body str/bytes | ✅ Handles bytes natively |
| JSON | ✅ Good | ✅ Good |
| Config | YAML cassettes | `@responses.activate` decorator |
| E2E with parquet | ❌ Don't use | ✅ Use |

```python
# WRONG with VCR and binary data
# VCR serializes body.string as str in YAML, requests expects bytes

# CORRECT — use responses for binary data
responses.add(
    responses.GET,
    url,
    body=parquet_content,  # bytes directly
    status=200,
    content_type="application/octet-stream",
)
```

### MinIO via testcontainers

```python
@pytest.fixture(scope="module")
def minio_endpoint():
    with MinioContainer(
        image="minio/minio:latest",
        access_key="minioadmin",
        secret_key="minioadmin",
    ) as mc:
        host = mc.get_container_host_ip()
        port = mc.get_exposed_port(mc.port)
        yield f"http://{host}:{port}"
```

> Use `scope="module"` — container starts once, not per test.

### Autouse fixture for mocking

```python
@pytest.fixture(autouse=True)
def mock_tlc_api():
    """Auto-use fixture to mock all TLC CDN requests."""
    with responses.RequestsMock() as rsps:
        for url, (body, status) in PARQUET_MAP.items():
            rsps.add(responses.GET, url, body=body, status=status)
        yield rsps
```

---

## 8. Workflow

### Recommended order

```bash
# 1. Write and run unit tests (pytest)
python -m pytest <package>/tests/ -v --ignore=<package>/tests/test_fuzz.py

# 2. Add property tests (hypothesis)
python -m pytest <package>/tests/test_properties.py -v

# 3. Run mutation testing to validate coverage
rm -rf mutants/ && python -m mutmut run

# 4. Fuzz for edge cases and unexpected inputs
python <package>/tests/test_fuzz.py -runs=10000

# 5. E2E tests (if available)
python -m pytest <package>/tests/test_e2e.py -v
```

### How tools complement each other

| Tool | Tests | Doesn't test |
|------|-------|--------------|
| **pytest** | Specific cases, types, exceptions | Generalized behavior |
| **hypothesis** | Mathematical properties (commutativity, associativity) | Specific edge cases |
| **mutmut** | Whether tests detect code changes | Whether code is correct |
| **atheris** | Unexpected inputs, NaN, overflow, edge cases | Business-specific logic |
| **E2E** | Infrastructure integration (S3, external APIs) | Isolated unit tests |

### Coverage thresholds

Every source file and the total must meet minimum coverage requirements:

| Scope | Minimum coverage |
|---|---|
| **Each individual source file** | ≥85% |
| **Total (all source files combined)** | >85% |

Source files are the `*.py` files under the package's `core/` directory. Test files, `__init__.py`, `conftest.py`, and `exceptions.py` are excluded from the per-file check (they can have lower coverage since they're scaffolding).

To verify per-file coverage:

```bash
# Run tests with coverage on source files only
python -m coverage run --include='<package>/core/*.py' -m pytest <package>/tests/ --ignore=<package>/tests/test_fuzz.py -q

# Show per-file coverage
python -m coverage report --show-missing
```

Example output:

```
Name                              Stmts   Miss  Cover   Missing
-------------------------------------------------------------
extract/core/__init__.py              0      0   100%
extract/core/catalog.py              24      0   100%
extract/core/downloader.py           94      7    93%   53, 60, 95, 111-112, 129, 135
extract/core/interrupt.py            30      1    97%   36
extract/core/state.py                51      0   100%
extract/core/state_manager.py        60      1    98%   41
-------------------------------------------------------------
TOTAL                               259      9    97%
```

Any file below 85% must be fixed — add missing test paths, mark with `# pragma: no cover` only when justified, or refactor to remove dead code.

### Expected results

| Tool | Expected result |
|------|-----------------|
| pytest | 100% passed |
| hypothesis | 100% passed |
| mutmut | ≥80% killed |
| atheris | 0 crashes |
| E2E | 100% passed |
| coverage (per-file) | ≥85% each |
| coverage (total) | >85% |

---

## 9. FAQ — Errors and Solutions

### Setup

| Error | Cause | Solution |
|-------|-------|---------|
| `ModuleNotFoundError` | Package not installed | `pip install -e .` |
| `Multiple top-level packages discovered` | `mutants/` in project root | Add `mutants*` to `exclude` in `setup.cfg` |
| `BackendUnavailable` | Wrong build-backend | Use `build-backend = "setuptools.build_meta"` |
| `RuntimeError: context has already been set` | mutmut fork + hypothesis/atheris spawn | Patch mutmut or use `MUTMUT_TESTING=1` |

### Configuration

| Error | Cause | Solution |
|-------|-------|---------|
| `No interesting inputs` (mutmut) | `tests_dir` missing | Add `tests_dir = <package>/tests/` in `setup.cfg` |
| `WARNING: ignoring pytest config` | Config duplicated | Keep pytest config in `setup.cfg` |
| `[pytest] no longer supported` | Old syntax | Change to `[tool:pytest]` |
| mutmut stuck on spinner | hypothesis with 200 examples | Reduce to `max_examples = 30` |

### pytest

| Error | Cause | Solution |
|-------|-------|---------|
| `function uses no argument 'a'` | Signature with fixture + @parametrize | Use `def test(a, b, expected)` |
| Float comparison fails | `1.1 + 2.2 != 3.3` | Use `pytest.approx()` |
| `match` doesn't catch mutated error | Regex without anchors | Use `match="^exact$"` |

### hypothesis

| Error | Cause | Solution |
|-------|-------|---------|
| `tuple() takes no keyword arguments` | `st.builds(tuple, ...)` | Use `st.tuples(...)` |
| `FailedHealthCheck` | `assume()` rejects too many inputs | Use `.filter()` on strategies |
| `b'\x00' is falsy` | `min_size=1` generates `b"\x00"` | Use `min_size=2` |
| Test hangs (no response) | Real boto3/network call | Mock boto3 with `return_value` |
| `unexpected keyword argument` | `@given` defines arg not used | Remove from `@given` and signature |
| `float overflow: 1.79e+308` | `st.floats()` without limits | Use `min_value`, `max_value` |
| `Cannot assign hypothesis.settings` | Setting in conftest | Use `[tool.hypothesis]` in pyproject.toml |
| Too slow (>60s) | Large data, many examples | `max_examples=30`, limit `max_size` |
| **Large file generation** | `st.binary()` without `max_size` or writing to disk | **Use `max_size` limits, process in memory, use `tempfile`** |

### mutmut

| Error | Cause | Solution |
|-------|-------|---------|
| `No interesting inputs` | `tests_dir` missing | Add `tests_dir = <package>/tests/` |
| `Could not figure out where to mutate` | `paths_to_mutate` missing or no `/` | Add `paths_to_mutate = <package>/` |
| `ModuleNotFoundError` | Package not installed | `pip install -e .` |
| `RuntimeError: context has already been set` | Multiprocessing fork conflict | Patch mutmut or `MUTMUT_TESTING=1` |
| `test_fuzz.py` causing error | Atheris not compatible with mutmut | Add `--ignore=.../test_fuzz.py` |
| mutmut tries to run e2e tests | Docker not available in mutmut | Add `--ignore=.../test_e2e.py` |

### atheris

| Error | Cause | Solution |
|-------|-------|---------|
| `ModuleNotFoundError` | Package not installed | `pip install -e .` |
| `No interesting inputs` | Not instrumented | `with atheris.instrument_imports()` |
| `Choose` doesn't exist | v3 API changed | Use `list[fdp.ConsumeIntInRange(0, n)]` |
| `ConsumeUnicodeNoSurrogates(10, 200)` | v3 API — 1 arg | `fdp.ConsumeUnicodeNoSurrogates(100)` |
| `ConsumeBytesOfMinimumSize` doesn't exist | v3 API changed | Use `fdp.ConsumeBytes(size)` |
| Crash — `Uncaught Python exception` | **BUG found!** | Save crash input, reproduce, fix |
| Very low exec/s | I/O or network in TestOneInput | Remove I/O, use mocks |

### E2E

| Error | Cause | Solution |
|-------|-------|---------|
| `DID NOT RAISE` | Mock without `return_value` | Add `mock_boto.return_value = mock_instance` |
| VCR with binary data fails | YAML serializes as str | Use `responses` library |
| MinIO too slow per test | Fixture without `scope="module"` | Use `scope="module"` |

---

## Quick Commands

```bash
# Setup
python3 -m venv .venv && source .venv/bin/activate
pip install -e .[test]

# pytest (unit + property)
python -m pytest <package>/tests/ -v --ignore=<package>/tests/test_fuzz.py

# pytest (unit only)
python -m pytest <package>/tests/test_core.py -v

# hypothesis
python -m pytest <package>/tests/test_properties.py -v

# mutmut
rm -rf mutants/ && python -m mutmut run
python -m mutmut results
python -m mutmut show <key>

# atheris
python <package>/tests/test_fuzz.py -runs=10000

# E2E
python -m pytest <package>/tests/test_e2e.py -v

# Patch mutmut (if needed)
sed -i "1152s/.*/# set_start_method('fork')/" \
  /path/to/mutmut/__main__.py

# Cleanup
rm -rf mutants/ .hypothesis/ .pytest_cache/ __pycache__/
```
