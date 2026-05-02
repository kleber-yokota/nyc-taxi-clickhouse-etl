# extract/REVIEW_EXCEPTION.md — Review Exceptions

> **For the agent**: This file documents exceptions to the rules defined in the root `REVIEW.md`.

---

## Purpose

Document exceptions to the rules defined in the root `REVIEW.md` for the `extract` module.

## Rules

- Exceptions are **module-specific** — there is no root `REVIEW_EXCEPTION.md`
- Every exception must include: the rule being waived, the affected file(s), and the reason
- If no exceptions exist, this file does not need to be created

## Exceptions

| Rule | File(s) | Reason |
|------|---------|--------|
| Top-Level Imports Only | `tests/conftest.py` | Lazy imports (`from extract.core.catalog import Catalog`) in `mock_404` and `mock_500` fixtures to avoid importing `Catalog` at module level — these fixtures are rarely used and `Catalog` has substantial initialization overhead |
