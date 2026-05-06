"""ETL entry point — runs extract then push in a single script.

Backward compatible: delegates to etl.Orchestrator.
"""

from __future__ import annotations

import logging
from pathlib import Path

from etl.orchestrator import ETLConfig, Orchestrator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    """Run extract then push with auto-delete (backward compat)."""
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    Orchestrator(ETLConfig()).run()


if __name__ == "__main__":
    main()
