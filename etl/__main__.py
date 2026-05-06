"""ETL entry point — run the full extract -> push pipeline.

Usage:
    uv run python -m etl                    # incremental mode
    ETL_MODE=full uv run python -m etl       # full mode
"""

from __future__ import annotations

import json
import os
import sys

from .orchestrator import ETLConfig, Orchestrator


def main() -> None:
    """Run the ETL pipeline with configuration from environment."""
    mode = os.environ.get("ETL_MODE", "incremental")
    if mode not in ("incremental", "full"):
        print(
            f"Error: ETL_MODE must be 'incremental' or 'full', got '{mode}'",
            file=sys.stderr,
        )
        sys.exit(1)

    config = ETLConfig(mode=mode)
    orchestrator = Orchestrator(config)
    result = orchestrator.run()

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
