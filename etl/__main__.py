"""Entry point: python -m etl."""

from etl.config import ETLConfig
from etl.orchestrator import Orchestrator

if __name__ == "__main__":
    config = ETLConfig()
    orchestrator = Orchestrator(config)
    result = orchestrator.run()
    print(result)
