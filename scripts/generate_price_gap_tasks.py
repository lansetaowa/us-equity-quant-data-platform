from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv

from quant_platform.prices.gap_tasks import main


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = PROJECT_ROOT / ".env"


if __name__ == "__main__":
    load_dotenv(dotenv_path=ENV_PATH.resolve())
    main()