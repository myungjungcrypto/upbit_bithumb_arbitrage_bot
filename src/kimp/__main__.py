from __future__ import annotations

import argparse
import asyncio
import logging

from .app import run
from .config import load_config


def main() -> None:
    p = argparse.ArgumentParser(prog="kimp-collect", description="김프 P0 시세 수집기")
    p.add_argument("--config", default="config/default.yaml")
    p.add_argument("--log-level", default="INFO")
    args = p.parse_args()

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    cfg = load_config(args.config)
    asyncio.run(run(cfg))


if __name__ == "__main__":
    main()
