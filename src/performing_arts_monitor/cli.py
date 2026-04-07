from __future__ import annotations

from performing_arts_monitor.config import build_parser, load_config
from performing_arts_monitor.pipeline import run_pipeline


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    config = load_config(args)
    run_dir = run_pipeline(config)
    print(f"Performing arts monitor build completed: {run_dir}")
