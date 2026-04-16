#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Batch backfill post-hoc picker evaluations."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.stock_picker.service import PICKER_EVAL_WINDOWS, StockPickerService


def _parse_since(value: str):
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError("`--since` 必须为 YYYY-MM-DD 格式") from exc


def _parse_windows(values: Sequence[int]) -> list[int]:
    windows = [int(item) for item in values]
    invalid = [item for item in windows if item not in PICKER_EVAL_WINDOWS]
    if invalid:
        allowed = ", ".join(str(item) for item in PICKER_EVAL_WINDOWS)
        raise argparse.ArgumentTypeError(f"`--window-days` 仅支持 {allowed}")
    return windows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="批量回补 AI 选股候选的 5/10/20 日后验评估")
    parser.add_argument("--task-id", help="仅回补指定 task_id")
    parser.add_argument(
        "--window-days",
        type=int,
        nargs="+",
        default=list(PICKER_EVAL_WINDOWS),
        help="评估窗口，可传多个值，例如 --window-days 5 10 20",
    )
    parser.add_argument("--since", type=_parse_since, help="仅处理该日期及之后创建的任务，格式 YYYY-MM-DD")
    parser.add_argument("--limit", type=int, help="最多处理多少个已完成任务")
    parser.add_argument("--dry-run", action="store_true", help="只统计，不写回数据库")
    parser.add_argument("--force", action="store_true", help="强制覆盖已完成窗口的评估结果")
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    windows = _parse_windows(args.window_days)

    service = StockPickerService()
    result = service.backfill_evaluations(
        task_id=args.task_id,
        window_days=windows,
        since=args.since,
        limit=args.limit,
        dry_run=bool(args.dry_run),
        force=bool(args.force),
    )

    print("AI 选股后验回补结果")
    print(f"- 任务数: {result['task_count']}")
    print(f"- 窗口: {', '.join(str(item) for item in result['window_days'])}")
    print(f"- 基准: {result['benchmark_code']}")
    print(f"- Dry run: {'yes' if result['dry_run'] else 'no'}")
    print(f"- Force: {'yes' if result['force'] else 'no'}")
    if result.get("since"):
        print(f"- Since: {result['since']}")
    if args.task_id:
        print(f"- Task ID: {args.task_id}")

    for window in result["window_days"]:
        item = result["per_window"][window]
        print("")
        print(f"[{window}日]")
        print(f"candidate_count={item.get('candidate_count', 0)}")
        print(f"completed={item.get('completed', 0)}")
        print(f"pending={item.get('pending', 0)}")
        print(f"benchmark_unavailable={item.get('benchmark_unavailable', 0)}")
        print(f"invalid={item.get('invalid', 0)}")
        print(f"skipped_completed={item.get('skipped_completed', 0)}")
        print(f"skipped_non_cn={item.get('skipped_non_cn', 0)}")
        print(f"skipped_missing_analysis_date={item.get('skipped_missing_analysis_date', 0)}")
        print("note=回补结果会同步写入 return/max_drawdown/mfe/mae 等评估字段")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
