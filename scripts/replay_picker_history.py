#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Run validation-only historical picker replay on a past trading date."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Sequence

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.stock_picker.service import PICKER_EVAL_WINDOWS, StockPickerService


def _parse_date(value: str):
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError("`--target-date` 必须为 YYYY-MM-DD 格式") from exc


def _parse_windows(values: Sequence[int]) -> list[int]:
    windows = [int(item) for item in values]
    invalid = [item for item in windows if item not in PICKER_EVAL_WINDOWS]
    if invalid:
        allowed = ", ".join(str(item) for item in PICKER_EVAL_WINDOWS)
        raise argparse.ArgumentTypeError(f"`--window-days` 仅支持 {allowed}")
    return windows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="按历史日期回放 AI 选股，并直接计算 5/10/20 日后验（验证用途，不写 picker_tasks）"
    )
    parser.add_argument("--target-date", required=True, type=_parse_date, help="历史回放日期，格式 YYYY-MM-DD")
    parser.add_argument(
        "--template-id",
        required=True,
        choices=["trend_breakout", "strong_pullback", "balanced"],
        help="模板 ID",
    )
    parser.add_argument(
        "--mode",
        default="watchlist",
        choices=["watchlist", "sector"],
        help="运行模式：自选股或板块模式",
    )
    parser.add_argument(
        "--sector-ids",
        nargs="*",
        default=[],
        help="板块模式下的行业板块 ID，可传多个",
    )
    parser.add_argument("--limit", type=int, default=20, help="返回候选数量上限（1-30）")
    parser.add_argument(
        "--window-days",
        type=int,
        nargs="+",
        default=list(PICKER_EVAL_WINDOWS),
        help="后验窗口，可传多个值，例如 --window-days 5 10 20",
    )
    parser.add_argument(
        "--sector-ranking-mode",
        choices=["neutral", "live"],
        default="neutral",
        help="板块强弱口径：neutral=禁用历史回放中的板块加分以避免未来信息泄漏；live=复用当前实时榜，仅做近似验证",
    )
    parser.add_argument(
        "--benchmark-mode",
        choices=["local_only", "fetch_missing"],
        default="local_only",
        help="基准口径：local_only=仅使用本地库中的 000300，缺失则直接记为 benchmark_unavailable；fetch_missing=缺失时尝试联网补数",
    )
    parser.add_argument("--force-refresh", action="store_true", help="强制刷新本地日线缓存")
    parser.add_argument("--output", help="将完整 JSON 结果写入文件")
    return parser


def _print_summary(result: dict) -> None:
    summary = result["summary"]
    print("AI 选股历史回放结果")
    print(f"- 历史日期: {result['target_date']}")
    print(f"- 模板: {result['template_name']} ({result['template_id']})")
    print(f"- 模式: {'板块模式' if result['mode'] == 'sector' else '自选股模式'}")
    if result.get("sector_names"):
        print(f"- 板块: {'、'.join(result['sector_names'])}")
    print(f"- 板块强弱口径: {result['sector_ranking_mode']}")
    print(f"- 基准口径: {result['benchmark_mode']}")
    print(f"- 基准: {result['benchmark_code']}")
    print(f"- 回放说明: 新闻/AI 已禁用，避免把未来新闻泄漏到历史回放")
    print(
        f"- 候选概况: total={summary['total_stocks']} "
        f"scored={summary['scored_count']} selected={summary['selected_count']} "
        f"strict={summary['strict_match_count']} fallback={summary['fallback_count']}"
    )
    if summary.get("insufficient_reason_breakdown"):
        print(f"- 未入选原因: {json.dumps(summary['insufficient_reason_breakdown'], ensure_ascii=False)}")

    print("")
    print("后验统计")
    for window in result["window_days"]:
        item = summary["evaluation_summary"][int(window)]
        print(
            f"- {window}日: completed={item.get('completed', 0)} "
            f"pending={item.get('pending', 0)} "
            f"benchmark_unavailable={item.get('benchmark_unavailable', 0)} "
            f"invalid={item.get('invalid', 0)}"
        )

    print("")
    print("Top 候选")
    for candidate in result["candidates"][: min(10, len(result["candidates"]))]:
        eval_map = {
            int(item["window_days"]): item.get("eval_status")
            for item in candidate.get("evaluations") or []
        }
        eval_text = ", ".join(
            f"{window}日={eval_map.get(int(window), 'n/a')}" for window in result["window_days"]
        )
        print(
            f"- #{candidate['rank']} {candidate['name']} ({candidate['code']}) "
            f"score={candidate['total_score']} latest={candidate['latest_date']} [{eval_text}]"
        )


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    windows = _parse_windows(args.window_days)

    service = StockPickerService()
    result = service.replay_historical_run(
        target_date=args.target_date,
        template_id=args.template_id,
        mode=args.mode,
        sector_ids=args.sector_ids,
        limit=args.limit,
        force_refresh=bool(args.force_refresh),
        window_days=windows,
        sector_ranking_mode=args.sector_ranking_mode,
        benchmark_mode=args.benchmark_mode,
    )

    _print_summary(result)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(result, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        print("")
        print(f"完整结果已写入: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
