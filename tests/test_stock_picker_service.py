# -*- coding: utf-8 -*-
import json
import threading
import unittest
from datetime import date, datetime
from unittest.mock import Mock, patch

import pandas as pd

from src.stock_picker.service import StockPickerService


class _FakeAnalyzer:
    def __init__(self) -> None:
        self.prompts = []

    def generate_text(self, prompt: str, max_tokens: int = 800, temperature: float = 0.2) -> str:
        self.prompts.append(prompt)
        return json.dumps(
            {
                "summary": "测试摘要",
                "rationale": ["理由1", "理由2", "理由3"],
                "risks": ["风险1", "风险2"],
                "watchpoints": ["观察点1", "观察点2"],
            },
            ensure_ascii=False,
        )


def _build_daily_rows(latest_day: date, count: int = 40):
    dates = pd.date_range(end=pd.Timestamp(latest_day), periods=count, freq="D")
    rows = []
    for index, day in enumerate(dates, start=1):
        close = 100 + index
        rows.append(
            {
                "date": day.date(),
                "open": close - 1,
                "high": close + 1,
                "low": close - 2,
                "close": close,
                "volume": 100000 + index * 100,
                "amount": 1000000 + index * 1000,
                "pct_chg": 1.0,
                "ma5": close - 1,
                "ma10": close - 2,
                "ma20": close - 3,
                "volume_ratio": 1.2,
            }
        )
    return rows


def _build_daily_frame(latest_day: date, count: int = 40) -> pd.DataFrame:
    return pd.DataFrame(_build_daily_rows(latest_day, count=count))


def _latest_day(frame: pd.DataFrame) -> date:
    latest = frame.iloc[-1]["date"]
    if hasattr(latest, "date"):
        return latest.date()
    return latest


class _FakeRepo:
    def __init__(self, rows):
        self.rows = list(rows)

    def get_recent_daily_rows(self, code: str, *, limit: int = 120):
        return list(self.rows)


class _CaptureRepo:
    def __init__(self) -> None:
        self.created = None

    def mark_incomplete_tasks_failed(self) -> int:
        return 0

    def create_task(self, **kwargs) -> None:
        self.created = kwargs


class _FakeStockRepo:
    def __init__(self, range_rows=None) -> None:
        self.saved = []
        self.range_rows = list(range_rows or [])

    def save_dataframe(self, df: pd.DataFrame, code: str, data_source: str = "") -> None:
        self.saved.append((code, data_source, len(df)))

    def get_range(self, code: str, start_date: date, end_date: date):
        return list(self.range_rows)


class _FakeFetcherManager:
    def __init__(
        self,
        refresh_df: pd.DataFrame | None = None,
        source_name: str = "fake",
        fundamental_context: dict | None = None,
        chip_distribution: dict | None = None,
    ) -> None:
        self.refresh_df = refresh_df
        self.source_name = source_name
        self.daily_calls = []
        self.fundamental_context = fundamental_context or {}
        self.chip_distribution = chip_distribution or {}

    def get_daily_data(self, code: str, days: int = 120):
        self.daily_calls.append((code, days))
        return self.refresh_df, self.source_name

    def get_stock_name(self, code: str, allow_realtime: bool = False) -> str:
        return "测试股票"

    def get_belong_boards(self, code: str):
        return []

    def get_sector_rankings(self, n: int = 10):
        return [], []

    def get_fundamental_context(self, code: str, budget_seconds: float | None = None):
        return self.fundamental_context

    def get_chip_distribution(self, code: str):
        return self.chip_distribution


class _FakeExecutor:
    def __init__(self) -> None:
        self.calls = []

    def submit(self, fn, *args, **kwargs):
        self.calls.append((fn, args, kwargs))
        return object()


class TestStockPickerService(unittest.TestCase):
    def test_build_sector_catalog_from_stock_list_with_industry(self) -> None:
        frame = pd.DataFrame(
            [
                {"code": "600519", "name": "贵州茅台", "industry": "白酒"},
                {"code": "000858", "name": "五粮液", "industry": "白酒"},
                {"code": "600036", "name": "招商银行", "industry": "银行"},
                {"code": "AAPL", "name": "Apple", "industry": "US"},
            ]
        )

        class _Fetcher:
            name = "fake"

            def get_stock_list(self):
                return frame

        manager = object.__new__(_FakeFetcherManager)
        manager._fetchers = [_Fetcher()]
        manager._fetchers_lock = None
        manager._fetcher_call_locks = None
        manager._fetcher_call_locks_lock = None
        manager._stock_name_cache = None
        manager._stock_name_cache_lock = None
        manager.get_sector_rankings = Mock(
            return_value=(
                [{"name": "白酒", "change_pct": 3.2}],
                [{"name": "银行", "change_pct": -1.8}],
            )
        )

        catalog = StockPickerService._build_sector_catalog(manager)

        self.assertEqual([item["name"] for item in catalog["items"]], ["白酒", "银行"])
        self.assertEqual(catalog["items"][0]["stock_count"], 2)
        self.assertEqual(catalog["items"][0]["strength_label"], "强势")
        self.assertEqual(catalog["items"][0]["rank_direction"], "top")
        self.assertEqual(catalog["items"][1]["strength_label"], "弱势")
        self.assertEqual(catalog["items"][1]["rank_direction"], "bottom")
        self.assertEqual(catalog["code_by_sector"]["白酒"], ["600519", "000858"])
        self.assertEqual(catalog["catalog_policy"], "dynamic_a_share_industry_from_stock_list")
        self.assertEqual(catalog["source_name"], "fake")
        self.assertTrue(catalog["catalog_signature"])

    def test_submit_task_accepts_sector_mode_with_controlled_parameters(self) -> None:
        service = object.__new__(StockPickerService)
        capture_repo = _CaptureRepo()
        service._repo = capture_repo
        service._executor = _FakeExecutor()
        service._futures = {}
        service._futures_lock = unittest.mock.MagicMock()
        service._load_sector_catalog = Mock(return_value={
            "items": [
                {"sector_id": "白酒", "name": "白酒", "market": "cn", "stock_count": 2},
                {"sector_id": "银行", "name": "银行", "market": "cn", "stock_count": 1},
            ],
            "code_by_sector": {"白酒": ["600519", "000858"], "银行": ["600036"]},
        })

        payload = service.submit_task(
            template_id="balanced",
            template_overrides={},
            universe_id="watchlist",
            mode="sector",
            sector_ids=["白酒", "银行"],
            limit=12,
            ai_top_k=10,
            force_refresh=True,
            notify=True,
        )

        self.assertEqual(payload["status"], "queued")
        self.assertIsNotNone(capture_repo.created)
        self.assertEqual(capture_repo.created["universe_id"], "sector")
        self.assertEqual(capture_repo.created["template_version"], "v4_2_phase2")
        self.assertEqual(capture_repo.created["ai_top_k"], 10)
        self.assertTrue(capture_repo.created["request_payload"]["notify"])
        self.assertEqual(capture_repo.created["request_payload"]["request_policy_version"], "v4_2_phase2")
        self.assertEqual(capture_repo.created["request_payload"]["sector_ids"], ["白酒", "银行"])
        self.assertEqual(capture_repo.created["request_payload"]["sector_names"], ["白酒", "银行"])
        self.assertEqual(
            capture_repo.created["request_payload"]["benchmark_policy"]["benchmark_code"],
            "000300",
        )
        self.assertEqual(
            capture_repo.created["request_payload"]["sector_catalog_request"]["selected_sector_count"],
            2,
        )

    def test_submit_task_rejects_ai_top_k_above_ten(self) -> None:
        service = object.__new__(StockPickerService)
        service._repo = _CaptureRepo()
        service._executor = _FakeExecutor()
        service._futures = {}
        service._futures_lock = unittest.mock.MagicMock()
        service._load_sector_catalog = Mock(return_value={"items": [], "code_by_sector": {}})

        with self.assertRaisesRegex(ValueError, "ai_top_k 必须介于 1 和 10 之间"):
            service.submit_task(
                template_id="balanced",
                template_overrides={},
                universe_id="watchlist",
                mode="watchlist",
                sector_ids=[],
                limit=20,
                ai_top_k=11,
                force_refresh=False,
                notify=False,
            )

    def test_get_task_returns_stored_payload_without_refreshing_evaluations(self) -> None:
        service = object.__new__(StockPickerService)
        payload_from_repo = {
            "task_id": "picker-task-1",
            "template_id": "balanced",
            "status": "completed",
            "universe_id": "watchlist",
            "request_payload": {"mode": "watchlist"},
            "candidates": [{"code": "600519", "evaluations": [{"window_days": 5, "eval_status": "completed"}]}],
        }
        service._repo = Mock()
        service._repo.get_task.return_value = payload_from_repo
        service._ensure_task_evaluations = Mock()
        service._decorate_task = Mock(side_effect=lambda payload: payload)

        payload = StockPickerService.get_task(service, "picker-task-1")

        self.assertIsNotNone(payload)
        service._ensure_task_evaluations.assert_not_called()
        service._repo.get_task.assert_called_once_with("picker-task-1", include_candidates=True)
        self.assertEqual(payload["candidates"][0]["evaluations"][0]["window_days"], 5)

    def test_decorate_task_builds_runtime_summary_defaults_for_running_tasks(self) -> None:
        service = object.__new__(StockPickerService)
        service._load_sector_catalog = Mock(
            return_value={
                "items": [
                    {
                        "sector_id": "黄金",
                        "name": "黄金",
                        "strength_label": "强势",
                        "rank_direction": "top",
                        "rank_position": 3,
                        "change_pct": 2.6,
                        "is_ranked_today": True,
                        "stock_count": 10,
                    }
                ]
            }
        )

        payload = StockPickerService._decorate_task(
            service,
            {
                "task_id": "picker-task-running",
                "template_id": "balanced",
                "status": "running",
                "template_version": "v4_1_phase2",
                "universe_id": "sector",
                "limit": 20,
                "ai_top_k": 10,
                "force_refresh": False,
                "total_stocks": 10,
                "processed_stocks": 1,
                "candidate_count": 0,
                "progress_percent": 16,
                "progress_message": "已扫描 1/10 支股票",
                "summary": {},
                "request_payload": {
                    "mode": "sector",
                    "notify": False,
                    "sector_ids": ["黄金"],
                    "sector_names": ["黄金"],
                    "benchmark_policy": {"benchmark_code": "000300"},
                    "sector_catalog_request": {
                        "selected_sector_count": 1,
                        "selected_sector_names": ["黄金"],
                        "selected_stock_count": 10,
                        "sector_count": 50,
                        "catalog_stock_count": 800,
                    },
                },
            },
        )

        self.assertEqual(payload["summary"]["total_stocks"], 10)
        self.assertEqual(payload["summary"]["scored_count"], 1)
        self.assertEqual(payload["summary"]["benchmark_policy"]["benchmark_code"], "000300")
        self.assertEqual(payload["summary"]["selection_quality_gate"]["selection_policy"], "strict_match_first_then_quality_gated_fallback")
        self.assertEqual(payload["summary"]["sector_quality_summary"]["selected_sector_count"], 1)
        self.assertEqual(payload["summary"]["ranked_sector_breakdown"][0]["name"], "黄金")

    def test_decorate_task_preserves_and_infers_v4_summary_counts(self) -> None:
        service = object.__new__(StockPickerService)
        service._load_sector_catalog = Mock(return_value={"items": []})

        payload = StockPickerService._decorate_task(
            service,
            {
                "task_id": "picker-task-completed",
                "template_id": "trend_breakout",
                "status": "completed",
                "template_version": "v4_2_phase2",
                "universe_id": "watchlist",
                "limit": 20,
                "ai_top_k": 10,
                "force_refresh": False,
                "total_stocks": 20,
                "processed_stocks": 20,
                "candidate_count": 2,
                "progress_percent": 100,
                "progress_message": "选股完成",
                "summary": {},
                "request_payload": {
                    "mode": "watchlist",
                    "notify": False,
                },
                "candidates": [
                    {
                        "code": "600519",
                        "advanced_factors": {"status": "enriched", "factor_total": 7.0},
                        "ai_review": {"veto_level": "soft_veto"},
                    },
                    {
                        "code": "000858",
                        "advanced_factors": {"status": "enriched", "factor_total": 3.0},
                        "ai_review": {"veto_level": "pass"},
                    },
                ],
            },
        )

        self.assertEqual(payload["summary"]["advanced_enriched_count"], 2)
        self.assertEqual(payload["summary"]["ai_reviewed_count"], 2)
        self.assertEqual(payload["summary"]["ai_soft_veto_count"], 1)

    def test_get_task_refreshes_legacy_summary_when_score_or_action_is_stale(self) -> None:
        service = object.__new__(StockPickerService)
        service._repo = Mock()
        service._repo.get_task.return_value = {
                "task_id": "picker-task-completed",
                "template_id": "trend_breakout",
                "status": "completed",
                "template_version": "v4_2_phase2",
                "universe_id": "sector",
                "limit": 20,
                "ai_top_k": 10,
                "force_refresh": False,
                "total_stocks": 20,
                "processed_stocks": 20,
                "candidate_count": 1,
                "progress_percent": 100,
                "progress_message": "选股完成",
                "summary": {},
                "request_payload": {
                    "mode": "sector",
                    "notify": False,
                    "sector_ids": ["半导体"],
                },
                "candidates": [
                    {
                        "code": "603991",
                        "name": "至正股份",
                        "market": "cn",
                        "selection_reason": "strict_match",
                        "strict_match": True,
                        "total_score": 75.0,
                        "board_names": ["半导体"],
                        "volume_ratio": 1.92,
                        "distance_to_high_pct": 0.13,
                        "trend_score": 28.0,
                        "setup_score": 25.0,
                        "volume_score": 12.0,
                        "sector_score": 10.0,
                        "news_score": 0.0,
                        "score_breakdown": [],
                        "explanation_summary": "至正股份符合趋势突破模板，综合得分81.0，技术信号强劲，处于关键跟踪阶段",
                        "explanation_rationale": [],
                        "explanation_risks": [],
                        "explanation_watchpoints": [],
                        "trade_plan": {"action": "observe"},
                        "technical_snapshot": {
                            "selection_context": {
                                "strict_match": True,
                                "strict_reasons": ["满足主要模板条件"],
                            },
                            "market_regime_label": "震荡整理",
                            "environment_fit_label": "环境谨慎",
                            "change20d_pct": 4.2,
                            "pullback_from_high_pct": -1.8,
                            "ma10": 108.6,
                            "ma20": 103.2,
                            "ma20_slope_pct": 0.7,
                            "trade_plan": {"action": "observe"},
                        },
                    }
                ],
            }
        service._load_sector_catalog = Mock(return_value={"items": []})

        payload = StockPickerService.get_task(service, "picker-task-completed")

        refreshed = payload["candidates"][0]["explanation_summary"]
        self.assertIn("综合得分 75.0", refreshed)
        self.assertIn("当前更偏观察", refreshed)

    def test_run_task_sector_mode_initializes_analyzer_with_runtime_config(self) -> None:
        service = object.__new__(StockPickerService)
        service._repo = Mock()
        service._repo.get_task.return_value = {
            "task_id": "picker-task-1",
            "template_id": "balanced",
            "status": "queued",
            "universe_id": "sector",
            "limit": 1,
            "ai_top_k": 1,
            "force_refresh": False,
            "request_payload": {
                "mode": "sector",
                "sector_ids": ["白酒"],
                "notify": False,
            },
        }
        service._resolve_task_stock_codes = Mock(return_value=(["600519"], ["白酒"]))
        service._evaluate_candidate = Mock(
            return_value={
                "rank": 0,
                "code": "600519",
                "name": "贵州茅台",
                "market": "cn",
                "selection_reason": "strict_match",
                "strict_match": True,
                "latest_date": date(2026, 4, 14),
                "latest_close": 100.0,
                "change_pct": 1.2,
                "volume_ratio": 1.1,
                "distance_to_high_pct": -0.5,
                "trend_score": 30.0,
                "setup_score": 30.0,
                "volume_score": 20.0,
                "sector_score": 10.0,
                "news_score": 0.0,
                "risk_penalty": 0.0,
                "total_score": 90.0,
                "board_names": ["白酒"],
                "news_briefs": [],
                "score_breakdown": [],
                "technical_snapshot": {"ma5": 10.0, "ma10": 9.8},
            }
        )
        service._build_search_service = Mock()
        service._fetch_news_briefs = Mock(return_value=None)
        service._load_sector_rankings = Mock(return_value=([], []))
        service._load_sector_catalog = Mock(
            return_value={
                "items": [{"sector_id": "白酒", "name": "白酒", "stock_count": 1}],
                "code_by_sector": {"白酒": ["600519"]},
                "catalog_policy": "dynamic_a_share_industry_from_stock_list",
                "source_name": "fake",
                "sector_count": 1,
                "stock_count": 1,
                "catalog_signature": "unit-test",
            }
        )
        service._select_candidates = Mock(side_effect=lambda candidates, limit: list(candidates)[:limit])
        service._build_fallback_explanation = Mock(
            return_value={
                "summary": "fallback",
                "rationale": ["r1"],
                "risks": ["risk1"],
                "watchpoints": ["watch1"],
            }
        )
        service._build_ai_explanation = Mock(
            return_value={
                "summary": "ai",
                "rationale": ["r1"],
                "risks": ["risk1"],
                "watchpoints": ["watch1"],
            }
        )
        service._ensure_task_evaluations = Mock()
        service._send_task_notification = Mock()
        service._build_market_regime_snapshot = Mock(
            return_value={"regime": "trend_up", "regime_label": "上行趋势"}
        )
        service._futures = {}
        service._futures_lock = unittest.mock.MagicMock()

        config = object()
        analyzer_instance = Mock()
        with (
            patch("src.stock_picker.service.get_config", return_value=config),
            patch("src.stock_picker.service.DataFetcherManager"),
            patch("src.stock_picker.service.GeminiAnalyzer", return_value=analyzer_instance) as analyzer_cls,
        ):
            StockPickerService._run_task(service, "picker-task-1")

        analyzer_cls.assert_called_once_with(config=config)
        service._repo.start_task.assert_called_once_with("picker-task-1", total_stocks=1)
        service._repo.fail_task.assert_not_called()
        service._repo.save_candidates.assert_called_once()
        saved_summary = service._repo.save_candidates.call_args.kwargs["summary"]
        self.assertEqual(saved_summary["benchmark_policy"]["benchmark_unavailable_status"], "benchmark_unavailable")
        self.assertEqual(saved_summary["sector_catalog_snapshot"]["selected_sector_count"], 1)
        self.assertEqual(saved_summary["sector_quality_summary"]["strong_count"], 0)
        self.assertIn("cn", saved_summary["trading_date_policy"]["market_target_dates"])
        self.assertEqual(saved_summary["market_regime_snapshot"]["regime"], "trend_up")
        service._ensure_task_evaluations.assert_called_once_with("picker-task-1")

    def test_list_template_stats_uses_comparable_rows_for_win_rate(self) -> None:
        service = object.__new__(StockPickerService)
        service._repo = Mock()
        service._ensure_template_evaluations = Mock()
        service._repo.list_evaluation_rows_for_window.return_value = [
            {
                "market": "cn",
                "template_id": "balanced",
                "eval_status": "benchmark_unavailable",
                "return_pct": 4.0,
                "excess_return_pct": None,
                "max_drawdown_pct": 2.0,
            },
            {
                "market": "cn",
                "template_id": "balanced",
                "eval_status": "completed",
                "return_pct": 6.0,
                "excess_return_pct": 1.5,
                "max_drawdown_pct": 3.0,
            },
        ]

        payload = StockPickerService.list_template_stats(service, window_days=5)
        balanced_row = next(item for item in payload["items"] if item["template_id"] == "balanced")

        service._ensure_template_evaluations.assert_not_called()
        self.assertEqual(balanced_row["total_evaluations"], 2)
        self.assertEqual(balanced_row["comparable_evaluations"], 1)
        self.assertEqual(balanced_row["benchmark_unavailable_evaluations"], 1)
        self.assertEqual(balanced_row["win_rate_pct"], 100.0)
        self.assertEqual(balanced_row["avg_return_pct"], 5.0)
        self.assertEqual(balanced_row["avg_excess_return_pct"], 1.5)

    def test_list_stratified_stats_groups_market_regime_rank_and_signal(self) -> None:
        service = object.__new__(StockPickerService)
        service._repo = Mock()
        service._repo.list_evaluation_rows_for_window.return_value = [
            {
                "market": "cn",
                "template_id": "balanced",
                "rank": 1,
                "signal_bucket": "high",
                "market_regime": "trend_up",
                "eval_status": "completed",
                "return_pct": 4.0,
                "excess_return_pct": 1.0,
                "max_drawdown_pct": 2.0,
            },
            {
                "market": "cn",
                "template_id": "balanced",
                "rank": 8,
                "signal_bucket": "medium",
                "market_regime": "range_bound",
                "eval_status": "benchmark_unavailable",
                "return_pct": 1.0,
                "excess_return_pct": None,
                "max_drawdown_pct": 1.5,
            },
        ]

        payload = StockPickerService.list_stratified_stats(service, window_days=5)

        self.assertEqual(payload["window_days"], 5)
        self.assertEqual(payload["by_market_regime"][0]["bucket_key"], "trend_up")
        self.assertEqual(payload["by_rank_bucket"][0]["bucket_key"], "top_1_3")
        self.assertEqual(payload["by_signal_bucket"][0]["bucket_key"], "high")

    def test_list_stratified_stats_returns_fixed_empty_buckets(self) -> None:
        service = object.__new__(StockPickerService)
        service._repo = Mock()
        service._repo.list_evaluation_rows_for_window.return_value = []

        payload = StockPickerService.list_stratified_stats(service, window_days=10)

        self.assertEqual(
            [item["bucket_key"] for item in payload["by_market_regime"]],
            ["trend_up", "range_bound", "risk_off", "unknown"],
        )
        self.assertEqual(
            [item["bucket_key"] for item in payload["by_rank_bucket"]],
            ["top_1_3", "top_4_10", "top_11_plus"],
        )
        self.assertEqual(
            [item["bucket_key"] for item in payload["by_signal_bucket"]],
            ["high", "medium", "low"],
        )
        self.assertTrue(all(item["total_evaluations"] == 0 for item in payload["by_signal_bucket"]))

    def test_build_market_regime_snapshot_classifies_trend_up(self) -> None:
        service = object.__new__(StockPickerService)
        service._load_daily_frame = Mock(return_value=_build_daily_frame(date(2026, 4, 14), count=60))

        snapshot = StockPickerService._build_market_regime_snapshot(
            service,
            fetcher_manager=_FakeFetcherManager(),
            force_refresh=False,
            reference_time=datetime(2026, 4, 14, 15, 0, 0),
        )

        self.assertEqual(snapshot["regime"], "trend_up")
        self.assertEqual(snapshot["regime_label"], "上行趋势")

    def test_build_market_regime_snapshot_refreshes_benchmark_when_cached_frame_is_insufficient(self) -> None:
        service = object.__new__(StockPickerService)
        service._load_benchmark_daily_frame = Mock(return_value=_build_daily_frame(date(2026, 4, 14), count=10))

        snapshot = StockPickerService._build_market_regime_snapshot(
            service,
            fetcher_manager=_FakeFetcherManager(),
            force_refresh=False,
            reference_time=datetime(2026, 4, 14, 15, 0, 0),
        )

        service._load_benchmark_daily_frame.assert_called_once()
        self.assertEqual(snapshot["regime"], "trend_up")

    def test_load_benchmark_daily_frame_uses_special_refresh_path_without_generic_fetch(self) -> None:
        service = object.__new__(StockPickerService)
        service._repo = Mock()
        service._repo.get_recent_daily_rows.side_effect = [
            [],
            _build_daily_rows(date(2026, 4, 14), count=60),
        ]
        service._refresh_benchmark_daily_data = Mock(return_value=True)

        fetcher_manager = _FakeFetcherManager(refresh_df=_build_daily_frame(date(2026, 4, 14), count=60))

        frame = StockPickerService._load_benchmark_daily_frame(
            service,
            fetcher_manager=fetcher_manager,
            force_refresh=False,
            current_time=datetime(2026, 4, 14, 15, 0, 0),
        )

        self.assertIsNotNone(frame)
        self.assertEqual(len(frame), 60)
        self.assertEqual(fetcher_manager.daily_calls, [])
        service._refresh_benchmark_daily_data.assert_called_once()

    def test_build_market_regime_snapshot_degrades_to_unknown_when_benchmark_refresh_fails(self) -> None:
        service = object.__new__(StockPickerService)
        service._load_benchmark_daily_frame = Mock(return_value=None)

        snapshot = StockPickerService._build_market_regime_snapshot(
            service,
            fetcher_manager=_FakeFetcherManager(),
            force_refresh=False,
            reference_time=datetime(2026, 4, 14, 15, 0, 0),
        )

        self.assertEqual(snapshot["regime"], "unknown")
        self.assertEqual(snapshot["reason"], "benchmark_daily_data_unavailable")

    def test_build_execution_constraints_marks_not_fillable_limit_up_like_case(self) -> None:
        payload = StockPickerService._build_execution_constraints(
            market="cn",
            metrics={
                "latest_pct_chg": 9.95,
                "gap_from_prev_close_pct": 3.2,
                "amount": 3_000_000,
                "intraday_range_pct": 0.05,
                "high": 10.0,
                "low": 10.0,
            },
        )

        self.assertEqual(payload["status"], "untradable")
        self.assertTrue(payload["not_fillable"])
        self.assertEqual(payload["liquidity_bucket"], "low")
        self.assertEqual(payload["gap_risk"], "high")
        self.assertGreater(payload["execution_penalty"], 10.0)

    def test_build_research_confidence_degrades_before_calibration(self) -> None:
        service = object.__new__(StockPickerService)
        service._repo = Mock()
        service._repo.list_evaluation_rows_for_window.return_value = [
            {
                "market": "cn",
                "template_id": "trend_breakout",
                "rule_version": "v4_1_phase2",
                "market_regime": "trend_up",
                "signal_bucket": "high",
                "eval_status": "completed",
                "excess_return_pct": 1.2,
            }
            for _ in range(5)
        ] + [
            {
                "market": "cn",
                "template_id": "trend_breakout",
                "rule_version": "v4_1_phase2",
                "market_regime": "range_bound",
                "signal_bucket": "medium",
                "eval_status": "completed",
                "excess_return_pct": 0.8,
            }
            for _ in range(4)
        ]

        payload = StockPickerService._build_research_confidence(
            service,
            template_id="trend_breakout",
            market_regime="trend_up",
            signal_bucket="high",
            rule_version="v4_1_phase2",
        )

        self.assertEqual(payload["status"], "calibration_pending")
        self.assertEqual(payload["label"], "观察中（待校准）")
        self.assertIsNotNone(payload["score"])
        self.assertLess(payload["score"], 0.8)
        self.assertEqual(payload["high_confidence_gate"]["status"], "blocked")

    def test_build_execution_confidence_maps_status_to_conservative_score(self) -> None:
        payload = StockPickerService._build_execution_confidence(
            execution_constraints={
                "status": "cautious",
                "slippage_bps": 15,
                "liquidity_bucket": "medium",
                "gap_risk": "medium",
                "not_fillable": False,
                "estimated_cost_model": "cn_equity_v4_1_minimal",
            }
        )

        self.assertEqual(payload["status"], "cautious")
        self.assertEqual(payload["label"], "执行谨慎")
        self.assertEqual(payload["score"], 0.45)
        self.assertEqual(payload["cost_model"], "cn_equity_v4_1_minimal")

    def test_build_research_confidence_can_promote_high_confidence_after_calibration(self) -> None:
        service = object.__new__(StockPickerService)
        service._repo = Mock()
        high_rows = [
            {
                "market": "cn",
                "template_id": "trend_breakout",
                "rule_version": "v4_1_phase2",
                "market_regime": "trend_up",
                "signal_bucket": "high",
                "eval_status": "completed",
                "return_pct": 4.0,
                "excess_return_pct": 1.0,
                "max_drawdown_pct": 2.0,
            }
            for _ in range(35)
        ] + [
            {
                "market": "cn",
                "template_id": "trend_breakout",
                "rule_version": "v4_1_phase2",
                "market_regime": "trend_up",
                "signal_bucket": "high",
                "eval_status": "completed",
                "return_pct": 1.0,
                "excess_return_pct": -0.3,
                "max_drawdown_pct": 2.5,
            }
            for _ in range(15)
        ]
        medium_rows = [
            {
                "market": "cn",
                "template_id": "trend_breakout",
                "rule_version": "v4_1_phase2",
                "market_regime": "trend_up",
                "signal_bucket": "medium",
                "eval_status": "completed",
                "return_pct": 2.0,
                "excess_return_pct": 0.6,
                "max_drawdown_pct": 2.5,
            }
            for _ in range(7)
        ] + [
            {
                "market": "cn",
                "template_id": "trend_breakout",
                "rule_version": "v4_1_phase2",
                "market_regime": "trend_up",
                "signal_bucket": "medium",
                "eval_status": "completed",
                "return_pct": 0.8,
                "excess_return_pct": -0.1,
                "max_drawdown_pct": 2.7,
            }
            for _ in range(5)
        ]
        low_rows = [
            {
                "market": "cn",
                "template_id": "trend_breakout",
                "rule_version": "v4_1_phase2",
                "market_regime": "trend_up",
                "signal_bucket": "low",
                "eval_status": "completed",
                "return_pct": 1.0,
                "excess_return_pct": -0.2,
                "max_drawdown_pct": 3.0,
            }
            for _ in range(6)
        ] + [
            {
                "market": "cn",
                "template_id": "trend_breakout",
                "rule_version": "v4_1_phase2",
                "market_regime": "trend_up",
                "signal_bucket": "low",
                "eval_status": "completed",
                "return_pct": 1.3,
                "excess_return_pct": 0.1,
                "max_drawdown_pct": 2.9,
            }
            for _ in range(4)
        ]
        service._repo.list_evaluation_rows_for_window.return_value = high_rows + medium_rows + low_rows

        payload = StockPickerService._build_research_confidence(
            service,
            template_id="trend_breakout",
            market_regime="trend_up",
            signal_bucket="high",
            rule_version="v4_1_phase2",
        )

        self.assertEqual(payload["status"], "high_confidence")
        self.assertEqual(payload["label"], "高置信度")
        self.assertTrue(payload["high_confidence_gate"]["passed"])
        self.assertEqual(payload["calibration"]["calibration_status"], "calibrated")

    def test_list_calibration_stats_returns_bucket_level_gate_status(self) -> None:
        service = object.__new__(StockPickerService)
        service._repo = Mock()
        service._repo.list_evaluation_rows_for_window.return_value = [
            {
                "market": "cn",
                "template_id": "trend_breakout",
                "rule_version": "v4_1_phase2",
                "market_regime": "trend_up",
                "signal_bucket": "high",
                "eval_status": "completed",
                "return_pct": 4.0,
                "excess_return_pct": 1.0,
                "max_drawdown_pct": 2.0,
            }
            for _ in range(12)
        ] + [
            {
                "market": "cn",
                "template_id": "trend_breakout",
                "rule_version": "v4_1_phase2",
                "market_regime": "trend_up",
                "signal_bucket": "high",
                "eval_status": "completed",
                "return_pct": 1.0,
                "excess_return_pct": -0.2,
                "max_drawdown_pct": 2.8,
            }
            for _ in range(6)
        ]

        payload = StockPickerService.list_calibration_stats(service, window_days=10)

        self.assertEqual(payload["window_days"], 10)
        self.assertEqual(payload["items"][0]["template_id"], "trend_breakout")
        self.assertEqual(payload["items"][0]["bucket_key"], "high")
        self.assertEqual(payload["items"][0]["calibration_status"], "calibrated")
        self.assertEqual(payload["items"][0]["high_confidence_gate"]["status"], "blocked")

    def test_score_sector_with_detail_prefers_token_match_before_fuzzy(self) -> None:
        score, detail = StockPickerService._score_sector_with_detail(
            ["白酒行业", "消费"],
            [{"name": "白酒", "change_pct": 3.6}],
            [{"name": "银行", "change_pct": -1.2}],
        )

        self.assertGreater(score, 0)
        self.assertEqual(detail["matched_top_sectors"][0]["match_type"], "token")
        self.assertEqual(detail["matched_top_sectors"][0]["matched_board"], "白酒行业")
        self.assertEqual(detail["matched_top_sectors"][0]["change_pct"], 3.6)

    def test_build_sector_quality_summary_counts_strength_buckets(self) -> None:
        summary, breakdown = StockPickerService._build_sector_quality_summary(
            {
                "items": [
                    {
                        "sector_id": "白酒",
                        "name": "白酒",
                        "strength_label": "强势",
                        "rank_direction": "top",
                        "rank_position": 2,
                        "change_pct": 3.2,
                        "is_ranked_today": True,
                        "stock_count": 32,
                    },
                    {
                        "sector_id": "银行",
                        "name": "银行",
                        "strength_label": "弱势",
                        "rank_direction": "bottom",
                        "rank_position": 1,
                        "change_pct": -2.3,
                        "is_ranked_today": True,
                        "stock_count": 40,
                    },
                    {
                        "sector_id": "家电",
                        "name": "家电",
                        "strength_label": "中性",
                        "rank_direction": None,
                        "rank_position": None,
                        "change_pct": None,
                        "is_ranked_today": False,
                        "stock_count": 28,
                    },
                ]
            },
            ["白酒", "银行", "家电"],
        )

        self.assertEqual(summary["selected_sector_count"], 3)
        self.assertEqual(summary["strong_count"], 1)
        self.assertEqual(summary["neutral_count"], 1)
        self.assertEqual(summary["weak_count"], 1)
        self.assertEqual(summary["top_ranked_count"], 1)
        self.assertEqual(summary["bottom_ranked_count"], 1)
        self.assertEqual(summary["avg_ranked_change_pct"], 0.45)
        self.assertEqual([item["name"] for item in breakdown], ["白酒", "银行"])

    def test_ensure_task_window_evaluations_skips_completed_non_cn_and_missing_dates(self) -> None:
        service = object.__new__(StockPickerService)
        service._repo = Mock()
        service._repo.get_task_candidate_rows.return_value = [
            {"candidate_id": 1, "code": "600519", "market": "cn", "latest_date": date(2026, 4, 10)},
            {"candidate_id": 2, "code": "AAPL", "market": "us", "latest_date": date(2026, 4, 10)},
            {"candidate_id": 3, "code": "000858", "market": "cn", "latest_date": None},
            {"candidate_id": 4, "code": "600036", "market": "cn", "latest_date": date(2026, 4, 10)},
        ]
        service._repo.list_task_evaluations.return_value = [
            {"candidate_id": 1, "window_days": 5, "eval_status": "completed"},
        ]
        service._repo.upsert_candidate_evaluation = Mock()
        service._evaluate_candidate_window = Mock(
            return_value={
                "eval_status": "pending",
                "entry_date": None,
                "entry_price": None,
                "exit_date": None,
                "exit_price": None,
                "benchmark_entry_price": None,
                "benchmark_exit_price": None,
                "return_pct": None,
                "benchmark_return_pct": None,
                "excess_return_pct": None,
                "max_drawdown_pct": None,
                "mfe_pct": None,
                "mae_pct": None,
            }
        )

        summary = StockPickerService._ensure_task_window_evaluations(
            service,
            task_id="picker-task-1",
            window_days=5,
            fetcher_manager=_FakeFetcherManager(),
        )

        self.assertEqual(summary["candidate_count"], 4)
        self.assertEqual(summary["skipped_completed"], 1)
        self.assertEqual(summary["skipped_non_cn"], 1)
        self.assertEqual(summary["skipped_missing_analysis_date"], 1)
        self.assertEqual(summary["pending"], 1)
        service._repo.upsert_candidate_evaluation.assert_called_once()

    def test_backfill_evaluations_aggregates_per_window_counts(self) -> None:
        service = object.__new__(StockPickerService)
        service._repo = Mock()
        service._repo.list_task_ids_for_backfill.return_value = ["task-1", "task-2"]
        service._ensure_task_window_evaluations = Mock(
            side_effect=[
                {"candidate_count": 2, "completed": 1, "pending": 1},
                {"candidate_count": 2, "completed": 2},
                {"candidate_count": 1, "skipped_completed": 1},
                {"candidate_count": 1, "benchmark_unavailable": 1},
            ]
        )

        with patch("src.stock_picker.service.DataFetcherManager", return_value=_FakeFetcherManager()):
            payload = StockPickerService.backfill_evaluations(
                service,
                window_days=[5, 10],
                since=date(2026, 4, 1),
                dry_run=True,
            )

        self.assertEqual(payload["task_count"], 2)
        self.assertEqual(payload["window_days"], [5, 10])
        self.assertTrue(payload["dry_run"])
        self.assertEqual(payload["per_window"][5]["candidate_count"], 3)
        self.assertEqual(payload["per_window"][5]["completed"], 1)
        self.assertEqual(payload["per_window"][5]["pending"], 1)
        self.assertEqual(payload["per_window"][10]["candidate_count"], 3)
        self.assertEqual(payload["per_window"][10]["completed"], 2)
        self.assertEqual(payload["per_window"][10]["benchmark_unavailable"], 1)

    def test_evaluate_candidate_window_returns_excursion_metrics(self) -> None:
        service = object.__new__(StockPickerService)
        candidate_bars = [
            type("Bar", (), {"date": date(2026, 4, 14), "open": 10.0, "high": 10.5, "low": 9.8, "close": 10.2})(),
            type("Bar", (), {"date": date(2026, 4, 15), "open": 10.1, "high": 10.8, "low": 9.9, "close": 10.6})(),
            type("Bar", (), {"date": date(2026, 4, 16), "open": 10.6, "high": 11.0, "low": 10.4, "close": 10.9})(),
            type("Bar", (), {"date": date(2026, 4, 17), "open": 10.9, "high": 11.2, "low": 10.7, "close": 11.0})(),
            type("Bar", (), {"date": date(2026, 4, 18), "open": 11.0, "high": 11.3, "low": 10.8, "close": 11.1})(),
        ]
        benchmark_bars = [
            type("Bar", (), {"date": date(2026, 4, 14), "open": 4.0, "high": 4.05, "low": 3.98, "close": 4.02})(),
            type("Bar", (), {"date": date(2026, 4, 15), "open": 4.01, "high": 4.08, "low": 4.0, "close": 4.04})(),
            type("Bar", (), {"date": date(2026, 4, 16), "open": 4.03, "high": 4.1, "low": 4.02, "close": 4.07})(),
            type("Bar", (), {"date": date(2026, 4, 17), "open": 4.07, "high": 4.11, "low": 4.05, "close": 4.09})(),
            type("Bar", (), {"date": date(2026, 4, 18), "open": 4.08, "high": 4.12, "low": 4.06, "close": 4.1})(),
        ]
        service._load_forward_bars = Mock(side_effect=[candidate_bars, benchmark_bars])

        payload = StockPickerService._evaluate_candidate_window(
            service,
            code="600519",
            analysis_date=date(2026, 4, 11),
            window_days=5,
            fetcher_manager=_FakeFetcherManager(),
            refresh_missing_data=False,
        )

        self.assertEqual(payload["eval_status"], "completed")
        self.assertAlmostEqual(payload["mfe_pct"], 13.0, places=2)
        self.assertAlmostEqual(payload["mae_pct"], -2.0, places=2)
        self.assertAlmostEqual(payload["max_drawdown_pct"], 2.0, places=2)

    def test_list_validation_stats_uses_time_split_and_monthly_rollup(self) -> None:
        service = object.__new__(StockPickerService)
        service._repo = Mock()
        rows = []
        for index in range(20):
            current_date = date(2026, 1, 1) + pd.Timedelta(days=index)
            rows.append(
                {
                    "market": "cn",
                    "template_id": "trend_breakout",
                    "rule_version": "v4_2_phase2",
                    "analysis_date": current_date.isoformat(),
                    "eval_status": "completed",
                    "return_pct": 4.0 if index >= 14 else 2.0,
                    "excess_return_pct": 1.0 if index >= 14 else 0.5,
                    "max_drawdown_pct": 2.0 if index >= 14 else 1.5,
                }
            )
        service._repo.list_evaluation_rows_for_window.return_value = rows

        payload = StockPickerService.list_validation_stats(service, window_days=10)

        self.assertEqual(payload["window_days"], 10)
        self.assertEqual(len(payload["out_of_sample_by_template"]), 1)
        holdout = payload["out_of_sample_by_template"][0]
        self.assertEqual(holdout["sample_status"], "ready")
        self.assertEqual(holdout["in_sample_count"], 14)
        self.assertEqual(holdout["out_of_sample_count"], 6)
        self.assertAlmostEqual(holdout["out_of_sample_win_rate_pct"], 100.0, places=2)
        self.assertTrue(payload["rolling_monthly_by_template"])

    def test_list_risk_stats_returns_distribution_metrics(self) -> None:
        service = object.__new__(StockPickerService)
        service._repo = Mock()
        rows = []
        for index in range(20):
            rows.append(
                {
                    "market": "cn",
                    "template_id": "trend_breakout",
                    "rule_version": "v4_2_phase2",
                    "analysis_date": (date(2026, 2, 1) + pd.Timedelta(days=index)).isoformat(),
                    "eval_status": "completed",
                    "return_pct": 1.0 + index * 0.2,
                    "excess_return_pct": -1.0 if index % 5 == 0 else 0.5 + index * 0.1,
                    "max_drawdown_pct": 1.0 + index * 0.05,
                    "mfe_pct": 2.0 + index * 0.15,
                    "mae_pct": -2.0 + index * 0.03,
                }
            )
        service._repo.list_evaluation_rows_for_window.return_value = rows

        payload = StockPickerService.list_risk_stats(service, window_days=10)

        self.assertEqual(payload["window_days"], 10)
        self.assertEqual(len(payload["items"]), 1)
        item = payload["items"][0]
        self.assertEqual(item["sample_status"], "ready")
        self.assertIsNotNone(item["profit_factor"])
        self.assertIsNotNone(item["return_pct_p50"])
        self.assertIsNotNone(item["mfe_pct_p75"])
        self.assertIsNotNone(item["mae_pct_p25"])

    def test_load_sector_catalog_invalidates_cache_when_effective_trading_date_changes(self) -> None:
        service = object.__new__(StockPickerService)
        service._sector_cache_lock = threading.Lock()
        service._sector_catalog_cache = None
        service._sector_catalog_cache_key = None
        service._build_sector_catalog = Mock(
            side_effect=[
                {"items": [{"sector_id": "白酒"}], "code_by_sector": {}},
                {"items": [{"sector_id": "银行"}], "code_by_sector": {}},
            ]
        )

        with (
            patch("src.stock_picker.service.get_effective_trading_date", side_effect=[date(2026, 4, 14), date(2026, 4, 14), date(2026, 4, 15)]),
            patch("src.stock_picker.service.DataFetcherManager", return_value=Mock()),
        ):
            first = StockPickerService._load_sector_catalog(service)
            second = StockPickerService._load_sector_catalog(service)
            third = StockPickerService._load_sector_catalog(service)

        self.assertEqual(first["items"][0]["sector_id"], "白酒")
        self.assertIs(first, second)
        self.assertEqual(third["items"][0]["sector_id"], "银行")
        self.assertEqual(service._build_sector_catalog.call_count, 2)

    def test_run_task_tracks_insufficient_reason_breakdown(self) -> None:
        service = object.__new__(StockPickerService)
        service._repo = Mock()
        service._repo.get_task.return_value = {
            "task_id": "picker-task-1",
            "template_id": "balanced",
            "status": "queued",
            "universe_id": "watchlist",
            "limit": 1,
            "ai_top_k": 1,
            "force_refresh": False,
            "request_payload": {
                "mode": "watchlist",
                "notify": False,
            },
        }
        service._resolve_task_stock_codes = Mock(return_value=(["600519", "000858"], []))
        service._evaluate_candidate = Mock(
            side_effect=[
                {
                    "candidate_state": "skipped",
                    "code": "600519",
                    "skip_reason": "stale_trading_date",
                    "skip_detail": {},
                },
                {
                    "rank": 0,
                    "code": "000858",
                    "name": "五粮液",
                    "market": "cn",
                    "selection_reason": "strict_match",
                    "strict_match": True,
                    "latest_date": date(2026, 4, 14),
                    "latest_close": 100.0,
                    "change_pct": 1.2,
                    "volume_ratio": 1.1,
                    "distance_to_high_pct": -0.5,
                    "trend_score": 30.0,
                    "setup_score": 30.0,
                    "volume_score": 20.0,
                    "sector_score": 10.0,
                    "news_score": 0.0,
                    "risk_penalty": 0.0,
                    "total_score": 90.0,
                    "board_names": [],
                    "news_briefs": [],
                    "score_breakdown": [],
                    "technical_snapshot": {"ma5": 10.0, "ma10": 9.8},
                },
            ]
        )
        service._build_search_service = Mock()
        service._fetch_news_briefs = Mock(return_value=None)
        service._load_sector_rankings = Mock(return_value=([], []))
        service._select_candidates = Mock(side_effect=lambda candidates, limit: list(candidates)[:limit])
        service._build_ai_explanation = Mock(return_value=None)
        service._ensure_task_evaluations = Mock()
        service._send_task_notification = Mock()
        service._build_market_regime_snapshot = Mock(
            return_value={"regime": "range_bound", "regime_label": "震荡整理"}
        )
        service._load_sector_catalog = Mock(
            return_value={
                "items": [],
                "code_by_sector": {},
                "catalog_policy": "dynamic_a_share_industry_from_stock_list",
                "source_name": "fake",
                "sector_count": 0,
                "stock_count": 0,
                "catalog_signature": "empty",
            }
        )
        service._futures = {}
        service._futures_lock = unittest.mock.MagicMock()

        config = object()
        analyzer_instance = Mock()
        with (
            patch("src.stock_picker.service.get_config", return_value=config),
            patch("src.stock_picker.service.DataFetcherManager"),
            patch("src.stock_picker.service.GeminiAnalyzer", return_value=analyzer_instance),
        ):
            StockPickerService._run_task(service, "picker-task-1")

        saved_summary = service._repo.save_candidates.call_args.kwargs["summary"]
        self.assertEqual(saved_summary["insufficient_count"], 1)
        self.assertEqual(saved_summary["insufficient_reason_breakdown"], {"stale_trading_date": 1})
        self.assertEqual(saved_summary["market_regime_snapshot"]["regime"], "range_bound")

    def test_build_ai_explanation_uses_compact_prompt_payloads(self) -> None:
        service = object.__new__(StockPickerService)
        analyzer = _FakeAnalyzer()
        candidate = {
            "name": "测试股票",
            "code": "600519",
            "market": "cn",
            "total_score": 88.6,
            "technical_snapshot": {
                "ma5": 10.1,
                "ma10": 9.9,
            },
            "board_names": [f"板块{i}" for i in range(10)],
            "news_briefs": [
                {
                    "title": "非常长的标题" * 20,
                    "source": "测试源",
                    "published_date": "2026-04-13",
                    "url": "https://example.com/very/long/url",
                    "snippet": "很长的新闻摘要" * 50,
                }
            ],
            "score_breakdown": [
                {"score_name": "trend_score", "score_label": "趋势结构", "score_value": 30.0, "detail": {"raw": 1}},
                {"score_name": "news_score", "score_label": "新闻情绪", "score_value": 4.0, "detail": {"raw": 2}},
                {"score_name": "total_score", "score_label": "综合得分", "score_value": 88.6, "detail": {"raw": 3}},
            ],
        }

        payload = service._build_ai_explanation(
            analyzer=analyzer,
            template_name="趋势突破",
            candidate=candidate,
        )

        self.assertIsNotNone(payload)
        self.assertEqual(payload["summary"], "测试摘要")
        prompt = analyzer.prompts[0]
        self.assertIn("核心板块", prompt)
        self.assertIn("最近新闻摘要", prompt)
        self.assertNotIn("https://example.com/very/long/url", prompt)
        self.assertNotIn('"detail"', prompt)
        self.assertNotIn("板块6", prompt)
        self.assertIn("结构化解释草案", prompt)
        self.assertLess(len(prompt), 2000)

    def test_select_candidates_skips_unqualified_fallback_fill(self) -> None:
        candidates = [
            {
                "code": "600519",
                "strict_match": True,
                "fallback_eligible": True,
                "selection_reason": "strict_match",
            },
            {
                "code": "000858",
                "strict_match": False,
                "fallback_eligible": False,
                "selection_reason": "fallback_fill",
            },
        ]

        selected = StockPickerService._select_candidates(candidates, limit=5)

        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]["code"], "600519")

    def test_rank_candidates_is_deterministic_for_same_scores(self) -> None:
        candidates = [
            {
                "code": "000002",
                "strict_match": True,
                "total_score": 88.0,
                "trend_score": 30.0,
                "setup_score": 28.0,
                "volume_score": 12.0,
                "sector_score": 5.0,
            },
            {
                "code": "000001",
                "strict_match": True,
                "total_score": 88.0,
                "trend_score": 30.0,
                "setup_score": 28.0,
                "volume_score": 12.0,
                "sector_score": 5.0,
            },
        ]

        ranked = StockPickerService._rank_candidates(candidates)

        self.assertEqual([item["code"] for item in ranked], ["000001", "000002"])

    def test_build_structured_explanation_uses_selection_context(self) -> None:
        candidate = {
            "name": "测试股票",
            "code": "600519",
            "total_score": 88.6,
            "distance_to_high_pct": -1.2,
            "volume_ratio": 1.3,
            "board_names": ["白酒"],
            "score_breakdown": [
                {"score_name": "trend_score", "score_value": 30.0, "detail": {}},
                {"score_name": "setup_score", "score_value": 28.0, "detail": {}},
                {"score_name": "volume_score", "score_value": 11.0, "detail": {}},
                {"score_name": "sector_score", "score_value": 6.0, "detail": {}},
                {
                    "score_name": "risk_penalty",
                    "score_value": -6.0,
                    "detail": {"flags": [{"label": "偏离 MA20 过大", "penalty": 6.0}]},
                },
            ],
            "technical_snapshot": {
                "ma10": 10.0,
                "ma20": 9.8,
                "change20d_pct": 8.5,
                "pullback_from_high_pct": -3.0,
                "ma20_slope_pct": 1.2,
                "selection_context": {
                    "strict_match": True,
                    "strict_reasons": ["收盘价站上 MA10", "MA5 > MA10 > MA20"],
                    "fallback_failures": [],
                },
            },
            "news_briefs": [],
        }

        payload = StockPickerService._build_structured_explanation("趋势突破", candidate)

        self.assertIn("严格命中条件", payload["rationale"][0])
        self.assertIn("偏离 MA20 过大", payload["risks"][0])
        self.assertTrue(payload["summary"].startswith("测试股票 当前为趋势突破严格命中候选"))

    def test_load_daily_frame_uses_trading_date_for_cache_freshness(self) -> None:
        reference_time = datetime(2026, 4, 13, 16, 0, 0)
        refreshed_frame = _build_daily_frame(date(2026, 4, 13))

        cases = (
            ("stale_cache_refresh", _build_daily_rows(date(2026, 4, 9)), 1, date(2026, 4, 13)),
            ("fresh_cache_reuse", _build_daily_rows(date(2026, 4, 13)), 0, date(2026, 4, 13)),
        )

        with (
            patch("src.stock_picker.service.get_market_for_stock", return_value="cn"),
            patch("src.stock_picker.service.get_effective_trading_date", return_value=date(2026, 4, 13)),
        ):
            for label, cached_rows, expected_fetch_calls, expected_latest_day in cases:
                with self.subTest(label=label):
                    service = object.__new__(StockPickerService)
                    service._repo = _FakeRepo(cached_rows)
                    service._stock_repo = _FakeStockRepo()
                    fetcher_manager = _FakeFetcherManager(refresh_df=refreshed_frame)

                    frame = service._load_daily_frame(
                        code="600519",
                        fetcher_manager=fetcher_manager,
                        force_refresh=False,
                        current_time=reference_time,
                    )

                    self.assertIsNotNone(frame)
                    self.assertEqual(len(fetcher_manager.daily_calls), expected_fetch_calls)
                    self.assertEqual(len(service._stock_repo.saved), expected_fetch_calls)
                    self.assertEqual(_latest_day(frame), expected_latest_day)

    def test_evaluate_candidate_rejects_stale_frame_by_trading_date(self) -> None:
        reference_time = datetime(2026, 4, 13, 16, 0, 0)
        stale_frame = _build_daily_frame(date(2026, 4, 9))
        service = object.__new__(StockPickerService)
        service._load_daily_frame = Mock(return_value=stale_frame)

        with (
            patch("src.stock_picker.service.get_market_for_stock", return_value="cn"),
            patch("src.stock_picker.service.get_effective_trading_date", return_value=date(2026, 4, 13)),
        ):
            candidate = service._evaluate_candidate(
                code="600519",
                template_id="balanced",
                market_regime_snapshot={"regime": "trend_up"},
                fetcher_manager=_FakeFetcherManager(),
                force_refresh=False,
                top_sectors=[],
                bottom_sectors=[],
                current_time=reference_time,
            )

        self.assertIsNotNone(candidate)
        self.assertEqual(candidate["candidate_state"], "skipped")
        self.assertEqual(candidate["skip_reason"], "stale_trading_date")

    def test_load_daily_frame_for_target_date_slices_cached_rows(self) -> None:
        service = object.__new__(StockPickerService)
        service._stock_repo = _FakeStockRepo(range_rows=_build_daily_rows(date(2026, 4, 18), count=60))
        fetcher_manager = _FakeFetcherManager(refresh_df=None)

        frame = service._load_daily_frame_for_target_date(
            code="600519",
            target_date=date(2026, 4, 10),
            fetcher_manager=fetcher_manager,
            force_refresh=False,
        )

        self.assertIsNotNone(frame)
        self.assertEqual(_latest_day(frame), date(2026, 4, 10))
        self.assertEqual(fetcher_manager.daily_calls, [])

    def test_replay_historical_run_builds_evaluation_summary(self) -> None:
        service = object.__new__(StockPickerService)
        service._resolve_task_stock_codes = Mock(return_value=(["600519", "000333"], []))
        service._build_market_regime_snapshot = Mock(
            return_value={"regime": "trend_up", "regime_label": "上行趋势"}
        )
        service._evaluate_candidate_for_target_date = Mock(
            side_effect=[
                {
                    "rank": 0,
                    "code": "600519",
                    "name": "贵州茅台",
                    "market": "cn",
                    "selection_reason": "strict_match",
                    "strict_match": True,
                    "latest_date": date(2026, 3, 20),
                    "latest_close": 100.0,
                    "change_pct": 1.2,
                    "volume_ratio": 1.3,
                    "distance_to_high_pct": -1.0,
                    "trend_score": 20.0,
                    "setup_score": 20.0,
                    "volume_score": 10.0,
                    "sector_score": 0.0,
                    "news_score": 0.0,
                    "risk_penalty": 2.0,
                    "total_score": 48.0,
                    "board_names": [],
                    "news_briefs": [],
                    "score_breakdown": [
                        {"score_name": "trend_score", "score_label": "趋势结构", "score_value": 20.0, "detail": {}},
                        {"score_name": "setup_score", "score_label": "模板匹配", "score_value": 20.0, "detail": {}},
                        {"score_name": "volume_score", "score_label": "量能配合", "score_value": 10.0, "detail": {}},
                        {"score_name": "sector_score", "score_label": "板块强度", "score_value": 0.0, "detail": {}},
                        {"score_name": "news_score", "score_label": "新闻情绪", "score_value": 0.0, "detail": {}},
                        {"score_name": "risk_penalty", "score_label": "风险扣分", "score_value": -2.0, "detail": {"flags": []}},
                        {"score_name": "total_score", "score_label": "综合得分", "score_value": 48.0, "detail": {}},
                    ],
                    "technical_snapshot": {
                        "ma10": 98.0,
                        "ma20": 95.0,
                        "change20d_pct": 6.0,
                        "pullback_from_high_pct": -2.0,
                        "ma20_slope_pct": 1.0,
                        "selection_context": {
                            "strict_match": True,
                            "strict_reasons": ["收盘价站上 MA10"],
                            "fallback_failures": [],
                        },
                    },
                    "fallback_eligible": True,
                },
                {
                    "candidate_state": "skipped",
                    "code": "000333",
                    "skip_reason": "insufficient_history",
                },
            ]
        )
        service._evaluate_candidate_window = Mock(
            side_effect=[
                {"eval_status": "completed", "entry_date": date(2026, 3, 23), "entry_price": 10.0, "exit_date": date(2026, 3, 27), "exit_price": 11.0, "benchmark_entry_price": 20.0, "benchmark_exit_price": 20.5, "return_pct": 10.0, "benchmark_return_pct": 2.5, "excess_return_pct": 7.5, "max_drawdown_pct": 1.0},
                {"eval_status": "pending", "entry_date": None, "entry_price": None, "exit_date": None, "exit_price": None, "benchmark_entry_price": None, "benchmark_exit_price": None, "return_pct": None, "benchmark_return_pct": None, "excess_return_pct": None, "max_drawdown_pct": None},
            ]
        )
        service._build_structured_explanation = Mock(
            return_value={
                "summary": "结构化说明",
                "rationale": ["理由1"],
                "risks": ["风险1"],
                "watchpoints": ["观察1"],
            }
        )

        with patch("src.stock_picker.service.DataFetcherManager", return_value=_FakeFetcherManager()):
            payload = StockPickerService.replay_historical_run(
                service,
                target_date=date(2026, 3, 20),
                template_id="balanced",
                mode="watchlist",
                limit=5,
                window_days=[5, 10],
            )

        self.assertEqual(payload["summary"]["selected_count"], 1)
        self.assertEqual(payload["summary"]["insufficient_reason_breakdown"]["insufficient_history"], 1)
        self.assertEqual(payload["summary"]["evaluation_summary"][5]["completed"], 1)
        self.assertEqual(payload["summary"]["evaluation_summary"][10]["pending"], 1)
        self.assertEqual(payload["candidates"][0]["evaluations"][0]["eval_status"], "completed")
        self.assertEqual(payload["summary"]["replay_policy"]["news_mode"], "disabled")
        self.assertEqual(payload["summary"]["market_regime_snapshot"]["regime"], "trend_up")

    def test_load_forward_bars_refreshes_benchmark_via_special_path(self) -> None:
        service = object.__new__(StockPickerService)
        forward_bars = [object(), object(), object(), object(), object()]
        service._stock_repo = Mock()
        service._stock_repo.get_forward_bars.side_effect = [
            [],
            forward_bars,
        ]
        service._refresh_benchmark_daily_data = Mock(return_value=True)

        bars = service._load_forward_bars(
            code="000300",
            analysis_date=date(2026, 3, 20),
            window_days=5,
            fetcher_manager=_FakeFetcherManager(),
            refresh_missing_data=True,
        )

        self.assertEqual(bars, forward_bars)
        service._refresh_benchmark_daily_data.assert_called_once()
        self.assertEqual(service._stock_repo.get_forward_bars.call_count, 2)

    def test_enrich_shortlist_candidates_adds_advanced_factor_score_and_failure_flags(self) -> None:
        service = object.__new__(StockPickerService)
        service._build_research_confidence = Mock(
            return_value={
                "status": "calibrated_neutral",
                "label": "中性（已校准）",
                "score": 0.6,
            }
        )
        candidate = {
            "code": "600519",
            "name": "贵州茅台",
            "market": "cn",
            "template_id": "trend_breakout",
            "strict_match": True,
            "selection_reason": "strict_match",
            "latest_close": 100.0,
            "distance_to_high_pct": -7.5,
            "trend_score": 30.0,
            "setup_score": 24.0,
            "volume_score": 11.0,
            "sector_score": 6.0,
            "news_score": -3.5,
            "total_score": 72.0,
            "board_names": ["白酒"],
            "execution_constraints": {
                "status": "cautious",
                "status_label": "执行谨慎",
                "not_fillable": False,
                "liquidity_bucket": "medium",
            },
            "trade_plan": {"action": "buy"},
            "score_breakdown": [
                {"score_name": "news_score", "score_label": "新闻情绪", "score_value": -3.5, "detail": {}},
                {
                    "score_name": "total_score",
                    "score_label": "综合得分",
                    "score_value": 72.0,
                    "detail": {"component_scores": {}},
                },
            ],
            "technical_snapshot": {
                "template_id": "trend_breakout",
                "market_regime": "trend_up",
                "environment_fit": "suitable",
                "signal_bucket": "high",
                "change20d_pct": 12.0,
                "pullback_from_high_pct": -4.0,
                "ma20": 101.0,
                "ma20_slope_pct": -1.2,
                "amount": 18_000_000,
                "avg_amount20": 12_000_000,
            },
        }
        fetcher_manager = _FakeFetcherManager(
            fundamental_context={
                "status": "ok",
                "coverage": {"capital_flow": "ok", "earnings": "ok"},
                "growth": {"payload": {"net_profit_yoy": 22.0, "revenue_yoy": 12.0}},
                "earnings": {"payload": {"dividend": {"ttm_dividend_yield_pct": 2.4}}},
                "capital_flow": {"payload": {"stock_flow": {"main_net_inflow": 80_000_000, "inflow_5d": 20_000_000}}},
                "dragon_tiger": {"payload": {"is_on_list": True}},
            },
            chip_distribution={"profit_ratio": 0.72, "concentration_90": 0.12},
        )

        enriched_count = StockPickerService._enrich_shortlist_candidates(
            service,
            candidates=[candidate],
            fetcher_manager=fetcher_manager,
            market_regime_snapshot={"signals": {"change20d_pct": 5.0}},
            top_sectors=[{"name": "白酒", "change_pct": 3.5}],
            bottom_sectors=[],
        )

        self.assertEqual(enriched_count, 1)
        self.assertGreater(candidate["total_score"], 72.0)
        self.assertIn("advanced_factors", candidate)
        self.assertEqual(candidate["score_breakdown"][1]["score_name"], "total_score")
        advanced_row = next(item for item in candidate["score_breakdown"] if item["score_name"] == "advanced_factor_score")
        self.assertGreater(advanced_row["score_value"], 0.0)
        self.assertIn("advanced_factor_total", candidate["score_breakdown"][1]["detail"]["component_scores"])
        self.assertTrue(candidate["template_failure_flags"])
        self.assertEqual(candidate["template_failure_flags"][0]["source"], "rule_engine")

    def test_build_template_failure_flags_adds_quality_headwind_flags(self) -> None:
        service = object.__new__(StockPickerService)
        candidate = {
            "template_id": "trend_breakout",
            "latest_close": 100.0,
            "distance_to_high_pct": -2.0,
            "volume_ratio": 0.9,
            "execution_constraints": {
                "status": "cautious",
                "not_fillable": False,
            },
            "technical_snapshot": {
                "ma20": 99.0,
                "ma20_slope_pct": 0.4,
            },
            "advanced_factors": {
                "relative_strength": {"excess_change20d_pct": -1.5},
                "board_leadership": {"matched_top_count": 0, "matched_bottom_count": 1},
                "liquidity_quality": {"amount_ratio": 0.55},
            },
        }

        flags = StockPickerService._build_template_failure_flags(service, candidate)
        flag_names = {item["flag"] for item in flags}

        self.assertIn("relative_strength_negative", flag_names)
        self.assertIn("board_headwind", flag_names)
        self.assertIn("liquidity_support_weak", flag_names)

    def test_rank_candidates_prefers_actionable_and_lower_risk_candidate_on_same_score(self) -> None:
        candidates = [
            {
                "code": "600519",
                "total_score": 88.0,
                "strict_match": True,
                "trend_score": 28.0,
                "setup_score": 24.0,
                "volume_score": 10.0,
                "sector_score": 5.0,
                "trade_plan": {"action": "observe"},
                "research_confidence": {"status": "observe_only"},
                "execution_confidence": {"status": "cautious"},
                "ai_review": {"veto_level": "caution"},
                "advanced_factors": {"factor_total": 3.0},
                "template_failure_flags": [
                    {"flag": "environment_mismatch", "label": "环境失配", "severity": "critical"}
                ],
                "technical_snapshot": {},
            },
            {
                "code": "000858",
                "total_score": 88.0,
                "strict_match": True,
                "trend_score": 28.0,
                "setup_score": 24.0,
                "volume_score": 10.0,
                "sector_score": 5.0,
                "trade_plan": {"action": "buy"},
                "research_confidence": {"status": "calibrated_neutral"},
                "execution_confidence": {"status": "tradable"},
                "ai_review": {"veto_level": "pass"},
                "advanced_factors": {"factor_total": 3.0},
                "template_failure_flags": [],
                "technical_snapshot": {},
            },
        ]

        ranked = StockPickerService._rank_candidates(candidates)

        self.assertEqual([item["code"] for item in ranked], ["000858", "600519"])

    def test_build_structured_explanation_surfaces_quality_signals_consistently(self) -> None:
        candidate = {
            "code": "600519",
            "name": "贵州茅台",
            "template_id": "trend_breakout",
            "selection_reason": "strict_match",
            "strict_match": True,
            "total_score": 81.5,
            "distance_to_high_pct": -1.2,
            "volume_ratio": 1.15,
            "board_names": ["白酒"],
            "news_score": 0.0,
            "trade_plan": {"action": "observe", "stop_loss_rule": "跌破 MA20 止损"},
            "research_confidence": {"status": "observe_only", "label": "观察中"},
            "execution_confidence": {"status": "cautious", "label": "执行谨慎"},
            "advanced_factors": {
                "factor_total": 6.2,
                "relative_strength": {"excess_change20d_pct": 4.8},
            },
            "ai_review": {
                "veto_level": "soft_veto",
                "counter_points": ["高位波动放大"],
                "veto_reasons": ["执行约束与高位波动叠加，先观察"],
            },
            "template_failure_flags": [
                {"flag": "execution_untradable", "label": "执行约束显示近似不可成交，不适合直接执行。", "severity": "critical"}
            ],
            "score_breakdown": [
                {
                    "score_name": "total_score",
                    "score_label": "综合得分",
                    "score_value": 81.5,
                    "detail": {"selection_context": {"strict_match": True, "strict_reasons": ["MA5 > MA10 > MA20"]}},
                },
                {
                    "score_name": "risk_penalty",
                    "score_label": "风险扣分",
                    "score_value": -3.0,
                    "detail": {"flags": [{"label": "高位波动放大"}]},
                },
            ],
            "technical_snapshot": {
                "selection_context": {"strict_match": True, "strict_reasons": ["MA5 > MA10 > MA20"]},
                "change20d_pct": 11.8,
                "pullback_from_high_pct": -3.2,
                "ma10": 98.4,
                "ma20": 95.2,
                "ma20_slope_pct": 0.6,
                "market_regime_label": "上行趋势",
                "environment_fit_label": "环境匹配",
            },
        }

        payload = StockPickerService._build_structured_explanation("趋势突破", candidate)

        self.assertIn("观察", payload["summary"])
        self.assertTrue(any("高级因子总加分" in item for item in payload["rationale"]))
        self.assertTrue(any("执行约束显示近似不可成交" in item for item in payload["risks"]))
        self.assertTrue(any("复核反例" in item for item in payload["watchpoints"]))

    def test_build_ai_review_returns_penalty_for_soft_veto(self) -> None:
        service = object.__new__(StockPickerService)
        analyzer = Mock()
        analyzer.generate_text.return_value = json.dumps(
            {
                "review_summary": "结构尚可，但执行条件与波动不匹配。",
                "supporting_points": ["趋势结构仍在"],
                "counter_points": ["高位波动放大"],
                "veto_level": "soft_veto",
                "veto_reasons": ["高位波动与执行约束叠加，暂不适合直接执行"],
                "confidence_comment": "更适合作为观察样本。",
            },
            ensure_ascii=False,
        )
        candidate = {
            "code": "600519",
            "name": "贵州茅台",
            "market": "cn",
            "template_id": "trend_breakout",
            "selection_reason": "strict_match",
            "environment_fit": "suitable",
            "signal_bucket": "high",
            "total_score": 88.0,
            "news_briefs": [],
            "execution_constraints": {"status": "cautious", "status_label": "执行谨慎"},
            "research_confidence": {"status": "calibrated_neutral", "label": "中性（已校准）", "score": 0.66},
            "advanced_factors": {"factor_total": 9.5},
            "template_failure_flags": [{"flag": "negative_event_pressure", "label": "负面事件压力"}],
            "technical_snapshot": {"market_regime": "trend_up"},
        }

        payload = StockPickerService._build_ai_review(
            service,
            analyzer=analyzer,
            template_name="趋势突破",
            candidate=candidate,
            base_explanation={
                "summary": "结构化摘要",
                "rationale": ["理由1"],
                "risks": ["风险1"],
                "watchpoints": ["观察1"],
            },
        )

        self.assertIsNotNone(payload)
        self.assertEqual(payload["veto_level"], "soft_veto")
        self.assertEqual(payload["penalty_score"], 6.0)
        self.assertEqual(payload["review_scope"]["rule_version"], "v4_2_phase2")

    def test_run_task_applies_ai_soft_veto_penalty_and_summary_counts(self) -> None:
        service = object.__new__(StockPickerService)
        service._repo = Mock()
        service._repo.get_task.return_value = {
            "task_id": "picker-task-1",
            "template_id": "trend_breakout",
            "status": "queued",
            "universe_id": "watchlist",
            "limit": 2,
            "ai_top_k": 2,
            "force_refresh": False,
            "request_payload": {"mode": "watchlist", "notify": False},
        }
        service._resolve_task_stock_codes = Mock(return_value=(["600519", "000858"], []))
        service._evaluate_candidate = Mock(
            side_effect=[
                {
                    "rank": 0,
                    "code": "600519",
                    "name": "贵州茅台",
                    "market": "cn",
                    "template_id": "trend_breakout",
                    "selection_reason": "strict_match",
                    "strict_match": True,
                    "latest_date": date(2026, 4, 14),
                    "latest_close": 100.0,
                    "change_pct": 1.2,
                    "volume_ratio": 1.1,
                    "distance_to_high_pct": -0.5,
                    "trend_score": 30.0,
                    "setup_score": 28.0,
                    "volume_score": 12.0,
                    "sector_score": 6.0,
                    "news_score": 0.0,
                    "risk_penalty": 0.0,
                    "total_score": 88.0,
                    "board_names": ["白酒"],
                    "news_briefs": [],
                    "score_breakdown": [
                        {
                            "score_name": "total_score",
                            "score_label": "综合得分",
                            "score_value": 88.0,
                            "detail": {"component_scores": {}},
                        }
                    ],
                    "technical_snapshot": {
                        "template_id": "trend_breakout",
                        "market_regime": "trend_up",
                        "signal_bucket": "high",
                        "trade_plan": {"action": "buy"},
                    },
                    "trade_plan": {"action": "buy"},
                    "execution_constraints": {"status": "tradable", "status_label": "可执行", "not_fillable": False},
                    "research_confidence": {"status": "calibrated_neutral", "label": "中性（已校准）", "score": 0.65},
                    "execution_confidence": {"status": "tradable", "label": "可执行", "score": 0.8},
                    "advanced_factors": {},
                    "ai_review": {},
                    "template_failure_flags": [],
                    "fallback_eligible": True,
                },
                {
                    "rank": 0,
                    "code": "000858",
                    "name": "五粮液",
                    "market": "cn",
                    "template_id": "trend_breakout",
                    "selection_reason": "strict_match",
                    "strict_match": True,
                    "latest_date": date(2026, 4, 14),
                    "latest_close": 100.0,
                    "change_pct": 1.2,
                    "volume_ratio": 1.1,
                    "distance_to_high_pct": -0.5,
                    "trend_score": 28.0,
                    "setup_score": 26.0,
                    "volume_score": 11.0,
                    "sector_score": 5.0,
                    "news_score": 0.0,
                    "risk_penalty": 0.0,
                    "total_score": 84.0,
                    "board_names": ["白酒"],
                    "news_briefs": [],
                    "score_breakdown": [
                        {
                            "score_name": "total_score",
                            "score_label": "综合得分",
                            "score_value": 84.0,
                            "detail": {"component_scores": {}},
                        }
                    ],
                    "technical_snapshot": {
                        "template_id": "trend_breakout",
                        "market_regime": "trend_up",
                        "signal_bucket": "high",
                        "trade_plan": {"action": "buy"},
                    },
                    "trade_plan": {"action": "buy"},
                    "execution_constraints": {"status": "tradable", "status_label": "可执行", "not_fillable": False},
                    "research_confidence": {"status": "calibrated_neutral", "label": "中性（已校准）", "score": 0.62},
                    "execution_confidence": {"status": "tradable", "label": "可执行", "score": 0.78},
                    "advanced_factors": {},
                    "ai_review": {},
                    "template_failure_flags": [],
                    "fallback_eligible": True,
                },
            ]
        )
        service._build_search_service = Mock()
        service._fetch_news_briefs = Mock(return_value=None)
        service._load_sector_rankings = Mock(return_value=([], []))
        service._enrich_shortlist_candidates = Mock(return_value=2)
        service._build_structured_explanation = Mock(
            side_effect=lambda _template_name, candidate: {
                "summary": f"score={candidate['total_score']:.1f} action={(candidate.get('trade_plan') or {}).get('action')}",
                "rationale": ["理由1"],
                "risks": ["风险1"],
                "watchpoints": ["观察1"],
            }
        )
        service._build_ai_explanation = Mock(return_value=None)
        service._build_ai_review = Mock(
            side_effect=[
                {
                    "review_summary": "保持通过",
                    "supporting_points": ["趋势结构稳定"],
                    "counter_points": [],
                    "veto_level": "pass",
                    "veto_reasons": [],
                    "confidence_comment": "通过",
                    "review_scope": {"rule_version": "v4_2_phase2"},
                    "penalty_score": 0.0,
                },
                {
                    "review_summary": "执行质量不足，先观察。",
                    "supporting_points": ["趋势结构仍在"],
                    "counter_points": ["执行质量不足"],
                    "veto_level": "soft_veto",
                    "veto_reasons": ["执行质量不足，先观察。"],
                    "confidence_comment": "仅观察",
                    "review_scope": {"rule_version": "v4_2_phase2"},
                    "penalty_score": 6.0,
                },
            ]
        )
        service._ensure_task_evaluations = Mock()
        service._send_task_notification = Mock()
        service._build_market_regime_snapshot = Mock(
            return_value={"regime": "trend_up", "regime_label": "上行趋势", "signals": {"change20d_pct": 5.0}}
        )
        service._load_sector_catalog = Mock(
            return_value={
                "items": [],
                "code_by_sector": {},
                "catalog_policy": "dynamic_a_share_industry_from_stock_list",
                "source_name": "fake",
                "sector_count": 0,
                "stock_count": 0,
                "catalog_signature": "empty",
            }
        )
        service._futures = {}
        service._futures_lock = unittest.mock.MagicMock()

        with (
            patch("src.stock_picker.service.get_config", return_value=object()),
            patch("src.stock_picker.service.DataFetcherManager"),
            patch("src.stock_picker.service.GeminiAnalyzer", return_value=Mock()),
        ):
            StockPickerService._run_task(service, "picker-task-1")

        saved_summary = service._repo.save_candidates.call_args.kwargs["summary"]
        saved_candidates = service._repo.save_candidates.call_args.kwargs["candidates"]
        self.assertEqual(saved_summary["advanced_enriched_count"], 2)
        self.assertEqual(saved_summary["ai_reviewed_count"], 2)
        self.assertEqual(saved_summary["ai_soft_veto_count"], 1)
        soft_veto_candidate = next(item for item in saved_candidates if item["code"] == "000858")
        self.assertEqual(soft_veto_candidate["trade_plan"]["action"], "observe")
        self.assertEqual(soft_veto_candidate["ai_review"]["veto_level"], "soft_veto")
        self.assertEqual(soft_veto_candidate["explanation_summary"], "score=78.0 action=observe")
        self.assertTrue(any(item["score_name"] == "ai_review_penalty" for item in soft_veto_candidate["score_breakdown"]))


if __name__ == "__main__":
    unittest.main()
