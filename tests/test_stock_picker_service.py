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
    def __init__(self, refresh_df: pd.DataFrame | None = None, source_name: str = "fake") -> None:
        self.refresh_df = refresh_df
        self.source_name = source_name
        self.daily_calls = []

    def get_daily_data(self, code: str, days: int = 120):
        self.daily_calls.append((code, days))
        return self.refresh_df, self.source_name

    def get_stock_name(self, code: str, allow_realtime: bool = False) -> str:
        return "测试股票"

    def get_belong_boards(self, code: str):
        return []

    def get_sector_rankings(self, n: int = 10):
        return [], []


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
        self.assertEqual(capture_repo.created["template_version"], "v3_phase2")
        self.assertEqual(capture_repo.created["ai_top_k"], 10)
        self.assertTrue(capture_repo.created["request_payload"]["notify"])
        self.assertEqual(capture_repo.created["request_payload"]["request_policy_version"], "v3_phase2")
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
                "template_version": "v3_phase2",
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


if __name__ == "__main__":
    unittest.main()
