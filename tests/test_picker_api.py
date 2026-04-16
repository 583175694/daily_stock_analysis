# -*- coding: utf-8 -*-
"""Focused API contract tests for picker endpoints."""

from __future__ import annotations

import unittest

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.deps import get_stock_picker_service
from api.v1.endpoints.picker import router as picker_router


class _FakeStockPickerService:
    def __init__(self) -> None:
        self.received_window_days: int | None = None
        self.received_stratified_window_days: int | None = None
        self.received_calibration_window_days: int | None = None
        self.received_validation_window_days: int | None = None
        self.received_risk_window_days: int | None = None

    def list_template_stats(self, *, window_days: int) -> dict:
        if window_days not in (5, 10, 20):
            raise ValueError("window_days 必须为 5, 10, 20 之一。")
        self.received_window_days = window_days
        return {
            "window_days": window_days,
            "benchmark_code": "000300",
            "items": [
                {
                    "template_id": "trend_breakout",
                    "template_name": "趋势突破",
                    "window_days": window_days,
                    "total_evaluations": 0,
                    "comparable_evaluations": 0,
                    "benchmark_unavailable_evaluations": 0,
                    "win_rate_pct": None,
                    "avg_return_pct": None,
                    "avg_excess_return_pct": None,
                    "avg_max_drawdown_pct": None,
                }
            ],
        }

    def list_stratified_stats(self, *, window_days: int) -> dict:
        if window_days not in (5, 10, 20):
            raise ValueError("window_days 必须为 5, 10, 20 之一。")
        self.received_stratified_window_days = window_days
        item = {
            "bucket_key": "trend_up",
            "bucket_label": "上行趋势",
            "total_evaluations": 1,
            "comparable_evaluations": 1,
            "benchmark_unavailable_evaluations": 0,
            "win_rate_pct": 100.0,
            "avg_return_pct": 3.2,
            "avg_excess_return_pct": 1.8,
            "avg_max_drawdown_pct": 1.1,
        }
        return {
            "window_days": window_days,
            "benchmark_code": "000300",
            "by_market_regime": [item],
            "by_template": [{**item, "bucket_key": "trend_breakout", "bucket_label": "趋势突破"}],
            "by_rank_bucket": [{**item, "bucket_key": "top_1_3", "bucket_label": "Top 1-3"}],
            "by_signal_bucket": [{**item, "bucket_key": "high", "bucket_label": "高信号"}],
        }

    def get_task(self, task_id: str) -> dict | None:
        return {
            "task_id": task_id,
            "status": "completed",
            "template_id": "trend_breakout",
            "template_version": "v4_2_phase2",
            "universe_id": "watchlist",
            "limit": 20,
            "ai_top_k": 5,
            "force_refresh": False,
            "total_stocks": 1,
            "processed_stocks": 1,
            "candidate_count": 1,
            "progress_percent": 100,
            "progress_message": "已完成",
            "summary": {
                "advanced_enriched_count": 1,
                "ai_reviewed_count": 1,
                "ai_soft_veto_count": 1,
            },
            "error_message": None,
            "request_payload": {"mode": "watchlist"},
            "created_at": None,
            "started_at": None,
            "finished_at": None,
            "updated_at": None,
            "candidates": [
                {
                    "rank": 1,
                    "code": "600519",
                    "name": "贵州茅台",
                    "market": "cn",
                    "selection_reason": "strict_match",
                    "technical_snapshot": {},
                    "execution_constraints": {
                        "status": "cautious",
                        "status_label": "执行谨慎",
                        "slippage_bps": 15,
                    },
                    "research_confidence": {
                        "status": "calibration_pending",
                        "label": "观察中（待校准）",
                        "comparable_samples": 12,
                    },
                    "execution_confidence": {
                        "status": "cautious",
                        "label": "执行谨慎",
                        "score": 0.45,
                    },
                    "trade_plan": {"action": "buy"},
                    "advanced_factors": {
                        "factor_total": 8.5,
                    },
                    "ai_review": {
                        "veto_level": "soft_veto",
                        "review_summary": "执行质量不足，先观察。",
                    },
                    "template_failure_flags": [
                        {
                            "flag": "execution_untradable",
                            "label": "执行约束恶化",
                            "severity": "high",
                            "source": "rule_engine",
                        }
                    ],
                    "score_breakdown": [],
                    "evaluations": [
                        {
                            "window_days": 5,
                            "benchmark_code": "000300",
                            "eval_status": "completed",
                            "benchmark_status": "completed",
                            "is_comparable": True,
                            "entry_date": "2026-04-13",
                            "entry_price": 1666.66,
                            "exit_date": "2026-04-18",
                            "exit_price": 1710.88,
                            "benchmark_entry_price": 3900.0,
                            "benchmark_exit_price": 3945.0,
                            "return_pct": 2.65,
                            "benchmark_return_pct": 1.15,
                            "excess_return_pct": 1.5,
                            "max_drawdown_pct": 1.2,
                            "mfe_pct": 3.6,
                            "mae_pct": -0.8,
                        }
                    ],
                }
            ],
        }

    def list_calibration_stats(self, *, window_days: int) -> dict:
        if window_days not in (5, 10, 20):
            raise ValueError("window_days 必须为 5, 10, 20 之一。")
        self.received_calibration_window_days = window_days
        return {
            "window_days": window_days,
            "benchmark_code": "000300",
            "items": [
                {
                    "template_id": "trend_breakout",
                    "template_name": "趋势突破",
                    "market_regime": "trend_up",
                    "market_regime_label": "上行趋势",
                    "rule_version": "v4_2_phase2",
                    "bucket_key": "high",
                    "bucket_label": "高信号桶",
                    "window_days": window_days,
                    "samples": 18,
                    "nominal_probability_pct": 70.0,
                    "actual_win_rate_pct": 66.0,
                    "calibration_gap_pct": 4.0,
                    "avg_return_pct": 4.9,
                    "avg_excess_return_pct": 2.4,
                    "avg_max_drawdown_pct": 2.2,
                    "calibration_status": "calibrated",
                    "calibration_label": "校准通过",
                    "high_confidence_gate": {
                        "status": "blocked",
                        "label": "未达高置信度门槛",
                        "passed": False,
                    },
                }
            ],
        }

    def list_validation_stats(self, *, window_days: int) -> dict:
        if window_days not in (5, 10, 20):
            raise ValueError("window_days 必须为 5, 10, 20 之一。")
        self.received_validation_window_days = window_days
        return {
            "window_days": window_days,
            "benchmark_code": "000300",
            "out_of_sample_by_template": [
                {
                    "template_id": "trend_breakout",
                    "template_name": "趋势突破",
                    "rule_version": "v4_2_phase2",
                    "window_days": window_days,
                    "sample_status": "ready",
                    "comparable_samples": 20,
                    "in_sample_count": 14,
                    "out_of_sample_count": 6,
                    "split_ratio": 0.7,
                    "analysis_date_start": "2026-01-01",
                    "analysis_date_end": "2026-01-20",
                    "out_of_sample_win_rate_pct": 66.7,
                    "out_of_sample_avg_return_pct": 4.1,
                    "out_of_sample_avg_excess_return_pct": 2.0,
                    "out_of_sample_avg_max_drawdown_pct": 1.8,
                }
            ],
            "rolling_monthly_by_template": [
                {
                    "template_id": "trend_breakout",
                    "template_name": "趋势突破",
                    "rule_version": "v4_2_phase2",
                    "window_days": window_days,
                    "rolling_month": "2026-01",
                    "sample_status": "ready",
                    "rolling_count": 12,
                    "rolling_win_rate_pct": 58.3,
                    "rolling_avg_excess_return_pct": 1.6,
                    "rolling_avg_max_drawdown_pct": 2.1,
                }
            ],
        }

    def list_risk_stats(self, *, window_days: int) -> dict:
        if window_days not in (5, 10, 20):
            raise ValueError("window_days 必须为 5, 10, 20 之一。")
        self.received_risk_window_days = window_days
        return {
            "window_days": window_days,
            "benchmark_code": "000300",
            "items": [
                {
                    "template_id": "trend_breakout",
                    "template_name": "趋势突破",
                    "rule_version": "v4_2_phase2",
                    "window_days": window_days,
                    "sample_status": "ready",
                    "sample_count": 20,
                    "avg_return_pct": 4.8,
                    "avg_excess_return_pct": 2.2,
                    "avg_max_drawdown_pct": 2.1,
                    "avg_mfe_pct": 5.3,
                    "avg_mae_pct": -1.4,
                    "profit_factor": 1.8,
                    "return_drawdown_ratio": 1.05,
                    "return_pct_p25": 2.4,
                    "return_pct_p50": 4.3,
                    "return_pct_p75": 6.2,
                    "excess_return_pct_p25": 0.8,
                    "excess_return_pct_p50": 2.0,
                    "excess_return_pct_p75": 3.4,
                    "max_drawdown_pct_p25": 1.3,
                    "max_drawdown_pct_p50": 2.0,
                    "max_drawdown_pct_p75": 2.8,
                    "mfe_pct_p25": 3.1,
                    "mfe_pct_p50": 5.0,
                    "mfe_pct_p75": 6.8,
                    "mae_pct_p25": -2.2,
                    "mae_pct_p50": -1.5,
                    "mae_pct_p75": -0.9,
                }
            ],
        }


class PickerApiTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.fake_service = _FakeStockPickerService()
        app = FastAPI()
        app.include_router(picker_router, prefix="/api/v1/picker")
        app.dependency_overrides[get_stock_picker_service] = (
            lambda: self.fake_service
        )
        self.client = TestClient(app)

    def test_template_stats_accepts_browser_query_string_window_days(self) -> None:
        for window_days in ("5", "10", "20"):
            with self.subTest(window_days=window_days):
                response = self.client.get(
                    "/api/v1/picker/stats/templates",
                    params={"window_days": window_days},
                )

                self.assertEqual(response.status_code, 200)
                self.assertEqual(self.fake_service.received_window_days, int(window_days))
                payload = response.json()
                self.assertEqual(payload["window_days"], int(window_days))
                self.assertEqual(payload["benchmark_code"], "000300")
                self.assertEqual(payload["items"][0]["window_days"], int(window_days))

    def test_template_stats_rejects_unsupported_window_days(self) -> None:
        response = self.client.get(
            "/api/v1/picker/stats/templates",
            params={"window_days": "7"},
        )

        self.assertEqual(response.status_code, 400)
        payload = response.json()
        self.assertEqual(payload["detail"]["error"], "invalid_params")
        self.assertIn("window_days", payload["detail"]["message"])

    def test_stratified_stats_accepts_browser_query_string_window_days(self) -> None:
        response = self.client.get(
            "/api/v1/picker/stats/stratified",
            params={"window_days": "10"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.fake_service.received_stratified_window_days, 10)
        payload = response.json()
        self.assertEqual(payload["window_days"], 10)
        self.assertEqual(payload["benchmark_code"], "000300")
        self.assertEqual(payload["by_market_regime"][0]["bucket_key"], "trend_up")

    def test_task_detail_exposes_execution_and_confidence_fields(self) -> None:
        response = self.client.get("/api/v1/picker/tasks/picker-task-1")

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        candidate = payload["candidates"][0]
        self.assertEqual(candidate["execution_constraints"]["status"], "cautious")
        self.assertEqual(candidate["research_confidence"]["label"], "观察中（待校准）")
        self.assertEqual(candidate["execution_confidence"]["score"], 0.45)
        self.assertEqual(payload["summary"]["advanced_enriched_count"], 1)
        self.assertEqual(candidate["advanced_factors"]["factor_total"], 8.5)
        self.assertEqual(candidate["ai_review"]["veto_level"], "soft_veto")
        self.assertEqual(candidate["template_failure_flags"][0]["source"], "rule_engine")

    def test_calibration_stats_accepts_browser_query_string_window_days(self) -> None:
        response = self.client.get(
            "/api/v1/picker/stats/calibration",
            params={"window_days": "10"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.fake_service.received_calibration_window_days, 10)
        payload = response.json()
        self.assertEqual(payload["window_days"], 10)
        self.assertEqual(payload["items"][0]["bucket_key"], "high")
        self.assertEqual(payload["items"][0]["calibration_status"], "calibrated")

    def test_validation_stats_accepts_browser_query_string_window_days(self) -> None:
        response = self.client.get(
            "/api/v1/picker/stats/validation",
            params={"window_days": "10"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.fake_service.received_validation_window_days, 10)
        payload = response.json()
        self.assertEqual(payload["window_days"], 10)
        self.assertEqual(payload["out_of_sample_by_template"][0]["rule_version"], "v4_2_phase2")
        self.assertEqual(payload["rolling_monthly_by_template"][0]["rolling_month"], "2026-01")

    def test_risk_stats_accepts_browser_query_string_window_days(self) -> None:
        response = self.client.get(
            "/api/v1/picker/stats/risk",
            params={"window_days": "10"},
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(self.fake_service.received_risk_window_days, 10)
        payload = response.json()
        self.assertEqual(payload["window_days"], 10)
        self.assertEqual(payload["items"][0]["sample_status"], "ready")
        self.assertEqual(payload["items"][0]["mfe_pct_p50"], 5.0)


if __name__ == "__main__":
    unittest.main()
