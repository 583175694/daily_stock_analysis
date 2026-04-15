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

    def list_template_stats(self, *, window_days: int) -> dict:
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


if __name__ == "__main__":
    unittest.main()
