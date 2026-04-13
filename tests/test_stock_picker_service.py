# -*- coding: utf-8 -*-
import json
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


class _FakeStockRepo:
    def __init__(self) -> None:
        self.saved = []

    def save_dataframe(self, df: pd.DataFrame, code: str, data_source: str = "") -> None:
        self.saved.append((code, data_source, len(df)))


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


class TestStockPickerService(unittest.TestCase):
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
        self.assertLess(len(prompt), 2000)

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

        self.assertIsNone(candidate)


if __name__ == "__main__":
    unittest.main()
