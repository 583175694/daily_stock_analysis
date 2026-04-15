# -*- coding: utf-8 -*-
import unittest
from datetime import date, datetime

import pandas as pd

from src.storage import DatabaseManager, PickerTask
from src.stock_picker.repository import StockPickerRepository


class TestStockPickerRepository(unittest.TestCase):
    def setUp(self) -> None:
        DatabaseManager.reset_instance()
        self.db = DatabaseManager(db_url="sqlite:///:memory:")
        self.repo = StockPickerRepository(db_manager=self.db)

    def tearDown(self) -> None:
        DatabaseManager.reset_instance()

    def test_get_recent_daily_rows_returns_detached_safe_snapshots(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "date": date(2026, 4, 10),
                    "open": 10.0,
                    "high": 10.5,
                    "low": 9.8,
                    "close": 10.2,
                    "volume": 1000.0,
                    "amount": 10200.0,
                    "pct_chg": 2.0,
                    "ma5": 10.0,
                    "ma10": 9.9,
                    "ma20": 9.7,
                    "volume_ratio": 1.2,
                },
                {
                    "date": date(2026, 4, 11),
                    "open": 10.2,
                    "high": 10.9,
                    "low": 10.1,
                    "close": 10.8,
                    "volume": 1200.0,
                    "amount": 12960.0,
                    "pct_chg": 5.88,
                    "ma5": 10.1,
                    "ma10": 10.0,
                    "ma20": 9.8,
                    "volume_ratio": 1.35,
                },
            ]
        )
        self.db.save_daily_data(frame, code="600519", data_source="unit-test")

        rows = self.repo.get_recent_daily_rows("600519", limit=5)

        self.assertEqual(len(rows), 2)
        self.assertIsInstance(rows[0], dict)
        self.assertEqual(rows[0]["date"], date(2026, 4, 11))
        self.assertEqual(rows[0]["close"], 10.8)
        self.assertEqual(rows[1]["date"], date(2026, 4, 10))

    def test_task_detail_includes_candidate_evaluations(self) -> None:
        self.repo.create_task(
            task_id="picker-task-1",
            template_id="balanced",
            template_version="v3_phase1",
            universe_id="watchlist",
            limit=20,
            ai_top_k=5,
            force_refresh=False,
            request_payload={"mode": "watchlist"},
        )
        self.repo.save_candidates(
            "picker-task-1",
            summary={"selected_count": 1},
            candidates=[
                {
                    "rank": 1,
                    "code": "600519",
                    "name": "贵州茅台",
                    "market": "cn",
                    "selection_reason": "strict_match",
                    "latest_date": date(2026, 4, 11),
                    "latest_close": 10.8,
                    "change_pct": 5.88,
                    "volume_ratio": 1.35,
                    "distance_to_high_pct": -0.8,
                    "total_score": 92.4,
                    "board_names": ["白酒"],
                    "news_briefs": [],
                    "explanation_summary": "测试摘要",
                    "explanation_rationale": ["理由1"],
                    "explanation_risks": ["风险1"],
                    "explanation_watchpoints": ["观察点1"],
                    "technical_snapshot": {},
                    "score_breakdown": [],
                }
            ],
        )
        candidate_rows = self.repo.get_task_candidate_rows("picker-task-1")
        self.assertEqual(len(candidate_rows), 1)

        self.repo.upsert_candidate_evaluation(
            picker_candidate_id=candidate_rows[0]["candidate_id"],
            window_days=5,
            benchmark_code="000300",
            eval_status="completed",
            entry_date=date(2026, 4, 14),
            entry_price=10.5,
            exit_date=date(2026, 4, 18),
            exit_price=11.1,
            benchmark_entry_price=4.0,
            benchmark_exit_price=4.1,
            return_pct=5.71,
            benchmark_return_pct=2.5,
            excess_return_pct=3.21,
            max_drawdown_pct=1.8,
        )

        payload = self.repo.get_task("picker-task-1", include_candidates=True)

        self.assertIsNotNone(payload)
        self.assertEqual(len(payload["candidates"]), 1)
        evaluations = payload["candidates"][0]["evaluations"]
        self.assertEqual(len(evaluations), 1)
        self.assertEqual(evaluations[0]["window_days"], 5)
        self.assertEqual(evaluations[0]["eval_status"], "completed")
        self.assertEqual(evaluations[0]["benchmark_status"], "completed")
        self.assertTrue(evaluations[0]["is_comparable"])
        self.assertAlmostEqual(evaluations[0]["excess_return_pct"], 3.21, places=2)

    def test_list_task_ids_can_filter_completed_tasks(self) -> None:
        self.repo.create_task(
            task_id="picker-task-queued",
            template_id="balanced",
            template_version="v3_phase1",
            universe_id="watchlist",
            limit=20,
            ai_top_k=5,
            force_refresh=False,
            request_payload={"mode": "watchlist"},
        )
        self.repo.create_task(
            task_id="picker-task-completed",
            template_id="balanced",
            template_version="v3_phase1",
            universe_id="watchlist",
            limit=20,
            ai_top_k=5,
            force_refresh=False,
            request_payload={"mode": "watchlist"},
        )
        self.repo.save_candidates(
            "picker-task-completed",
            summary={"selected_count": 0},
            candidates=[],
        )

        completed_ids = self.repo.list_task_ids(status="completed")
        all_ids = self.repo.list_task_ids()

        self.assertEqual(completed_ids, ["picker-task-completed"])
        self.assertEqual(all_ids, ["picker-task-completed", "picker-task-queued"])

    def test_list_task_ids_for_backfill_supports_since_and_limit(self) -> None:
        self.repo.create_task(
            task_id="picker-task-older",
            template_id="balanced",
            template_version="v3_phase2",
            universe_id="watchlist",
            limit=20,
            ai_top_k=5,
            force_refresh=False,
            request_payload={"mode": "watchlist"},
        )
        self.repo.create_task(
            task_id="picker-task-newer",
            template_id="balanced",
            template_version="v3_phase2",
            universe_id="watchlist",
            limit=20,
            ai_top_k=5,
            force_refresh=False,
            request_payload={"mode": "watchlist"},
        )

        with self.db.session_scope() as session:
            tasks = {row.task_id: row for row in session.query(PickerTask).all()}
            tasks["picker-task-older"].created_at = datetime(2026, 4, 1, 9, 0, 0)
            tasks["picker-task-newer"].created_at = datetime(2026, 4, 12, 9, 0, 0)

        filtered_ids = self.repo.list_task_ids_for_backfill(since=date(2026, 4, 10), limit=1)

        self.assertEqual(filtered_ids, ["picker-task-newer"])

    def test_task_detail_preserves_benchmark_unavailable_status(self) -> None:
        self.repo.create_task(
            task_id="picker-task-benchmark-gap",
            template_id="balanced",
            template_version="v3_phase1",
            universe_id="watchlist",
            limit=20,
            ai_top_k=5,
            force_refresh=False,
            request_payload={"mode": "watchlist"},
        )
        self.repo.save_candidates(
            "picker-task-benchmark-gap",
            summary={"selected_count": 1},
            candidates=[
                {
                    "rank": 1,
                    "code": "000858",
                    "name": "五粮液",
                    "market": "cn",
                    "selection_reason": "strict_match",
                    "latest_date": date(2026, 4, 11),
                    "latest_close": 10.8,
                    "change_pct": 5.88,
                    "volume_ratio": 1.35,
                    "distance_to_high_pct": -0.8,
                    "total_score": 92.4,
                    "board_names": ["白酒"],
                    "news_briefs": [],
                    "explanation_summary": "测试摘要",
                    "explanation_rationale": ["理由1"],
                    "explanation_risks": ["风险1"],
                    "explanation_watchpoints": ["观察点1"],
                    "technical_snapshot": {},
                    "score_breakdown": [],
                }
            ],
        )
        candidate_rows = self.repo.get_task_candidate_rows("picker-task-benchmark-gap")
        self.repo.upsert_candidate_evaluation(
            picker_candidate_id=candidate_rows[0]["candidate_id"],
            window_days=10,
            benchmark_code="000300",
            eval_status="benchmark_unavailable",
            entry_date=date(2026, 4, 14),
            entry_price=10.5,
            exit_date=date(2026, 4, 24),
            exit_price=11.1,
            benchmark_entry_price=None,
            benchmark_exit_price=None,
            return_pct=5.71,
            benchmark_return_pct=None,
            excess_return_pct=None,
            max_drawdown_pct=1.8,
        )

        payload = self.repo.get_task("picker-task-benchmark-gap", include_candidates=True)

        evaluation = payload["candidates"][0]["evaluations"][0]
        self.assertEqual(evaluation["eval_status"], "benchmark_unavailable")
        self.assertEqual(evaluation["benchmark_status"], "unavailable")
        self.assertFalse(evaluation["is_comparable"])
        self.assertAlmostEqual(evaluation["return_pct"], 5.71, places=2)


if __name__ == "__main__":
    unittest.main()
