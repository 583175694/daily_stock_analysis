from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import delete, desc, select

from src.storage import (
    DatabaseManager,
    PickerCandidate,
    PickerCandidateEvaluation,
    PickerCandidateScore,
    PickerTask,
    StockDaily,
)


def _json_dumps(value: Any) -> str:
    return json.dumps(value if value is not None else {}, ensure_ascii=False)


def _json_loads(value: Optional[str], default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except Exception:
        return default


def _iso_datetime(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat() if value else None


def _iso_date(value: Optional[date]) -> Optional[str]:
    return value.isoformat() if value else None


def _benchmark_status(eval_status: str, benchmark_return_pct: Optional[float], excess_return_pct: Optional[float]) -> str:
    if eval_status == "benchmark_unavailable":
        return "unavailable"
    if eval_status != "completed":
        return eval_status
    if benchmark_return_pct is None or excess_return_pct is None:
        return "unavailable"
    return "completed"


class StockPickerRepository:
    """Persistence adapter for stock-picker tasks and results."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db = db_manager or DatabaseManager.get_instance()

    def mark_incomplete_tasks_failed(self) -> int:
        with self.db.session_scope() as session:
            rows = session.execute(
                select(PickerTask).where(PickerTask.status.in_(("queued", "running")))
            ).scalars().all()
            for row in rows:
                row.status = "failed"
                row.error_message = "服务重启，任务未完成，请重新发起。"
                row.progress_message = "服务重启后任务被终止"
                row.progress_percent = row.progress_percent or 0
                row.finished_at = datetime.now()
                row.updated_at = datetime.now()
            return len(rows)

    def create_task(
        self,
        *,
        task_id: str,
        template_id: str,
        template_version: str,
        universe_id: str,
        limit: int,
        ai_top_k: int,
        force_refresh: bool,
        request_payload: Dict[str, Any],
    ) -> None:
        with self.db.session_scope() as session:
            session.add(
                PickerTask(
                    task_id=task_id,
                    status="queued",
                    template_id=template_id,
                    template_version=template_version,
                    universe_id=universe_id,
                    result_limit=limit,
                    ai_top_k=ai_top_k,
                    force_refresh=force_refresh,
                    progress_percent=0,
                    progress_message="任务已创建，等待执行",
                    request_payload_json=_json_dumps(request_payload),
                )
            )

    def start_task(self, task_id: str, *, total_stocks: int) -> None:
        with self.db.session_scope() as session:
            task = session.execute(
                select(PickerTask).where(PickerTask.task_id == task_id)
            ).scalar_one()
            task.status = "running"
            task.total_stocks = total_stocks
            task.processed_stocks = 0
            task.progress_percent = 5 if total_stocks > 0 else 0
            task.progress_message = "开始扫描股票池"
            task.started_at = datetime.now()
            task.updated_at = datetime.now()

    def update_progress(
        self,
        task_id: str,
        *,
        progress_percent: int,
        progress_message: str,
        processed_stocks: Optional[int] = None,
        total_stocks: Optional[int] = None,
    ) -> None:
        with self.db.session_scope() as session:
            task = session.execute(
                select(PickerTask).where(PickerTask.task_id == task_id)
            ).scalar_one()
            task.progress_percent = max(0, min(100, int(progress_percent)))
            task.progress_message = progress_message
            if processed_stocks is not None:
                task.processed_stocks = processed_stocks
            if total_stocks is not None:
                task.total_stocks = total_stocks
            task.updated_at = datetime.now()

    def save_candidates(
        self,
        task_id: str,
        *,
        summary: Dict[str, Any],
        candidates: List[Dict[str, Any]],
    ) -> None:
        with self.db.session_scope() as session:
            task = session.execute(
                select(PickerTask).where(PickerTask.task_id == task_id)
            ).scalar_one()

            session.execute(
                delete(PickerCandidateEvaluation).where(
                    PickerCandidateEvaluation.picker_candidate_id.in_(
                        select(PickerCandidate.id).where(PickerCandidate.picker_task_id == task.id)
                    )
                )
            )
            session.execute(
                delete(PickerCandidateScore).where(
                    PickerCandidateScore.picker_candidate_id.in_(
                        select(PickerCandidate.id).where(PickerCandidate.picker_task_id == task.id)
                    )
                )
            )
            session.execute(delete(PickerCandidate).where(PickerCandidate.picker_task_id == task.id))

            for candidate in candidates:
                row = PickerCandidate(
                    picker_task_id=task.id,
                    rank=int(candidate["rank"]),
                    code=str(candidate["code"]),
                    name=str(candidate.get("name") or ""),
                    market=str(candidate.get("market") or "cn"),
                    selection_reason=str(candidate.get("selection_reason") or "strict_match"),
                    latest_date=candidate.get("latest_date"),
                    latest_close=candidate.get("latest_close"),
                    change_pct=candidate.get("change_pct"),
                    volume_ratio=candidate.get("volume_ratio"),
                    distance_to_high_pct=candidate.get("distance_to_high_pct"),
                    total_score=candidate.get("total_score"),
                    board_names_json=_json_dumps(candidate.get("board_names") or []),
                    news_briefs_json=_json_dumps(candidate.get("news_briefs") or []),
                    explanation_summary=candidate.get("explanation_summary"),
                    explanation_rationale_json=_json_dumps(candidate.get("explanation_rationale") or []),
                    explanation_risks_json=_json_dumps(candidate.get("explanation_risks") or []),
                    explanation_watchpoints_json=_json_dumps(candidate.get("explanation_watchpoints") or []),
                    technical_snapshot_json=_json_dumps(candidate.get("technical_snapshot") or {}),
                )
                session.add(row)
                session.flush()

                for score in candidate.get("score_breakdown") or []:
                    session.add(
                        PickerCandidateScore(
                            picker_candidate_id=row.id,
                            score_name=str(score.get("score_name") or ""),
                            score_label=str(score.get("score_label") or ""),
                            score_value=float(score.get("score_value") or 0.0),
                            detail_json=_json_dumps(score.get("detail") or {}),
                        )
                    )

            task.status = "completed"
            task.candidate_count = len(candidates)
            task.progress_percent = 100
            task.progress_message = "选股完成"
            task.summary_json = _json_dumps(summary)
            task.error_message = None
            task.finished_at = datetime.now()
            task.updated_at = datetime.now()

    def fail_task(self, task_id: str, *, error_message: str) -> None:
        with self.db.session_scope() as session:
            task = session.execute(
                select(PickerTask).where(PickerTask.task_id == task_id)
            ).scalar_one_or_none()
            if task is None:
                return
            task.status = "failed"
            task.error_message = error_message
            task.progress_message = "任务失败"
            task.finished_at = datetime.now()
            task.updated_at = datetime.now()

    def list_tasks(self, *, limit: int = 20) -> List[Dict[str, Any]]:
        with self.db.session_scope() as session:
            rows = session.execute(
                select(PickerTask)
                .order_by(desc(PickerTask.created_at))
                .limit(limit)
            ).scalars().all()
            return [self._serialize_task(row) for row in rows]

    def list_task_ids(self, *, status: Optional[str] = None) -> List[str]:
        return self.list_task_ids_for_backfill(status=status)

    def list_task_ids_for_backfill(
        self,
        *,
        status: Optional[str] = None,
        since: Optional[date] = None,
        limit: Optional[int] = None,
    ) -> List[str]:
        with self.db.session_scope() as session:
            query = select(PickerTask.task_id).order_by(desc(PickerTask.created_at), desc(PickerTask.id))
            if status:
                query = query.where(PickerTask.status == status)
            if since is not None:
                query = query.where(PickerTask.created_at >= datetime.combine(since, datetime.min.time()))
            if limit is not None:
                query = query.limit(max(1, int(limit)))
            rows = session.execute(query).scalars().all()
            return [str(row) for row in rows]

    def get_task(self, task_id: str, *, include_candidates: bool = True) -> Optional[Dict[str, Any]]:
        with self.db.session_scope() as session:
            task = session.execute(
                select(PickerTask).where(PickerTask.task_id == task_id)
            ).scalar_one_or_none()
            if task is None:
                return None

            payload = self._serialize_task(task)
            if not include_candidates:
                return payload

            candidate_rows = session.execute(
                select(PickerCandidate)
                .where(PickerCandidate.picker_task_id == task.id)
                .order_by(PickerCandidate.rank.asc(), PickerCandidate.total_score.desc())
            ).scalars().all()
            if not candidate_rows:
                payload["candidates"] = []
                return payload

            candidate_ids = [row.id for row in candidate_rows]
            score_rows = session.execute(
                select(PickerCandidateScore)
                .where(PickerCandidateScore.picker_candidate_id.in_(candidate_ids))
                .order_by(
                    PickerCandidateScore.picker_candidate_id.asc(),
                    PickerCandidateScore.id.asc(),
                )
            ).scalars().all()
            score_map: Dict[int, List[PickerCandidateScore]] = {}
            for score_row in score_rows:
                score_map.setdefault(score_row.picker_candidate_id, []).append(score_row)

            evaluation_rows = session.execute(
                select(PickerCandidateEvaluation)
                .where(PickerCandidateEvaluation.picker_candidate_id.in_(candidate_ids))
                .order_by(
                    PickerCandidateEvaluation.picker_candidate_id.asc(),
                    PickerCandidateEvaluation.window_days.asc(),
                )
            ).scalars().all()
            evaluation_map: Dict[int, List[PickerCandidateEvaluation]] = {}
            for evaluation_row in evaluation_rows:
                evaluation_map.setdefault(evaluation_row.picker_candidate_id, []).append(evaluation_row)

            payload["candidates"] = [
                self._serialize_candidate(
                    row,
                    score_map.get(row.id, []),
                    evaluation_map.get(row.id, []),
                )
                for row in candidate_rows
            ]
            return payload

    def get_task_candidate_rows(self, task_id: str) -> List[Dict[str, Any]]:
        with self.db.session_scope() as session:
            task = session.execute(
                select(PickerTask).where(PickerTask.task_id == task_id)
            ).scalar_one_or_none()
            if task is None:
                return []

            candidate_rows = session.execute(
                select(PickerCandidate)
                .where(PickerCandidate.picker_task_id == task.id)
                .order_by(PickerCandidate.rank.asc(), PickerCandidate.total_score.desc())
            ).scalars().all()
            return [
                {
                    "candidate_id": row.id,
                    "code": row.code,
                    "market": row.market,
                    "latest_date": row.latest_date,
                }
                for row in candidate_rows
            ]

    def upsert_candidate_evaluation(
        self,
        *,
        picker_candidate_id: int,
        window_days: int,
        benchmark_code: str,
        eval_status: str,
        entry_date: Optional[date],
        entry_price: Optional[float],
        exit_date: Optional[date],
        exit_price: Optional[float],
        benchmark_entry_price: Optional[float],
        benchmark_exit_price: Optional[float],
        return_pct: Optional[float],
        benchmark_return_pct: Optional[float],
        excess_return_pct: Optional[float],
        max_drawdown_pct: Optional[float],
    ) -> None:
        with self.db.session_scope() as session:
            row = session.execute(
                select(PickerCandidateEvaluation).where(
                    PickerCandidateEvaluation.picker_candidate_id == picker_candidate_id,
                    PickerCandidateEvaluation.window_days == window_days,
                )
            ).scalar_one_or_none()
            if row is None:
                row = PickerCandidateEvaluation(
                    picker_candidate_id=picker_candidate_id,
                    window_days=window_days,
                )
                session.add(row)

            row.benchmark_code = benchmark_code
            row.eval_status = eval_status
            row.entry_date = entry_date
            row.entry_price = entry_price
            row.exit_date = exit_date
            row.exit_price = exit_price
            row.benchmark_entry_price = benchmark_entry_price
            row.benchmark_exit_price = benchmark_exit_price
            row.return_pct = return_pct
            row.benchmark_return_pct = benchmark_return_pct
            row.excess_return_pct = excess_return_pct
            row.max_drawdown_pct = max_drawdown_pct
            row.updated_at = datetime.now()

    def list_task_evaluations(
        self,
        task_id: str,
        *,
        window_days: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        with self.db.session_scope() as session:
            task = session.execute(
                select(PickerTask).where(PickerTask.task_id == task_id)
            ).scalar_one_or_none()
            if task is None:
                return []

            query = (
                select(PickerCandidateEvaluation, PickerCandidate.code)
                .join(PickerCandidate, PickerCandidate.id == PickerCandidateEvaluation.picker_candidate_id)
                .where(PickerCandidate.picker_task_id == task.id)
            )
            if window_days is not None:
                query = query.where(PickerCandidateEvaluation.window_days == window_days)

            rows = session.execute(
                query.order_by(
                    PickerCandidateEvaluation.window_days.asc(),
                    PickerCandidate.rank.asc(),
                )
            ).all()
            return [
                {
                    "candidate_id": evaluation.picker_candidate_id,
                    "code": code,
                    "window_days": evaluation.window_days,
                    "benchmark_code": evaluation.benchmark_code,
                    "eval_status": evaluation.eval_status,
                    "benchmark_status": _benchmark_status(
                        evaluation.eval_status,
                        evaluation.benchmark_return_pct,
                        evaluation.excess_return_pct,
                    ),
                    "is_comparable": evaluation.benchmark_return_pct is not None and evaluation.excess_return_pct is not None,
                    "entry_date": _iso_date(evaluation.entry_date),
                    "entry_price": evaluation.entry_price,
                    "exit_date": _iso_date(evaluation.exit_date),
                    "exit_price": evaluation.exit_price,
                    "benchmark_entry_price": evaluation.benchmark_entry_price,
                    "benchmark_exit_price": evaluation.benchmark_exit_price,
                    "return_pct": evaluation.return_pct,
                    "benchmark_return_pct": evaluation.benchmark_return_pct,
                    "excess_return_pct": evaluation.excess_return_pct,
                    "max_drawdown_pct": evaluation.max_drawdown_pct,
                }
                for evaluation, code in rows
            ]

    def list_evaluation_rows_for_window(self, window_days: int) -> List[Dict[str, Any]]:
        with self.db.session_scope() as session:
            rows = session.execute(
                select(
                    PickerCandidateEvaluation,
                    PickerCandidate.code,
                    PickerCandidate.market,
                    PickerTask.template_id,
                )
                .join(PickerCandidate, PickerCandidate.id == PickerCandidateEvaluation.picker_candidate_id)
                .join(PickerTask, PickerTask.id == PickerCandidate.picker_task_id)
                .where(PickerCandidateEvaluation.window_days == window_days)
                .order_by(PickerCandidateEvaluation.updated_at.desc())
            ).all()
            return [
                {
                    "candidate_id": evaluation.picker_candidate_id,
                    "code": code,
                    "market": market,
                    "template_id": template_id,
                    "window_days": evaluation.window_days,
                    "eval_status": evaluation.eval_status,
                    "benchmark_status": _benchmark_status(
                        evaluation.eval_status,
                        evaluation.benchmark_return_pct,
                        evaluation.excess_return_pct,
                    ),
                    "is_comparable": evaluation.benchmark_return_pct is not None and evaluation.excess_return_pct is not None,
                    "return_pct": evaluation.return_pct,
                    "benchmark_return_pct": evaluation.benchmark_return_pct,
                    "excess_return_pct": evaluation.excess_return_pct,
                    "max_drawdown_pct": evaluation.max_drawdown_pct,
                }
                for evaluation, code, market, template_id in rows
            ]

    def get_recent_daily_rows(self, code: str, *, limit: int = 90) -> List[Dict[str, Any]]:
        with self.db.session_scope() as session:
            rows = session.execute(
                select(
                    StockDaily.date.label("date"),
                    StockDaily.open.label("open"),
                    StockDaily.high.label("high"),
                    StockDaily.low.label("low"),
                    StockDaily.close.label("close"),
                    StockDaily.volume.label("volume"),
                    StockDaily.amount.label("amount"),
                    StockDaily.pct_chg.label("pct_chg"),
                    StockDaily.ma5.label("ma5"),
                    StockDaily.ma10.label("ma10"),
                    StockDaily.ma20.label("ma20"),
                    StockDaily.volume_ratio.label("volume_ratio"),
                )
                .where(StockDaily.code == code)
                .order_by(desc(StockDaily.date))
                .limit(limit)
            ).mappings().all()
            return [dict(row) for row in rows]

    @staticmethod
    def _serialize_task(row: PickerTask) -> Dict[str, Any]:
        return {
            "task_id": row.task_id,
            "status": row.status,
            "template_id": row.template_id,
            "template_version": row.template_version,
            "universe_id": row.universe_id,
            "limit": row.result_limit,
            "ai_top_k": row.ai_top_k,
            "force_refresh": row.force_refresh,
            "total_stocks": row.total_stocks or 0,
            "processed_stocks": row.processed_stocks or 0,
            "candidate_count": row.candidate_count or 0,
            "progress_percent": row.progress_percent or 0,
            "progress_message": row.progress_message or "",
            "summary": _json_loads(row.summary_json, {}),
            "error_message": row.error_message,
            "request_payload": _json_loads(row.request_payload_json, {}),
            "created_at": _iso_datetime(row.created_at),
            "started_at": _iso_datetime(row.started_at),
            "finished_at": _iso_datetime(row.finished_at),
            "updated_at": _iso_datetime(row.updated_at),
        }

    @staticmethod
    def _serialize_candidate(
        row: PickerCandidate,
        score_rows: List[PickerCandidateScore],
        evaluation_rows: List[PickerCandidateEvaluation],
    ) -> Dict[str, Any]:
        return {
            "rank": row.rank,
            "code": row.code,
            "name": row.name,
            "market": row.market,
            "selection_reason": row.selection_reason,
            "latest_date": _iso_date(row.latest_date),
            "latest_close": row.latest_close,
            "change_pct": row.change_pct,
            "volume_ratio": row.volume_ratio,
            "distance_to_high_pct": row.distance_to_high_pct,
            "total_score": row.total_score,
            "board_names": _json_loads(row.board_names_json, []),
            "news_briefs": _json_loads(row.news_briefs_json, []),
            "explanation_summary": row.explanation_summary,
            "explanation_rationale": _json_loads(row.explanation_rationale_json, []),
            "explanation_risks": _json_loads(row.explanation_risks_json, []),
            "explanation_watchpoints": _json_loads(row.explanation_watchpoints_json, []),
            "technical_snapshot": _json_loads(row.technical_snapshot_json, {}),
            "score_breakdown": [
                {
                    "score_name": score_row.score_name,
                    "score_label": score_row.score_label,
                    "score_value": score_row.score_value,
                    "detail": _json_loads(score_row.detail_json, {}),
                }
                for score_row in score_rows
            ],
            "evaluations": [
                {
                    "window_days": evaluation_row.window_days,
                    "benchmark_code": evaluation_row.benchmark_code,
                    "eval_status": evaluation_row.eval_status,
                    "benchmark_status": _benchmark_status(
                        evaluation_row.eval_status,
                        evaluation_row.benchmark_return_pct,
                        evaluation_row.excess_return_pct,
                    ),
                    "is_comparable": evaluation_row.benchmark_return_pct is not None and evaluation_row.excess_return_pct is not None,
                    "entry_date": _iso_date(evaluation_row.entry_date),
                    "entry_price": evaluation_row.entry_price,
                    "exit_date": _iso_date(evaluation_row.exit_date),
                    "exit_price": evaluation_row.exit_price,
                    "benchmark_entry_price": evaluation_row.benchmark_entry_price,
                    "benchmark_exit_price": evaluation_row.benchmark_exit_price,
                    "return_pct": evaluation_row.return_pct,
                    "benchmark_return_pct": evaluation_row.benchmark_return_pct,
                    "excess_return_pct": evaluation_row.excess_return_pct,
                    "max_drawdown_pct": evaluation_row.max_drawdown_pct,
                }
                for evaluation_row in evaluation_rows
            ],
        }
