from __future__ import annotations

import json
import hashlib
import logging
import re
import threading
import uuid
from collections import defaultdict
from concurrent.futures import Future, ThreadPoolExecutor
from copy import deepcopy
from datetime import date, datetime, time, timedelta
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import pandas as pd

from data_provider.base import DataFetcherManager, canonical_stock_code, normalize_stock_code
from src.notification import NotificationService
from src.analyzer import GeminiAnalyzer
from src.config import get_config
from src.core.trading_calendar import get_effective_trading_date, get_market_for_stock
from src.repositories.stock_repo import StockRepository
from src.search_service import SearchResponse, SearchService
from src.stock_picker.repository import StockPickerRepository
from src.stock_picker.templates import get_template, list_templates

logger = logging.getLogger(__name__)

DEFAULT_PICKER_BENCHMARK_CODE = "000300"
PICKER_POLICY_VERSION = "v3_phase2"
PICKER_EVAL_WINDOWS = (5, 10, 20)
PICKER_FALLBACK_RULES: Dict[str, Dict[str, float]] = {
    "trend_breakout": {"min_total_score": 58.0, "min_trend_score": 20.0, "max_risk_penalty": 10.0},
    "strong_pullback": {"min_total_score": 54.0, "min_trend_score": 16.0, "max_risk_penalty": 10.0},
    "balanced": {"min_total_score": 52.0, "min_trend_score": 14.0, "max_risk_penalty": 12.0},
}
PICKER_SKIP_REASON_LABELS: Dict[str, str] = {
    "daily_data_unavailable": "日线数据缺失",
    "insufficient_history": "历史行情不足 20 个交易日",
    "stale_trading_date": "最新行情日期落后于目标交易日",
    "unsupported_template": "模板未命中受支持的评分逻辑",
    "unknown": "未知原因",
}

_POSITIVE_NEWS_KEYWORDS = (
    "增持", "回购", "中标", "签约", "订单", "突破", "预增", "超预期", "新高",
    "合作", "涨停", "加仓", "buyback", "beat", "upgrade", "record", "contract",
)
_NEGATIVE_NEWS_KEYWORDS = (
    "减持", "问询", "处罚", "诉讼", "亏损", "下修", "下调", "风险", "停牌",
    "违约", "跳水", "大跌", "downgrade", "miss", "fraud", "lawsuit", "warning",
)


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        result = float(value)
        if pd.isna(result):
            return default
        return result
    except Exception:
        return default


def _dedupe_codes(codes: Sequence[str]) -> List[str]:
    deduped: List[str] = []
    seen = set()
    for code in codes:
        canonical = canonical_stock_code(code)
        if not canonical or canonical in seen:
            continue
        seen.add(canonical)
        deduped.append(canonical)
    return deduped


def _row_value(row: Any, key: str) -> Any:
    if isinstance(row, Mapping):
        return row.get(key)
    if hasattr(row, "_mapping"):
        return row._mapping.get(key)
    return getattr(row, key, None)


def _rows_to_dataframe(rows: Sequence[Any]) -> pd.DataFrame:
    records: List[Dict[str, Any]] = []
    for row in reversed(list(rows)):
        records.append(
            {
                "date": _row_value(row, "date"),
                "open": _row_value(row, "open"),
                "high": _row_value(row, "high"),
                "low": _row_value(row, "low"),
                "close": _row_value(row, "close"),
                "volume": _row_value(row, "volume"),
                "amount": _row_value(row, "amount"),
                "pct_chg": _row_value(row, "pct_chg"),
                "ma5": _row_value(row, "ma5"),
                "ma10": _row_value(row, "ma10"),
                "ma20": _row_value(row, "ma20"),
                "volume_ratio": _row_value(row, "volume_ratio"),
            }
        )
    return pd.DataFrame(records)


def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    frame = df.copy()
    if "date" not in frame.columns:
        raise ValueError("daily data missing date column")
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
    for column in ("open", "high", "low", "close", "volume", "amount", "pct_chg"):
        if column not in frame.columns:
            frame[column] = 0.0
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame = frame.dropna(subset=["date", "close"]).sort_values("date").reset_index(drop=True)
    if len(frame) < 20:
        return frame

    frame["ma5"] = frame["close"].rolling(5).mean()
    frame["ma10"] = frame["close"].rolling(10).mean()
    frame["ma20"] = frame["close"].rolling(20).mean()
    frame["ma60"] = frame["close"].rolling(60).mean()
    frame["volume_ma5"] = frame["volume"].rolling(5).mean()
    frame["volume_ma20"] = frame["volume"].rolling(20).mean()
    if frame["pct_chg"].isna().all():
        frame["pct_chg"] = frame["close"].pct_change() * 100
    return frame


def _frame_latest_day(frame: Optional[pd.DataFrame]) -> Optional[date]:
    if frame is None or frame.empty or "date" not in frame.columns:
        return None
    latest = frame.iloc[-1]["date"]
    if isinstance(latest, pd.Timestamp):
        return latest.date()
    if isinstance(latest, datetime):
        return latest.date()
    if hasattr(latest, "date"):
        return latest.date()
    return latest


def _slice_frame_to_target_date(frame: Optional[pd.DataFrame], target_date: date) -> Optional[pd.DataFrame]:
    if frame is None or frame.empty:
        return frame
    sliced = frame[frame["date"] <= pd.Timestamp(target_date)].copy()
    if sliced.empty:
        return sliced.reset_index(drop=True)
    return sliced.sort_values("date").reset_index(drop=True)


def _clean_json_block(text: str) -> Optional[Dict[str, Any]]:
    if not text:
        return None
    candidate = text.strip()
    candidate = re.sub(r"^```(?:json)?", "", candidate, flags=re.IGNORECASE).strip()
    candidate = re.sub(r"```$", "", candidate).strip()
    if not candidate.startswith("{"):
        match = re.search(r"\{.*\}", candidate, flags=re.S)
        if match:
            candidate = match.group(0)
    try:
        return json.loads(candidate)
    except Exception:
        return None


def _truncate_text(value: Any, limit: int = 140) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "…"


def _score_value(candidate: Dict[str, Any], score_name: str) -> float:
    for item in candidate.get("score_breakdown") or []:
        if item.get("score_name") == score_name:
            return _safe_float(item.get("score_value"))
    return 0.0


def _normalize_sector_name(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    return re.sub(r"[\s\-_/\.\(\)（）\[\]【】·:：]+", "", text)


def _sector_name_tokens(value: Any) -> List[str]:
    normalized = _normalize_sector_name(value)
    if not normalized:
        return []
    tokens = {normalized}
    for suffix in ("板块", "行业", "概念"):
        if normalized.endswith(suffix) and len(normalized) > len(suffix):
            tokens.add(normalized[: -len(suffix)])
    return [token for token in tokens if token]


class StockPickerService:
    """Async stock-picker V1 service."""

    _instance: Optional["StockPickerService"] = None
    _instance_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(
        self,
        repo: Optional[StockPickerRepository] = None,
        stock_repo: Optional[StockRepository] = None,
        max_workers: int = 2,
    ):
        if getattr(self, "_initialized", False):
            return
        self._repo = repo or StockPickerRepository()
        self._stock_repo = stock_repo or StockRepository()
        self._max_workers = max(1, int(max_workers))
        self._executor: Optional[ThreadPoolExecutor] = None
        self._futures: Dict[str, Future[Any]] = {}
        self._futures_lock = threading.Lock()
        self._sector_cache_lock = threading.Lock()
        self._sector_catalog_cache: Optional[Dict[str, Any]] = None
        self._sector_catalog_cache_key: Optional[str] = None
        recovered = self._repo.mark_incomplete_tasks_failed()
        if recovered:
            logger.info("[StockPicker] recovered %s incomplete task(s)", recovered)
        self._initialized = True

    @property
    def executor(self) -> ThreadPoolExecutor:
        if self._executor is None:
            self._executor = ThreadPoolExecutor(
                max_workers=self._max_workers,
                thread_name_prefix="stock_picker_",
            )
        return self._executor

    def shutdown(self) -> None:
        executor = self._executor
        self._executor = None
        if executor is not None:
            executor.shutdown(wait=False)

    def list_templates(self) -> List[Dict[str, object]]:
        return list_templates()

    def list_universes(self) -> List[Dict[str, object]]:
        config = get_config()
        config.refresh_stock_list()
        stock_codes = _dedupe_codes(config.stock_list)
        return [
            {
                "universe_id": "watchlist",
                "name": "当前自选股池",
                "description": "基于 STOCK_LIST 扫描当前自选股池。",
                "stock_count": len(stock_codes),
                "codes": stock_codes,
            }
        ]

    def list_sectors(self) -> List[Dict[str, object]]:
        catalog = self._load_sector_catalog()
        return list(catalog["items"])

    def list_template_stats(self, *, window_days: int) -> Dict[str, Any]:
        if window_days not in PICKER_EVAL_WINDOWS:
            raise ValueError(f"window_days 必须为 {', '.join(str(item) for item in PICKER_EVAL_WINDOWS)} 之一。")

        rows = self._repo.list_evaluation_rows_for_window(window_days)
        grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for row in rows:
            if row["market"] != "cn" or row["eval_status"] not in {"completed", "benchmark_unavailable"}:
                continue
            grouped[str(row["template_id"])].append(row)

        items: List[Dict[str, Any]] = []
        for template in list_templates():
            template_rows = grouped.get(str(template["template_id"]), [])
            total = len(template_rows)
            if total == 0:
                items.append(
                    {
                        "template_id": template["template_id"],
                        "template_name": template["name"],
                        "window_days": window_days,
                        "total_evaluations": 0,
                        "comparable_evaluations": 0,
                        "benchmark_unavailable_evaluations": 0,
                        "win_rate_pct": None,
                        "avg_return_pct": None,
                        "avg_excess_return_pct": None,
                        "avg_max_drawdown_pct": None,
                    }
                )
                continue

            comparable_rows = [
                row
                for row in template_rows
                if row.get("eval_status") == "completed" and row.get("excess_return_pct") is not None
            ]
            benchmark_unavailable_count = sum(1 for row in template_rows if row.get("eval_status") == "benchmark_unavailable")
            win_count = sum(1 for row in comparable_rows if float(row.get("excess_return_pct") or 0.0) > 0)
            avg_return = sum(float(row.get("return_pct") or 0.0) for row in template_rows) / total
            excess_rows = [float(row["excess_return_pct"]) for row in template_rows if row.get("excess_return_pct") is not None]
            drawdown_rows = [float(row["max_drawdown_pct"]) for row in template_rows if row.get("max_drawdown_pct") is not None]
            items.append(
                {
                    "template_id": template["template_id"],
                    "template_name": template["name"],
                    "window_days": window_days,
                    "total_evaluations": total,
                    "comparable_evaluations": len(comparable_rows),
                    "benchmark_unavailable_evaluations": benchmark_unavailable_count,
                    "win_rate_pct": round(win_count / len(comparable_rows) * 100, 2) if comparable_rows else None,
                    "avg_return_pct": round(avg_return, 2),
                    "avg_excess_return_pct": round(sum(excess_rows) / len(excess_rows), 2) if excess_rows else None,
                    "avg_max_drawdown_pct": round(sum(drawdown_rows) / len(drawdown_rows), 2) if drawdown_rows else None,
                }
            )

        return {
            "window_days": window_days,
            "benchmark_code": DEFAULT_PICKER_BENCHMARK_CODE,
            "items": items,
        }

    def submit_task(
        self,
        *,
        template_id: str,
        universe_id: str,
        mode: str,
        sector_ids: Optional[Sequence[str]],
        limit: int,
        ai_top_k: int,
        force_refresh: bool,
        notify: bool,
        template_overrides: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if template_overrides:
            raise ValueError("V2 暂不支持复杂模板参数，请使用顶部的数量参数。")
        get_template(template_id)
        task_mode = str(mode or "watchlist").strip().lower()
        if task_mode not in {"watchlist", "sector"}:
            raise ValueError("mode 仅支持 watchlist 或 sector。")
        if task_mode == "watchlist" and universe_id != "watchlist":
            raise ValueError("自选股模式仅支持 watchlist 股票池。")
        if task_mode == "sector":
            universe_id = "sector"
        task_limit = int(limit or 20)
        if task_limit < 1 or task_limit > 30:
            raise ValueError("limit 必须介于 1 和 30 之间。")
        task_ai_top_k = int(ai_top_k or 5)
        if task_ai_top_k < 1 or task_ai_top_k > 10:
            raise ValueError("ai_top_k 必须介于 1 和 10 之间。")
        if task_ai_top_k > task_limit:
            raise ValueError("ai_top_k 不能大于 limit。")

        selected_sector_ids: List[str] = []
        selected_sector_names: List[str] = []
        if task_mode == "sector":
            selected_sector_ids = [str(item).strip() for item in (sector_ids or []) if str(item).strip()]
            selected_sector_ids = list(dict.fromkeys(selected_sector_ids))
            if not selected_sector_ids:
                raise ValueError("板块模式至少需要选择 1 个板块。")
            if len(selected_sector_ids) > 5:
                raise ValueError("板块模式最多选择 5 个板块。")
            catalog = self._load_sector_catalog()
            available_by_id = {str(item["sector_id"]): item for item in catalog["items"]}
            missing = [item for item in selected_sector_ids if item not in available_by_id]
            if missing:
                raise ValueError(f"存在无效板块：{', '.join(missing)}")
            selected_sector_names = [str(available_by_id[item]["name"]) for item in selected_sector_ids]

        sector_catalog_request = {}
        if task_mode == "sector":
            sector_catalog_request = self._build_sector_catalog_snapshot(
                catalog=catalog,
                selected_sector_names=selected_sector_names,
                selected_stock_codes=[
                    code
                    for sector_name in selected_sector_names
                    for code in catalog.get("code_by_sector", {}).get(sector_name, [])
                ],
            )

        task_id = uuid.uuid4().hex
        self._repo.create_task(
            task_id=task_id,
            template_id=template_id,
            template_version=PICKER_POLICY_VERSION,
            universe_id=universe_id,
            limit=task_limit,
            ai_top_k=task_ai_top_k,
            force_refresh=bool(force_refresh),
            request_payload={
                "template_id": template_id,
                "universe_id": universe_id,
                "limit": task_limit,
                "ai_top_k": task_ai_top_k,
                "mode": task_mode,
                "sector_ids": selected_sector_ids,
                "sector_names": selected_sector_names,
                "force_refresh": bool(force_refresh),
                "notify": bool(notify),
                "benchmark_policy": self._build_benchmark_policy(),
                "sector_catalog_request": sector_catalog_request,
                "request_policy_version": PICKER_POLICY_VERSION,
                "template_overrides": {},
            },
        )

        future = self.executor.submit(self._run_task, task_id)
        with self._futures_lock:
            self._futures[task_id] = future
        return {"task_id": task_id, "status": "queued"}

    def list_tasks(self, *, limit: int = 20) -> List[Dict[str, Any]]:
        rows = self._repo.list_tasks(limit=limit)
        return [self._decorate_task(item) for item in rows]

    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        payload = self._repo.get_task(task_id, include_candidates=True)
        if payload is None:
            return None
        return self._decorate_task(payload)

    def replay_historical_run(
        self,
        *,
        target_date: date,
        template_id: str,
        mode: str = "watchlist",
        sector_ids: Optional[Sequence[str]] = None,
        limit: int = 20,
        force_refresh: bool = False,
        window_days: Optional[Sequence[int]] = None,
        sector_ranking_mode: str = "neutral",
        benchmark_mode: str = "local_only",
    ) -> Dict[str, Any]:
        """Run a validation-only historical replay without writing picker tasks."""
        template = get_template(template_id)
        task_mode = str(mode or "watchlist").strip().lower()
        if task_mode not in {"watchlist", "sector"}:
            raise ValueError("mode 仅支持 watchlist 或 sector。")
        if sector_ranking_mode not in {"neutral", "live"}:
            raise ValueError("sector_ranking_mode 仅支持 neutral 或 live。")
        if benchmark_mode not in {"local_only", "fetch_missing"}:
            raise ValueError("benchmark_mode 仅支持 local_only 或 fetch_missing。")

        replay_limit = int(limit or 20)
        if replay_limit < 1 or replay_limit > 30:
            raise ValueError("limit 必须介于 1 和 30 之间。")

        windows = [int(item) for item in (window_days or PICKER_EVAL_WINDOWS)]
        invalid_windows = [item for item in windows if item not in PICKER_EVAL_WINDOWS]
        if invalid_windows:
            raise ValueError(f"window_days 必须为 {', '.join(str(item) for item in PICKER_EVAL_WINDOWS)} 之一。")

        stock_codes, selected_sector_names = self._resolve_task_stock_codes(
            task_mode=task_mode,
            universe_id="sector" if task_mode == "sector" else "watchlist",
            sector_ids=sector_ids or [],
        )
        if task_mode == "sector" and not selected_sector_names:
            raise ValueError("板块模式至少需要选择 1 个有效板块。")

        fetcher_manager = DataFetcherManager()
        if sector_ranking_mode == "live":
            top_sectors, bottom_sectors = self._load_sector_rankings(fetcher_manager)
        else:
            top_sectors, bottom_sectors = [], []

        scored_candidates: List[Dict[str, Any]] = []
        insufficient_count = 0
        insufficient_reason_breakdown: Dict[str, int] = defaultdict(int)
        error_count = 0
        target_dates_by_market: Dict[str, str] = {}

        for code in stock_codes:
            try:
                effective_target_date = self._resolve_effective_target_trading_date(
                    code=code,
                    target_date=target_date,
                )
                target_dates_by_market[self._detect_market(code)] = effective_target_date.isoformat()
                candidate = self._evaluate_candidate_for_target_date(
                    code=code,
                    template_id=template.template_id,
                    fetcher_manager=fetcher_manager,
                    force_refresh=bool(force_refresh),
                    top_sectors=top_sectors,
                    bottom_sectors=bottom_sectors,
                    target_date=effective_target_date,
                )
                if candidate is None:
                    insufficient_count += 1
                    insufficient_reason_breakdown["unknown"] += 1
                elif candidate.get("candidate_state") == "skipped":
                    insufficient_count += 1
                    skip_reason = str(candidate.get("skip_reason") or "unknown")
                    insufficient_reason_breakdown[skip_reason] += 1
                else:
                    scored_candidates.append(candidate)
            except Exception as exc:
                error_count += 1
                logger.warning("[StockPicker] historical replay evaluate %s failed: %s", code, exc, exc_info=True)

        ranked_candidates = self._rank_candidates(scored_candidates)
        selected_candidates = self._select_candidates(ranked_candidates, limit=replay_limit)
        evaluation_summary: Dict[int, Dict[str, int]] = {
            int(item): {
                "candidate_count": len(selected_candidates),
                "completed": 0,
                "pending": 0,
                "benchmark_unavailable": 0,
                "invalid": 0,
            }
            for item in windows
        }

        for candidate in selected_candidates:
            structured = self._build_structured_explanation(template.name, candidate)
            candidate["explanation_summary"] = structured["summary"]
            candidate["explanation_rationale"] = structured["rationale"]
            candidate["explanation_risks"] = structured["risks"]
            candidate["explanation_watchpoints"] = structured["watchpoints"]
            candidate["technical_snapshot"]["explanation_source"] = "structured_replay"
            candidate_evaluations = []
            for current_window in windows:
                payload = self._evaluate_candidate_window(
                    code=str(candidate["code"]),
                    analysis_date=candidate["latest_date"],
                    window_days=current_window,
                    fetcher_manager=fetcher_manager,
                    refresh_missing_data=(benchmark_mode == "fetch_missing"),
                )
                evaluation_summary[int(current_window)][str(payload["eval_status"])] += 1
                candidate_evaluations.append(
                    {
                        "window_days": int(current_window),
                        "benchmark_code": DEFAULT_PICKER_BENCHMARK_CODE,
                        **payload,
                    }
                )
            candidate["evaluations"] = candidate_evaluations

        summary = {
            "template_id": template.template_id,
            "template_name": template.name,
            "mode": task_mode,
            "sector_names": selected_sector_names,
            "total_stocks": len(stock_codes),
            "scored_count": len(scored_candidates),
            "insufficient_count": insufficient_count,
            "error_count": error_count,
            "strict_match_count": sum(1 for item in ranked_candidates if item["strict_match"]),
            "selected_count": len(selected_candidates),
            "qualified_fallback_count": sum(1 for item in ranked_candidates if item.get("fallback_eligible")),
            "fallback_count": sum(1 for item in selected_candidates if item["selection_reason"] == "fallback_fill"),
            "explained_count": len(selected_candidates),
            "insufficient_reason_breakdown": dict(sorted(insufficient_reason_breakdown.items())),
            "insufficient_reason_labels": deepcopy(PICKER_SKIP_REASON_LABELS),
            "benchmark_policy": self._build_benchmark_policy(),
            "selection_quality_gate": {
                "fallback_rules": deepcopy(PICKER_FALLBACK_RULES),
                "selection_policy": "strict_match_first_then_quality_gated_fallback",
            },
            "replay_policy": {
                "mode": "historical_validation_replay",
                "target_date": target_date.isoformat(),
                "market_target_dates": dict(sorted(target_dates_by_market.items())),
                "sector_ranking_mode": sector_ranking_mode,
                "benchmark_mode": benchmark_mode,
                "news_mode": "disabled",
                "ai_mode": "disabled",
                "membership_snapshot": "current_sector_catalog",
            },
            "evaluation_summary": evaluation_summary,
        }
        return {
            "target_date": target_date.isoformat(),
            "template_id": template.template_id,
            "template_name": template.name,
            "mode": task_mode,
            "sector_names": selected_sector_names,
            "sector_ranking_mode": sector_ranking_mode,
            "benchmark_mode": benchmark_mode,
            "benchmark_code": DEFAULT_PICKER_BENCHMARK_CODE,
            "window_days": windows,
            "summary": summary,
            "candidates": selected_candidates,
        }

    def _decorate_task(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        task = deepcopy(payload)
        template = get_template(task["template_id"])
        request_payload = task.get("request_payload") or {}
        task_mode = str(request_payload.get("mode") or ("sector" if task["universe_id"] == "sector" else "watchlist"))
        task["template_name"] = template.name
        task["mode"] = task_mode
        task["mode_label"] = "板块模式" if task_mode == "sector" else "自选股模式"
        task["notify"] = bool(request_payload.get("notify", False))
        task["sector_ids"] = [str(item) for item in request_payload.get("sector_ids") or []]
        task["sector_names"] = [str(item) for item in request_payload.get("sector_names") or []]
        if task["universe_id"] == "watchlist":
            task["universe_name"] = "当前自选股池"
        elif task["universe_id"] == "sector":
            task["universe_name"] = "A股行业板块"
        else:
            task["universe_name"] = task["universe_id"]
        task["status_label"] = {
            "queued": "排队中",
            "running": "运行中",
            "completed": "已完成",
            "failed": "失败",
        }.get(task["status"], task["status"])
        task["summary"] = self._build_task_summary_defaults(
            task=task,
            template_name=template.name,
        )
        return task

    def _build_task_summary_defaults(
        self,
        *,
        task: Dict[str, Any],
        template_name: str,
    ) -> Dict[str, Any]:
        request_payload = task.get("request_payload") or {}
        task_mode = str(task.get("mode") or request_payload.get("mode") or "watchlist")
        summary = deepcopy(task.get("summary") or {})
        if task_mode == "sector" and task.get("sector_names") and not summary.get("sector_quality_summary"):
            try:
                sector_quality_summary, ranked_sector_breakdown = self._build_sector_quality_summary(
                    self._load_sector_catalog(),
                    task.get("sector_names") or [],
                )
            except Exception:
                sector_quality_summary, ranked_sector_breakdown = {}, []
        else:
            sector_quality_summary = summary.get("sector_quality_summary") or {}
            ranked_sector_breakdown = summary.get("ranked_sector_breakdown") or []

        fallback_summary = {
            "template_id": task.get("template_id"),
            "template_name": template_name,
            "universe_id": task.get("universe_id"),
            "mode": task_mode,
            "total_stocks": int(task.get("total_stocks") or 0),
            "scored_count": int(summary.get("scored_count") or task.get("processed_stocks") or 0),
            "insufficient_count": int(summary.get("insufficient_count") or 0),
            "error_count": int(summary.get("error_count") or 0),
            "strict_match_count": int(summary.get("strict_match_count") or 0),
            "selected_count": int(summary.get("selected_count") or task.get("candidate_count") or 0),
            "qualified_fallback_count": int(summary.get("qualified_fallback_count") or 0),
            "fallback_count": int(summary.get("fallback_count") or 0),
            "explained_count": int(summary.get("explained_count") or 0),
            "insufficient_reason_breakdown": deepcopy(summary.get("insufficient_reason_breakdown") or {}),
            "insufficient_reason_labels": deepcopy(summary.get("insufficient_reason_labels") or PICKER_SKIP_REASON_LABELS),
            "trading_date_policy": deepcopy(summary.get("trading_date_policy") or {}),
            "sector_catalog_snapshot": deepcopy(
                summary.get("sector_catalog_snapshot") or request_payload.get("sector_catalog_request") or {}
            ),
            "sector_quality_summary": deepcopy(sector_quality_summary),
            "ranked_sector_breakdown": deepcopy(ranked_sector_breakdown),
            "benchmark_policy": deepcopy(
                summary.get("benchmark_policy") or request_payload.get("benchmark_policy") or self._build_benchmark_policy()
            ),
            "selection_quality_gate": deepcopy(
                summary.get("selection_quality_gate")
                or {
                    "fallback_rules": deepcopy(PICKER_FALLBACK_RULES),
                    "selection_policy": "strict_match_first_then_quality_gated_fallback",
                }
            ),
        }
        return fallback_summary

    def _resolve_task_stock_codes(
        self,
        *,
        task_mode: str,
        universe_id: str,
        sector_ids: Sequence[str],
    ) -> Tuple[List[str], List[str]]:
        if task_mode == "sector":
            catalog = self._load_sector_catalog()
            code_by_sector = catalog["code_by_sector"]
            sector_names = [str(item) for item in sector_ids if str(item) in code_by_sector]
            stock_codes: List[str] = []
            for sector_name in sector_names:
                stock_codes.extend(code_by_sector.get(sector_name, []))
            return _dedupe_codes(stock_codes), sector_names

        config = get_config()
        config.refresh_stock_list()
        stock_codes = _dedupe_codes(config.stock_list)
        if universe_id != "watchlist":
            logger.warning("[StockPicker] unexpected universe %s for watchlist mode", universe_id)
        return stock_codes, []

    @staticmethod
    def _build_skip_result(
        code: str,
        reason_code: str,
        *,
        detail: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        return {
            "candidate_state": "skipped",
            "code": canonical_stock_code(code),
            "skip_reason": reason_code,
            "skip_detail": detail or {},
        }

    @staticmethod
    def _build_benchmark_policy() -> Dict[str, Any]:
        return {
            "benchmark_code": DEFAULT_PICKER_BENCHMARK_CODE,
            "benchmark_market": "cn",
            "comparability_rule": "same_window_benchmark_return_required",
            "benchmark_unavailable_status": "benchmark_unavailable",
        }

    @staticmethod
    def _build_task_trading_date_policy(
        stock_codes: Sequence[str],
        *,
        reference_time: datetime,
    ) -> Dict[str, Any]:
        markets = sorted({StockPickerService._detect_market(code) for code in stock_codes}) or ["cn"]
        return {
            "policy": "market_effective_trading_date",
            "reference_time": reference_time.isoformat(),
            "market_target_dates": {
                market: get_effective_trading_date(market, current_time=reference_time).isoformat()
                for market in markets
            },
        }

    @staticmethod
    def _build_sector_catalog_snapshot(
        *,
        catalog: Dict[str, Any],
        selected_sector_names: Sequence[str],
        selected_stock_codes: Sequence[str],
    ) -> Dict[str, Any]:
        return {
            "catalog_policy": str(catalog.get("catalog_policy") or "dynamic_a_share_industry_from_stock_list"),
            "source_name": catalog.get("source_name"),
            "catalog_signature": str(catalog.get("catalog_signature") or "empty"),
            "sector_count": int(catalog.get("sector_count") or len(catalog.get("items") or [])),
            "catalog_stock_count": int(catalog.get("stock_count") or 0),
            "selected_sector_count": len(list(selected_sector_names)),
            "selected_sector_names": [str(item) for item in selected_sector_names],
            "selected_stock_count": len(_dedupe_codes(selected_stock_codes)),
        }

    @staticmethod
    def _build_sector_quality_index(
        top_sectors: Sequence[Dict[str, Any]],
        bottom_sectors: Sequence[Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        quality_index: Dict[str, Dict[str, Any]] = {}

        def register(
            sector_name: Any,
            *,
            rank_direction: str,
            rank_position: int,
            change_pct: Optional[float],
        ) -> None:
            name = str(sector_name or "").strip()
            if not name:
                return
            payload = {
                "name": name,
                "strength_label": "强势" if rank_direction == "top" else "弱势",
                "rank_direction": rank_direction,
                "rank_position": rank_position,
                "change_pct": round(_safe_float(change_pct), 2) if change_pct is not None else None,
                "is_ranked_today": True,
                "strength_priority": 2 if rank_direction == "top" else 0,
            }
            for token in _sector_name_tokens(name):
                existing = quality_index.get(token)
                if existing is None or int(payload["rank_position"]) < int(existing.get("rank_position") or 999):
                    quality_index[token] = payload

        for index, item in enumerate(top_sectors[:10], start=1):
            register(
                item.get("name"),
                rank_direction="top",
                rank_position=index,
                change_pct=_safe_float(item.get("change_pct")) if item.get("change_pct") is not None else None,
            )
        for index, item in enumerate(bottom_sectors[:10], start=1):
            register(
                item.get("name"),
                rank_direction="bottom",
                rank_position=index,
                change_pct=_safe_float(item.get("change_pct")) if item.get("change_pct") is not None else None,
            )
        return quality_index

    @staticmethod
    def _resolve_sector_quality(
        sector_name: Any,
        quality_index: Mapping[str, Dict[str, Any]],
    ) -> Dict[str, Any]:
        normalized = _normalize_sector_name(sector_name)
        tokens = _sector_name_tokens(sector_name)
        for token in [normalized, *tokens]:
            if not token:
                continue
            payload = quality_index.get(token)
            if payload:
                return {
                    "strength_label": payload.get("strength_label"),
                    "rank_direction": payload.get("rank_direction"),
                    "rank_position": payload.get("rank_position"),
                    "change_pct": payload.get("change_pct"),
                    "is_ranked_today": bool(payload.get("is_ranked_today")),
                    "strength_priority": int(payload.get("strength_priority") or 1),
                    "matched_ranking_name": payload.get("name"),
                }

        return {
            "strength_label": "中性",
            "rank_direction": None,
            "rank_position": None,
            "change_pct": None,
            "is_ranked_today": False,
            "strength_priority": 1,
            "matched_ranking_name": None,
        }

    @staticmethod
    def _build_sector_quality_summary(
        catalog: Dict[str, Any],
        selected_sector_names: Sequence[str],
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        catalog_items = {
            str(item.get("name") or ""): item
            for item in catalog.get("items") or []
        }
        ranked_breakdown: List[Dict[str, Any]] = []
        summary = {
            "selected_sector_count": len(list(selected_sector_names)),
            "ranked_count": 0,
            "strong_count": 0,
            "neutral_count": 0,
            "weak_count": 0,
            "top_ranked_count": 0,
            "bottom_ranked_count": 0,
            "avg_ranked_change_pct": None,
        }

        ranked_changes: List[float] = []
        for sector_name in selected_sector_names:
            item = catalog_items.get(str(sector_name))
            if not item:
                continue
            strength_label = str(item.get("strength_label") or "中性")
            if strength_label == "强势":
                summary["strong_count"] += 1
            elif strength_label == "弱势":
                summary["weak_count"] += 1
            else:
                summary["neutral_count"] += 1

            if item.get("rank_direction") == "top":
                summary["top_ranked_count"] += 1
            elif item.get("rank_direction") == "bottom":
                summary["bottom_ranked_count"] += 1

            if item.get("is_ranked_today"):
                summary["ranked_count"] += 1
                change_pct = item.get("change_pct")
                if change_pct is not None:
                    ranked_changes.append(float(change_pct))
                ranked_breakdown.append(
                    {
                        "sector_id": item.get("sector_id"),
                        "name": item.get("name"),
                        "strength_label": strength_label,
                        "rank_direction": item.get("rank_direction"),
                        "rank_position": item.get("rank_position"),
                        "change_pct": item.get("change_pct"),
                        "stock_count": item.get("stock_count"),
                    }
                )

        if ranked_changes:
            summary["avg_ranked_change_pct"] = round(sum(ranked_changes) / len(ranked_changes), 2)

        ranked_breakdown.sort(
            key=lambda item: (
                0 if item.get("rank_direction") == "top" else 1,
                int(item.get("rank_position") or 999),
                str(item.get("name") or ""),
            )
        )
        return summary, ranked_breakdown

    @staticmethod
    def _match_sector_name(board_names: Sequence[str], sector_name: Any) -> Optional[Dict[str, Any]]:
        sector_display_name = str(sector_name or "").strip()
        sector_normalized = _normalize_sector_name(sector_display_name)
        sector_tokens = set(_sector_name_tokens(sector_display_name))
        if not sector_normalized:
            return None

        for board_name in board_names:
            board_display_name = str(board_name or "").strip()
            board_normalized = _normalize_sector_name(board_display_name)
            if board_normalized and board_normalized == sector_normalized:
                return {"matched_board": board_display_name, "match_type": "exact"}

        for board_name in board_names:
            board_display_name = str(board_name or "").strip()
            board_tokens = set(_sector_name_tokens(board_display_name))
            if sector_tokens & board_tokens:
                return {"matched_board": board_display_name, "match_type": "token"}

        for board_name in board_names:
            board_display_name = str(board_name or "").strip()
            board_normalized = _normalize_sector_name(board_display_name)
            if not board_normalized:
                continue
            if sector_normalized in board_normalized or board_normalized in sector_normalized:
                return {"matched_board": board_display_name, "match_type": "fuzzy"}

        return None

    def _run_task(self, task_id: str) -> None:
        try:
            task = self._repo.get_task(task_id, include_candidates=False)
            if task is None:
                return

            template = get_template(task["template_id"])
            config = get_config()
            request_payload = task.get("request_payload") or {}
            task_mode = str(request_payload.get("mode") or ("sector" if task["universe_id"] == "sector" else "watchlist"))
            stock_codes, selected_sector_names = self._resolve_task_stock_codes(
                task_mode=task_mode,
                universe_id=task["universe_id"],
                sector_ids=request_payload.get("sector_ids") or [],
            )
            reference_time = datetime.now()
            trading_date_policy = self._build_task_trading_date_policy(
                stock_codes,
                reference_time=reference_time,
            )
            sector_catalog_snapshot = self._build_sector_catalog_snapshot(
                catalog=self._load_sector_catalog(),
                selected_sector_names=selected_sector_names,
                selected_stock_codes=stock_codes,
            )
            sector_quality_summary: Dict[str, Any] = {}
            ranked_sector_breakdown: List[Dict[str, Any]] = []
            if task_mode == "sector" and selected_sector_names:
                sector_quality_summary, ranked_sector_breakdown = self._build_sector_quality_summary(
                    self._load_sector_catalog(),
                    selected_sector_names,
                )
            benchmark_policy = self._build_benchmark_policy()
            self._repo.start_task(task_id, total_stocks=len(stock_codes))

            if not stock_codes:
                self._repo.fail_task(task_id, error_message="股票池为空，无法执行选股。")
                return

            fetcher_manager = DataFetcherManager()
            search_service = self._build_search_service()
            analyzer = GeminiAnalyzer(config=config)

            top_sectors, bottom_sectors = self._load_sector_rankings(fetcher_manager)
            scored_candidates: List[Dict[str, Any]] = []
            insufficient_count = 0
            insufficient_reason_breakdown: Dict[str, int] = defaultdict(int)
            error_count = 0

            for index, code in enumerate(stock_codes, start=1):
                try:
                    candidate = self._evaluate_candidate(
                        code=code,
                        template_id=template.template_id,
                        fetcher_manager=fetcher_manager,
                        force_refresh=bool(task["force_refresh"]),
                        top_sectors=top_sectors,
                        bottom_sectors=bottom_sectors,
                        current_time=reference_time,
                    )
                    if candidate is None:
                        insufficient_count += 1
                        insufficient_reason_breakdown["unknown"] += 1
                    elif candidate.get("candidate_state") == "skipped":
                        insufficient_count += 1
                        skip_reason = str(candidate.get("skip_reason") or "unknown")
                        insufficient_reason_breakdown[skip_reason] += 1
                    else:
                        scored_candidates.append(candidate)
                except Exception as exc:
                    error_count += 1
                    logger.warning("[StockPicker] evaluate %s failed: %s", code, exc, exc_info=True)

                progress = 10 + int(index / max(len(stock_codes), 1) * 60)
                self._repo.update_progress(
                    task_id,
                    progress_percent=progress,
                    progress_message=f"已扫描 {index}/{len(stock_codes)} 支股票",
                    processed_stocks=index,
                )

            if not scored_candidates:
                self._repo.fail_task(
                    task_id,
                    error_message="没有筛出可用候选，可能是行情数据不足或股票池过小。",
                )
                return

            news_target = min(max(task["limit"] * 2, task["ai_top_k"] * 2), len(scored_candidates))
            for index, candidate in enumerate(scored_candidates[:news_target], start=1):
                news_payload = self._fetch_news_briefs(
                    search_service=search_service,
                    code=candidate["code"],
                    name=candidate["name"],
                )
                if news_payload:
                    candidate["news_briefs"] = news_payload["news_briefs"]
                    candidate["news_score"] = news_payload["news_score"]
                    candidate["total_score"] = round(candidate["total_score"] + candidate["news_score"], 2)
                    self._replace_score(candidate, "news_score", "新闻情绪", candidate["news_score"])

                progress = 72 + int(index / max(news_target, 1) * 10)
                self._repo.update_progress(
                    task_id,
                    progress_percent=progress,
                    progress_message=f"正在补充新闻信号 {index}/{news_target}",
                    processed_stocks=len(stock_codes),
                )

            ranked_candidates = self._rank_candidates(scored_candidates)
            selected_candidates = self._select_candidates(ranked_candidates, limit=task["limit"])
            if not selected_candidates:
                self._repo.fail_task(
                    task_id,
                    error_message="没有筛出满足质量门槛的候选，建议放宽股票池或切换模板。",
                )
                return
            self._repo.update_progress(
                task_id,
                progress_percent=86,
                progress_message="已完成排序，开始生成候选说明",
                processed_stocks=len(stock_codes),
            )

            explain_count = min(task["ai_top_k"], len(selected_candidates))
            for index, candidate in enumerate(selected_candidates, start=1):
                structured = self._build_structured_explanation(template.name, candidate)
                candidate["explanation_summary"] = structured["summary"]
                candidate["explanation_rationale"] = structured["rationale"]
                candidate["explanation_risks"] = structured["risks"]
                candidate["explanation_watchpoints"] = structured["watchpoints"]
                candidate["technical_snapshot"]["explanation_source"] = "structured"

                if index <= explain_count:
                    ai_payload = self._build_ai_explanation(
                        analyzer=analyzer,
                        template_name=template.name,
                        candidate=candidate,
                        base_explanation=structured,
                    )
                    if ai_payload:
                        candidate["explanation_summary"] = ai_payload["summary"]
                        candidate["explanation_rationale"] = ai_payload["rationale"]
                        candidate["explanation_risks"] = ai_payload["risks"]
                        candidate["explanation_watchpoints"] = ai_payload["watchpoints"]
                        candidate["technical_snapshot"]["explanation_source"] = "structured_plus_ai_summary"

                progress = 88 + int(index / max(len(selected_candidates), 1) * 10)
                self._repo.update_progress(
                    task_id,
                    progress_percent=progress,
                    progress_message=f"正在生成候选说明 {index}/{len(selected_candidates)}",
                    processed_stocks=len(stock_codes),
                )

            summary = {
                "template_id": template.template_id,
                "template_name": template.name,
                "universe_id": task["universe_id"],
                "mode": task_mode,
                "sector_names": selected_sector_names,
                "total_stocks": len(stock_codes),
                "scored_count": len(scored_candidates),
                "insufficient_count": insufficient_count,
                "error_count": error_count,
                "strict_match_count": sum(1 for item in ranked_candidates if item["strict_match"]),
                "selected_count": len(selected_candidates),
                "qualified_fallback_count": sum(1 for item in ranked_candidates if item.get("fallback_eligible")),
                "fallback_count": sum(1 for item in selected_candidates if item["selection_reason"] == "fallback_fill"),
                "explained_count": explain_count,
                "insufficient_reason_breakdown": dict(sorted(insufficient_reason_breakdown.items())),
                "insufficient_reason_labels": deepcopy(PICKER_SKIP_REASON_LABELS),
                "trading_date_policy": trading_date_policy,
                "sector_catalog_snapshot": sector_catalog_snapshot,
                "sector_quality_summary": sector_quality_summary,
                "ranked_sector_breakdown": ranked_sector_breakdown,
                "benchmark_policy": benchmark_policy,
                "selection_quality_gate": {
                    "fallback_rules": deepcopy(PICKER_FALLBACK_RULES),
                    "selection_policy": "strict_match_first_then_quality_gated_fallback",
                },
            }
            self._repo.save_candidates(task_id, summary=summary, candidates=selected_candidates)
            self._ensure_task_evaluations(task_id)
            if bool(request_payload.get("notify", False)):
                self._send_task_notification(
                    task_id=task_id,
                    template_name=template.name,
                    mode=task_mode,
                    sector_names=selected_sector_names,
                    candidates=selected_candidates,
                )
        except Exception as exc:
            logger.error("[StockPicker] task %s failed: %s", task_id, exc, exc_info=True)
            self._repo.fail_task(task_id, error_message=str(exc) or "AI 选股任务执行失败")
        finally:
            with self._futures_lock:
                self._futures.pop(task_id, None)

    def _evaluate_candidate(
        self,
        *,
        code: str,
        template_id: str,
        fetcher_manager: DataFetcherManager,
        force_refresh: bool,
        top_sectors: List[Dict[str, Any]],
        bottom_sectors: List[Dict[str, Any]],
        current_time: Optional[datetime] = None,
    ) -> Optional[Dict[str, Any]]:
        normalized_code = normalize_stock_code(code)
        daily_frame = self._load_daily_frame(
            code=normalized_code,
            fetcher_manager=fetcher_manager,
            force_refresh=force_refresh,
            current_time=current_time,
        )
        if daily_frame is None:
            return self._build_skip_result(code, "daily_data_unavailable")
        if len(daily_frame) < 20:
            return self._build_skip_result(
                code,
                "insufficient_history",
                detail={"available_rows": int(len(daily_frame))},
            )

        last_row = daily_frame.iloc[-1]
        latest_date = last_row["date"].date() if hasattr(last_row["date"], "date") else last_row["date"]
        if isinstance(latest_date, pd.Timestamp):
            latest_date = latest_date.date()
        if isinstance(latest_date, datetime):
            latest_date = latest_date.date()
        target_date = self._resolve_target_trading_date(normalized_code, current_time=current_time)
        if isinstance(latest_date, date) and latest_date < target_date:
            return self._build_skip_result(
                code,
                "stale_trading_date",
                detail={
                    "latest_date": latest_date.isoformat(),
                    "target_trading_date": target_date.isoformat(),
                },
            )

        return self._build_candidate_from_frame(
            code=code,
            template_id=template_id,
            daily_frame=daily_frame,
            fetcher_manager=fetcher_manager,
            top_sectors=top_sectors,
            bottom_sectors=bottom_sectors,
            target_date=target_date,
        )

    def _evaluate_candidate_for_target_date(
        self,
        *,
        code: str,
        template_id: str,
        fetcher_manager: DataFetcherManager,
        force_refresh: bool,
        top_sectors: List[Dict[str, Any]],
        bottom_sectors: List[Dict[str, Any]],
        target_date: date,
    ) -> Optional[Dict[str, Any]]:
        normalized_code = normalize_stock_code(code)
        daily_frame = self._load_daily_frame_for_target_date(
            code=normalized_code,
            target_date=target_date,
            fetcher_manager=fetcher_manager,
            force_refresh=force_refresh,
        )
        if daily_frame is None:
            return self._build_skip_result(code, "daily_data_unavailable")
        if len(daily_frame) < 20:
            return self._build_skip_result(
                code,
                "insufficient_history",
                detail={"available_rows": int(len(daily_frame))},
            )

        latest_date = _frame_latest_day(daily_frame)
        if isinstance(latest_date, date) and latest_date < target_date:
            return self._build_skip_result(
                code,
                "stale_trading_date",
                detail={
                    "latest_date": latest_date.isoformat(),
                    "target_trading_date": target_date.isoformat(),
                },
            )

        return self._build_candidate_from_frame(
            code=code,
            template_id=template_id,
            daily_frame=daily_frame,
            fetcher_manager=fetcher_manager,
            top_sectors=top_sectors,
            bottom_sectors=bottom_sectors,
            target_date=target_date,
        )

    def _build_candidate_from_frame(
        self,
        *,
        code: str,
        template_id: str,
        daily_frame: pd.DataFrame,
        fetcher_manager: DataFetcherManager,
        top_sectors: List[Dict[str, Any]],
        bottom_sectors: List[Dict[str, Any]],
        target_date: date,
    ) -> Optional[Dict[str, Any]]:
        latest_date = _frame_latest_day(daily_frame)

        name = fetcher_manager.get_stock_name(code, allow_realtime=False) or code
        market = self._detect_market(code)
        board_names = []
        if market == "cn":
            board_names = [
                str(item.get("name") or "").strip()
                for item in (fetcher_manager.get_belong_boards(code) or [])
                if str(item.get("name") or "").strip()
            ]

        metrics = self._build_metrics(daily_frame)
        scoring = self._score_candidate(
            template_id=template_id,
            metrics=metrics,
            board_names=board_names,
            top_sectors=top_sectors,
            bottom_sectors=bottom_sectors,
        )
        if scoring is None:
            return self._build_skip_result(
                code,
                "unsupported_template",
                detail={"template_id": template_id},
            )

        candidate = {
            "rank": 0,
            "code": canonical_stock_code(code),
            "name": name,
            "market": market,
            "selection_reason": "strict_match" if scoring["strict_match"] else "fallback_fill",
            "strict_match": scoring["strict_match"],
            "latest_date": latest_date,
            "latest_close": round(metrics["close"], 3),
            "change_pct": round(metrics["latest_pct_chg"], 2),
            "volume_ratio": round(metrics["volume_ratio"], 2),
            "distance_to_high_pct": round(metrics["distance_to_high_pct"], 2),
            "trend_score": scoring["trend_score"],
            "setup_score": scoring["setup_score"],
            "volume_score": scoring["volume_score"],
            "sector_score": scoring["sector_score"],
            "news_score": 0.0,
            "risk_penalty": scoring["risk_penalty"],
            "total_score": scoring["total_score"],
            "board_names": board_names,
            "news_briefs": [],
            "score_breakdown": [
                {"score_name": "trend_score", "score_label": "趋势结构", "score_value": scoring["trend_score"], "detail": deepcopy(scoring["trend_detail"])},
                {"score_name": "setup_score", "score_label": "模板匹配", "score_value": scoring["setup_score"], "detail": deepcopy(scoring["setup_detail"])},
                {"score_name": "volume_score", "score_label": "量能配合", "score_value": scoring["volume_score"], "detail": deepcopy(scoring["volume_detail"])},
                {"score_name": "sector_score", "score_label": "板块强度", "score_value": scoring["sector_score"], "detail": deepcopy(scoring["sector_detail"])},
                {"score_name": "news_score", "score_label": "新闻情绪", "score_value": 0.0, "detail": {}},
                {"score_name": "risk_penalty", "score_label": "风险扣分", "score_value": -scoring["risk_penalty"], "detail": deepcopy(scoring["risk_detail"])},
                {
                    "score_name": "total_score",
                    "score_label": "综合得分",
                    "score_value": scoring["total_score"],
                    "detail": {
                        "selection_context": deepcopy(scoring["selection_context"]),
                        "component_scores": {
                            "trend_score": scoring["trend_score"],
                            "setup_score": scoring["setup_score"],
                            "volume_score": scoring["volume_score"],
                            "sector_score": scoring["sector_score"],
                            "risk_penalty": scoring["risk_penalty"],
                        },
                    },
                },
            ],
            "technical_snapshot": {
                "ma5": round(metrics["ma5"], 3),
                "ma10": round(metrics["ma10"], 3),
                "ma20": round(metrics["ma20"], 3),
                "ma60": round(metrics["ma60"], 3),
                "change5d_pct": round(metrics["change_5d_pct"], 2),
                "change20d_pct": round(metrics["change_20d_pct"], 2),
                "pullback_from_high_pct": round(metrics["pullback_from_high_pct"], 2),
                "latest_pct_chg": round(metrics["latest_pct_chg"], 2),
                "ma20_slope_pct": round(metrics["ma20_slope_pct"], 2),
                "target_trading_date": target_date.isoformat(),
                "selection_context": deepcopy(scoring["selection_context"]),
                "explanation_source": "structured",
            },
            "fallback_eligible": scoring["selection_context"]["fallback_eligible"],
        }
        return candidate

    def _load_daily_frame(
        self,
        *,
        code: str,
        fetcher_manager: DataFetcherManager,
        force_refresh: bool,
        current_time: Optional[datetime] = None,
    ) -> Optional[pd.DataFrame]:
        cached_rows = self._repo.get_recent_daily_rows(code, limit=120)
        if cached_rows and not force_refresh:
            frame = _normalize_dataframe(_rows_to_dataframe(cached_rows))
            if len(frame) >= 30:
                target_date = self._resolve_target_trading_date(code, current_time=current_time)
                latest_date = frame.iloc[-1]["date"]
                latest_day = latest_date.date() if hasattr(latest_date, "date") else latest_date
                if isinstance(latest_day, pd.Timestamp):
                    latest_day = latest_day.date()
                if isinstance(latest_day, date) and latest_day >= target_date:
                    return frame

        df, source_name = fetcher_manager.get_daily_data(code, days=120)
        if df is None or df.empty:
            return None
        self._stock_repo.save_dataframe(df, code, data_source=source_name)
        return _normalize_dataframe(df)

    def _load_daily_frame_for_target_date(
        self,
        *,
        code: str,
        target_date: date,
        fetcher_manager: DataFetcherManager,
        force_refresh: bool,
    ) -> Optional[pd.DataFrame]:
        history_days = 240
        range_start = target_date - timedelta(days=history_days * 2)
        cached_rows = self._stock_repo.get_range(
            normalize_stock_code(code),
            start_date=range_start,
            end_date=target_date,
        )
        cached_frame = _normalize_dataframe(_rows_to_dataframe(cached_rows)) if cached_rows else None
        cached_latest = _frame_latest_day(cached_frame)
        if not force_refresh and cached_frame is not None and len(cached_frame) >= 30:
            if isinstance(cached_latest, date) and cached_latest >= target_date:
                return _slice_frame_to_target_date(cached_frame, target_date)

        try:
            df, source_name = fetcher_manager.get_daily_data(code, days=history_days)
        except Exception as exc:
            logger.debug("[StockPicker] load historical daily frame failed for %s: %s", code, exc)
            return cached_frame
        if df is None or df.empty:
            return cached_frame
        self._stock_repo.save_dataframe(df, normalize_stock_code(code), data_source=source_name)
        normalized = _normalize_dataframe(df)
        return _slice_frame_to_target_date(normalized, target_date)

    @staticmethod
    def _resolve_target_trading_date(
        code: str, current_time: Optional[datetime] = None
    ) -> date:
        market = get_market_for_stock(normalize_stock_code(code))
        return get_effective_trading_date(market, current_time=current_time)

    @staticmethod
    def _resolve_effective_target_trading_date(code: str, target_date: date) -> date:
        return StockPickerService._resolve_target_trading_date(
            code,
            current_time=datetime.combine(target_date, time(15, 0)),
        )

    @staticmethod
    def _build_metrics(daily_frame: pd.DataFrame) -> Dict[str, float]:
        close = daily_frame["close"]
        high = daily_frame["high"]
        volume = daily_frame["volume"].fillna(0.0)
        ma5 = _safe_float(daily_frame["ma5"].iloc[-1], _safe_float(close.tail(5).mean()))
        ma10 = _safe_float(daily_frame["ma10"].iloc[-1], _safe_float(close.tail(10).mean()))
        ma20 = _safe_float(daily_frame["ma20"].iloc[-1], _safe_float(close.tail(20).mean()))
        ma60 = _safe_float(daily_frame.get("ma60", pd.Series(dtype=float)).iloc[-1] if "ma60" in daily_frame else 0.0, ma20)
        ma20_prev = _safe_float(
            daily_frame["ma20"].iloc[-6] if len(daily_frame) >= 26 else daily_frame["ma20"].iloc[0],
            ma20,
        )
        current_close = _safe_float(close.iloc[-1])
        prev_close = _safe_float(close.iloc[-2], current_close)
        avg_volume20 = _safe_float(daily_frame["volume_ma20"].iloc[-1], _safe_float(volume.tail(20).mean(), 1.0))
        current_volume = _safe_float(volume.iloc[-1], avg_volume20)
        prior_high20 = _safe_float(high.iloc[-21:-1].max() if len(high) >= 21 else high.max(), current_close)
        recent_high20 = _safe_float(high.tail(20).max(), current_close)
        change_5d_pct = ((current_close / _safe_float(close.iloc[-6], current_close)) - 1) * 100 if len(close) >= 6 else 0.0
        change_20d_pct = ((current_close / _safe_float(close.iloc[-21], current_close)) - 1) * 100 if len(close) >= 21 else 0.0
        distance_to_high_pct = ((current_close / prior_high20) - 1) * 100 if prior_high20 > 0 else 0.0
        pullback_from_high_pct = ((current_close / recent_high20) - 1) * 100 if recent_high20 > 0 else 0.0
        latest_pct_chg = _safe_float(daily_frame["pct_chg"].iloc[-1], ((current_close / prev_close) - 1) * 100 if prev_close else 0.0)
        ma20_slope_pct = ((ma20 / ma20_prev) - 1) * 100 if ma20_prev else 0.0
        volume_ratio = current_volume / avg_volume20 if avg_volume20 > 0 else 1.0
        return {
            "close": current_close,
            "ma5": ma5,
            "ma10": ma10,
            "ma20": ma20,
            "ma60": ma60,
            "change_5d_pct": change_5d_pct,
            "change_20d_pct": change_20d_pct,
            "distance_to_high_pct": distance_to_high_pct,
            "pullback_from_high_pct": pullback_from_high_pct,
            "latest_pct_chg": latest_pct_chg,
            "ma20_slope_pct": ma20_slope_pct,
            "volume_ratio": volume_ratio,
        }

    def _score_candidate(
        self,
        *,
        template_id: str,
        metrics: Dict[str, float],
        board_names: List[str],
        top_sectors: List[Dict[str, Any]],
        bottom_sectors: List[Dict[str, Any]],
    ) -> Optional[Dict[str, float]]:
        ma5 = metrics["ma5"]
        ma10 = metrics["ma10"]
        ma20 = metrics["ma20"]
        close = metrics["close"]

        trend_checks = [
            {"label": "收盘价站上 MA20", "passed": close > ma20, "value": round(close, 3), "threshold": round(ma20, 3)},
            {"label": "MA5 高于 MA10", "passed": ma5 > ma10, "value": round(ma5, 3), "threshold": round(ma10, 3)},
            {"label": "MA10 高于 MA20", "passed": ma10 > ma20, "value": round(ma10, 3), "threshold": round(ma20, 3)},
            {"label": "MA20 斜率为正", "passed": metrics["ma20_slope_pct"] > 0, "value": round(metrics["ma20_slope_pct"], 2), "threshold": 0.0},
            {"label": "收盘价站上 MA60", "passed": close > metrics["ma60"] > 0, "value": round(close, 3), "threshold": round(metrics["ma60"], 3)},
        ]
        trend_score = 0.0
        if trend_checks[0]["passed"]:
            trend_score += 10
        if trend_checks[1]["passed"]:
            trend_score += 8
        if trend_checks[2]["passed"]:
            trend_score += 10
        if trend_checks[3]["passed"]:
            trend_score += 8
        if trend_checks[4]["passed"]:
            trend_score += 4
        trend_score = _clamp(trend_score, 0, 40)
        trend_detail = {
            "checks": trend_checks,
            "close": round(close, 3),
            "ma5": round(ma5, 3),
            "ma10": round(ma10, 3),
            "ma20": round(ma20, 3),
            "ma60": round(metrics["ma60"], 3),
            "ma20_slope_pct": round(metrics["ma20_slope_pct"], 2),
        }

        volume_score = 0.0
        volume_bucket = "low"
        if metrics["volume_ratio"] >= 1.4:
            volume_score = 15
            volume_bucket = "high_expansion"
        elif metrics["volume_ratio"] >= 1.1:
            volume_score = 11
            volume_bucket = "supportive"
        elif metrics["volume_ratio"] >= 0.9:
            volume_score = 7
            volume_bucket = "neutral"
        elif metrics["volume_ratio"] >= 0.7:
            volume_score = 4
            volume_bucket = "slightly_weak"
        volume_detail = {
            "volume_ratio": round(metrics["volume_ratio"], 2),
            "bucket": volume_bucket,
        }

        sector_score, sector_detail = self._score_sector_with_detail(board_names, top_sectors, bottom_sectors)
        risk_penalty = 0.0
        risk_flags: List[Dict[str, Any]] = []
        if close < ma20:
            risk_penalty += 8
            risk_flags.append({"label": "收盘价跌破 MA20", "penalty": 8.0})
        if metrics["ma20_slope_pct"] < 0:
            risk_penalty += 6
            risk_flags.append({"label": "MA20 斜率转负", "penalty": 6.0})
        if metrics["latest_pct_chg"] < -5:
            risk_penalty += 5
            risk_flags.append({"label": "单日跌幅过大", "penalty": 5.0})
        if ma20 > 0 and (close / ma20 - 1) * 100 > 12:
            risk_penalty += 6
            risk_flags.append({"label": "偏离 MA20 过大", "penalty": 6.0})
        risk_detail = {
            "flags": risk_flags,
            "total_penalty": round(risk_penalty, 2),
        }

        setup_score = 0.0
        strict_match = False
        strict_checks: List[Dict[str, Any]] = []
        setup_detail: Dict[str, Any] = {
            "template_id": template_id,
            "change_20d_pct": round(metrics["change_20d_pct"], 2),
            "distance_to_high_pct": round(metrics["distance_to_high_pct"], 2),
            "pullback_from_high_pct": round(metrics["pullback_from_high_pct"], 2),
            "latest_pct_chg": round(metrics["latest_pct_chg"], 2),
        }
        if template_id == "trend_breakout":
            if metrics["distance_to_high_pct"] >= 0:
                setup_score += 18
            elif metrics["distance_to_high_pct"] >= -3:
                setup_score += 14
            elif metrics["distance_to_high_pct"] >= -6:
                setup_score += 9
            if metrics["latest_pct_chg"] >= 0:
                setup_score += 5
            if metrics["change_20d_pct"] > 5:
                setup_score += 7
            strict_checks = [
                {"label": "收盘价站上 MA10", "passed": close > ma10},
                {"label": "MA5 > MA10 > MA20", "passed": ma5 > ma10 > ma20},
                {"label": "距前高不低于 -3.5%", "passed": metrics["distance_to_high_pct"] >= -3.5},
                {"label": "量能比不低于 0.85", "passed": metrics["volume_ratio"] >= 0.85},
            ]
            strict_match = all(item["passed"] for item in strict_checks)
        elif template_id == "strong_pullback":
            if metrics["change_20d_pct"] > 4:
                setup_score += 10
            if -4.5 <= metrics["pullback_from_high_pct"] <= -0.5:
                setup_score += 10
            if ma10 > 0 and abs((close / ma10 - 1) * 100) <= 2.5:
                setup_score += 8
            if metrics["latest_pct_chg"] <= 1.5:
                setup_score += 4
            if metrics["volume_ratio"] <= 1.1:
                setup_score += 4
            strict_checks = [
                {"label": "MA10 高于 MA20", "passed": ma10 > ma20},
                {"label": "收盘价不低于 MA20 的 98%", "passed": close >= ma20 * 0.98},
                {"label": "回撤位于 -6% 到 0%", "passed": -6.0 <= metrics["pullback_from_high_pct"] <= 0},
                {"label": "近 20 日涨幅大于 2%", "passed": metrics["change_20d_pct"] > 2},
            ]
            strict_match = all(item["passed"] for item in strict_checks)
        elif template_id == "balanced":
            if metrics["change_20d_pct"] > 0:
                setup_score += 8
            if close >= ma10:
                setup_score += 8
            if metrics["distance_to_high_pct"] >= -8:
                setup_score += 6
            if metrics["latest_pct_chg"] > -2:
                setup_score += 4
            if metrics["volume_ratio"] >= 0.8:
                setup_score += 4
            strict_checks = [
                {"label": "收盘价不低于 MA20 的 99%", "passed": close >= ma20 * 0.99},
                {"label": "近 20 日涨幅大于 -2%", "passed": metrics["change_20d_pct"] > -2},
            ]
            strict_match = all(item["passed"] for item in strict_checks)
        else:
            return None

        setup_score = _clamp(setup_score, 0, 30)
        total_score = _clamp(trend_score + setup_score + volume_score + sector_score - risk_penalty, 0, 100)
        rules = PICKER_FALLBACK_RULES.get(template_id, PICKER_FALLBACK_RULES["balanced"])
        fallback_checks = [
            {"label": f"综合得分不低于 {rules['min_total_score']}", "passed": total_score >= rules["min_total_score"]},
            {"label": f"趋势分不低于 {rules['min_trend_score']}", "passed": trend_score >= rules["min_trend_score"]},
            {"label": f"风险扣分不高于 {rules['max_risk_penalty']}", "passed": risk_penalty <= rules["max_risk_penalty"]},
            {"label": "收盘价不低于 MA20 的 95%", "passed": close >= ma20 * 0.95},
        ]
        fallback_eligible = all(item["passed"] for item in fallback_checks)
        selection_context = {
            "strict_match": strict_match,
            "strict_reasons": [item["label"] for item in strict_checks if item["passed"]],
            "strict_failures": [item["label"] for item in strict_checks if not item["passed"]],
            "fallback_eligible": fallback_eligible,
            "fallback_reasons": [item["label"] for item in fallback_checks if item["passed"]],
            "fallback_failures": [item["label"] for item in fallback_checks if not item["passed"]],
            "selection_policy": "strict_match_first_then_quality_gated_fallback",
        }
        setup_detail["strict_checks"] = strict_checks
        setup_detail["fallback_checks"] = fallback_checks
        setup_detail["strict_match"] = strict_match
        setup_detail["fallback_eligible"] = fallback_eligible
        return {
            "strict_match": strict_match,
            "trend_score": round(trend_score, 2),
            "setup_score": round(setup_score, 2),
            "volume_score": round(volume_score, 2),
            "sector_score": round(sector_score, 2),
            "risk_penalty": round(risk_penalty, 2),
            "total_score": round(total_score, 2),
            "trend_detail": trend_detail,
            "setup_detail": setup_detail,
            "volume_detail": volume_detail,
            "sector_detail": sector_detail,
            "risk_detail": risk_detail,
            "selection_context": selection_context,
        }

    @staticmethod
    def _score_sector(
        board_names: List[str],
        top_sectors: List[Dict[str, Any]],
        bottom_sectors: List[Dict[str, Any]],
    ) -> float:
        score, _ = StockPickerService._score_sector_with_detail(board_names, top_sectors, bottom_sectors)
        return score

    @staticmethod
    def _score_sector_with_detail(
        board_names: List[str],
        top_sectors: List[Dict[str, Any]],
        bottom_sectors: List[Dict[str, Any]],
    ) -> Tuple[float, Dict[str, Any]]:
        if not board_names:
            return 0.0, {"matched_top_sectors": [], "matched_bottom_sectors": []}
        positive_score = 0.0
        negative_score = 0.0
        matched_top: List[Dict[str, Any]] = []
        matched_bottom: List[Dict[str, Any]] = []
        for index, item in enumerate(top_sectors[:10]):
            sector_name = str(item.get("name") or "").strip()
            match_info = StockPickerService._match_sector_name(board_names, sector_name)
            if not match_info:
                continue
            matched_score = max(3.0, 10.0 - index * 1.2)
            if match_info["match_type"] == "fuzzy":
                matched_score = max(2.5, matched_score - 1.0)
            positive_score = max(positive_score, matched_score)
            matched_top.append(
                {
                    "name": item.get("name"),
                    "rank": index + 1,
                    "change_pct": round(_safe_float(item.get("change_pct")), 2) if item.get("change_pct") is not None else None,
                    "matched_board": match_info["matched_board"],
                    "match_type": match_info["match_type"],
                }
            )
        for index, item in enumerate(bottom_sectors[:10]):
            sector_name = str(item.get("name") or "").strip()
            match_info = StockPickerService._match_sector_name(board_names, sector_name)
            if not match_info:
                continue
            matched_score = max(2.0, 7.0 - index * 0.8)
            if match_info["match_type"] == "fuzzy":
                matched_score = max(1.5, matched_score - 0.8)
            negative_score = min(negative_score, -matched_score)
            matched_bottom.append(
                {
                    "name": item.get("name"),
                    "rank": index + 1,
                    "change_pct": round(_safe_float(item.get("change_pct")), 2) if item.get("change_pct") is not None else None,
                    "matched_board": match_info["matched_board"],
                    "match_type": match_info["match_type"],
                }
            )
        score = _clamp(positive_score + negative_score, -7.0, 10.0)
        return round(score, 2), {
            "matched_top_sectors": matched_top[:3],
            "matched_bottom_sectors": matched_bottom[:3],
            "score_components": {
                "positive_score": round(positive_score, 2),
                "negative_score": round(negative_score, 2),
            },
        }

    @staticmethod
    def _rank_candidates(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return sorted(
            candidates,
            key=lambda item: (
                -_safe_float(item.get("total_score")),
                -int(bool(item.get("strict_match"))),
                -_safe_float(item.get("trend_score")),
                -_safe_float(item.get("setup_score")),
                -_safe_float(item.get("volume_score")),
                -_safe_float(item.get("sector_score")),
                str(item.get("code") or ""),
            ),
        )

    @staticmethod
    def _select_candidates(candidates: List[Dict[str, Any]], *, limit: int) -> List[Dict[str, Any]]:
        selected: List[Dict[str, Any]] = []
        seen = set()

        for candidate in candidates:
            if candidate["strict_match"]:
                selected.append(deepcopy(candidate))
                seen.add(candidate["code"])
            if len(selected) >= limit:
                break

        if len(selected) < limit:
            for candidate in candidates:
                if candidate["code"] in seen or not candidate.get("fallback_eligible", False):
                    continue
                clone = deepcopy(candidate)
                clone["selection_reason"] = "fallback_fill"
                selected.append(clone)
                seen.add(candidate["code"])
                if len(selected) >= limit:
                    break

        for index, candidate in enumerate(selected, start=1):
            candidate["rank"] = index
            candidate["selection_reason"] = (
                "strict_match" if candidate.get("strict_match") else candidate["selection_reason"]
            )
        return selected

    def _fetch_news_briefs(
        self,
        *,
        search_service: SearchService,
        code: str,
        name: str,
    ) -> Optional[Dict[str, Any]]:
        if not search_service.is_available:
            return None
        try:
            response = search_service.search_stock_news(code, name, max_results=4)
        except Exception as exc:
            logger.debug("[StockPicker] search news failed for %s: %s", code, exc)
            return None
        if not response.success or not response.results:
            return None

        news_briefs = [
            {
                "title": result.title,
                "source": result.source,
                "published_date": result.published_date,
                "url": result.url,
                "snippet": result.snippet,
            }
            for result in response.results[:4]
        ]
        news_score = self._score_news(response)
        return {"news_briefs": news_briefs, "news_score": news_score}

    @staticmethod
    def _score_news(response: SearchResponse) -> float:
        score = 0.0
        for result in response.results[:4]:
            haystack = f"{result.title} {result.snippet}".lower()
            for keyword in _POSITIVE_NEWS_KEYWORDS:
                if keyword.lower() in haystack:
                    score += 1.8
            for keyword in _NEGATIVE_NEWS_KEYWORDS:
                if keyword.lower() in haystack:
                    score -= 2.2
        return round(_clamp(score, -8, 8), 2)

    def _build_ai_explanation(
        self,
        *,
        analyzer: GeminiAnalyzer,
        template_name: str,
        candidate: Dict[str, Any],
        base_explanation: Optional[Dict[str, List[str] | str]] = None,
    ) -> Optional[Dict[str, Any]]:
        structured = base_explanation or self._build_structured_explanation(template_name, candidate)
        compact_news = [
            {
                "title": _truncate_text(item.get("title"), 80),
                "source": item.get("source"),
                "published_date": item.get("published_date"),
                "snippet": _truncate_text(item.get("snippet"), 120),
            }
            for item in (candidate.get("news_briefs") or [])[:3]
        ]
        compact_scores = [
            {
                "score_name": item.get("score_name"),
                "score_label": item.get("score_label"),
                "score_value": item.get("score_value"),
            }
            for item in (candidate.get("score_breakdown") or [])
            if item.get("score_name") != "total_score"
        ]
        prompt = (
            "你是股票候选解释助手。不要重新排序，不要给出任何收益承诺，也不要引入结构化数据中不存在的新事实。\n"
            "请只润色摘要，不要改写结构化理由、风险和观察点。\n"
            "输出严格 JSON，不要添加 Markdown 代码块。\n"
            "JSON 结构如下：\n"
            "{\n"
            '  "summary": "40字以内总结"\n'
            "}\n\n"
            f"模板：{template_name}\n"
            f"股票：{candidate['name']} ({candidate['code']})\n"
            f"市场：{candidate['market']}\n"
            f"综合得分：{candidate['total_score']}\n"
            f"技术快照：{json.dumps(candidate['technical_snapshot'], ensure_ascii=False)}\n"
            f"核心板块：{json.dumps((candidate.get('board_names') or [])[:6], ensure_ascii=False)}\n"
            f"最近新闻摘要：{json.dumps(compact_news, ensure_ascii=False)}\n"
            f"评分拆解：{json.dumps(compact_scores, ensure_ascii=False)}\n"
            f"结构化解释草案：{json.dumps(structured, ensure_ascii=False)}\n"
        )
        raw_text = analyzer.generate_text(prompt, max_tokens=800, temperature=0.2)
        payload = _clean_json_block(raw_text or "")
        if not payload:
            return None
        summary = str(payload.get("summary") or "").strip()
        if not summary:
            return None
        return {
            "summary": summary,
            "rationale": list(structured["rationale"])[:4],
            "risks": list(structured["risks"])[:3],
            "watchpoints": list(structured["watchpoints"])[:3],
        }

    @staticmethod
    def _build_structured_explanation(template_name: str, candidate: Dict[str, Any]) -> Dict[str, List[str] | str]:
        snapshot = candidate["technical_snapshot"]
        selection_context = snapshot.get("selection_context") or {}
        change20d_pct = round(_safe_float(snapshot.get("change20d_pct")), 2)
        pullback_from_high_pct = round(_safe_float(snapshot.get("pullback_from_high_pct")), 2)
        ma10 = round(_safe_float(snapshot.get("ma10")), 3)
        ma20 = round(_safe_float(snapshot.get("ma20")), 3)
        ma20_slope_pct = round(_safe_float(snapshot.get("ma20_slope_pct")), 2)
        distance_to_high_pct = round(_safe_float(candidate.get("distance_to_high_pct")), 2)
        volume_ratio = round(_safe_float(candidate.get("volume_ratio"), 1.0), 2)
        score_map = {
            "trend_score": _score_value(candidate, "trend_score"),
            "setup_score": _score_value(candidate, "setup_score"),
            "volume_score": _score_value(candidate, "volume_score"),
            "sector_score": _score_value(candidate, "sector_score"),
            "news_score": _score_value(candidate, "news_score"),
        }
        rationale: List[str] = []
        if selection_context.get("strict_match"):
            reasons = "、".join(selection_context.get("strict_reasons") or ["满足主要模板条件"])
            rationale.append(f"严格命中条件：{reasons}")
        else:
            reasons = "、".join(selection_context.get("fallback_reasons") or ["满足补位质量门槛"])
            rationale.append(f"补位候选依据：{reasons}")
        rationale.append(
            "结构化得分："
            f"趋势 {score_map['trend_score']:.1f}、模板 {score_map['setup_score']:.1f}、"
            f"量能 {score_map['volume_score']:.1f}、板块 {score_map['sector_score']:.1f}"
        )
        rationale.append(
            f"技术快照：近20日涨跌幅 {change20d_pct}%，距阶段高点 {distance_to_high_pct}%，"
            f"量能比 {volume_ratio}"
        )
        if candidate["board_names"]:
            rationale.append(f"所属板块：{'、'.join(candidate['board_names'][:3])}")
        elif score_map["news_score"] != 0:
            rationale.append(f"新闻情绪得分 {score_map['news_score']:.1f}")

        risks: List[str] = []
        risk_detail = next(
            (item.get("detail") for item in candidate["score_breakdown"] if item.get("score_name") == "risk_penalty"),
            {},
        ) or {}
        for flag in risk_detail.get("flags") or []:
            label = str(flag.get("label") or "").strip()
            if label:
                risks.append(label)
        if pullback_from_high_pct < -6:
            risks.append("距近 20 日高点回撤偏大，需防趋势衰减")
        if ma20_slope_pct < 0:
            risks.append("MA20 仍在走弱，确认性不足")
        if not risks:
            risks.append("重点留意量价是否继续配合，避免假突破或回踩失守")

        watchpoints: List[str] = [
            f"关注 MA10/MA20 附近支撑：{ma10} / {ma20}",
            f"关注量能是否维持在均量以上，当前量能比 {volume_ratio}",
        ]
        if distance_to_high_pct > -2:
            watchpoints.append("关注能否有效站稳阶段高点附近")
        fallback_failures = selection_context.get("fallback_failures") or []
        if fallback_failures:
            watchpoints.append(f"补位未满足项：{'、'.join(fallback_failures[:2])}")

        return {
            "summary": (
                f"{candidate['name']} 当前为{template_name}"
                f"{'严格命中' if selection_context.get('strict_match') else '补位'}候选，综合得分 {candidate['total_score']}。"
            ),
            "rationale": rationale[:4],
            "risks": risks[:3],
            "watchpoints": watchpoints[:3],
        }

    @staticmethod
    def _build_fallback_explanation(template_name: str, candidate: Dict[str, Any]) -> Dict[str, List[str] | str]:
        return StockPickerService._build_structured_explanation(template_name, candidate)

    @staticmethod
    def _replace_score(candidate: Dict[str, Any], score_name: str, score_label: str, score_value: float) -> None:
        for item in candidate["score_breakdown"]:
            if item["score_name"] == score_name:
                item["score_value"] = round(score_value, 2)
                break
        else:
            candidate["score_breakdown"].append(
                {
                    "score_name": score_name,
                    "score_label": score_label,
                    "score_value": round(score_value, 2),
                    "detail": {},
                }
            )
        for item in candidate["score_breakdown"]:
            if item["score_name"] == "total_score":
                item["score_value"] = round(candidate["total_score"], 2)
                break

    @staticmethod
    def _detect_market(code: str) -> str:
        normalized = canonical_stock_code(code)
        if normalized.startswith("HK") or normalized.endswith(".HK") or (normalized.isdigit() and len(normalized) == 5):
            return "hk"
        if normalized.isalpha() and 1 <= len(normalized) <= 6:
            return "us"
        return "cn"

    @staticmethod
    def _load_sector_rankings(fetcher_manager: DataFetcherManager) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        try:
            top, bottom = fetcher_manager.get_sector_rankings(10)
            return top or [], bottom or []
        except Exception as exc:
            logger.debug("[StockPicker] load sector rankings failed: %s", exc)
            return [], []

    def _load_sector_catalog(self) -> Dict[str, Any]:
        cache_key = get_effective_trading_date("cn", current_time=datetime.now()).isoformat()
        cached = self._sector_catalog_cache
        if cached is not None and getattr(self, "_sector_catalog_cache_key", None) == cache_key:
            return cached

        with self._sector_cache_lock:
            if (
                self._sector_catalog_cache is not None
                and getattr(self, "_sector_catalog_cache_key", None) == cache_key
            ):
                return self._sector_catalog_cache

            fetcher_manager = DataFetcherManager()
            catalog = self._build_sector_catalog(fetcher_manager)
            self._sector_catalog_cache = catalog
            self._sector_catalog_cache_key = cache_key
            return catalog

    @staticmethod
    def _build_sector_catalog(fetcher_manager: DataFetcherManager) -> Dict[str, Any]:
        top_sectors, bottom_sectors = StockPickerService._load_sector_rankings(fetcher_manager)
        quality_index = StockPickerService._build_sector_quality_index(top_sectors, bottom_sectors)
        get_fetchers_snapshot = getattr(fetcher_manager, "_get_fetchers_snapshot", None)
        if callable(get_fetchers_snapshot):
            fetchers = get_fetchers_snapshot()
        else:
            fetchers = list(getattr(fetcher_manager, "_fetchers", []))

        call_fetcher_method = getattr(fetcher_manager, "_call_fetcher_method", None)
        for fetcher in fetchers:
            if not hasattr(fetcher, "get_stock_list"):
                continue
            try:
                if callable(call_fetcher_method):
                    raw_df = call_fetcher_method(fetcher, "get_stock_list")
                else:
                    raw_df = fetcher.get_stock_list()
            except Exception as exc:
                logger.debug("[StockPicker] stock list fetch failed from %s: %s", getattr(fetcher, "name", fetcher), exc)
                continue
            if raw_df is None or raw_df.empty or "industry" not in raw_df.columns or "code" not in raw_df.columns:
                continue

            frame = raw_df.copy()
            frame["code"] = frame["code"].astype(str).map(normalize_stock_code)
            frame["industry"] = frame["industry"].astype(str).str.strip()
            frame = frame[
                frame["code"].str.fullmatch(r"\d{6}", na=False)
                & frame["industry"].ne("")
                & frame["industry"].ne("nan")
            ]
            if frame.empty:
                continue

            grouped = frame.groupby("industry")["code"].apply(list)
            items: List[Dict[str, Any]] = []
            code_by_sector: Dict[str, List[str]] = {}
            for industry_name, codes in grouped.items():
                deduped_codes = _dedupe_codes(codes)
                if not deduped_codes:
                    continue
                code_by_sector[str(industry_name)] = deduped_codes
                quality = StockPickerService._resolve_sector_quality(industry_name, quality_index)
                items.append(
                    {
                        "sector_id": str(industry_name),
                        "name": str(industry_name),
                        "description": f"基于股票清单行业字段动态构建的 A股行业板块：{industry_name}",
                        "market": "cn",
                        "stock_count": len(deduped_codes),
                        "strength_label": quality["strength_label"],
                        "rank_direction": quality["rank_direction"],
                        "rank_position": quality["rank_position"],
                        "change_pct": quality["change_pct"],
                        "is_ranked_today": quality["is_ranked_today"],
                    }
                )

            items.sort(
                key=lambda item: (
                    -int(
                        StockPickerService._resolve_sector_quality(
                            item["name"],
                            quality_index,
                        )["strength_priority"]
                    ),
                    int(item.get("rank_position") or 999),
                    -int(item["stock_count"]),
                    str(item["name"]),
                )
            )
            if items:
                signature_source = "|".join(
                    f"{str(item['sector_id'])}:{item.get('rank_direction') or 'neutral'}:{item.get('rank_position') or 0}:{int(item['stock_count'])}"
                    for item in items
                )
                return {
                    "items": items,
                    "code_by_sector": code_by_sector,
                    "catalog_policy": "dynamic_a_share_industry_from_stock_list",
                    "source_name": str(getattr(fetcher, "name", fetcher.__class__.__name__)),
                    "sector_count": len(items),
                    "stock_count": len(_dedupe_codes([code for codes in code_by_sector.values() for code in codes])),
                    "catalog_signature": hashlib.sha1(signature_source.encode("utf-8")).hexdigest()[:12],
                    "quality_policy": "sector_rankings_top_bottom_augmented_sort",
                }

        logger.warning("[StockPicker] no A-share sector catalog available from current data sources")
        return {
            "items": [],
            "code_by_sector": {},
            "catalog_policy": "dynamic_a_share_industry_from_stock_list",
            "source_name": None,
            "sector_count": 0,
            "stock_count": 0,
            "catalog_signature": "empty",
            "quality_policy": "sector_rankings_top_bottom_augmented_sort",
        }

    def _ensure_task_evaluations(self, task_id: str) -> None:
        for window_days in PICKER_EVAL_WINDOWS:
            self._ensure_task_window_evaluations(task_id=task_id, window_days=window_days)

    def _ensure_template_evaluations(self, *, window_days: int) -> None:
        completed_task_ids = self._repo.list_task_ids(status="completed")
        for task_id in completed_task_ids:
            self._ensure_task_window_evaluations(task_id=task_id, window_days=window_days)

    def _ensure_task_window_evaluations(
        self,
        *,
        task_id: str,
        window_days: int,
        fetcher_manager: Optional[DataFetcherManager] = None,
        force: bool = False,
        dry_run: bool = False,
    ) -> Dict[str, int]:
        summary: Dict[str, int] = defaultdict(int)
        candidate_rows = self._repo.get_task_candidate_rows(task_id)
        if not candidate_rows:
            return {"candidate_count": 0}

        existing = {
            (int(item["candidate_id"]), int(item["window_days"])): item
            for item in self._repo.list_task_evaluations(task_id, window_days=window_days)
        }
        summary["candidate_count"] = len(candidate_rows)
        active_fetcher = fetcher_manager or DataFetcherManager()
        for candidate in candidate_rows:
            if candidate["market"] != "cn" or not candidate.get("latest_date"):
                if candidate["market"] != "cn":
                    summary["skipped_non_cn"] += 1
                else:
                    summary["skipped_missing_analysis_date"] += 1
                continue
            key = (int(candidate["candidate_id"]), int(window_days))
            existing_payload = existing.get(key)
            if not force and existing_payload and existing_payload.get("eval_status") == "completed":
                summary["skipped_completed"] += 1
                continue
            payload = self._evaluate_candidate_window(
                code=str(candidate["code"]),
                analysis_date=candidate["latest_date"],
                window_days=window_days,
                fetcher_manager=active_fetcher,
            )
            summary[str(payload["eval_status"])] += 1
            if not dry_run:
                self._repo.upsert_candidate_evaluation(
                    picker_candidate_id=int(candidate["candidate_id"]),
                    window_days=window_days,
                    benchmark_code=DEFAULT_PICKER_BENCHMARK_CODE,
                    eval_status=str(payload["eval_status"]),
                    entry_date=payload.get("entry_date"),
                    entry_price=payload.get("entry_price"),
                    exit_date=payload.get("exit_date"),
                    exit_price=payload.get("exit_price"),
                    benchmark_entry_price=payload.get("benchmark_entry_price"),
                    benchmark_exit_price=payload.get("benchmark_exit_price"),
                    return_pct=payload.get("return_pct"),
                    benchmark_return_pct=payload.get("benchmark_return_pct"),
                    excess_return_pct=payload.get("excess_return_pct"),
                    max_drawdown_pct=payload.get("max_drawdown_pct"),
                )
        return dict(summary)

    def backfill_evaluations(
        self,
        *,
        task_id: Optional[str] = None,
        window_days: Optional[Sequence[int]] = None,
        since: Optional[date] = None,
        limit: Optional[int] = None,
        dry_run: bool = False,
        force: bool = False,
    ) -> Dict[str, Any]:
        windows = [int(item) for item in (window_days or PICKER_EVAL_WINDOWS)]
        invalid_windows = [item for item in windows if item not in PICKER_EVAL_WINDOWS]
        if invalid_windows:
            raise ValueError(f"window_days 必须为 {', '.join(str(item) for item in PICKER_EVAL_WINDOWS)} 之一。")

        task_ids = [str(task_id)] if task_id else self._repo.list_task_ids_for_backfill(
            status="completed",
            since=since,
            limit=limit,
        )
        fetcher_manager = DataFetcherManager()
        per_window: Dict[int, Dict[str, int]] = {}
        for item in windows:
            per_window[item] = {
                "candidate_count": 0,
                "completed": 0,
                "pending": 0,
                "benchmark_unavailable": 0,
                "invalid": 0,
                "skipped_completed": 0,
                "skipped_non_cn": 0,
                "skipped_missing_analysis_date": 0,
            }

        for current_task_id in task_ids:
            for current_window in windows:
                window_summary = self._ensure_task_window_evaluations(
                    task_id=current_task_id,
                    window_days=current_window,
                    fetcher_manager=fetcher_manager,
                    force=force,
                    dry_run=dry_run,
                )
                for key, value in window_summary.items():
                    per_window[current_window][key] = per_window[current_window].get(key, 0) + int(value)

        return {
            "task_count": len(task_ids),
            "task_ids": task_ids,
            "window_days": windows,
            "benchmark_code": DEFAULT_PICKER_BENCHMARK_CODE,
            "dry_run": bool(dry_run),
            "force": bool(force),
            "since": since.isoformat() if since else None,
            "per_window": per_window,
        }

    def _evaluate_candidate_window(
        self,
        *,
        code: str,
        analysis_date: date,
        window_days: int,
        fetcher_manager: DataFetcherManager,
        refresh_missing_data: bool = True,
    ) -> Dict[str, Any]:
        candidate_bars = self._load_forward_bars(
            code=code,
            analysis_date=analysis_date,
            window_days=window_days,
            fetcher_manager=fetcher_manager,
            refresh_missing_data=refresh_missing_data,
        )
        if len(candidate_bars) < window_days:
            return {
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

        entry_bar = candidate_bars[0]
        exit_bar = candidate_bars[window_days - 1]
        entry_price = _safe_float(getattr(entry_bar, "open", None))
        exit_price = _safe_float(getattr(exit_bar, "close", None))
        min_low = min(_safe_float(getattr(item, "low", None), entry_price) for item in candidate_bars[:window_days])
        if entry_price <= 0 or exit_price <= 0:
            return {
                "eval_status": "invalid",
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

        return_pct = round((exit_price / entry_price - 1) * 100, 2)
        max_drawdown_pct = round(max(0.0, (entry_price - min_low) / entry_price * 100), 2)

        benchmark_bars = self._load_forward_bars(
            code=DEFAULT_PICKER_BENCHMARK_CODE,
            analysis_date=analysis_date,
            window_days=window_days,
            fetcher_manager=fetcher_manager,
            refresh_missing_data=refresh_missing_data,
        )
        benchmark_entry_price = None
        benchmark_exit_price = None
        benchmark_return_pct = None
        excess_return_pct = None
        if len(benchmark_bars) >= window_days:
            benchmark_entry_price = _safe_float(getattr(benchmark_bars[0], "open", None))
            benchmark_exit_price = _safe_float(getattr(benchmark_bars[window_days - 1], "close", None))
            if benchmark_entry_price > 0 and benchmark_exit_price > 0:
                benchmark_return_pct = round((benchmark_exit_price / benchmark_entry_price - 1) * 100, 2)
                excess_return_pct = round(return_pct - benchmark_return_pct, 2)

        return {
            "eval_status": "completed" if excess_return_pct is not None else "benchmark_unavailable",
            "entry_date": getattr(entry_bar, "date", None),
            "entry_price": round(entry_price, 3),
            "exit_date": getattr(exit_bar, "date", None),
            "exit_price": round(exit_price, 3),
            "benchmark_entry_price": round(benchmark_entry_price, 3) if benchmark_entry_price else None,
            "benchmark_exit_price": round(benchmark_exit_price, 3) if benchmark_exit_price else None,
            "return_pct": return_pct,
            "benchmark_return_pct": benchmark_return_pct,
            "excess_return_pct": excess_return_pct,
            "max_drawdown_pct": max_drawdown_pct,
        }

    def _load_forward_bars(
        self,
        *,
        code: str,
        analysis_date: date,
        window_days: int,
        fetcher_manager: DataFetcherManager,
        refresh_missing_data: bool = True,
    ) -> List[Any]:
        bars = self._stock_repo.get_forward_bars(
            code=normalize_stock_code(code),
            analysis_date=analysis_date,
            eval_window_days=window_days,
        )
        if len(bars) >= window_days:
            return bars

        if not refresh_missing_data:
            return bars

        normalized_code = normalize_stock_code(code)
        if normalized_code == DEFAULT_PICKER_BENCHMARK_CODE:
            refreshed = self._refresh_benchmark_daily_data(
                fetcher_manager=fetcher_manager,
                days=max(120, window_days * 12),
            )
            if refreshed:
                return self._stock_repo.get_forward_bars(
                    code=DEFAULT_PICKER_BENCHMARK_CODE,
                    analysis_date=analysis_date,
                    eval_window_days=window_days,
                )

        refresh_days = max(90, window_days * 8)
        try:
            df, source_name = fetcher_manager.get_daily_data(code, days=refresh_days)
        except Exception as exc:
            logger.debug("[StockPicker] refresh forward bars failed for %s: %s", code, exc)
            return bars
        if df is None or df.empty:
            return bars
        self._stock_repo.save_dataframe(df, normalize_stock_code(code), data_source=source_name)
        return self._stock_repo.get_forward_bars(
            code=normalize_stock_code(code),
            analysis_date=analysis_date,
            eval_window_days=window_days,
        )

    def _refresh_benchmark_daily_data(
        self,
        *,
        fetcher_manager: DataFetcherManager,
        days: int,
    ) -> bool:
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=max(days * 2, 240))).strftime("%Y-%m-%d")

        get_fetchers_snapshot = getattr(fetcher_manager, "_get_fetchers_snapshot", None)
        fetchers = list(get_fetchers_snapshot()) if callable(get_fetchers_snapshot) else list(getattr(fetcher_manager, "_fetchers", []))

        # 1) Prefer Tushare index_daily for CN benchmark to preserve domestic session semantics.
        for fetcher in fetchers:
            if getattr(fetcher, "name", "") != "TushareFetcher":
                continue
            api = getattr(fetcher, "_api", None)
            if api is None:
                continue
            try:
                raw_df = api.index_daily(
                    ts_code="000300.SH",
                    start_date=start_date.replace("-", ""),
                    end_date=end_date.replace("-", ""),
                )
                if raw_df is None or raw_df.empty:
                    continue
                normalized = fetcher._normalize_data(raw_df, "000300.SH")
                normalized = fetcher._clean_data(normalized)
                normalized = fetcher._calculate_indicators(normalized)
                self._stock_repo.save_dataframe(
                    normalized,
                    DEFAULT_PICKER_BENCHMARK_CODE,
                    data_source="TushareFetcher:index_daily",
                )
                return True
            except Exception as exc:
                logger.debug("[StockPicker] benchmark refresh via Tushare index_daily failed: %s", exc, exc_info=True)

        # 2) Fallback to Yahoo Finance A-share index symbol mapping.
        for fetcher in fetchers:
            if getattr(fetcher, "name", "") != "YfinanceFetcher":
                continue
            try:
                normalized = fetcher.get_daily_data(
                    "000300.SS",
                    start_date=start_date,
                    end_date=end_date,
                    days=max(days, 120),
                )
                if normalized is None or normalized.empty:
                    continue
                self._stock_repo.save_dataframe(
                    normalized,
                    DEFAULT_PICKER_BENCHMARK_CODE,
                    data_source="YfinanceFetcher:000300.SS",
                )
                return True
            except Exception as exc:
                logger.debug("[StockPicker] benchmark refresh via YFinance failed: %s", exc, exc_info=True)

        return False

    @staticmethod
    def _build_task_notification_content(
        *,
        template_name: str,
        mode: str,
        sector_names: Sequence[str],
        candidates: Sequence[Dict[str, Any]],
    ) -> str:
        lines = [
            "# AI 选股摘要",
            "",
            f"- 模式：{'板块模式' if mode == 'sector' else '自选股模式'}",
            f"- 模板：{template_name}",
        ]
        if mode == "sector" and sector_names:
            lines.append(f"- 板块：{'、'.join(str(item) for item in sector_names[:5])}")
        lines.append("")
        lines.append("## Top 候选")
        for candidate in list(candidates)[:5]:
            lines.append(
                f"- {candidate['rank']}. {candidate['name']} ({candidate['code']}) "
                f"得分 {candidate['total_score']}：{candidate.get('explanation_summary') or '暂无摘要'}"
            )
        return "\n".join(lines)

    def _send_task_notification(
        self,
        *,
        task_id: str,
        template_name: str,
        mode: str,
        sector_names: Sequence[str],
        candidates: Sequence[Dict[str, Any]],
    ) -> None:
        notifier = NotificationService()
        if not notifier.is_available():
            logger.info("[StockPicker] skip notification for %s because no notification channel is configured", task_id)
            return
        content = self._build_task_notification_content(
            template_name=template_name,
            mode=mode,
            sector_names=sector_names,
            candidates=candidates,
        )
        try:
            notifier.send(content)
        except Exception as exc:
            logger.warning("[StockPicker] notification failed for %s: %s", task_id, exc, exc_info=True)

    @staticmethod
    def _build_search_service() -> SearchService:
        config = get_config()
        return SearchService(
            bocha_keys=getattr(config, "bocha_api_keys", None) or [],
            tavily_keys=getattr(config, "tavily_api_keys", None) or [],
            anspire_keys=getattr(config, "anspire_api_keys", None) or [],
            brave_keys=getattr(config, "brave_api_keys", None) or [],
            serpapi_keys=getattr(config, "serpapi_keys", None) or [],
            minimax_keys=getattr(config, "minimax_api_keys", None) or [],
            searxng_base_urls=getattr(config, "searxng_base_urls", None) or [],
            searxng_public_instances_enabled=bool(
                getattr(config, "searxng_public_instances_enabled", True)
            ),
            news_max_age_days=int(getattr(config, "news_max_age_days", 3) or 3),
            news_strategy_profile=str(getattr(config, "news_strategy_profile", "short") or "short"),
        )
