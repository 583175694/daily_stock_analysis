from __future__ import annotations

import json
import logging
import re
import threading
import uuid
from concurrent.futures import Future, ThreadPoolExecutor
from copy import deepcopy
from datetime import date, datetime
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import pandas as pd

from data_provider.base import DataFetcherManager, canonical_stock_code, normalize_stock_code
from src.analyzer import GeminiAnalyzer
from src.config import get_config
from src.core.trading_calendar import get_effective_trading_date, get_market_for_stock
from src.repositories.stock_repo import StockRepository
from src.search_service import SearchResponse, SearchService
from src.stock_picker.repository import StockPickerRepository
from src.stock_picker.templates import get_template, list_templates

logger = logging.getLogger(__name__)

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
                "description": "V1 仅支持使用 STOCK_LIST 作为选股范围。",
                "stock_count": len(stock_codes),
                "codes": stock_codes,
            }
        ]

    def submit_task(
        self,
        *,
        template_id: str,
        universe_id: str,
        limit: int,
        force_refresh: bool,
        template_overrides: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if template_overrides:
            raise ValueError("V1 暂不支持自定义模板参数，请直接使用内置模板。")
        get_template(template_id)
        if universe_id != "watchlist":
            raise ValueError("V1 仅支持 watchlist 股票池。")

        task_limit = int(limit or 20)
        if task_limit < 1 or task_limit > 50:
            raise ValueError("limit 必须介于 1 和 50 之间。")

        task_id = uuid.uuid4().hex
        self._repo.create_task(
            task_id=task_id,
            template_id=template_id,
            universe_id=universe_id,
            limit=task_limit,
            ai_top_k=min(10, task_limit),
            force_refresh=bool(force_refresh),
            request_payload={
                "template_id": template_id,
                "universe_id": universe_id,
                "limit": task_limit,
                "force_refresh": bool(force_refresh),
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

    def _decorate_task(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        task = deepcopy(payload)
        template = get_template(task["template_id"])
        task["template_name"] = template.name
        task["universe_name"] = "当前自选股池" if task["universe_id"] == "watchlist" else task["universe_id"]
        task["status_label"] = {
            "queued": "排队中",
            "running": "运行中",
            "completed": "已完成",
            "failed": "失败",
        }.get(task["status"], task["status"])
        return task

    def _run_task(self, task_id: str) -> None:
        try:
            task = self._repo.get_task(task_id, include_candidates=False)
            if task is None:
                return

            template = get_template(task["template_id"])
            config = get_config()
            config.refresh_stock_list()
            stock_codes = _dedupe_codes(config.stock_list)
            self._repo.start_task(task_id, total_stocks=len(stock_codes))

            if not stock_codes:
                self._repo.fail_task(task_id, error_message="股票池为空，无法执行选股。")
                return

            fetcher_manager = DataFetcherManager()
            search_service = self._build_search_service()
            analyzer = GeminiAnalyzer(config=config)
            reference_time = datetime.now()

            top_sectors, bottom_sectors = self._load_sector_rankings(fetcher_manager)
            scored_candidates: List[Dict[str, Any]] = []
            insufficient_count = 0
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

            ranked_candidates = sorted(
                scored_candidates,
                key=lambda item: (
                    item["total_score"],
                    item["trend_score"],
                    item["setup_score"],
                    item["volume_score"],
                ),
                reverse=True,
            )
            selected_candidates = self._select_candidates(ranked_candidates, limit=task["limit"])
            self._repo.update_progress(
                task_id,
                progress_percent=86,
                progress_message="已完成排序，开始生成候选说明",
                processed_stocks=len(stock_codes),
            )

            explain_count = min(task["ai_top_k"], len(selected_candidates))
            for index, candidate in enumerate(selected_candidates, start=1):
                fallback = self._build_fallback_explanation(template.name, candidate)
                candidate["explanation_summary"] = fallback["summary"]
                candidate["explanation_rationale"] = fallback["rationale"]
                candidate["explanation_risks"] = fallback["risks"]
                candidate["explanation_watchpoints"] = fallback["watchpoints"]

                if index <= explain_count:
                    ai_payload = self._build_ai_explanation(
                        analyzer=analyzer,
                        template_name=template.name,
                        candidate=candidate,
                    )
                    if ai_payload:
                        candidate["explanation_summary"] = ai_payload["summary"]
                        candidate["explanation_rationale"] = ai_payload["rationale"]
                        candidate["explanation_risks"] = ai_payload["risks"]
                        candidate["explanation_watchpoints"] = ai_payload["watchpoints"]

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
                "total_stocks": len(stock_codes),
                "scored_count": len(scored_candidates),
                "insufficient_count": insufficient_count,
                "error_count": error_count,
                "strict_match_count": sum(1 for item in ranked_candidates if item["strict_match"]),
                "selected_count": len(selected_candidates),
                "fallback_count": sum(1 for item in selected_candidates if item["selection_reason"] == "fallback_fill"),
                "explained_count": explain_count,
            }
            self._repo.save_candidates(task_id, summary=summary, candidates=selected_candidates)
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
        if daily_frame is None or len(daily_frame) < 20:
            return None

        last_row = daily_frame.iloc[-1]
        latest_date = last_row["date"].date() if hasattr(last_row["date"], "date") else last_row["date"]
        if isinstance(latest_date, pd.Timestamp):
            latest_date = latest_date.date()
        if isinstance(latest_date, datetime):
            latest_date = latest_date.date()
        target_date = self._resolve_target_trading_date(normalized_code, current_time=current_time)
        if isinstance(latest_date, date) and latest_date < target_date:
            return None

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
            return None

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
                {"score_name": "trend_score", "score_label": "趋势结构", "score_value": scoring["trend_score"], "detail": {}},
                {"score_name": "setup_score", "score_label": "模板匹配", "score_value": scoring["setup_score"], "detail": {}},
                {"score_name": "volume_score", "score_label": "量能配合", "score_value": scoring["volume_score"], "detail": {}},
                {"score_name": "sector_score", "score_label": "板块强度", "score_value": scoring["sector_score"], "detail": {}},
                {"score_name": "news_score", "score_label": "新闻情绪", "score_value": 0.0, "detail": {}},
                {"score_name": "risk_penalty", "score_label": "风险扣分", "score_value": -scoring["risk_penalty"], "detail": {}},
                {"score_name": "total_score", "score_label": "综合得分", "score_value": scoring["total_score"], "detail": {}},
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
            },
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

    @staticmethod
    def _resolve_target_trading_date(
        code: str, current_time: Optional[datetime] = None
    ) -> date:
        market = get_market_for_stock(normalize_stock_code(code))
        return get_effective_trading_date(market, current_time=current_time)

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
        trend_score = 0.0
        if close > ma20:
            trend_score += 10
        if ma5 > ma10:
            trend_score += 8
        if ma10 > ma20:
            trend_score += 10
        if metrics["ma20_slope_pct"] > 0:
            trend_score += 8
        if close > metrics["ma60"] > 0:
            trend_score += 4
        trend_score = _clamp(trend_score, 0, 40)

        volume_score = 0.0
        if metrics["volume_ratio"] >= 1.4:
            volume_score = 15
        elif metrics["volume_ratio"] >= 1.1:
            volume_score = 11
        elif metrics["volume_ratio"] >= 0.9:
            volume_score = 7
        elif metrics["volume_ratio"] >= 0.7:
            volume_score = 4

        sector_score = self._score_sector(board_names, top_sectors, bottom_sectors)
        risk_penalty = 0.0
        if close < ma20:
            risk_penalty += 8
        if metrics["ma20_slope_pct"] < 0:
            risk_penalty += 6
        if metrics["latest_pct_chg"] < -5:
            risk_penalty += 5
        if ma20 > 0 and (close / ma20 - 1) * 100 > 12:
            risk_penalty += 6

        setup_score = 0.0
        strict_match = False
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
            strict_match = (
                close > ma10
                and ma5 > ma10 > ma20
                and metrics["distance_to_high_pct"] >= -3.5
                and metrics["volume_ratio"] >= 0.85
            )
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
            strict_match = (
                ma10 > ma20
                and close >= ma20 * 0.98
                and -6.0 <= metrics["pullback_from_high_pct"] <= 0
                and metrics["change_20d_pct"] > 2
            )
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
            strict_match = close >= ma20 * 0.99 and metrics["change_20d_pct"] > -2
        else:
            return None

        setup_score = _clamp(setup_score, 0, 30)
        total_score = _clamp(trend_score + setup_score + volume_score + sector_score - risk_penalty, 0, 100)
        return {
            "strict_match": strict_match,
            "trend_score": round(trend_score, 2),
            "setup_score": round(setup_score, 2),
            "volume_score": round(volume_score, 2),
            "sector_score": round(sector_score, 2),
            "risk_penalty": round(risk_penalty, 2),
            "total_score": round(total_score, 2),
        }

    @staticmethod
    def _score_sector(
        board_names: List[str],
        top_sectors: List[Dict[str, Any]],
        bottom_sectors: List[Dict[str, Any]],
    ) -> float:
        if not board_names:
            return 0.0
        normalized_boards = {name.strip().lower() for name in board_names if name.strip()}
        score = 0.0
        for index, item in enumerate(top_sectors[:10]):
            sector_name = str(item.get("name") or "").strip().lower()
            if not sector_name:
                continue
            if any(sector_name in board or board in sector_name for board in normalized_boards):
                score = max(score, max(3.0, 10.0 - index * 1.2))
        for index, item in enumerate(bottom_sectors[:10]):
            sector_name = str(item.get("name") or "").strip().lower()
            if not sector_name:
                continue
            if any(sector_name in board or board in sector_name for board in normalized_boards):
                score = min(score, -max(2.0, 7.0 - index * 0.8))
        return round(score, 2)

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
                if candidate["code"] in seen:
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
    ) -> Optional[Dict[str, Any]]:
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
            "你是股票候选解释助手。不要重新排序，不要给出任何收益承诺。\n"
            "请基于给定结构化数据，输出严格 JSON，不要添加 Markdown 代码块。\n"
            "JSON 结构如下：\n"
            "{\n"
            '  "summary": "40字以内总结",\n'
            '  "rationale": ["理由1", "理由2", "理由3"],\n'
            '  "risks": ["风险1", "风险2"],\n'
            '  "watchpoints": ["观察点1", "观察点2"]\n'
            "}\n\n"
            f"模板：{template_name}\n"
            f"股票：{candidate['name']} ({candidate['code']})\n"
            f"市场：{candidate['market']}\n"
            f"综合得分：{candidate['total_score']}\n"
            f"技术快照：{json.dumps(candidate['technical_snapshot'], ensure_ascii=False)}\n"
            f"核心板块：{json.dumps((candidate.get('board_names') or [])[:6], ensure_ascii=False)}\n"
            f"最近新闻摘要：{json.dumps(compact_news, ensure_ascii=False)}\n"
            f"评分拆解：{json.dumps(compact_scores, ensure_ascii=False)}\n"
        )
        raw_text = analyzer.generate_text(prompt, max_tokens=800, temperature=0.2)
        payload = _clean_json_block(raw_text or "")
        if not payload:
            return None
        rationale = [str(item).strip() for item in payload.get("rationale", []) if str(item).strip()]
        risks = [str(item).strip() for item in payload.get("risks", []) if str(item).strip()]
        watchpoints = [str(item).strip() for item in payload.get("watchpoints", []) if str(item).strip()]
        summary = str(payload.get("summary") or "").strip()
        if not summary:
            return None
        return {
            "summary": summary,
            "rationale": rationale[:4],
            "risks": risks[:3],
            "watchpoints": watchpoints[:3],
        }

    @staticmethod
    def _build_fallback_explanation(template_name: str, candidate: Dict[str, Any]) -> Dict[str, List[str] | str]:
        snapshot = candidate["technical_snapshot"]
        rationale = [
            f"{template_name} 模板得分 {candidate['total_score']}，趋势结构得分 {candidate['trend_score']}",
            f"近 20 日涨跌幅 {snapshot['change20d_pct']}%，距阶段高点 {candidate['distance_to_high_pct']}%",
        ]
        if candidate["board_names"]:
            rationale.append(f"所属板块：{'、'.join(candidate['board_names'][:3])}")
        if candidate["news_briefs"]:
            rationale.append(f"近端资讯偏 {'正面' if candidate['news_score'] >= 0 else '谨慎'}")

        risks = []
        if candidate["risk_penalty"] > 0:
            risks.append(f"风险扣分 {candidate['risk_penalty']}，说明当前形态并非无瑕疵")
        if snapshot["pullback_from_high_pct"] < -6:
            risks.append("距近 20 日高点回撤偏大，需防趋势衰减")
        if snapshot["ma20_slope_pct"] < 0:
            risks.append("MA20 仍在走弱，确认性不足")
        if not risks:
            risks.append("重点留意量价是否继续配合，避免假突破或回踩失守")

        watchpoints = [
            f"关注 MA10/MA20 附近支撑：{snapshot['ma10']} / {snapshot['ma20']}",
            f"关注量能是否维持在均量以上，当前量能比 {candidate['volume_ratio']}",
        ]
        if candidate["distance_to_high_pct"] > -2:
            watchpoints.append("关注能否有效站稳阶段高点附近")

        return {
            "summary": f"{candidate['name']} 当前为 {template_name} 候选，综合得分 {candidate['total_score']}。",
            "rationale": rationale[:4],
            "risks": risks[:3],
            "watchpoints": watchpoints[:3],
        }

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
