# -*- coding: utf-8 -*-
"""Stock picker API schemas."""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class PickerTemplateItem(BaseModel):
    template_id: str
    name: str
    description: str
    focus: str
    risk_level: str
    style: str
    scoring_notes: List[str] = Field(default_factory=list)


class PickerTemplatesResponse(BaseModel):
    items: List[PickerTemplateItem] = Field(default_factory=list)


class PickerUniverseItem(BaseModel):
    universe_id: str
    name: str
    description: str
    stock_count: int
    codes: List[str] = Field(default_factory=list)


class PickerUniversesResponse(BaseModel):
    items: List[PickerUniverseItem] = Field(default_factory=list)


class PickerRunRequest(BaseModel):
    template_id: str = Field(..., description="内置模板 ID")
    template_overrides: Dict[str, Any] = Field(default_factory=dict, description="V1 保留字段，当前必须为空")
    universe_id: str = Field("watchlist", description="股票池 ID，V1 仅支持 watchlist")
    limit: int = Field(20, ge=1, le=50, description="返回候选数量上限")
    force_refresh: bool = Field(False, description="是否强制刷新行情数据")


class PickerRunResponse(BaseModel):
    task_id: str
    status: Literal["queued", "running", "completed", "failed"]


class PickerTaskSummary(BaseModel):
    template_id: Optional[str] = None
    template_name: Optional[str] = None
    universe_id: Optional[str] = None
    total_stocks: int = 0
    scored_count: int = 0
    insufficient_count: int = 0
    error_count: int = 0
    strict_match_count: int = 0
    selected_count: int = 0
    fallback_count: int = 0
    explained_count: int = 0


class PickerTaskItem(BaseModel):
    task_id: str
    status: str
    status_label: Optional[str] = None
    template_id: str
    template_name: Optional[str] = None
    template_version: str
    universe_id: str
    universe_name: Optional[str] = None
    limit: int
    ai_top_k: int
    force_refresh: bool
    total_stocks: int = 0
    processed_stocks: int = 0
    candidate_count: int = 0
    progress_percent: int = 0
    progress_message: str = ""
    summary: PickerTaskSummary = Field(default_factory=PickerTaskSummary)
    error_message: Optional[str] = None
    request_payload: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[str] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    updated_at: Optional[str] = None


class PickerTaskListResponse(BaseModel):
    items: List[PickerTaskItem] = Field(default_factory=list)


class PickerScoreItem(BaseModel):
    score_name: str
    score_label: str
    score_value: float
    detail: Dict[str, Any] = Field(default_factory=dict)


class PickerNewsBrief(BaseModel):
    title: str
    source: Optional[str] = None
    published_date: Optional[str] = None
    url: Optional[str] = None
    snippet: Optional[str] = None


class PickerCandidateItem(BaseModel):
    rank: int
    code: str
    name: Optional[str] = None
    market: str
    selection_reason: str
    latest_date: Optional[str] = None
    latest_close: Optional[float] = None
    change_pct: Optional[float] = None
    volume_ratio: Optional[float] = None
    distance_to_high_pct: Optional[float] = None
    total_score: Optional[float] = None
    board_names: List[str] = Field(default_factory=list)
    news_briefs: List[PickerNewsBrief] = Field(default_factory=list)
    explanation_summary: Optional[str] = None
    explanation_rationale: List[str] = Field(default_factory=list)
    explanation_risks: List[str] = Field(default_factory=list)
    explanation_watchpoints: List[str] = Field(default_factory=list)
    technical_snapshot: Dict[str, Any] = Field(default_factory=dict)
    score_breakdown: List[PickerScoreItem] = Field(default_factory=list)


class PickerTaskDetailResponse(PickerTaskItem):
    candidates: List[PickerCandidateItem] = Field(default_factory=list)
