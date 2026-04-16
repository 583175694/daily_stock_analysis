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
    alpha_hypothesis: str = ""
    suitable_regimes: List[str] = Field(default_factory=list)
    caution_regimes: List[str] = Field(default_factory=list)
    invalid_regimes: List[str] = Field(default_factory=list)
    exclusion_conditions: List[str] = Field(default_factory=list)
    trade_rules: Dict[str, Any] = Field(default_factory=dict)


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


class PickerSectorItem(BaseModel):
    sector_id: str
    name: str
    description: Optional[str] = None
    market: str = "cn"
    stock_count: int
    strength_label: Optional[str] = None
    rank_direction: Optional[Literal["top", "bottom"]] = None
    rank_position: Optional[int] = None
    change_pct: Optional[float] = None
    is_ranked_today: bool = False


class PickerSectorsResponse(BaseModel):
    items: List[PickerSectorItem] = Field(default_factory=list)


class PickerRunRequest(BaseModel):
    template_id: str = Field(..., description="内置模板 ID")
    template_overrides: Dict[str, Any] = Field(default_factory=dict, description="V2 仅保留受控少量参数，当前仍建议保持为空")
    universe_id: str = Field("watchlist", description="股票池 ID，V1 仅支持 watchlist")
    mode: Literal["watchlist", "sector"] = Field("watchlist", description="运行模式：自选股或板块模式")
    sector_ids: List[str] = Field(default_factory=list, description="板块模式下所选板块 ID 列表")
    limit: int = Field(20, ge=1, le=30, description="返回候选数量上限")
    ai_top_k: int = Field(5, ge=1, le=10, description="AI 解释候选数量上限")
    force_refresh: bool = Field(False, description="是否强制刷新行情数据")
    notify: bool = Field(False, description="任务完成后是否发送摘要通知")


class PickerRunResponse(BaseModel):
    task_id: str
    status: Literal["queued", "running", "completed", "failed"]


class PickerTaskSummary(BaseModel):
    template_id: Optional[str] = None
    template_name: Optional[str] = None
    universe_id: Optional[str] = None
    mode: Optional[str] = None
    total_stocks: int = 0
    scored_count: int = 0
    insufficient_count: int = 0
    error_count: int = 0
    strict_match_count: int = 0
    selected_count: int = 0
    qualified_fallback_count: int = 0
    fallback_count: int = 0
    explained_count: int = 0
    advanced_enriched_count: int = 0
    ai_reviewed_count: int = 0
    ai_soft_veto_count: int = 0
    insufficient_reason_breakdown: Dict[str, int] = Field(default_factory=dict)
    insufficient_reason_labels: Dict[str, str] = Field(default_factory=dict)
    trading_date_policy: Dict[str, Any] = Field(default_factory=dict)
    sector_catalog_snapshot: Dict[str, Any] = Field(default_factory=dict)
    sector_quality_summary: Dict[str, Any] = Field(default_factory=dict)
    ranked_sector_breakdown: List[Dict[str, Any]] = Field(default_factory=list)
    benchmark_policy: Dict[str, Any] = Field(default_factory=dict)
    selection_quality_gate: Dict[str, Any] = Field(default_factory=dict)
    market_regime_snapshot: Dict[str, Any] = Field(default_factory=dict)


class PickerTaskItem(BaseModel):
    task_id: str
    status: str
    status_label: Optional[str] = None
    template_id: str
    template_name: Optional[str] = None
    template_version: str
    universe_id: str
    universe_name: Optional[str] = None
    mode: Literal["watchlist", "sector"] | str = "watchlist"
    mode_label: Optional[str] = None
    sector_ids: List[str] = Field(default_factory=list)
    sector_names: List[str] = Field(default_factory=list)
    limit: int
    ai_top_k: int
    force_refresh: bool
    notify: bool = False
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


class PickerExecutionConstraints(BaseModel):
    market: Optional[str] = None
    status: Optional[str] = None
    status_label: Optional[str] = None
    not_fillable: bool = False
    liquidity_bucket: Optional[str] = None
    gap_risk: Optional[str] = None
    slippage_bps: Optional[int] = None
    execution_penalty: Optional[float] = None
    estimated_cost_model: Optional[str] = None
    signals: Dict[str, Any] = Field(default_factory=dict)
    note: Optional[str] = None


class PickerResearchConfidence(BaseModel):
    status: Optional[str] = None
    label: Optional[str] = None
    score: Optional[float] = None
    window_days: Optional[int] = None
    benchmark_code: Optional[str] = None
    template_id: Optional[str] = None
    market_regime: Optional[str] = None
    signal_bucket: Optional[str] = None
    comparable_samples: int = 0
    regime_comparable_samples: int = 0
    template_win_rate_pct: Optional[float] = None
    regime_win_rate_pct: Optional[float] = None
    template_avg_excess_return_pct: Optional[float] = None
    regime_avg_excess_return_pct: Optional[float] = None
    nominal_probability_pct: Optional[float] = None
    calibrated_win_rate_pct: Optional[float] = None
    calibration_gap_pct: Optional[float] = None
    rule_version: Optional[str] = None
    calibration: Dict[str, Any] = Field(default_factory=dict)
    high_confidence_gate: Dict[str, Any] = Field(default_factory=dict)
    note: Optional[str] = None


class PickerExecutionConfidence(BaseModel):
    status: Optional[str] = None
    label: Optional[str] = None
    score: Optional[float] = None
    slippage_bps: Optional[int] = None
    liquidity_bucket: Optional[str] = None
    gap_risk: Optional[str] = None
    not_fillable: bool = False
    cost_model: Optional[str] = None
    note: Optional[str] = None


class PickerCandidateEvaluationItem(BaseModel):
    window_days: int
    benchmark_code: Optional[str] = None
    eval_status: str
    benchmark_status: Optional[str] = None
    is_comparable: bool = False
    entry_date: Optional[str] = None
    entry_price: Optional[float] = None
    exit_date: Optional[str] = None
    exit_price: Optional[float] = None
    benchmark_entry_price: Optional[float] = None
    benchmark_exit_price: Optional[float] = None
    return_pct: Optional[float] = None
    benchmark_return_pct: Optional[float] = None
    excess_return_pct: Optional[float] = None
    max_drawdown_pct: Optional[float] = None
    mfe_pct: Optional[float] = None
    mae_pct: Optional[float] = None


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
    environment_fit: Optional[str] = None
    environment_fit_label: Optional[str] = None
    signal_bucket: Optional[str] = None
    board_names: List[str] = Field(default_factory=list)
    news_briefs: List[PickerNewsBrief] = Field(default_factory=list)
    explanation_summary: Optional[str] = None
    explanation_rationale: List[str] = Field(default_factory=list)
    explanation_risks: List[str] = Field(default_factory=list)
    explanation_watchpoints: List[str] = Field(default_factory=list)
    technical_snapshot: Dict[str, Any] = Field(default_factory=dict)
    execution_constraints: PickerExecutionConstraints = Field(default_factory=PickerExecutionConstraints)
    research_confidence: PickerResearchConfidence = Field(default_factory=PickerResearchConfidence)
    execution_confidence: PickerExecutionConfidence = Field(default_factory=PickerExecutionConfidence)
    trade_plan: Dict[str, Any] = Field(default_factory=dict)
    advanced_factors: Dict[str, Any] = Field(default_factory=dict)
    ai_review: Dict[str, Any] = Field(default_factory=dict)
    template_failure_flags: List[Dict[str, Any]] = Field(default_factory=list)
    score_breakdown: List[PickerScoreItem] = Field(default_factory=list)
    evaluations: List[PickerCandidateEvaluationItem] = Field(default_factory=list)


class PickerTaskDetailResponse(PickerTaskItem):
    candidates: List[PickerCandidateItem] = Field(default_factory=list)


class PickerTemplateStatItem(BaseModel):
    template_id: str
    template_name: str
    window_days: int
    total_evaluations: int = 0
    comparable_evaluations: int = 0
    benchmark_unavailable_evaluations: int = 0
    win_rate_pct: Optional[float] = None
    avg_return_pct: Optional[float] = None
    avg_excess_return_pct: Optional[float] = None
    avg_max_drawdown_pct: Optional[float] = None


class PickerTemplateStatsResponse(BaseModel):
    window_days: int
    benchmark_code: str = "000300"
    items: List[PickerTemplateStatItem] = Field(default_factory=list)


class PickerStratifiedStatItem(BaseModel):
    bucket_key: str
    bucket_label: str
    total_evaluations: int = 0
    comparable_evaluations: int = 0
    benchmark_unavailable_evaluations: int = 0
    win_rate_pct: Optional[float] = None
    avg_return_pct: Optional[float] = None
    avg_excess_return_pct: Optional[float] = None
    avg_max_drawdown_pct: Optional[float] = None


class PickerStratifiedStatsResponse(BaseModel):
    window_days: int
    benchmark_code: str = "000300"
    by_market_regime: List[PickerStratifiedStatItem] = Field(default_factory=list)
    by_template: List[PickerStratifiedStatItem] = Field(default_factory=list)
    by_rank_bucket: List[PickerStratifiedStatItem] = Field(default_factory=list)
    by_signal_bucket: List[PickerStratifiedStatItem] = Field(default_factory=list)


class PickerCalibrationStatItem(BaseModel):
    template_id: str
    template_name: str
    market_regime: str
    market_regime_label: str
    rule_version: str
    bucket_key: str
    bucket_label: str
    window_days: int
    samples: int = 0
    nominal_probability_pct: Optional[float] = None
    actual_win_rate_pct: Optional[float] = None
    calibration_gap_pct: Optional[float] = None
    avg_return_pct: Optional[float] = None
    avg_excess_return_pct: Optional[float] = None
    avg_max_drawdown_pct: Optional[float] = None
    calibration_status: str
    calibration_label: str
    high_confidence_gate: Dict[str, Any] = Field(default_factory=dict)


class PickerCalibrationStatsResponse(BaseModel):
    window_days: int
    benchmark_code: str = "000300"
    items: List[PickerCalibrationStatItem] = Field(default_factory=list)


class PickerValidationHoldoutStatItem(BaseModel):
    template_id: str
    template_name: str
    rule_version: str
    window_days: int
    sample_status: str
    comparable_samples: int = 0
    in_sample_count: int = 0
    out_of_sample_count: int = 0
    split_ratio: float = 0.7
    analysis_date_start: Optional[str] = None
    analysis_date_end: Optional[str] = None
    out_of_sample_win_rate_pct: Optional[float] = None
    out_of_sample_avg_return_pct: Optional[float] = None
    out_of_sample_avg_excess_return_pct: Optional[float] = None
    out_of_sample_avg_max_drawdown_pct: Optional[float] = None


class PickerValidationRollingStatItem(BaseModel):
    template_id: str
    template_name: str
    rule_version: str
    window_days: int
    rolling_month: str
    sample_status: str
    rolling_count: int = 0
    rolling_win_rate_pct: Optional[float] = None
    rolling_avg_excess_return_pct: Optional[float] = None
    rolling_avg_max_drawdown_pct: Optional[float] = None


class PickerValidationStatsResponse(BaseModel):
    window_days: int
    benchmark_code: str = "000300"
    out_of_sample_by_template: List[PickerValidationHoldoutStatItem] = Field(default_factory=list)
    rolling_monthly_by_template: List[PickerValidationRollingStatItem] = Field(default_factory=list)


class PickerRiskStatItem(BaseModel):
    template_id: str
    template_name: str
    rule_version: str
    window_days: int
    sample_status: str
    sample_count: int = 0
    avg_return_pct: Optional[float] = None
    avg_excess_return_pct: Optional[float] = None
    avg_max_drawdown_pct: Optional[float] = None
    avg_mfe_pct: Optional[float] = None
    avg_mae_pct: Optional[float] = None
    profit_factor: Optional[float] = None
    return_drawdown_ratio: Optional[float] = None
    return_pct_p25: Optional[float] = None
    return_pct_p50: Optional[float] = None
    return_pct_p75: Optional[float] = None
    excess_return_pct_p25: Optional[float] = None
    excess_return_pct_p50: Optional[float] = None
    excess_return_pct_p75: Optional[float] = None
    max_drawdown_pct_p25: Optional[float] = None
    max_drawdown_pct_p50: Optional[float] = None
    max_drawdown_pct_p75: Optional[float] = None
    mfe_pct_p25: Optional[float] = None
    mfe_pct_p50: Optional[float] = None
    mfe_pct_p75: Optional[float] = None
    mae_pct_p25: Optional[float] = None
    mae_pct_p50: Optional[float] = None
    mae_pct_p75: Optional[float] = None


class PickerRiskStatsResponse(BaseModel):
    window_days: int
    benchmark_code: str = "000300"
    items: List[PickerRiskStatItem] = Field(default_factory=list)
