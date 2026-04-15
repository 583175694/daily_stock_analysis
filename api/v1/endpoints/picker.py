# -*- coding: utf-8 -*-
"""Stock picker endpoints."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Query

from api.deps import get_stock_picker_service
from api.v1.schemas.common import ErrorResponse
from api.v1.schemas.picker import (
    PickerRunRequest,
    PickerRunResponse,
    PickerSectorsResponse,
    PickerSectorItem,
    PickerTaskDetailResponse,
    PickerTaskItem,
    PickerTemplateStatItem,
    PickerTemplateStatsResponse,
    PickerTaskListResponse,
    PickerTemplatesResponse,
    PickerTemplateItem,
    PickerUniverseItem,
    PickerUniversesResponse,
)
from src.stock_picker.service import StockPickerService

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/templates",
    response_model=PickerTemplatesResponse,
    responses={200: {"description": "模板列表"}},
    summary="获取内置选股模板",
)
def get_picker_templates(
    service: StockPickerService = Depends(get_stock_picker_service),
) -> PickerTemplatesResponse:
    items = [PickerTemplateItem(**item) for item in service.list_templates()]
    return PickerTemplatesResponse(items=items)


@router.get(
    "/universes",
    response_model=PickerUniversesResponse,
    responses={200: {"description": "股票池列表"}},
    summary="获取可用股票池",
)
def get_picker_universes(
    service: StockPickerService = Depends(get_stock_picker_service),
) -> PickerUniversesResponse:
    items = [PickerUniverseItem(**item) for item in service.list_universes()]
    return PickerUniversesResponse(items=items)


@router.get(
    "/sectors",
    response_model=PickerSectorsResponse,
    responses={200: {"description": "板块列表"}},
    summary="获取可用行业板块列表",
)
def get_picker_sectors(
    service: StockPickerService = Depends(get_stock_picker_service),
) -> PickerSectorsResponse:
    items = [PickerSectorItem(**item) for item in service.list_sectors()]
    return PickerSectorsResponse(items=items)


@router.post(
    "/run",
    response_model=PickerRunResponse,
    responses={
        200: {"description": "任务已提交"},
        400: {"description": "参数错误", "model": ErrorResponse},
        500: {"description": "服务器错误", "model": ErrorResponse},
    },
    summary="启动 AI 选股任务",
)
def run_picker(
    request: PickerRunRequest,
    service: StockPickerService = Depends(get_stock_picker_service),
) -> PickerRunResponse:
    try:
        payload = service.submit_task(
            template_id=request.template_id,
            template_overrides=request.template_overrides,
            universe_id=request.universe_id,
            mode=request.mode,
            sector_ids=request.sector_ids,
            limit=request.limit,
            ai_top_k=request.ai_top_k,
            force_refresh=request.force_refresh,
            notify=request.notify,
        )
        return PickerRunResponse(**payload)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_params", "message": str(exc)},
        )
    except Exception as exc:
        logger.error("Failed to submit picker task: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": "提交 AI 选股任务失败"},
        )


@router.get(
    "/tasks",
    response_model=PickerTaskListResponse,
    responses={200: {"description": "任务列表"}},
    summary="获取 AI 选股任务列表",
)
def list_picker_tasks(
    limit: int = Query(20, ge=1, le=50, description="返回任务条数"),
    service: StockPickerService = Depends(get_stock_picker_service),
) -> PickerTaskListResponse:
    items = [PickerTaskItem(**item) for item in service.list_tasks(limit=limit)]
    return PickerTaskListResponse(items=items)


@router.get(
    "/tasks/{task_id}",
    response_model=PickerTaskDetailResponse,
    responses={
        200: {"description": "任务详情"},
        404: {"description": "任务不存在", "model": ErrorResponse},
    },
    summary="获取 AI 选股任务详情",
)
def get_picker_task(
    task_id: str,
    service: StockPickerService = Depends(get_stock_picker_service),
) -> PickerTaskDetailResponse:
    payload = service.get_task(task_id)
    if payload is None:
        raise HTTPException(
            status_code=404,
            detail={"error": "not_found", "message": f"选股任务 {task_id} 不存在"},
        )
    return PickerTaskDetailResponse(**payload)


@router.get(
    "/stats/templates",
    response_model=PickerTemplateStatsResponse,
    responses={
        200: {"description": "模板效果统计"},
        400: {"description": "参数错误", "model": ErrorResponse},
    },
    summary="获取 AI 选股模板效果统计",
)
def get_picker_template_stats(
    window_days: int = Query(10, ge=1, description="统计窗口，仅支持 5/10/20"),
    service: StockPickerService = Depends(get_stock_picker_service),
) -> PickerTemplateStatsResponse:
    try:
        payload = service.list_template_stats(window_days=window_days)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail={"error": "invalid_params", "message": str(exc)},
        )
    items = [PickerTemplateStatItem(**item) for item in payload["items"]]
    return PickerTemplateStatsResponse(
        window_days=payload["window_days"],
        benchmark_code=payload["benchmark_code"],
        items=items,
    )
