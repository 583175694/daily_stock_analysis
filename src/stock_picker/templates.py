from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List


@dataclass(frozen=True)
class PickerTemplate:
    """Built-in stock-picker template definition for V1."""

    template_id: str
    name: str
    description: str
    focus: str
    risk_level: str
    style: str
    scoring_notes: List[str]

    def to_dict(self) -> Dict[str, object]:
        return {
            "template_id": self.template_id,
            "name": self.name,
            "description": self.description,
            "focus": self.focus,
            "risk_level": self.risk_level,
            "style": self.style,
            "scoring_notes": list(self.scoring_notes),
        }


BUILTIN_TEMPLATES: List[PickerTemplate] = [
    PickerTemplate(
        template_id="trend_breakout",
        name="趋势突破",
        description="优先寻找多头结构、接近阶段新高且量能配合的强势票。",
        focus="强趋势延续",
        risk_level="medium_high",
        style="右侧跟随",
        scoring_notes=[
            "更看重均线多头排列与 MA20 斜率",
            "更看重接近/突破 20 日高点",
            "量能放大优先，追高风险会被额外惩罚",
        ],
    ),
    PickerTemplate(
        template_id="strong_pullback",
        name="强势回踩",
        description="优先寻找中期趋势未坏、回踩均线后的二次介入机会。",
        focus="趋势中继",
        risk_level="medium",
        style="回调低吸",
        scoring_notes=[
            "要求 MA10/MA20 仍维持偏强结构",
            "价格更靠近 MA5/MA10/MA20 回踩区间得分更高",
            "缩量回踩优于放量下跌",
        ],
    ),
    PickerTemplate(
        template_id="balanced",
        name="均衡筛选",
        description="以稳健为主，在趋势、量能、板块与资讯之间做均衡排序。",
        focus="综合排序",
        risk_level="medium_low",
        style="均衡配置",
        scoring_notes=[
            "降低单一突破信号权重，强调综合分数",
            "允许轻微回撤，但要求整体趋势不弱",
            "适合作为默认模板快速扫池",
        ],
    ),
]

TEMPLATE_MAP: Dict[str, PickerTemplate] = {
    template.template_id: template for template in BUILTIN_TEMPLATES
}


def list_templates() -> List[Dict[str, object]]:
    return [template.to_dict() for template in BUILTIN_TEMPLATES]


def get_template(template_id: str) -> PickerTemplate:
    try:
        return TEMPLATE_MAP[template_id]
    except KeyError as exc:
        raise ValueError(f"Unknown picker template: {template_id}") from exc
