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
    alpha_hypothesis: str
    suitable_regimes: List[str]
    caution_regimes: List[str]
    invalid_regimes: List[str]
    exclusion_conditions: List[str]
    trade_rules: Dict[str, object]

    def to_dict(self) -> Dict[str, object]:
        return {
            "template_id": self.template_id,
            "name": self.name,
            "description": self.description,
            "focus": self.focus,
            "risk_level": self.risk_level,
            "style": self.style,
            "scoring_notes": list(self.scoring_notes),
            "alpha_hypothesis": self.alpha_hypothesis,
            "suitable_regimes": list(self.suitable_regimes),
            "caution_regimes": list(self.caution_regimes),
            "invalid_regimes": list(self.invalid_regimes),
            "exclusion_conditions": list(self.exclusion_conditions),
            "trade_rules": dict(self.trade_rules),
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
        alpha_hypothesis="强趋势环境中，临近阶段新高且量价共振的个股更容易延续相对强势。",
        suitable_regimes=["trend_up"],
        caution_regimes=["range_bound"],
        invalid_regimes=["risk_off"],
        exclusion_conditions=[
            "收盘价重新跌回 MA20 下方",
            "放量冲高后快速回落且 20 日涨幅已过热",
            "市场环境转为 risk_off 时仅观察不追高",
        ],
        trade_rules={
            "entry_rule": "优先观察放量站稳前高/MA10 后的次日确认，不在连续大幅高开时追价。",
            "holding_rule": "趋势未破坏前以持有为主，重点观察 MA10 与量能延续。",
            "stop_loss_rule": "跌破 MA20 或放量跌破突破位时止损。",
            "take_profit_rule": "短期快速冲高、远离 MA20 且量价背离时分批止盈。",
            "timeout_exit_rule": "10 个交易日内未形成有效突破延续则超时退出。",
            "max_holding_days": 10,
        },
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
        alpha_hypothesis="趋势上行或震荡偏强环境中，强势股回踩关键均线后的二次启动具备更优性价比。",
        suitable_regimes=["trend_up", "range_bound"],
        caution_regimes=[],
        invalid_regimes=["risk_off"],
        exclusion_conditions=[
            "跌破 MA20 后仍无快速收复",
            "回踩过程中出现持续放量下跌",
            "近 20 日趋势已明显走弱",
        ],
        trade_rules={
            "entry_rule": "优先在回踩 MA10/MA20 后止跌企稳、次日确认转强时介入。",
            "holding_rule": "只要 MA10/MA20 结构未坏且回踩后恢复放量，可继续持有。",
            "stop_loss_rule": "跌破 MA20 且无法快速修复时止损。",
            "take_profit_rule": "回到前高附近或短期反弹过快时分批兑现。",
            "timeout_exit_rule": "10 个交易日内未完成修复或反弹延续，则超时退出。",
            "max_holding_days": 10,
        },
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
        alpha_hypothesis="在趋势不弱、板块与量能尚可的环境里，综合质量更高的候选更容易取得稳健的相对收益。",
        suitable_regimes=["trend_up", "range_bound"],
        caution_regimes=["risk_off"],
        invalid_regimes=[],
        exclusion_conditions=[
            "趋势与量能同时显著走弱",
            "板块与个股均缺乏相对强势支撑",
            "出现连续大跌或显著跌破中期均线后只保留观察价值",
        ],
        trade_rules={
            "entry_rule": "优先选择趋势未破坏且综合分数靠前的候选，分批观察性介入。",
            "holding_rule": "维持均衡跟踪，若趋势/板块/量能同步恶化则降低仓位预期。",
            "stop_loss_rule": "跌破 MA20 且总分显著下修时退出。",
            "take_profit_rule": "达到阶段目标或风险收益比明显下降时止盈。",
            "timeout_exit_rule": "15 个交易日内未体现相对强势则超时退出。",
            "max_holding_days": 15,
        },
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
