import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { MemoryRouter } from 'react-router-dom';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { analysisApi } from '../../api/analysis';
import { stockPickerApi } from '../../api/stockPicker';
import { systemConfigApi } from '../../api/systemConfig';
import StockPickerPage from '../StockPickerPage';

const navigateMock = vi.fn();

vi.mock('react-router-dom', async () => {
  const actual = await vi.importActual<typeof import('react-router-dom')>('react-router-dom');
  return {
    ...actual,
    useNavigate: () => navigateMock,
  };
});

vi.mock('../../api/stockPicker', () => ({
  stockPickerApi: {
    getTemplates: vi.fn(),
    getUniverses: vi.fn(),
    getSectors: vi.fn(),
    run: vi.fn(),
    listTasks: vi.fn(),
    getTask: vi.fn(),
    getTemplateStats: vi.fn(),
    getStratifiedStats: vi.fn(),
    getCalibrationStats: vi.fn(),
    getValidationStats: vi.fn(),
    getRiskStats: vi.fn(),
  },
}));

vi.mock('../../api/analysis', () => ({
  analysisApi: {
    analyzeAsync: vi.fn(),
  },
  DuplicateTaskError: class DuplicateTaskError extends Error {},
}));

vi.mock('../../api/systemConfig', () => ({
  systemConfigApi: {
    getConfig: vi.fn(),
    update: vi.fn(),
  },
}));

vi.mock('../../stores/agentChatStore', () => {
  const useAgentChatStore = (
    selector?: (state: {
      currentRoute: string;
      completionBadge: boolean;
      setCurrentRoute: (path: string) => void;
      clearCompletionBadge: () => void;
    }) => unknown
  ) => {
    const state = {
      currentRoute: '',
      completionBadge: false,
      setCurrentRoute: vi.fn(),
      clearCompletionBadge: vi.fn(),
    };
    return typeof selector === 'function' ? selector(state) : state;
  };

  useAgentChatStore.getState = () => ({
    setCurrentRoute: vi.fn(),
    clearCompletionBadge: vi.fn(),
  });

  return { useAgentChatStore };
});

const template = {
  templateId: 'trend_breakout',
  name: '趋势突破',
  description: '寻找趋势强化与放量突破的候选。',
  focus: '趋势',
  riskLevel: '中等',
  style: '进攻',
  scoringNotes: ['关注突破有效性', '关注量价共振'],
  alphaHypothesis: '强趋势环境中，临近新高且量价共振的个股更容易延续强势。',
  suitableRegimes: ['trend_up'],
  cautionRegimes: ['range_bound'],
  invalidRegimes: ['risk_off'],
  exclusionConditions: ['跌回 MA20 下方', '环境转弱时只观察'],
  tradeRules: {
    entryRule: '优先观察放量站稳前高后的次日确认。',
    holdingRule: '趋势未坏前持有。',
    stopLossRule: '跌破 MA20 止损。',
    takeProfitRule: '冲高背离分批止盈。',
    timeoutExitRule: '10 个交易日未延续则退出。',
    maxHoldingDays: 10,
  },
};

const universe = {
  universeId: 'watchlist',
  name: '当前自选股',
  description: '基于 STOCK_LIST 扫描。',
  stockCount: 3,
  codes: ['600519', 'AAPL', 'HK00700'],
};

const task = {
  taskId: 'picker-task-1',
  status: 'completed',
  templateId: 'trend_breakout',
  templateName: '趋势突破',
  templateVersion: 'v4_2_phase2',
  universeId: 'watchlist',
  universeName: '当前自选股',
  mode: 'watchlist',
  modeLabel: '自选股模式',
  sectorIds: [],
  sectorNames: [],
  limit: 20,
  aiTopK: 5,
  forceRefresh: false,
  notify: false,
  totalStocks: 3,
  processedStocks: 3,
  candidateCount: 1,
  progressPercent: 100,
  progressMessage: '已完成',
  summary: {
    totalStocks: 3,
    scoredCount: 3,
    insufficientCount: 0,
    errorCount: 0,
    strictMatchCount: 1,
    selectedCount: 1,
    fallbackCount: 0,
    explainedCount: 1,
    advancedEnrichedCount: 1,
    aiReviewedCount: 1,
    aiSoftVetoCount: 1,
    tradingDatePolicy: {
      marketTargetDates: {
        cn: '2026-04-12',
      },
    },
    benchmarkPolicy: {
      benchmarkCode: '000300',
    },
    selectionQualityGate: {
      selectionPolicy: 'strict_match_first_then_quality_gated_fallback',
    },
    marketRegimeSnapshot: {
      regime: 'trend_up',
      regimeLabel: '上行趋势',
      asOfDate: '2026-04-12',
      signals: {
        close: 3900,
        ma20: 3840,
        ma20SlopePct: 1.2,
        change20dPct: 5.4,
      },
    },
    sectorQualitySummary: {
      selectedSectorCount: 0,
    },
    rankedSectorBreakdown: [],
  },
  errorMessage: null,
  requestPayload: {
    requestPolicyVersion: 'v4_2_phase2',
  },
  createdAt: '2026-04-13T09:00:00Z',
  startedAt: '2026-04-13T09:01:00Z',
  finishedAt: '2026-04-13T09:02:00Z',
  updatedAt: '2026-04-13T09:02:00Z',
};

const candidate = {
  rank: 1,
  code: '600519',
  name: '贵州茅台',
  market: 'cn',
  selectionReason: 'strict_match',
  latestDate: '2026-04-12',
  latestClose: 1688.88,
  changePct: 2.56,
  volumeRatio: 1.42,
  distanceToHighPct: -1.23,
  totalScore: 92.4,
  boardNames: ['白酒'],
  newsBriefs: [],
  explanationSummary: '趋势延续，量价配合较好。',
  explanationRationale: ['均线维持多头排列'],
  explanationRisks: ['高位波动放大'],
  explanationWatchpoints: ['关注量能是否持续'],
  environmentFit: 'suitable',
  environmentFitLabel: '环境匹配',
  signalBucket: 'high',
  technicalSnapshot: {
    ma5: 1660.12,
    ma10: 1638.55,
    ma20: 1602.33,
    ma60: 1511.07,
    change5dPct: 4.12,
    change20dPct: 11.8,
    targetTradingDate: '2026-04-12',
    marketRegime: 'trend_up',
    marketRegimeLabel: '上行趋势',
    environmentFit: 'suitable',
    environmentFitLabel: '环境匹配',
    environmentScore: 0,
    signalBucket: 'high',
    explanationSource: 'structured_plus_ai_summary',
  },
  executionConstraints: {
    market: 'cn',
    status: 'cautious',
    statusLabel: '执行谨慎',
    notFillable: false,
    liquidityBucket: 'medium',
    gapRisk: 'medium',
    slippageBps: 15,
    executionPenalty: 5,
    estimatedCostModel: 'cn_equity_v4_1_minimal',
    signals: {
      amount: 12000000,
      latestPctChg: 2.56,
      gapFromPrevClosePct: 1.8,
      intradayRangePct: 3.2,
    },
  },
  researchConfidence: {
    status: 'calibrated_neutral',
    label: '中性（已校准）',
    score: 0.67,
    windowDays: 10,
    benchmarkCode: '000300',
    templateId: 'trend_breakout',
    marketRegime: 'trend_up',
    signalBucket: 'high',
    comparableSamples: 12,
    regimeComparableSamples: 5,
    templateWinRatePct: 66.7,
    regimeWinRatePct: 60,
    templateAvgExcessReturnPct: 2.1,
    regimeAvgExcessReturnPct: 1.6,
    nominalProbabilityPct: 70,
    calibratedWinRatePct: 66,
    calibrationGapPct: 4,
    ruleVersion: 'v4_2_phase2',
    calibration: {
      bucketKey: 'high',
      bucketLabel: '高信号桶',
      samples: 18,
      nominalProbabilityPct: 70,
      actualWinRatePct: 66,
      calibrationGapPct: 4,
      calibrationStatus: 'calibrated',
      calibrationLabel: '校准通过',
    },
    highConfidenceGate: {
      status: 'blocked',
      label: '未达高置信度门槛',
      passed: false,
      reasonLabels: ['高信号桶可比样本不足 50'],
    },
    note: '当前信号桶已通过基础校准，但尚未达到高置信度门槛。',
  },
  executionConfidence: {
    status: 'cautious',
    label: '执行谨慎',
    score: 0.45,
    slippageBps: 15,
    liquidityBucket: 'medium',
    gapRisk: 'medium',
    notFillable: false,
    costModel: 'cn_equity_v4_1_minimal',
    note: '执行置信度基于最小流动性、跳空与不可成交近似约束，仍不等同真实成交结果。',
  },
  tradePlan: {
    action: 'observe',
    entryRule: '优先观察放量站稳前高后的次日确认。',
    holdingRule: '趋势未坏前持有。',
    stopLossRule: '跌破 MA20 止损。',
    takeProfitRule: '冲高背离分批止盈。',
    timeoutExitRule: '10 个交易日未延续则退出。',
    maxHoldingDays: 10,
  },
  advancedFactors: {
    factorTotal: 10.5,
    relativeStrength: {
      score: 4,
      stockChange20dPct: 11.8,
      benchmarkChange20dPct: 5.4,
      excessChange20dPct: 6.4,
    },
    boardLeadership: {
      score: 2.5,
      matchedTopCount: 1,
      matchedBottomCount: 0,
    },
    liquidityQuality: {
      score: 2.5,
      amountRatio: 1.2,
      executionStatus: 'cautious',
    },
    eventStrength: {
      score: 1.5,
      mainNetInflow: 120000000,
      netProfitYoy: 18,
      dividendYieldPct: 2.3,
    },
  },
  aiReview: {
    reviewSummary: '结构仍在，但高位波动和执行约束要求先观察。',
    supportingPoints: ['趋势维持强势'],
    counterPoints: ['高位波动放大'],
    vetoLevel: 'soft_veto',
    vetoReasons: ['高位波动与执行约束叠加，暂不适合直接执行'],
    confidenceComment: '结构偏强，但执行质量不足以支持直接交易。',
    reviewScope: {
      templateId: 'trend_breakout',
      marketRegime: 'trend_up',
      ruleVersion: 'v4_2_phase2',
      signalBucket: 'high',
    },
  },
  templateFailureFlags: [
    {
      flag: 'negative_event_pressure',
      label: '负面新闻或事件压力偏强，需要谨慎处理。',
      severity: 'medium',
      source: 'rule_engine',
    },
    {
      flag: 'ai_review_1',
      label: '高位波动与执行约束叠加，暂不适合直接执行',
      severity: 'high',
      source: 'ai_review',
    },
  ],
  scoreBreakdown: [
    {
      scoreName: 'trend_score',
      scoreLabel: '趋势分',
      scoreValue: 32.5,
      detail: {},
    },
    {
      scoreName: 'advanced_factor_score',
      scoreLabel: '高级因子增强',
      scoreValue: 10.5,
      detail: {},
    },
    {
      scoreName: 'ai_review_penalty',
      scoreLabel: 'AI 二次复核',
      scoreValue: -6,
      detail: {},
    },
  ],
  evaluations: [
    {
      windowDays: 5,
      benchmarkCode: '000300',
      evalStatus: 'completed',
      entryDate: '2026-04-13',
      entryPrice: 1666.66,
      exitDate: '2026-04-18',
      exitPrice: 1710.88,
      benchmarkEntryPrice: 3900,
      benchmarkExitPrice: 3945,
      returnPct: 2.65,
      benchmarkReturnPct: 1.15,
      excessReturnPct: 1.5,
      maxDrawdownPct: 1.2,
      mfePct: 3.6,
      maePct: -0.8,
    },
  ],
};

const sector = {
  sectorId: '白酒',
  name: '白酒',
  market: 'cn',
  stockCount: 32,
  description: '基于股票清单行业字段动态构建的 A股行业板块：白酒',
  strengthLabel: '强势',
  rankDirection: 'top' as const,
  rankPosition: 2,
  changePct: 3.2,
  isRankedToday: true,
};

const templateStats = {
  windowDays: 10,
  benchmarkCode: '000300',
  items: [
    {
      templateId: 'trend_breakout',
      templateName: '趋势突破',
      windowDays: 10,
      totalEvaluations: 6,
      comparableEvaluations: 5,
      benchmarkUnavailableEvaluations: 1,
      winRatePct: 66.7,
      avgReturnPct: 4.2,
      avgExcessReturnPct: 2.1,
      avgMaxDrawdownPct: 3.5,
    },
  ],
};

const stratifiedStats = {
  windowDays: 10,
  benchmarkCode: '000300',
  byMarketRegime: [
    {
      bucketKey: 'trend_up',
      bucketLabel: '上行趋势',
      totalEvaluations: 5,
      comparableEvaluations: 4,
      benchmarkUnavailableEvaluations: 1,
      winRatePct: 75,
      avgReturnPct: 4.8,
      avgExcessReturnPct: 2.2,
      avgMaxDrawdownPct: 2.1,
    },
  ],
  byTemplate: [
    {
      bucketKey: 'trend_breakout',
      bucketLabel: '趋势突破',
      totalEvaluations: 6,
      comparableEvaluations: 5,
      benchmarkUnavailableEvaluations: 1,
      winRatePct: 66.7,
      avgReturnPct: 4.2,
      avgExcessReturnPct: 2.1,
      avgMaxDrawdownPct: 3.5,
    },
  ],
  byRankBucket: [
    {
      bucketKey: 'top_1_3',
      bucketLabel: 'Top 1-3',
      totalEvaluations: 3,
      comparableEvaluations: 3,
      benchmarkUnavailableEvaluations: 0,
      winRatePct: 66.7,
      avgReturnPct: 4.6,
      avgExcessReturnPct: 2.5,
      avgMaxDrawdownPct: 2.8,
    },
  ],
  bySignalBucket: [
    {
      bucketKey: 'high',
      bucketLabel: '高信号',
      totalEvaluations: 2,
      comparableEvaluations: 2,
      benchmarkUnavailableEvaluations: 0,
      winRatePct: 100,
      avgReturnPct: 5.3,
      avgExcessReturnPct: 3.1,
      avgMaxDrawdownPct: 2.0,
    },
  ],
};

const calibrationStats = {
  windowDays: 10,
  benchmarkCode: '000300',
  items: [
    {
      templateId: 'trend_breakout',
      templateName: '趋势突破',
      marketRegime: 'trend_up',
      marketRegimeLabel: '上行趋势',
      ruleVersion: 'v4_2_phase2',
      bucketKey: 'high',
      bucketLabel: '高信号桶',
      windowDays: 10,
      samples: 18,
      nominalProbabilityPct: 70,
      actualWinRatePct: 66,
      calibrationGapPct: 4,
      avgReturnPct: 4.9,
      avgExcessReturnPct: 2.4,
      avgMaxDrawdownPct: 2.2,
      calibrationStatus: 'calibrated',
      calibrationLabel: '校准通过',
      highConfidenceGate: {
        status: 'blocked',
        label: '未达高置信度门槛',
        passed: false,
        reasonLabels: ['高信号桶可比样本不足 50'],
      },
    },
  ],
};

const validationStats = {
  windowDays: 10,
  benchmarkCode: '000300',
  outOfSampleByTemplate: [
    {
      templateId: 'trend_breakout',
      templateName: '趋势突破',
      ruleVersion: 'v4_2_phase2',
      windowDays: 10,
      sampleStatus: 'ready',
      comparableSamples: 20,
      inSampleCount: 14,
      outOfSampleCount: 6,
      splitRatio: 0.7,
      analysisDateStart: '2026-01-01',
      analysisDateEnd: '2026-01-20',
      outOfSampleWinRatePct: 66.7,
      outOfSampleAvgReturnPct: 4.1,
      outOfSampleAvgExcessReturnPct: 2.0,
      outOfSampleAvgMaxDrawdownPct: 1.8,
    },
  ],
  rollingMonthlyByTemplate: [
    {
      templateId: 'trend_breakout',
      templateName: '趋势突破',
      ruleVersion: 'v4_2_phase2',
      windowDays: 10,
      rollingMonth: '2026-01',
      sampleStatus: 'ready',
      rollingCount: 12,
      rollingWinRatePct: 58.3,
      rollingAvgExcessReturnPct: 1.6,
      rollingAvgMaxDrawdownPct: 2.1,
    },
  ],
};

const riskStats = {
  windowDays: 10,
  benchmarkCode: '000300',
  items: [
    {
      templateId: 'trend_breakout',
      templateName: '趋势突破',
      ruleVersion: 'v4_2_phase2',
      windowDays: 10,
      sampleStatus: 'ready',
      sampleCount: 20,
      avgReturnPct: 4.8,
      avgExcessReturnPct: 2.2,
      avgMaxDrawdownPct: 2.1,
      avgMfePct: 5.3,
      avgMaePct: -1.4,
      profitFactor: 1.8,
      returnDrawdownRatio: 1.05,
      returnPctP25: 2.4,
      returnPctP50: 4.3,
      returnPctP75: 6.2,
      excessReturnPctP25: 0.8,
      excessReturnPctP50: 2.0,
      excessReturnPctP75: 3.4,
      maxDrawdownPctP25: 1.3,
      maxDrawdownPctP50: 2.0,
      maxDrawdownPctP75: 2.8,
      mfePctP25: 3.1,
      mfePctP50: 5.0,
      mfePctP75: 6.8,
      maePctP25: -2.2,
      maePctP50: -1.5,
      maePctP75: -0.9,
    },
  ],
};

describe('StockPickerPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    navigateMock.mockReset();
    vi.mocked(stockPickerApi.getTemplates).mockResolvedValue([template]);
    vi.mocked(stockPickerApi.getUniverses).mockResolvedValue([universe]);
    vi.mocked(stockPickerApi.getSectors).mockResolvedValue([sector]);
    vi.mocked(stockPickerApi.listTasks).mockResolvedValue([task]);
    vi.mocked(stockPickerApi.getTask).mockResolvedValue({
      ...task,
      candidates: [candidate],
    });
    vi.mocked(stockPickerApi.getTemplateStats).mockResolvedValue(templateStats);
    vi.mocked(stockPickerApi.getStratifiedStats).mockResolvedValue(stratifiedStats);
    vi.mocked(stockPickerApi.getCalibrationStats).mockResolvedValue(calibrationStats);
    vi.mocked(stockPickerApi.getValidationStats).mockResolvedValue(validationStats);
    vi.mocked(stockPickerApi.getRiskStats).mockResolvedValue(riskStats);
    vi.mocked(analysisApi.analyzeAsync).mockResolvedValue({
      taskId: 'analysis-task-1',
      status: 'pending',
    });
    vi.mocked(systemConfigApi.getConfig).mockResolvedValue({
      configVersion: 'config-1',
      maskToken: '******',
      items: [{ key: 'STOCK_LIST', value: '600519,AAPL,HK00700', rawValueExists: true, isMasked: false }],
    });
    vi.mocked(systemConfigApi.update).mockResolvedValue({
      success: true,
      configVersion: 'config-2',
      appliedCount: 1,
      skippedMaskedCount: 0,
      reloadTriggered: true,
      updatedKeys: ['STOCK_LIST'],
      warnings: [],
    });
  });

  it('renders the explicit stock universe selector and candidate actions in both list and drawer', async () => {
    render(
      <MemoryRouter>
        <StockPickerPage />
      </MemoryRouter>,
    );

    const universeSelect = await screen.findByLabelText('股票池');
    expect(universeSelect).toHaveValue('watchlist');
    expect(screen.getByText(/当前股票池预览：600519、AAPL、HK00700/)).toBeInTheDocument();
    expect(await screen.findByText('模板效果统计')).toBeInTheDocument();
    expect(screen.getByText('规则版本：v4_2_phase2')).toBeInTheDocument();
    expect(screen.getByText('高级因子增强：1')).toBeInTheDocument();
    expect(screen.getByText('AI复核：1')).toBeInTheDocument();
    expect(screen.getByText('AI软否决：1')).toBeInTheDocument();
    expect(screen.getByText('目标交易日：A股：2026-04-12')).toBeInTheDocument();
    expect(screen.getByText('可比样本 5 / 6')).toBeInTheDocument();
    expect(screen.getByText('市场环境')).toBeInTheDocument();
    expect(screen.getAllByText('上行趋势').length).toBeGreaterThan(0);
    expect(screen.getByText('基础分层统计')).toBeInTheDocument();
    expect(screen.getByText('置信度校准')).toBeInTheDocument();
    expect(screen.getByText('样本外与月滚动验证')).toBeInTheDocument();
    expect(screen.getByText('风险调整与分布指标')).toBeInTheDocument();
    expect(screen.getByText('趋势突破 · 上行趋势 · 高信号桶')).toBeInTheDocument();

    expect(await screen.findByRole('button', { name: '分析该股' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: '去问股' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: '查看详情' })).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: '查看详情' }));

    const dialog = await screen.findByRole('dialog');
    expect(within(dialog).getByRole('button', { name: '分析该股' })).toBeInTheDocument();
    expect(within(dialog).getByRole('button', { name: '去问股' })).toBeInTheDocument();
    expect(within(dialog).getByRole('button', { name: '已在自选股' })).toBeDisabled();
    expect(within(dialog).getByText('高级因子')).toBeInTheDocument();
    expect(within(dialog).getAllByText('AI 二次复核').length).toBeGreaterThan(0);
    expect(within(dialog).getByText('模板失效 / 反例挑战')).toBeInTheDocument();
    expect(within(dialog).getAllByText('AI 软否决').length).toBeGreaterThan(0);
  });

  it('submits single-stock analysis from the candidate action', async () => {
    render(
      <MemoryRouter>
        <StockPickerPage />
      </MemoryRouter>,
    );

    fireEvent.click(await screen.findByRole('button', { name: '分析该股' }));

    await waitFor(() => {
      expect(analysisApi.analyzeAsync).toHaveBeenCalledWith({
        stockCode: '600519',
        stockName: '贵州茅台',
        reportType: 'detailed',
        originalQuery: '600519',
        selectionSource: 'autocomplete',
        notify: false,
      });
    });
    expect(await screen.findByText(/600519 的单股分析任务已提交/)).toBeInTheDocument();
  });

  it('navigates to chat with stock context from the candidate action', async () => {
    render(
      <MemoryRouter>
        <StockPickerPage />
      </MemoryRouter>,
    );

    fireEvent.click(await screen.findByRole('button', { name: '去问股' }));

    expect(navigateMock).toHaveBeenCalledWith(
      '/chat?stock=600519&name=%E8%B4%B5%E5%B7%9E%E8%8C%85%E5%8F%B0',
    );
  });

  it('shows candidate post-hoc evaluations in the detail drawer', async () => {
    render(
      <MemoryRouter>
        <StockPickerPage />
      </MemoryRouter>,
    );

    fireEvent.click(await screen.findByRole('button', { name: '查看详情' }));

    expect(await screen.findByText('后验表现')).toBeInTheDocument();
    expect(screen.getByText('5日窗口')).toBeInTheDocument();
    expect(screen.getByText('收益率：2.65%')).toBeInTheDocument();
    expect(screen.getByText('超额收益：1.50%')).toBeInTheDocument();
    expect(screen.getByText('窗口内 MFE：3.60%')).toBeInTheDocument();
    expect(screen.getByText('窗口内 MAE：-0.80%')).toBeInTheDocument();
    expect(screen.getByText('解释来源：结构化解释 + AI 摘要润色')).toBeInTheDocument();
    expect(screen.getByText('执行约束与成本')).toBeInTheDocument();
    expect(screen.getByText('研究与执行置信度')).toBeInTheDocument();
    expect(screen.getByText('相对强弱：个股20日 11.80% / 基准20日 5.40% / 超额 6.40%')).toBeInTheDocument();
    expect(screen.getByText('支持要点：趋势维持强势')).toBeInTheDocument();
    expect(screen.getByText('反例挑战：高位波动放大')).toBeInTheDocument();
    expect(screen.getByText('否决原因：高位波动与执行约束叠加，暂不适合直接执行')).toBeInTheDocument();
    expect(screen.getByText('复核口径：trend_breakout / trend_up / v4_2_phase2 / high')).toBeInTheDocument();
    expect(screen.getByText('高位波动与执行约束叠加，暂不适合直接执行')).toBeInTheDocument();
    expect(screen.getAllByText('执行谨慎').length).toBeGreaterThan(0);
    expect(screen.getAllByText('滑点估计：15 bps').length).toBeGreaterThan(0);
    expect(screen.getByText('可比样本：模板 12 / 环境 5')).toBeInTheDocument();
    expect(screen.getByText('校准分桶：高信号桶')).toBeInTheDocument();
    expect(screen.getByText('高置信度门槛：未达高置信度门槛')).toBeInTheDocument();
  });

  it('shows benchmark unavailable hint when evaluation is not comparable', async () => {
    vi.mocked(stockPickerApi.getTask).mockResolvedValue({
      ...task,
      candidates: [
        {
          ...candidate,
          evaluations: [
            {
              windowDays: 10,
              benchmarkCode: '000300',
              evalStatus: 'benchmark_unavailable',
              entryDate: '2026-04-13',
              entryPrice: 1666.66,
              exitDate: '2026-04-24',
              exitPrice: 1710.88,
              benchmarkEntryPrice: null,
              benchmarkExitPrice: null,
              returnPct: 2.65,
              benchmarkReturnPct: null,
              excessReturnPct: null,
              maxDrawdownPct: 1.2,
              isComparable: false,
            },
          ],
        },
      ],
    });

    render(
      <MemoryRouter>
        <StockPickerPage />
      </MemoryRouter>,
    );

    fireEvent.click(await screen.findByRole('button', { name: '查看详情' }));

    expect(await screen.findByText('当前窗口已完成个股收益计算，但基准收益暂不可用，因此不计入可比胜率。')).toBeInTheDocument();
  });

  it('switches to sector mode and submits selected sectors with aiTopK and notify', async () => {
    vi.mocked(stockPickerApi.run).mockResolvedValue({ taskId: 'picker-task-2', status: 'queued' });

    render(
      <MemoryRouter>
        <StockPickerPage />
      </MemoryRouter>,
    );

    fireEvent.click(await screen.findByRole('button', { name: /板块模式/ }));
    fireEvent.change(await screen.findByLabelText('A股行业板块'), { target: { value: '白酒' } });
    fireEvent.click(screen.getByRole('button', { name: '添加板块' }));
    expect(await screen.findByText('强势')).toBeInTheDocument();
    expect(screen.getByText('涨幅榜 #2 · 3.20%')).toBeInTheDocument();
    fireEvent.click(screen.getByLabelText('完成后通知'));
    fireEvent.change(screen.getByLabelText('AI解释数量'), { target: { value: '4' } });
    fireEvent.click(screen.getByRole('button', { name: '开始选股' }));

    await waitFor(() => {
      expect(stockPickerApi.run).toHaveBeenCalledWith({
        templateId: 'trend_breakout',
        universeId: 'watchlist',
        mode: 'sector',
        sectorIds: ['白酒'],
        limit: 20,
        aiTopK: 4,
        forceRefresh: false,
        notify: true,
      });
    });
  });

  it('shows runtime fallback summary for running tasks', async () => {
    vi.mocked(stockPickerApi.listTasks).mockResolvedValue([
      {
        ...task,
        taskId: 'picker-task-running',
        status: 'running',
        mode: 'sector',
        modeLabel: '板块模式',
        universeId: 'sector',
        universeName: 'A股行业板块',
        sectorIds: ['白酒'],
        sectorNames: ['白酒'],
        totalStocks: 10,
        processedStocks: 1,
        candidateCount: 0,
        progressPercent: 16,
        progressMessage: '已扫描 1/10 支股票',
        summary: {
          totalStocks: 0,
          scoredCount: 0,
          insufficientCount: 0,
          errorCount: 0,
          strictMatchCount: 0,
          selectedCount: 0,
          fallbackCount: 0,
          explainedCount: 0,
          benchmarkPolicy: {
            benchmarkCode: '000300',
          },
          selectionQualityGate: {
            selectionPolicy: 'strict_match_first_then_quality_gated_fallback',
          },
          sectorCatalogSnapshot: {
            selectedSectorCount: 1,
            sectorCount: 50,
            selectedStockCount: 10,
            catalogStockCount: 800,
          },
          sectorQualitySummary: {
            selectedSectorCount: 1,
            strongCount: 1,
            neutralCount: 0,
            weakCount: 0,
            rankedCount: 1,
            topRankedCount: 1,
            bottomRankedCount: 0,
            avgRankedChangePct: 3.2,
          },
          rankedSectorBreakdown: [
            {
              name: '白酒',
              strengthLabel: '强势',
              rankDirection: 'top',
              rankPosition: 2,
              changePct: 3.2,
            },
          ],
        },
        requestPayload: {
          requestPolicyVersion: 'v4_2_phase2',
          mode: 'sector',
          benchmarkPolicy: { benchmarkCode: '000300' },
        },
      },
    ]);
    vi.mocked(stockPickerApi.getTask).mockResolvedValue({
      ...task,
      taskId: 'picker-task-running',
      status: 'running',
      mode: 'sector',
      modeLabel: '板块模式',
      universeId: 'sector',
      universeName: 'A股行业板块',
      sectorIds: ['白酒'],
      sectorNames: ['白酒'],
      totalStocks: 10,
      processedStocks: 1,
      candidateCount: 0,
      progressPercent: 16,
      progressMessage: '已扫描 1/10 支股票',
      summary: {
        totalStocks: 0,
        scoredCount: 0,
        insufficientCount: 0,
        errorCount: 0,
        strictMatchCount: 0,
        selectedCount: 0,
        fallbackCount: 0,
        explainedCount: 0,
        benchmarkPolicy: {
          benchmarkCode: '000300',
        },
        selectionQualityGate: {
          selectionPolicy: 'strict_match_first_then_quality_gated_fallback',
        },
        sectorCatalogSnapshot: {
          selectedSectorCount: 1,
          sectorCount: 50,
          selectedStockCount: 10,
          catalogStockCount: 800,
        },
        sectorQualitySummary: {
          selectedSectorCount: 1,
          strongCount: 1,
          neutralCount: 0,
          weakCount: 0,
          rankedCount: 1,
          topRankedCount: 1,
          bottomRankedCount: 0,
          avgRankedChangePct: 3.2,
        },
        rankedSectorBreakdown: [
          {
            name: '白酒',
            strengthLabel: '强势',
            rankDirection: 'top',
            rankPosition: 2,
            changePct: 3.2,
          },
        ],
      },
      requestPayload: {
        requestPolicyVersion: 'v4_2_phase2',
        mode: 'sector',
        benchmarkPolicy: { benchmarkCode: '000300' },
      },
      candidates: [],
    });

    render(
      <MemoryRouter>
        <StockPickerPage />
      </MemoryRouter>,
    );

    expect(await screen.findByText('股票池总数')).toBeInTheDocument();
    expect(screen.getAllByText('10').length).toBeGreaterThan(0);
    expect(screen.getAllByText('1').length).toBeGreaterThan(0);
    expect(screen.getAllByText('基准：沪深300 (000300)').length).toBeGreaterThan(0);
    expect(screen.getByText('选股策略：严格命中优先，其次质量门槛补位')).toBeInTheDocument();
    expect(screen.getByText('强势 1 / 中性 0 / 弱势 0')).toBeInTheDocument();
  });
});
