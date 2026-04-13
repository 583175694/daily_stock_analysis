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
    run: vi.fn(),
    listTasks: vi.fn(),
    getTask: vi.fn(),
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
  templateVersion: 'v1',
  universeId: 'watchlist',
  universeName: '当前自选股',
  limit: 20,
  aiTopK: 10,
  forceRefresh: false,
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
  },
  errorMessage: null,
  requestPayload: {},
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
  technicalSnapshot: {
    ma5: 1660.12,
    ma10: 1638.55,
    ma20: 1602.33,
    ma60: 1511.07,
    change5dPct: 4.12,
    change20dPct: 11.8,
  },
  scoreBreakdown: [
    {
      scoreName: 'trend_score',
      scoreLabel: '趋势分',
      scoreValue: 32.5,
      detail: {},
    },
  ],
};

describe('StockPickerPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    navigateMock.mockReset();
    vi.mocked(stockPickerApi.getTemplates).mockResolvedValue([template]);
    vi.mocked(stockPickerApi.getUniverses).mockResolvedValue([universe]);
    vi.mocked(stockPickerApi.listTasks).mockResolvedValue([task]);
    vi.mocked(stockPickerApi.getTask).mockResolvedValue({
      ...task,
      candidates: [candidate],
    });
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

    expect(await screen.findByRole('button', { name: '分析该股' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: '去问股' })).toBeInTheDocument();
    expect(screen.getByRole('button', { name: '查看详情' })).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: '查看详情' }));

    const dialog = await screen.findByRole('dialog');
    expect(within(dialog).getByRole('button', { name: '分析该股' })).toBeInTheDocument();
    expect(within(dialog).getByRole('button', { name: '去问股' })).toBeInTheDocument();
    expect(within(dialog).getByRole('button', { name: '已在自选股' })).toBeDisabled();
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
});
