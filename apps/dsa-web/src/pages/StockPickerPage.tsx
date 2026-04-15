import type React from "react";
import { useCallback, useEffect, useRef, useState } from "react";
import { Bot, RefreshCw, Sparkles, TrendingUp } from "lucide-react";
import { useNavigate } from "react-router-dom";
import { analysisApi, DuplicateTaskError } from "../api/analysis";
import { stockPickerApi } from "../api/stockPicker";
import { systemConfigApi } from "../api/systemConfig";
import {
  createParsedApiError,
  getParsedApiError,
  type ParsedApiError,
} from "../api/error";
import {
  ApiErrorAlert,
  Badge,
  Card,
  Drawer,
  EmptyState,
  PageHeader,
  ScrollArea,
  SectionCard,
  Select,
} from "../components/common";
import type {
  PickerCandidateEvaluationItem,
  PickerCandidateItem,
  PickerSectorItem,
  PickerTaskDetail,
  PickerTaskItem,
  PickerTaskSummary,
  PickerTemplateItem,
  PickerTemplateStatItem,
  PickerUniverseItem,
} from "../types/stockPicker";

const INPUT_CLASS =
  "input-surface input-focus-glow h-11 rounded-xl border bg-transparent px-4 text-sm transition-all focus:outline-none disabled:cursor-not-allowed disabled:opacity-60";
const PICKER_LIMIT_MIN = 1;
const PICKER_LIMIT_MAX = 30;
const PICKER_AI_TOP_K_MIN = 1;
const PICKER_AI_TOP_K_MAX = 10;

function formatNumber(value?: number | null, digits = 2): string {
  if (value == null || Number.isNaN(value)) return "--";
  return Number(value).toFixed(digits);
}

function formatPct(value?: number | null): string {
  if (value == null || Number.isNaN(value)) return "--";
  return `${Number(value).toFixed(2)}%`;
}

function formatDate(value?: string | null): string {
  if (!value) return "--";
  return value.slice(0, 10);
}

function statusBadge(status: string) {
  switch (status) {
    case "completed":
      return <Badge variant="success">已完成</Badge>;
    case "running":
      return <Badge variant="info">运行中</Badge>;
    case "queued":
      return <Badge variant="warning">排队中</Badge>;
    case "failed":
      return <Badge variant="danger">失败</Badge>;
    default:
      return <Badge variant="default">{status}</Badge>;
  }
}

function marketLabel(market: string): string {
  switch (market) {
    case "cn":
      return "A股";
    case "hk":
      return "港股";
    case "us":
      return "美股";
    default:
      return market.toUpperCase();
  }
}

function benchmarkLabel(code?: string | null): string {
  if (!code) return "--";
  if (code === "000300") return "沪深300 (000300)";
  return code;
}

function selectionReasonLabel(value: string): string {
  return value === "strict_match" ? "严格命中" : "补位候选";
}

function pickerModeLabel(mode: string): string {
  return mode === "sector" ? "板块模式" : "自选股模式";
}

function evaluationStatusLabel(status: string): string {
  switch (status) {
    case "completed":
      return "已完成";
    case "benchmark_unavailable":
      return "基准缺失";
    case "pending":
      return "待更新";
    case "invalid":
      return "数据无效";
    default:
      return status || "--";
  }
}

function evaluationStatusBadge(status: string) {
  switch (status) {
    case "completed":
      return <Badge variant="success">已完成</Badge>;
    case "benchmark_unavailable":
      return <Badge variant="warning">基准缺失</Badge>;
    case "pending":
      return <Badge variant="warning">待更新</Badge>;
    case "invalid":
      return <Badge variant="danger">数据无效</Badge>;
    default:
      return <Badge variant="default">{evaluationStatusLabel(status)}</Badge>;
  }
}

function hasComparableBenchmark(
  evaluation: PickerCandidateEvaluationItem,
): boolean {
  return (
    evaluation.excessReturnPct != null &&
    !Number.isNaN(Number(evaluation.excessReturnPct))
  );
}

function asRecord(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }
  return value as Record<string, unknown>;
}

function asString(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function asNumber(value: unknown): number | null {
  if (typeof value === "number" && !Number.isNaN(value)) return value;
  if (typeof value === "string" && value.trim() && !Number.isNaN(Number(value))) {
    return Number(value);
  }
  return null;
}

function pickerPolicyVersion(task: PickerTaskItem | PickerTaskDetail): string {
  const requestPayload = asRecord(task.requestPayload);
  return asString(requestPayload.requestPolicyVersion) ?? task.templateVersion;
}

function tradingDatePolicyLines(summary: PickerTaskSummary): string[] {
  const tradingDatePolicy = asRecord(summary.tradingDatePolicy);
  const marketTargetDates = asRecord(tradingDatePolicy.marketTargetDates);
  return Object.entries(marketTargetDates)
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([market, targetDate]) => {
      const formattedDate = formatDate(asString(targetDate));
      return `${marketLabel(market)}：${formattedDate}`;
    });
}

function resolvedTotalStocks(task: PickerTaskItem | PickerTaskDetail): number {
  if ((task.summary.totalStocks ?? 0) > 0) {
    return task.summary.totalStocks;
  }
  return task.totalStocks ?? 0;
}

function resolvedScoredCount(task: PickerTaskItem | PickerTaskDetail): number {
  if ((task.summary.scoredCount ?? 0) > 0) {
    return task.summary.scoredCount;
  }
  if ((task.processedStocks ?? 0) > 0 && task.status !== "queued") {
    return task.processedStocks;
  }
  return 0;
}

function selectionPolicyLabel(summary: PickerTaskSummary): string {
  const selectionQualityGate = asRecord(summary.selectionQualityGate);
  const selectionPolicy = asString(selectionQualityGate.selectionPolicy);
  if (selectionPolicy === "strict_match_first_then_quality_gated_fallback") {
    return "严格命中优先，其次质量门槛补位";
  }
  return selectionPolicy ?? "--";
}

function benchmarkCodeFromSummary(summary: PickerTaskSummary): string | null {
  const benchmarkPolicy = asRecord(summary.benchmarkPolicy);
  return asString(benchmarkPolicy.benchmarkCode);
}

function sectorCatalogSummary(summary: PickerTaskSummary): string | null {
  const sectorCatalogSnapshot = asRecord(summary.sectorCatalogSnapshot);
  const selectedSectorCount = asNumber(sectorCatalogSnapshot.selectedSectorCount);
  const sectorCount = asNumber(sectorCatalogSnapshot.sectorCount);
  const selectedStockCount = asNumber(sectorCatalogSnapshot.selectedStockCount);
  const catalogStockCount = asNumber(sectorCatalogSnapshot.catalogStockCount);
  if (
    selectedSectorCount == null &&
    sectorCount == null &&
    selectedStockCount == null &&
    catalogStockCount == null
  ) {
    return null;
  }
  return `选中 ${selectedSectorCount ?? 0} / ${sectorCount ?? 0} 个板块，覆盖 ${selectedStockCount ?? 0} / ${catalogStockCount ?? 0} 支股票`;
}

function insufficientReasonLines(summary: PickerTaskSummary): string[] {
  const breakdown = summary.insufficientReasonBreakdown ?? {};
  const labels = summary.insufficientReasonLabels ?? {};
  return Object.entries(breakdown)
    .filter(([, count]) => Number(count) > 0)
    .sort((left, right) => Number(right[1]) - Number(left[1]))
    .map(([reason, count]) => `${labels[reason] ?? reason}：${count}`);
}

function sectorStrengthBadge(item: PickerSectorItem) {
  const label = item.strengthLabel ?? "中性";
  if (label === "强势") {
    return <Badge variant="success">强势</Badge>;
  }
  if (label === "弱势") {
    return <Badge variant="warning">弱势</Badge>;
  }
  return <Badge variant="default">中性</Badge>;
}

function sectorRankSummary(item: PickerSectorItem): string | null {
  const parts: string[] = [];
  if (item.rankDirection && item.rankPosition != null) {
    parts.push(
      `${item.rankDirection === "top" ? "涨幅榜" : "跌幅榜"} #${item.rankPosition}`,
    );
  }
  if (item.changePct != null && !Number.isNaN(Number(item.changePct))) {
    parts.push(formatPct(item.changePct));
  }
  return parts.length ? parts.join(" · ") : null;
}

function sectorQualitySummaryLines(summary: PickerTaskSummary): string[] {
  const quality = asRecord(summary.sectorQualitySummary);
  const selectedSectorCount = asNumber(quality.selectedSectorCount) ?? 0;
  if (selectedSectorCount <= 0) {
    return [];
  }
  const lines = [
    `强势 ${asNumber(quality.strongCount) ?? 0} / 中性 ${asNumber(quality.neutralCount) ?? 0} / 弱势 ${asNumber(quality.weakCount) ?? 0}`,
    `当日上榜 ${asNumber(quality.rankedCount) ?? 0} 个，其中涨幅榜 ${asNumber(quality.topRankedCount) ?? 0} 个、跌幅榜 ${asNumber(quality.bottomRankedCount) ?? 0} 个`,
  ];
  const avgRankedChangePct = asNumber(quality.avgRankedChangePct);
  if (avgRankedChangePct != null) {
    lines.push(`上榜板块平均涨跌幅 ${formatPct(avgRankedChangePct)}`);
  }
  return lines;
}

function explanationSourceLabel(source: unknown): string {
  switch (source) {
    case "structured":
      return "结构化解释";
    case "structured_plus_ai_summary":
      return "结构化解释 + AI 摘要润色";
    default:
      return asString(source) ?? "--";
  }
}

const StockPickerPage: React.FC = () => {
  const navigate = useNavigate();

  useEffect(() => {
    document.title = "AI 选股 - DSA";
  }, []);

  const [templates, setTemplates] = useState<PickerTemplateItem[]>([]);
  const [universes, setUniverses] = useState<PickerUniverseItem[]>([]);
  const [sectors, setSectors] = useState<PickerSectorItem[]>([]);
  const [tasks, setTasks] = useState<PickerTaskItem[]>([]);
  const [selectedTemplateId, setSelectedTemplateId] = useState("");
  const [selectedUniverseId, setSelectedUniverseId] = useState("watchlist");
  const [selectedMode, setSelectedMode] = useState<"watchlist" | "sector">(
    "watchlist",
  );
  const [selectedSectorDraft, setSelectedSectorDraft] = useState("");
  const [selectedSectorIds, setSelectedSectorIds] = useState<string[]>([]);
  const [selectedTaskId, setSelectedTaskId] = useState<string | null>(null);
  const [taskDetail, setTaskDetail] = useState<PickerTaskDetail | null>(null);
  const [drawerCandidate, setDrawerCandidate] =
    useState<PickerCandidateItem | null>(null);
  const [limit, setLimit] = useState("20");
  const [aiTopK, setAiTopK] = useState("10");
  const [forceRefresh, setForceRefresh] = useState(false);
  const [notify, setNotify] = useState(false);
  const [statsWindowDays, setStatsWindowDays] = useState("10");
  const [templateStats, setTemplateStats] = useState<PickerTemplateStatItem[]>(
    [],
  );
  const [templateStatsBenchmarkCode, setTemplateStatsBenchmarkCode] =
    useState("000300");
  const [watchlistCodes, setWatchlistCodes] = useState<string[]>([]);
  const [isLoadingMeta, setIsLoadingMeta] = useState(true);
  const [isLoadingTasks, setIsLoadingTasks] = useState(true);
  const [isLoadingDetail, setIsLoadingDetail] = useState(false);
  const [isLoadingStats, setIsLoadingStats] = useState(true);
  const [isRunning, setIsRunning] = useState(false);
  const [isAddingWatchlist, setIsAddingWatchlist] = useState(false);
  const [isLaunchingAnalysisFor, setIsLaunchingAnalysisFor] = useState<
    string | null
  >(null);
  const [pageError, setPageError] = useState<ParsedApiError | null>(null);
  const [runError, setRunError] = useState<ParsedApiError | null>(null);
  const [addError, setAddError] = useState<ParsedApiError | null>(null);
  const [analysisError, setAnalysisError] = useState<ParsedApiError | null>(
    null,
  );
  const [actionMessage, setActionMessage] = useState("");
  const selectedTaskIdRef = useRef<string | null>(null);

  useEffect(() => {
    selectedTaskIdRef.current = selectedTaskId;
  }, [selectedTaskId]);

  const loadMeta = useCallback(async () => {
    setIsLoadingMeta(true);
    try {
      const [templateItems, universeItems, sectorItems] = await Promise.all([
        stockPickerApi.getTemplates(),
        stockPickerApi.getUniverses(),
        stockPickerApi.getSectors(),
      ]);
      setTemplates(templateItems);
      setUniverses(universeItems);
      setSectors(sectorItems);
      setSelectedTemplateId(
        (previous) => previous || templateItems[0]?.templateId || "",
      );
      setSelectedUniverseId((previous) => {
        if (
          previous &&
          universeItems.some((item) => item.universeId === previous)
        ) {
          return previous;
        }
        return universeItems[0]?.universeId || "";
      });
      setWatchlistCodes(
        universeItems
          .find((item) => item.universeId === "watchlist")
          ?.codes?.map((code) => code.toUpperCase()) ?? [],
      );
      setSelectedSectorDraft(
        (previous) => previous || sectorItems[0]?.sectorId || "",
      );
      setPageError(null);
    } catch (error: unknown) {
      setPageError(getParsedApiError(error));
    } finally {
      setIsLoadingMeta(false);
    }
  }, []);

  const loadStats = useCallback(async (windowDays: number) => {
    setIsLoadingStats(true);
    try {
      const response = await stockPickerApi.getTemplateStats(windowDays);
      setTemplateStats(response.items ?? []);
      setTemplateStatsBenchmarkCode(response.benchmarkCode ?? "000300");
      setPageError(null);
    } catch (error: unknown) {
      setPageError(getParsedApiError(error));
    } finally {
      setIsLoadingStats(false);
    }
  }, []);

  const loadTasks = useCallback(async (preferredTaskId?: string | null) => {
    setIsLoadingTasks(true);
    try {
      const items = await stockPickerApi.listTasks(12);
      setTasks(items);
      const availableIds = new Set(items.map((item) => item.taskId));
      const nextTaskId =
        preferredTaskId ??
        (selectedTaskIdRef.current &&
        availableIds.has(selectedTaskIdRef.current)
          ? selectedTaskIdRef.current
          : null) ??
        items[0]?.taskId ??
        null;
      setSelectedTaskId(nextTaskId);
      if (!nextTaskId) {
        setTaskDetail(null);
      }
    } catch (error: unknown) {
      setPageError(getParsedApiError(error));
    } finally {
      setIsLoadingTasks(false);
    }
  }, []);

  const loadTaskDetail = useCallback(async (taskId: string) => {
    setIsLoadingDetail(true);
    try {
      const detail = await stockPickerApi.getTask(taskId);
      setTaskDetail(detail);
      setPageError(null);
    } catch (error: unknown) {
      setPageError(getParsedApiError(error));
    } finally {
      setIsLoadingDetail(false);
    }
  }, []);

  useEffect(() => {
    void loadMeta();
    void loadTasks();
  }, [loadMeta, loadTasks]);

  useEffect(() => {
    void loadStats(Number(statsWindowDays) || 10);
  }, [loadStats, statsWindowDays]);

  useEffect(() => {
    if (!selectedTaskId) {
      setTaskDetail(null);
      return;
    }
    void loadTaskDetail(selectedTaskId);
  }, [loadTaskDetail, selectedTaskId]);

  useEffect(() => {
    const activeStatus =
      taskDetail?.status ??
      tasks.find((item) => item.taskId === selectedTaskId)?.status;
    if (
      !selectedTaskId ||
      (activeStatus !== "queued" && activeStatus !== "running")
    ) {
      return;
    }

    const timer = window.setInterval(() => {
      void loadTasks(selectedTaskId);
      void loadTaskDetail(selectedTaskId);
    }, 3000);

    return () => {
      window.clearInterval(timer);
    };
  }, [loadTaskDetail, loadTasks, selectedTaskId, taskDetail?.status, tasks]);

  async function handleRun() {
    if (!selectedTemplateId) {
      return;
    }
    if (selectedMode === "sector" && selectedSectorIds.length === 0) {
      setRunError(
        createParsedApiError({
          title: "参数错误",
          message: "板块模式至少需要选择 1 个行业板块。",
          status: 400,
          category: "http_error",
        }),
      );
      return;
    }

    setRunError(null);
    setAnalysisError(null);
    setActionMessage("");
    setIsRunning(true);
    try {
      const normalizedLimit = Math.min(
        PICKER_LIMIT_MAX,
        Math.max(PICKER_LIMIT_MIN, Number(limit) || 20),
      );
      const normalizedAiTopK = Math.min(
        normalizedLimit,
        Math.min(
          PICKER_AI_TOP_K_MAX,
          Math.max(PICKER_AI_TOP_K_MIN, Number(aiTopK) || 5),
        ),
      );
      const response = await stockPickerApi.run({
        templateId: selectedTemplateId,
        universeId: selectedUniverseId,
        mode: selectedMode,
        sectorIds: selectedMode === "sector" ? selectedSectorIds : [],
        limit: normalizedLimit,
        aiTopK: normalizedAiTopK,
        forceRefresh,
        notify,
      });
      setSelectedTaskId(response.taskId);
      await loadTasks(response.taskId);
      await loadTaskDetail(response.taskId);
      setActionMessage("选股任务已提交，结果会自动刷新。");
      await loadStats(Number(statsWindowDays) || 10);
    } catch (error: unknown) {
      setRunError(getParsedApiError(error));
    } finally {
      setIsRunning(false);
    }
  }

  function handleAddSector() {
    if (!selectedSectorDraft) {
      return;
    }
    setSelectedSectorIds((previous) => {
      if (previous.includes(selectedSectorDraft) || previous.length >= 5) {
        return previous;
      }
      return [...previous, selectedSectorDraft];
    });
  }

  function handleRemoveSector(sectorId: string) {
    setSelectedSectorIds((previous) =>
      previous.filter((item) => item !== sectorId),
    );
  }

  async function handleAddToWatchlist(candidate: PickerCandidateItem) {
    setIsAddingWatchlist(true);
    setAddError(null);
    setAnalysisError(null);
    setActionMessage("");
    try {
      const configPayload = await systemConfigApi.getConfig(false);
      const stockListItem = configPayload.items.find(
        (item) => item.key === "STOCK_LIST",
      );
      const currentCodes = (String(stockListItem?.value ?? "") || "")
        .split(",")
        .map((item) => item.trim().toUpperCase())
        .filter(Boolean);
      const nextCodes = Array.from(
        new Set([...currentCodes, candidate.code.toUpperCase()]),
      );

      await systemConfigApi.update({
        configVersion: configPayload.configVersion,
        maskToken: configPayload.maskToken,
        reloadNow: true,
        items: [{ key: "STOCK_LIST", value: nextCodes.join(",") }],
      });

      await loadMeta();
      setActionMessage(`${candidate.code} 已加入自选股。`);
    } catch (error: unknown) {
      setAddError(getParsedApiError(error));
    } finally {
      setIsAddingWatchlist(false);
    }
  }

  async function handleAnalyzeCandidate(candidate: PickerCandidateItem) {
    setIsLaunchingAnalysisFor(candidate.code);
    setAnalysisError(null);
    setActionMessage("");
    try {
      await analysisApi.analyzeAsync({
        stockCode: candidate.code,
        stockName: candidate.name ?? undefined,
        reportType: "detailed",
        originalQuery: candidate.code,
        selectionSource: "autocomplete",
        notify: false,
      });
      setActionMessage(
        `${candidate.code} 的单股分析任务已提交，可到首页查看进度与报告。`,
      );
    } catch (error: unknown) {
      if (error instanceof DuplicateTaskError) {
        setActionMessage(
          `股票 ${error.stockCode} 已有分析任务在运行，可到首页查看。`,
        );
      } else {
        setAnalysisError(getParsedApiError(error));
      }
    } finally {
      setIsLaunchingAnalysisFor(null);
    }
  }

  function handleAskCandidate(candidate: PickerCandidateItem) {
    setDrawerCandidate(null);
    navigate(
      `/chat?stock=${encodeURIComponent(candidate.code)}&name=${encodeURIComponent(candidate.name || candidate.code)}`,
    );
  }

  const selectedTask =
    taskDetail && taskDetail.taskId === selectedTaskId
      ? taskDetail
      : (tasks.find((item) => item.taskId === selectedTaskId) ?? null);
  const selectedTemplate =
    templates.find((item) => item.templateId === selectedTemplateId) ?? null;
  const selectedUniverse =
    universes.find((item) => item.universeId === selectedUniverseId) ?? null;
  const selectedUniverseCodes = selectedUniverse?.codes ?? [];
  const selectedSectorItems = sectors.filter((item) =>
    selectedSectorIds.includes(item.sectorId),
  );
  const sectorStockCount = selectedSectorItems.reduce(
    (sum, item) => sum + item.stockCount,
    0,
  );

  return (
    <div className="space-y-5">
      <PageHeader
        eyebrow="AI Stock Picker"
        title="AI 选股"
        description="V3 支持自选股模式与 A股行业板块模式，先做结构化筛选与评分，再结合后验与板块质量信息解释前排候选。"
        actions={
          <>
            <label className="flex items-center gap-2 rounded-xl border border-border/70 bg-card/70 px-3 py-2 text-sm text-secondary-text">
              <input
                type="checkbox"
                checked={forceRefresh}
                onChange={(event) => setForceRefresh(event.target.checked)}
              />
              强制刷新行情
            </label>
            <input
              type="number"
              min={PICKER_LIMIT_MIN}
              max={PICKER_LIMIT_MAX}
              value={limit}
              onChange={(event) => setLimit(event.target.value)}
              className={`${INPUT_CLASS} w-24`}
              aria-label="候选数量"
            />
            <input
              type="number"
              min={PICKER_AI_TOP_K_MIN}
              max={PICKER_AI_TOP_K_MAX}
              value={aiTopK}
              onChange={(event) => setAiTopK(event.target.value)}
              className={`${INPUT_CLASS} w-24`}
              aria-label="AI解释数量"
            />
            <label className="flex items-center gap-2 rounded-xl border border-border/70 bg-card/70 px-3 py-2 text-sm text-secondary-text">
              <input
                type="checkbox"
                checked={notify}
                onChange={(event) => setNotify(event.target.checked)}
              />
              完成后通知
            </label>
            <button
              type="button"
              className="btn-primary flex h-11 items-center gap-2"
              onClick={() => void handleRun()}
              disabled={isRunning || isLoadingMeta || !selectedTemplateId}
            >
              <Sparkles className="h-4 w-4" />
              {isRunning ? "提交中..." : "开始选股"}
            </button>
          </>
        }
      />

      {pageError ? <ApiErrorAlert error={pageError} /> : null}
      {runError ? <ApiErrorAlert error={runError} /> : null}
      {addError ? <ApiErrorAlert error={addError} /> : null}
      {analysisError ? <ApiErrorAlert error={analysisError} /> : null}
      {actionMessage ? (
        <div className="rounded-2xl border border-success/30 bg-success/10 px-4 py-3 text-sm text-success">
          {actionMessage}
        </div>
      ) : null}

      <div className="grid gap-5 xl:grid-cols-[1.2fr_0.8fr]">
        <SectionCard title="模板与运行范围" subtitle="Run Config">
          {isLoadingMeta ? (
            <div className="flex items-center gap-3 py-8 text-sm text-secondary-text">
              <RefreshCw className="h-4 w-4 animate-spin" />
              正在加载模板、股票池与板块...
            </div>
          ) : (
            <div className="space-y-5">
              <div className="grid gap-4 md:grid-cols-3">
                <button
                  type="button"
                  onClick={() => setSelectedMode("watchlist")}
                  aria-label="自选股模式"
                  aria-pressed={selectedMode === "watchlist"}
                  className={`rounded-2xl border p-4 text-left transition-all ${
                    selectedMode === "watchlist"
                      ? "border-cyan/50 bg-cyan/10 shadow-[0_0_0_1px_rgba(34,211,238,0.15)]"
                      : "border-border/70 bg-card/60 hover:border-border hover:bg-card"
                  }`}
                >
                  <div className="text-sm font-semibold text-foreground">
                    自选股模式
                  </div>
                  <div className="mt-1 text-xs text-secondary-text">
                    沿用当前 STOCK_LIST 直接选股。
                  </div>
                </button>
                <button
                  type="button"
                  onClick={() => setSelectedMode("sector")}
                  aria-label="板块模式"
                  aria-pressed={selectedMode === "sector"}
                  className={`rounded-2xl border p-4 text-left transition-all ${
                    selectedMode === "sector"
                      ? "border-cyan/50 bg-cyan/10 shadow-[0_0_0_1px_rgba(34,211,238,0.15)]"
                      : "border-border/70 bg-card/60 hover:border-border hover:bg-card"
                  }`}
                >
                  <div className="text-sm font-semibold text-foreground">
                    板块模式
                  </div>
                  <div className="mt-1 text-xs text-secondary-text">
                    先选 A股行业板块，再在板块内选股。
                  </div>
                </button>
                <div className="rounded-2xl border border-border/70 bg-card/60 p-4">
                  <div className="text-sm font-medium text-foreground">
                    本次参数
                  </div>
                  <div className="mt-2 space-y-1 text-xs text-secondary-text">
                    <div>候选数量：{limit || "20"}</div>
                    <div>AI 解释：{aiTopK || "5"}</div>
                    <div>通知：{notify ? "开启" : "关闭"}</div>
                  </div>
                </div>
              </div>

              <div>
                <div className="mb-3 flex items-center gap-2">
                  <TrendingUp className="h-4 w-4 text-cyan" />
                  <span className="text-sm font-medium text-foreground">
                    内置模板
                  </span>
                </div>
                <div className="grid gap-3 md:grid-cols-3">
                  {templates.map((template) => {
                    const selected = selectedTemplateId === template.templateId;
                    return (
                      <button
                        key={template.templateId}
                        type="button"
                        onClick={() =>
                          setSelectedTemplateId(template.templateId)
                        }
                        className={`rounded-2xl border p-4 text-left transition-all ${
                          selected
                            ? "border-cyan/50 bg-cyan/10 shadow-[0_0_0_1px_rgba(34,211,238,0.15)]"
                            : "border-border/70 bg-card/60 hover:border-border hover:bg-card"
                        }`}
                      >
                        <div className="flex items-start justify-between gap-3">
                          <div>
                            <h3 className="text-sm font-semibold text-foreground">
                              {template.name}
                            </h3>
                            <p className="mt-1 text-xs text-secondary-text">
                              {template.description}
                            </p>
                          </div>
                          {selected ? <Badge variant="info">已选</Badge> : null}
                        </div>
                        <div className="mt-3 flex flex-wrap gap-2">
                          <Badge variant="default">{template.focus}</Badge>
                          <Badge variant="default">{template.style}</Badge>
                        </div>
                      </button>
                    );
                  })}
                </div>
              </div>

              {selectedMode === "watchlist" ? (
                <div className="grid gap-4 md:grid-cols-2">
                  <Select
                    label="股票池"
                    value={selectedUniverseId}
                    onChange={setSelectedUniverseId}
                    options={universes.map((item) => ({
                      value: item.universeId,
                      label: `${item.name} (${item.stockCount})`,
                    }))}
                    disabled={universes.length <= 1}
                    className="max-w-md"
                  />
                  <div className="rounded-2xl border border-border/70 bg-card/60 p-4">
                    <div className="text-sm font-medium text-foreground">
                      当前范围说明
                    </div>
                    <div className="mt-2 text-sm text-secondary-text">
                      自选股模式基于 `STOCK_LIST`
                      扫描当前自选股池，不做全市场扫描。
                    </div>
                  </div>
                </div>
              ) : (
                <div className="grid gap-4 md:grid-cols-[minmax(0,1fr)_auto]">
                  <Select
                    label="A股行业板块"
                    value={selectedSectorDraft}
                    onChange={setSelectedSectorDraft}
                    options={sectors.map((item) => ({
                      value: item.sectorId,
                      label: `${item.name} (${item.stockCount})`,
                    }))}
                    placeholder={
                      sectors.length ? "请选择行业板块" : "暂无可用板块"
                    }
                    disabled={
                      sectors.length === 0 || selectedSectorIds.length >= 5
                    }
                  />
                  <button
                    type="button"
                    className="btn-secondary mt-7 h-11"
                    onClick={handleAddSector}
                    disabled={
                      !selectedSectorDraft ||
                      selectedSectorIds.includes(selectedSectorDraft) ||
                      selectedSectorIds.length >= 5
                    }
                  >
                    添加板块
                  </button>
                </div>
              )}

              <div className="rounded-2xl border border-border/70 bg-card/60 p-4">
                <div className="flex items-start justify-between gap-3">
                  <div>
                    <div className="label-uppercase">Universe</div>
                    <h3 className="mt-1 text-base font-semibold text-foreground">
                      {selectedMode === "sector"
                        ? "A股行业板块"
                        : (selectedUniverse?.name ?? "当前自选股池")}
                    </h3>
                    <p className="mt-1 text-sm text-secondary-text">
                      {selectedMode === "sector"
                        ? selectedSectorItems[0]?.description ??
                          "板块模式仅支持手动选择 A股行业板块，再在板块内做结构化筛选。"
                        : (selectedUniverse?.description ??
                          "基于 STOCK_LIST 扫描当前自选股池。")}
                    </p>
                  </div>
                  <Badge variant="info">
                    {selectedMode === "sector"
                      ? sectorStockCount
                      : (selectedUniverse?.stockCount ?? 0)}{" "}
                    支
                  </Badge>
                </div>
                {selectedMode === "sector" ? (
                    <div className="mt-3 space-y-3">
                      <p className="text-xs text-muted-text">
                        已选板块：
                      {selectedSectorItems
                        .map((item) => item.name)
                        .join("、") || "--"}
                      </p>
                      <div className="space-y-2">
                        {selectedSectorItems.map((item) => (
                          <div
                            key={`meta-${item.sectorId}`}
                            className="rounded-xl border border-border/60 bg-background/30 px-3 py-2 text-xs text-secondary-text"
                          >
                            <div className="font-medium text-foreground">
                              <span>{item.name}</span>
                              <span className="ml-2 inline-flex align-middle">
                                {sectorStrengthBadge(item)}
                              </span>
                            </div>
                            <div className="mt-1">
                              市场：{marketLabel(item.market)} · 覆盖 {item.stockCount} 支
                            </div>
                            {sectorRankSummary(item) ? (
                              <div className="mt-1 text-muted-text">
                                {sectorRankSummary(item)}
                              </div>
                            ) : null}
                            <div className="mt-1 text-muted-text">
                              {item.description || "暂无板块说明"}
                            </div>
                          </div>
                        ))}
                      </div>
                      <div className="flex flex-wrap gap-2">
                        {selectedSectorItems.map((item) => (
                          <button
                          key={item.sectorId}
                          type="button"
                          className="rounded-full border border-border/70 bg-background/40 px-3 py-1 text-xs text-secondary-text transition-colors hover:border-border hover:text-foreground"
                          onClick={() => handleRemoveSector(item.sectorId)}
                        >
                          {item.name} ×
                        </button>
                      ))}
                    </div>
                  </div>
                ) : (
                  <p className="mt-3 text-xs text-muted-text">
                    当前股票池预览：
                    {selectedUniverseCodes.slice(0, 8).join("、") || "--"}
                    {selectedUniverseCodes.length > 8 ? " ..." : ""}
                  </p>
                )}
                {selectedTemplate ? (
                  <div className="mt-4 rounded-xl border border-border/60 bg-background/40 p-3">
                    <div className="mb-2 text-xs font-medium text-secondary-text">
                      评分重点
                    </div>
                    <div className="space-y-1.5 text-xs text-muted-text">
                      {selectedTemplate.scoringNotes.map((item) => (
                        <div key={item}>{item}</div>
                      ))}
                    </div>
                  </div>
                ) : null}
              </div>
            </div>
          )}
        </SectionCard>

        <SectionCard
          title="最近任务"
          subtitle="Async Tasks"
          className="flex min-h-0 flex-col"
          actions={
            <button
              type="button"
              onClick={() => void loadTasks(selectedTaskId)}
              className="btn-secondary flex h-10 items-center gap-2"
            >
              <RefreshCw className="h-4 w-4" />
              刷新
            </button>
          }
        >
          {isLoadingTasks ? (
            <div className="flex items-center gap-3 py-8 text-sm text-secondary-text">
              <RefreshCw className="h-4 w-4 animate-spin" />
              正在加载任务...
            </div>
          ) : tasks.length === 0 ? (
            <EmptyState
              title="还没有选股任务"
              description="先选择模板并发起一次扫描，任务结果会保存在这里。"
            />
          ) : (
            <ScrollArea
              className="max-h-[min(44rem,calc(100vh-14rem))]"
              viewportClassName="pr-1"
            >
              <div className="space-y-3">
                {tasks.map((task) => {
                  const active = task.taskId === selectedTaskId;
                  return (
                    <button
                      key={task.taskId}
                      type="button"
                      onClick={() => setSelectedTaskId(task.taskId)}
                      className={`w-full rounded-2xl border p-4 text-left transition-all ${
                        active
                          ? "border-cyan/50 bg-cyan/10 shadow-[0_0_0_1px_rgba(34,211,238,0.15)]"
                          : "border-border/70 bg-card/60 hover:border-border hover:bg-card"
                      }`}
                    >
                      <div className="flex items-start justify-between gap-3">
                        <div>
                          <div className="text-sm font-semibold text-foreground">
                            {task.templateName}
                          </div>
                          <div className="mt-1 text-xs text-secondary-text">
                            {formatDate(task.createdAt)} ·{" "}
                            {pickerModeLabel(task.mode)} · {task.candidateCount}{" "}
                            个结果
                          </div>
                        </div>
                        {statusBadge(task.status)}
                      </div>
                      {task.sectorNames.length > 0 ? (
                        <div className="mt-2 text-xs text-muted-text">
                          {task.sectorNames.slice(0, 3).join("、")}
                          {task.sectorNames.length > 3 ? " ..." : ""}
                        </div>
                      ) : null}
                      <div className="mt-3 h-2 rounded-full bg-background/60">
                        <div
                          className="h-full rounded-full bg-primary-gradient transition-all"
                          style={{
                            width: `${Math.max(8, task.progressPercent)}%`,
                          }}
                        />
                      </div>
                      <div className="mt-2 text-xs text-muted-text">
                        {task.progressMessage || "等待执行"}
                      </div>
                    </button>
                  );
                })}
              </div>
            </ScrollArea>
          )}
        </SectionCard>
      </div>

      <div className="grid gap-5 xl:grid-cols-[0.95fr_1.05fr]">
        <SectionCard title="任务概览" subtitle="Task Summary">
          {!selectedTask ? (
            <EmptyState
              title="没有选中的任务"
              description="从右侧任务列表选择一条任务，即可查看执行进度和候选结果。"
            />
          ) : (
            <div className="space-y-4">
              <div className="flex flex-wrap items-center gap-2">
                {statusBadge(selectedTask.status)}
                <Badge variant="default">{selectedTask.templateName}</Badge>
                <Badge variant="default">
                  {selectedTask.modeLabel ?? pickerModeLabel(selectedTask.mode)}
                </Badge>
                <Badge variant="default">{selectedTask.universeName}</Badge>
                <Badge variant="default">
                  规则 {pickerPolicyVersion(selectedTask)}
                </Badge>
                {selectedTask.forceRefresh ? (
                  <Badge variant="warning">强制刷新</Badge>
                ) : null}
                {selectedTask.notify ? (
                  <Badge variant="info">完成后通知</Badge>
                ) : null}
              </div>

              {selectedTask.errorMessage ? (
                <div className="rounded-2xl border border-danger/30 bg-danger/10 px-4 py-3 text-sm text-danger">
                  {selectedTask.errorMessage}
                </div>
              ) : null}

              <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
                <Card variant="gradient" padding="md">
                  <div className="label-uppercase">Total</div>
                  <div className="mt-2 text-2xl font-semibold text-foreground">
                    {resolvedTotalStocks(selectedTask)}
                  </div>
                  <div className="mt-1 text-xs text-secondary-text">
                    股票池总数
                  </div>
                </Card>
                <Card variant="gradient" padding="md">
                  <div className="label-uppercase">Scored</div>
                  <div className="mt-2 text-2xl font-semibold text-foreground">
                    {resolvedScoredCount(selectedTask)}
                  </div>
                  <div className="mt-1 text-xs text-secondary-text">
                    成功打分
                  </div>
                </Card>
                <Card variant="gradient" padding="md">
                  <div className="label-uppercase">Strict</div>
                  <div className="mt-2 text-2xl font-semibold text-foreground">
                    {selectedTask.summary.strictMatchCount}
                  </div>
                  <div className="mt-1 text-xs text-secondary-text">
                    严格命中
                  </div>
                </Card>
                <Card variant="gradient" padding="md">
                  <div className="label-uppercase">AI Explained</div>
                  <div className="mt-2 text-2xl font-semibold text-foreground">
                    {selectedTask.summary.explainedCount}
                  </div>
                  <div className="mt-1 text-xs text-secondary-text">
                    AI 解释数量
                  </div>
                </Card>
              </div>

              <div className="rounded-2xl border border-border/70 bg-card/60 p-4">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <div className="text-sm font-medium text-foreground">
                      执行进度
                    </div>
                    <div className="mt-1 text-xs text-secondary-text">
                      {selectedTask.progressMessage || "等待执行"}
                    </div>
                  </div>
                  <div className="text-sm font-semibold text-foreground">
                    {selectedTask.progressPercent}%
                  </div>
                </div>
                <div className="mt-3 h-2 rounded-full bg-background/60">
                  <div
                    className="h-full rounded-full bg-primary-gradient transition-all"
                    style={{
                      width: `${Math.max(8, selectedTask.progressPercent)}%`,
                    }}
                  />
                </div>
              </div>

              <div className="grid gap-3 md:grid-cols-2">
                <div className="rounded-2xl border border-border/70 bg-card/60 p-4">
                  <div className="text-sm font-medium text-foreground">
                    任务时间
                  </div>
                  <div className="mt-3 space-y-2 text-xs text-secondary-text">
                    <div>创建：{formatDate(selectedTask.createdAt)}</div>
                    <div>开始：{formatDate(selectedTask.startedAt)}</div>
                    <div>完成：{formatDate(selectedTask.finishedAt)}</div>
                  </div>
                </div>
                <div className="rounded-2xl border border-border/70 bg-card/60 p-4">
                  <div className="text-sm font-medium text-foreground">
                    运行结果
                  </div>
                  <div className="mt-3 space-y-2 text-xs text-secondary-text">
                    <div>
                      模式：
                      {selectedTask.modeLabel ??
                        pickerModeLabel(selectedTask.mode)}
                    </div>
                    <div>规则版本：{pickerPolicyVersion(selectedTask)}</div>
                    <div>AI解释：Top {selectedTask.aiTopK}</div>
                    <div>补位候选：{selectedTask.summary.fallbackCount}</div>
                    <div>
                      数据不足：{selectedTask.summary.insufficientCount}
                    </div>
                    <div>扫描异常：{selectedTask.summary.errorCount}</div>
                  </div>
                </div>
              </div>
              <div className="grid gap-3 md:grid-cols-2">
                <div className="rounded-2xl border border-border/70 bg-card/60 p-4">
                  <div className="text-sm font-medium text-foreground">
                    口径与基准
                  </div>
                  <div className="mt-3 space-y-2 text-xs text-secondary-text">
                    {tradingDatePolicyLines(selectedTask.summary).map((item) => (
                      <div key={item}>目标交易日：{item}</div>
                    ))}
                    <div>
                      基准：{benchmarkLabel(benchmarkCodeFromSummary(selectedTask.summary))}
                    </div>
                    <div>选股策略：{selectionPolicyLabel(selectedTask.summary)}</div>
                    {sectorCatalogSummary(selectedTask.summary) ? (
                      <div>{sectorCatalogSummary(selectedTask.summary)}</div>
                    ) : null}
                  </div>
                </div>
                <div className="rounded-2xl border border-border/70 bg-card/60 p-4">
                  <div className="text-sm font-medium text-foreground">
                    数据不足明细
                  </div>
                  <div className="mt-3 space-y-2 text-xs text-secondary-text">
                    {insufficientReasonLines(selectedTask.summary).length > 0 ? (
                      insufficientReasonLines(selectedTask.summary).map((item) => (
                        <div key={item}>{item}</div>
                      ))
                    ) : (
                      <div>当前任务没有记录数据不足细分原因。</div>
                    )}
                  </div>
                </div>
              </div>
              {selectedTask.sectorNames.length > 0 ? (
                <div className="rounded-2xl border border-border/70 bg-card/60 p-4">
                  <div className="text-sm font-medium text-foreground">
                    板块范围
                  </div>
                  {sectorQualitySummaryLines(selectedTask.summary).length > 0 ? (
                    <div className="mt-3 space-y-2 text-xs text-secondary-text">
                      {sectorQualitySummaryLines(selectedTask.summary).map((item) => (
                        <div key={item}>{item}</div>
                      ))}
                    </div>
                  ) : null}
                  <div className="mt-3 flex flex-wrap gap-2">
                    {selectedTask.sectorNames.map((item) => (
                      <Badge key={item} variant="default">
                        {item}
                      </Badge>
                    ))}
                  </div>
                  {(selectedTask.summary.rankedSectorBreakdown ?? []).length > 0 ? (
                    <div className="mt-3 flex flex-wrap gap-2">
                      {(selectedTask.summary.rankedSectorBreakdown ?? []).map((item, index) => {
                        const row = asRecord(item);
                        const name = asString(row.name) ?? `板块 ${index + 1}`;
                        const strength = asString(row.strengthLabel) ?? "中性";
                        const rankDirection = asString(row.rankDirection);
                        const rankPosition = asNumber(row.rankPosition);
                        const changePct = asNumber(row.changePct);
                        const detail = [
                          strength,
                          rankDirection && rankPosition != null
                            ? `${rankDirection === "top" ? "涨幅榜" : "跌幅榜"} #${rankPosition}`
                            : null,
                          changePct != null ? formatPct(changePct) : null,
                        ]
                          .filter(Boolean)
                          .join(" · ");
                        return (
                          <Badge key={`${name}-${index}`} variant="info">
                            {name} {detail}
                          </Badge>
                        );
                      })}
                    </div>
                  ) : null}
                </div>
              ) : null}
            </div>
          )}
        </SectionCard>

        <SectionCard title="候选列表" subtitle="Candidates">
          {!selectedTask ? (
            <EmptyState
              title="暂无候选"
              description="先选择一条已完成任务，候选明细会展示在这里。"
            />
          ) : isLoadingDetail && selectedTask.status !== "completed" ? (
            <div className="flex items-center gap-3 py-8 text-sm text-secondary-text">
              <RefreshCw className="h-4 w-4 animate-spin" />
              正在同步任务详情...
            </div>
          ) : selectedTask.status !== "completed" ? (
            <EmptyState
              title="任务尚未完成"
              description="当前任务还在执行中，页面会自动刷新进度。"
            />
          ) : taskDetail?.candidates?.length ? (
            <div className="max-h-[min(44rem,calc(100vh-14rem))] overflow-y-auto pr-1 custom-scrollbar">
              <div className="space-y-3">
                {taskDetail.candidates.map((candidate) => (
                  <div
                    key={candidate.code}
                    className="rounded-2xl border border-border/70 bg-card/60 p-4 transition-all hover:border-border hover:bg-card"
                  >
                    <button
                      type="button"
                      onClick={() => setDrawerCandidate(candidate)}
                      className="w-full text-left"
                    >
                      <div className="flex flex-wrap items-start justify-between gap-3">
                        <div>
                          <div className="flex flex-wrap items-center gap-2">
                            <div className="text-sm font-semibold text-foreground">
                              #{candidate.rank} {candidate.name || candidate.code}
                            </div>
                            <Badge variant="default">{candidate.code}</Badge>
                            <Badge variant="default">
                              {marketLabel(candidate.market)}
                            </Badge>
                            <Badge
                              variant={
                                candidate.selectionReason === "strict_match"
                                  ? "success"
                                  : "warning"
                              }
                            >
                              {selectionReasonLabel(candidate.selectionReason)}
                            </Badge>
                          </div>
                          <p className="mt-2 text-sm text-secondary-text">
                            {candidate.explanationSummary || "暂无解释摘要"}
                          </p>
                        </div>
                        <div className="text-right">
                          <div className="text-2xl font-semibold text-foreground">
                            {formatNumber(candidate.totalScore, 1)}
                          </div>
                          <div className="mt-1 text-xs text-secondary-text">
                            综合得分
                          </div>
                        </div>
                      </div>
                      <div className="mt-4 grid gap-3 text-xs text-secondary-text md:grid-cols-4">
                        <div>
                          收盘价：{formatNumber(candidate.latestClose, 2)}
                        </div>
                        <div>当日涨跌：{formatPct(candidate.changePct)}</div>
                        <div>
                          量能比：{formatNumber(candidate.volumeRatio, 2)}
                        </div>
                        <div>
                          距 20 日高点：{formatPct(candidate.distanceToHighPct)}
                        </div>
                      </div>
                    </button>
                    <div className="mt-4 flex flex-wrap gap-2">
                      <button
                        type="button"
                        className="btn-secondary"
                        onClick={() => void handleAnalyzeCandidate(candidate)}
                        disabled={isLaunchingAnalysisFor === candidate.code}
                      >
                        {isLaunchingAnalysisFor === candidate.code
                          ? "提交中..."
                          : "分析该股"}
                      </button>
                      <button
                        type="button"
                        className="btn-secondary"
                        onClick={() => handleAskCandidate(candidate)}
                      >
                        去问股
                      </button>
                      <button
                        type="button"
                        className="btn-secondary"
                        onClick={() => setDrawerCandidate(candidate)}
                      >
                        查看详情
                      </button>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          ) : (
            <EmptyState
              title="本次没有生成候选"
              description="该任务已完成，但没有落库候选结果。可以尝试换模板或开启强制刷新。"
            />
          )}
        </SectionCard>
      </div>

      <SectionCard
        title="模板效果统计"
        subtitle="A-share Validation"
        actions={
          <Select
            label="统计窗口"
            value={statsWindowDays}
            onChange={(value) => setStatsWindowDays(value)}
            options={[
              { value: "5", label: "5日" },
              { value: "10", label: "10日" },
              { value: "20", label: "20日" },
            ]}
            className="min-w-28"
          />
        }
      >
        {isLoadingStats ? (
          <div className="flex items-center gap-3 py-8 text-sm text-secondary-text">
            <RefreshCw className="h-4 w-4 animate-spin" />
            正在加载模板统计...
          </div>
        ) : templateStats.length === 0 ? (
          <EmptyState
            title="暂无统计数据"
            description="完成更多 A股选股任务后，这里会展示各模板的胜率、收益率与回撤表现。"
          />
        ) : (
          <div className="space-y-3">
            {templateStats.map((item) => (
              <div
                key={`${item.templateId}-${item.windowDays}`}
                className="grid gap-3 rounded-2xl border border-border/70 bg-card/60 p-4 md:grid-cols-6"
              >
                <div>
                  <div className="text-sm font-semibold text-foreground">
                    {item.templateName}
                  </div>
                  <div className="mt-1 text-xs text-secondary-text">
                    {item.totalEvaluations} 次评估
                  </div>
                </div>
                <div className="text-sm text-secondary-text">
                  <div className="label-uppercase">胜率</div>
                  <div className="mt-1 font-semibold text-foreground">
                    {formatPct(item.winRatePct)}
                  </div>
                  <div className="mt-1 text-xs text-muted-text">
                    可比样本 {item.comparableEvaluations ?? 0} /{" "}
                    {item.totalEvaluations}
                  </div>
                </div>
                <div className="text-sm text-secondary-text">
                  <div className="label-uppercase">平均收益</div>
                  <div className="mt-1 font-semibold text-foreground">
                    {formatPct(item.avgReturnPct)}
                  </div>
                </div>
                <div className="text-sm text-secondary-text">
                  <div className="label-uppercase">平均超额</div>
                  <div className="mt-1 font-semibold text-foreground">
                    {formatPct(item.avgExcessReturnPct)}
                  </div>
                </div>
                <div className="text-sm text-secondary-text">
                  <div className="label-uppercase">平均回撤</div>
                  <div className="mt-1 font-semibold text-foreground">
                    {formatPct(item.avgMaxDrawdownPct)}
                  </div>
                </div>
                <div className="text-xs text-muted-text md:text-right">
                  基准：{benchmarkLabel(templateStatsBenchmarkCode)}
                  {item.benchmarkUnavailableEvaluations ? (
                    <div className="mt-1">
                      基准缺失 {item.benchmarkUnavailableEvaluations} 条
                    </div>
                  ) : null}
                </div>
              </div>
            ))}
          </div>
        )}
      </SectionCard>

      <Drawer
        isOpen={Boolean(drawerCandidate)}
        onClose={() => setDrawerCandidate(null)}
        title={
          drawerCandidate
            ? `${drawerCandidate.name || drawerCandidate.code} · ${drawerCandidate.code}`
            : ""
        }
        width="max-w-3xl"
      >
        {drawerCandidate ? (
          <div className="space-y-5">
            <div className="flex flex-wrap items-center gap-2">
              <Badge variant="info">#{drawerCandidate.rank}</Badge>
              <Badge variant="default">
                {marketLabel(drawerCandidate.market)}
              </Badge>
              <Badge
                variant={
                  drawerCandidate.selectionReason === "strict_match"
                    ? "success"
                    : "warning"
                }
              >
                {selectionReasonLabel(drawerCandidate.selectionReason)}
              </Badge>
              <Badge variant="default">
                得分 {formatNumber(drawerCandidate.totalScore, 1)}
              </Badge>
            </div>

            <div className="rounded-2xl border border-border/70 bg-card/60 p-4">
              <div className="mb-2 flex items-center gap-2 text-sm font-medium text-foreground">
                <Bot className="h-4 w-4 text-cyan" />
                AI 解读
              </div>
              <p className="text-sm leading-6 text-secondary-text">
                {drawerCandidate.explanationSummary || "暂无 AI 解读"}
              </p>
            </div>

            <div className="grid gap-4 md:grid-cols-2">
              <div className="rounded-2xl border border-border/70 bg-card/60 p-4">
                <div className="text-sm font-medium text-foreground">
                  核心理由
                </div>
                <div className="mt-3 space-y-2 text-sm text-secondary-text">
                  {drawerCandidate.explanationRationale.map((item) => (
                    <div key={item}>{item}</div>
                  ))}
                </div>
              </div>
              <div className="rounded-2xl border border-border/70 bg-card/60 p-4">
                <div className="text-sm font-medium text-foreground">
                  风险与观察点
                </div>
                <div className="mt-3 space-y-2 text-sm text-secondary-text">
                  {drawerCandidate.explanationRisks.map((item) => (
                    <div key={`risk-${item}`}>风险：{item}</div>
                  ))}
                  {drawerCandidate.explanationWatchpoints.map((item) => (
                    <div key={`watch-${item}`}>观察：{item}</div>
                  ))}
                </div>
              </div>
            </div>

            <div className="grid gap-4 md:grid-cols-2">
              <div className="rounded-2xl border border-border/70 bg-card/60 p-4">
                <div className="text-sm font-medium text-foreground">
                  技术快照
                </div>
                {drawerCandidate.market === "cn" ? (
                  <div className="mt-2 text-xs text-muted-text">
                    A股盘中运行默认使用上一已完成交易日的日线快照；收盘后会更新为当日数据。
                  </div>
                ) : null}
                <div className="mt-3 grid grid-cols-2 gap-3 text-sm text-secondary-text">
                  <div>
                    收盘价：{formatNumber(drawerCandidate.latestClose, 2)}
                  </div>
                  <div>日期：{formatDate(drawerCandidate.latestDate)}</div>
                  <div>
                    MA5：
                    {formatNumber(
                      Number(drawerCandidate.technicalSnapshot.ma5 ?? NaN),
                      2,
                    )}
                  </div>
                  <div>
                    MA10：
                    {formatNumber(
                      Number(drawerCandidate.technicalSnapshot.ma10 ?? NaN),
                      2,
                    )}
                  </div>
                  <div>
                    MA20：
                    {formatNumber(
                      Number(drawerCandidate.technicalSnapshot.ma20 ?? NaN),
                      2,
                    )}
                  </div>
                  <div>
                    MA60：
                    {formatNumber(
                      Number(drawerCandidate.technicalSnapshot.ma60 ?? NaN),
                      2,
                    )}
                  </div>
                  <div>
                    5日变化：
                    {formatPct(
                      Number(
                        drawerCandidate.technicalSnapshot.change5dPct ?? NaN,
                      ),
                    )}
                  </div>
                  <div>
                    20日变化：
                    {formatPct(
                      Number(
                        drawerCandidate.technicalSnapshot.change20dPct ?? NaN,
                      ),
                    )}
                  </div>
                  <div>
                    目标交易日：
                    {formatDate(
                      asString(drawerCandidate.technicalSnapshot.targetTradingDate),
                    )}
                  </div>
                  <div>
                    解释来源：
                    {explanationSourceLabel(
                      drawerCandidate.technicalSnapshot.explanationSource,
                    )}
                  </div>
                </div>
              </div>

              <div className="rounded-2xl border border-border/70 bg-card/60 p-4">
                <div className="text-sm font-medium text-foreground">
                  评分拆解
                </div>
                <div className="mt-3 space-y-2">
                  {drawerCandidate.scoreBreakdown.map((item) => (
                    <div
                      key={item.scoreName}
                      className="flex items-center justify-between gap-3 text-sm"
                    >
                      <span className="text-secondary-text">
                        {item.scoreLabel}
                      </span>
                      <span className="font-mono text-foreground">
                        {formatNumber(item.scoreValue, 2)}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            </div>

            <div className="rounded-2xl border border-border/70 bg-card/60 p-4">
              <div>
                <div className="text-sm font-medium text-foreground">
                  后验表现
                </div>
                <div className="mt-1 text-xs text-secondary-text">
                  固定观察窗口：5 / 10 / 20 日；超额收益基准为 {benchmarkLabel(drawerCandidate.evaluations[0]?.benchmarkCode)}。
                </div>
              </div>
              {drawerCandidate.evaluations.length > 0 ? (
                <div className="mt-4 grid gap-3 md:grid-cols-3">
                  {drawerCandidate.evaluations.map((item) => (
                    <div
                      key={`${drawerCandidate.code}-${item.windowDays}`}
                      className="rounded-xl border border-border/60 bg-background/30 p-3"
                    >
                      <div className="flex items-center justify-between gap-2">
                        <div className="text-sm font-semibold text-foreground">
                          {item.windowDays}日窗口
                        </div>
                        {evaluationStatusBadge(item.evalStatus)}
                      </div>
                      <div className="mt-3 space-y-1.5 text-xs text-secondary-text">
                        <div>
                          入场：{formatDate(item.entryDate)} /{" "}
                          {formatNumber(item.entryPrice, 2)}
                        </div>
                        <div>
                          出场：{formatDate(item.exitDate)} /{" "}
                          {formatNumber(item.exitPrice, 2)}
                        </div>
                        <div>收益率：{formatPct(item.returnPct)}</div>
                        <div>
                          基准收益：{formatPct(item.benchmarkReturnPct)}
                        </div>
                        <div>超额收益：{formatPct(item.excessReturnPct)}</div>
                        <div>最大回撤：{formatPct(item.maxDrawdownPct)}</div>
                      </div>
                      {item.evalStatus === "benchmark_unavailable" ? (
                        <div className="mt-3 text-xs text-muted-text">
                          当前窗口已完成个股收益计算，但基准收益暂不可用，因此不计入可比胜率。
                        </div>
                      ) : !hasComparableBenchmark(item) &&
                        item.evalStatus === "completed" ? (
                        <div className="mt-3 text-xs text-muted-text">
                          当前窗口已完成个股收益计算，但基准收益暂不可用。
                        </div>
                      ) : null}
                    </div>
                  ))}
                </div>
              ) : (
                <div className="mt-4 text-sm text-secondary-text">
                  暂无后验表现。若任务完成时间较近，系统会在后续窗口到齐后自动补算。
                </div>
              )}
            </div>

            <div className="rounded-2xl border border-border/70 bg-card/60 p-4">
              <div className="flex flex-wrap items-center justify-between gap-3">
                <div>
                  <div className="text-sm font-medium text-foreground">
                    板块与资讯
                  </div>
                  <div className="mt-1 text-xs text-secondary-text">
                    {drawerCandidate.boardNames.length > 0
                      ? drawerCandidate.boardNames.join("、")
                      : "暂无板块信息"}
                  </div>
                </div>
                <button
                  type="button"
                  className="btn-secondary"
                  disabled={
                    isAddingWatchlist ||
                    watchlistCodes.includes(drawerCandidate.code.toUpperCase())
                  }
                  onClick={() => void handleAddToWatchlist(drawerCandidate)}
                >
                  {watchlistCodes.includes(drawerCandidate.code.toUpperCase())
                    ? "已在自选股"
                    : "加入自选股"}
                </button>
              </div>

              <div className="mt-4 space-y-3">
                {drawerCandidate.newsBriefs.length > 0 ? (
                  drawerCandidate.newsBriefs.map((item) => (
                    <a
                      key={`${item.url ?? item.title}-${item.publishedDate ?? ""}`}
                      href={item.url ?? "#"}
                      target="_blank"
                      rel="noreferrer"
                      className="block rounded-xl border border-border/60 bg-background/30 px-3 py-3 transition-colors hover:border-border hover:bg-background/40"
                    >
                      <div className="text-sm font-medium text-foreground">
                        {item.title}
                      </div>
                      <div className="mt-1 text-xs text-secondary-text">
                        {[item.source, item.publishedDate]
                          .filter(Boolean)
                          .join(" · ") || "新闻摘要"}
                      </div>
                      {item.snippet ? (
                        <div className="mt-2 text-xs leading-5 text-muted-text">
                          {item.snippet}
                        </div>
                      ) : null}
                    </a>
                  ))
                ) : (
                  <div className="text-sm text-secondary-text">
                    暂无可展示的新闻摘要。
                  </div>
                )}
              </div>
            </div>

            <div className="flex flex-wrap gap-2">
              <button
                type="button"
                className="btn-secondary"
                disabled={isLaunchingAnalysisFor === drawerCandidate.code}
                onClick={() => void handleAnalyzeCandidate(drawerCandidate)}
              >
                {isLaunchingAnalysisFor === drawerCandidate.code
                  ? "提交中..."
                  : "分析该股"}
              </button>
              <button
                type="button"
                className="btn-secondary"
                onClick={() => handleAskCandidate(drawerCandidate)}
              >
                去问股
              </button>
            </div>
          </div>
        ) : null}
      </Drawer>
    </div>
  );
};

export default StockPickerPage;
