export interface PickerTemplateItem {
  templateId: string;
  name: string;
  description: string;
  focus: string;
  riskLevel: string;
  style: string;
  scoringNotes: string[];
  alphaHypothesis?: string;
  suitableRegimes?: string[];
  cautionRegimes?: string[];
  invalidRegimes?: string[];
  exclusionConditions?: string[];
  tradeRules?: Record<string, unknown>;
}

export interface PickerUniverseItem {
  universeId: string;
  name: string;
  description: string;
  stockCount: number;
  codes: string[];
}

export interface PickerSectorItem {
  sectorId: string;
  name: string;
  description?: string | null;
  market: string;
  stockCount: number;
  strengthLabel?: string | null;
  rankDirection?: 'top' | 'bottom' | null;
  rankPosition?: number | null;
  changePct?: number | null;
  isRankedToday?: boolean;
}

export interface PickerTaskSummary {
  templateId?: string | null;
  templateName?: string | null;
  universeId?: string | null;
  mode?: string | null;
  totalStocks: number;
  scoredCount: number;
  insufficientCount: number;
  errorCount: number;
  strictMatchCount: number;
  selectedCount: number;
  qualifiedFallbackCount?: number;
  fallbackCount: number;
  explainedCount: number;
  advancedEnrichedCount?: number;
  aiReviewedCount?: number;
  aiSoftVetoCount?: number;
  insufficientReasonBreakdown?: Record<string, number>;
  insufficientReasonLabels?: Record<string, string>;
  tradingDatePolicy?: Record<string, unknown>;
  sectorCatalogSnapshot?: Record<string, unknown>;
  sectorQualitySummary?: Record<string, unknown>;
  rankedSectorBreakdown?: Array<Record<string, unknown>>;
  benchmarkPolicy?: Record<string, unknown>;
  selectionQualityGate?: Record<string, unknown>;
  marketRegimeSnapshot?: Record<string, unknown>;
}

export interface PickerScoreItem {
  scoreName: string;
  scoreLabel: string;
  scoreValue: number;
  detail: Record<string, unknown>;
}

export interface PickerNewsBrief {
  title: string;
  source?: string | null;
  publishedDate?: string | null;
  url?: string | null;
  snippet?: string | null;
}

export interface PickerCandidateEvaluationItem {
  windowDays: number;
  benchmarkCode?: string | null;
  evalStatus: string;
  benchmarkStatus?: string | null;
  isComparable?: boolean;
  entryDate?: string | null;
  entryPrice?: number | null;
  exitDate?: string | null;
  exitPrice?: number | null;
  benchmarkEntryPrice?: number | null;
  benchmarkExitPrice?: number | null;
  returnPct?: number | null;
  benchmarkReturnPct?: number | null;
  excessReturnPct?: number | null;
  maxDrawdownPct?: number | null;
  mfePct?: number | null;
  maePct?: number | null;
}

export interface PickerExecutionConstraints {
  market?: string | null;
  status?: string | null;
  statusLabel?: string | null;
  notFillable?: boolean;
  liquidityBucket?: string | null;
  gapRisk?: string | null;
  slippageBps?: number | null;
  executionPenalty?: number | null;
  estimatedCostModel?: string | null;
  signals?: Record<string, unknown>;
  note?: string | null;
}

export interface PickerResearchConfidence {
  status?: string | null;
  label?: string | null;
  score?: number | null;
  windowDays?: number | null;
  benchmarkCode?: string | null;
  templateId?: string | null;
  marketRegime?: string | null;
  signalBucket?: string | null;
  comparableSamples?: number;
  regimeComparableSamples?: number;
  templateWinRatePct?: number | null;
  regimeWinRatePct?: number | null;
  templateAvgExcessReturnPct?: number | null;
  regimeAvgExcessReturnPct?: number | null;
  nominalProbabilityPct?: number | null;
  calibratedWinRatePct?: number | null;
  calibrationGapPct?: number | null;
  ruleVersion?: string | null;
  calibration?: Record<string, unknown>;
  highConfidenceGate?: Record<string, unknown>;
  note?: string | null;
}

export interface PickerExecutionConfidence {
  status?: string | null;
  label?: string | null;
  score?: number | null;
  slippageBps?: number | null;
  liquidityBucket?: string | null;
  gapRisk?: string | null;
  notFillable?: boolean;
  costModel?: string | null;
  note?: string | null;
}

export interface PickerCandidateItem {
  rank: number;
  code: string;
  name?: string | null;
  market: string;
  selectionReason: string;
  latestDate?: string | null;
  latestClose?: number | null;
  changePct?: number | null;
  volumeRatio?: number | null;
  distanceToHighPct?: number | null;
  totalScore?: number | null;
  environmentFit?: string | null;
  environmentFitLabel?: string | null;
  signalBucket?: string | null;
  boardNames: string[];
  newsBriefs: PickerNewsBrief[];
  explanationSummary?: string | null;
  explanationRationale: string[];
  explanationRisks: string[];
  explanationWatchpoints: string[];
  technicalSnapshot: Record<string, unknown>;
  executionConstraints?: PickerExecutionConstraints;
  researchConfidence?: PickerResearchConfidence;
  executionConfidence?: PickerExecutionConfidence;
  tradePlan?: Record<string, unknown>;
  advancedFactors?: Record<string, unknown>;
  aiReview?: Record<string, unknown>;
  templateFailureFlags?: Array<Record<string, unknown>>;
  scoreBreakdown: PickerScoreItem[];
  evaluations: PickerCandidateEvaluationItem[];
}

export interface PickerTaskItem {
  taskId: string;
  status: 'queued' | 'running' | 'completed' | 'failed' | string;
  statusLabel?: string | null;
  templateId: string;
  templateName?: string | null;
  templateVersion: string;
  universeId: string;
  universeName?: string | null;
  mode: 'watchlist' | 'sector' | string;
  modeLabel?: string | null;
  sectorIds: string[];
  sectorNames: string[];
  limit: number;
  aiTopK: number;
  forceRefresh: boolean;
  notify: boolean;
  totalStocks: number;
  processedStocks: number;
  candidateCount: number;
  progressPercent: number;
  progressMessage: string;
  summary: PickerTaskSummary;
  errorMessage?: string | null;
  requestPayload: Record<string, unknown>;
  createdAt?: string | null;
  startedAt?: string | null;
  finishedAt?: string | null;
  updatedAt?: string | null;
}

export interface PickerTaskDetail extends PickerTaskItem {
  candidates: PickerCandidateItem[];
}

export interface PickerRunRequest {
  templateId: string;
  templateOverrides?: Record<string, unknown>;
  universeId: string;
  mode?: 'watchlist' | 'sector';
  sectorIds?: string[];
  limit?: number;
  aiTopK?: number;
  forceRefresh?: boolean;
  notify?: boolean;
}

export interface PickerRunResponse {
  taskId: string;
  status: string;
}

export interface PickerTemplateStatItem {
  templateId: string;
  templateName: string;
  windowDays: number;
  totalEvaluations: number;
  comparableEvaluations?: number;
  benchmarkUnavailableEvaluations?: number;
  winRatePct?: number | null;
  avgReturnPct?: number | null;
  avgExcessReturnPct?: number | null;
  avgMaxDrawdownPct?: number | null;
}

export interface PickerTemplateStatsResponse {
  windowDays: number;
  benchmarkCode: string;
  items: PickerTemplateStatItem[];
}

export interface PickerStratifiedStatItem {
  bucketKey: string;
  bucketLabel: string;
  totalEvaluations: number;
  comparableEvaluations?: number;
  benchmarkUnavailableEvaluations?: number;
  winRatePct?: number | null;
  avgReturnPct?: number | null;
  avgExcessReturnPct?: number | null;
  avgMaxDrawdownPct?: number | null;
}

export interface PickerStratifiedStatsResponse {
  windowDays: number;
  benchmarkCode: string;
  byMarketRegime: PickerStratifiedStatItem[];
  byTemplate: PickerStratifiedStatItem[];
  byRankBucket: PickerStratifiedStatItem[];
  bySignalBucket: PickerStratifiedStatItem[];
}

export interface PickerCalibrationStatItem {
  templateId: string;
  templateName: string;
  marketRegime: string;
  marketRegimeLabel: string;
  ruleVersion: string;
  bucketKey: string;
  bucketLabel: string;
  windowDays: number;
  samples: number;
  nominalProbabilityPct?: number | null;
  actualWinRatePct?: number | null;
  calibrationGapPct?: number | null;
  avgReturnPct?: number | null;
  avgExcessReturnPct?: number | null;
  avgMaxDrawdownPct?: number | null;
  calibrationStatus: string;
  calibrationLabel: string;
  highConfidenceGate?: Record<string, unknown>;
}

export interface PickerCalibrationStatsResponse {
  windowDays: number;
  benchmarkCode: string;
  items: PickerCalibrationStatItem[];
}

export interface PickerValidationHoldoutStatItem {
  templateId: string;
  templateName: string;
  ruleVersion: string;
  windowDays: number;
  sampleStatus: string;
  comparableSamples: number;
  inSampleCount: number;
  outOfSampleCount: number;
  splitRatio: number;
  analysisDateStart?: string | null;
  analysisDateEnd?: string | null;
  outOfSampleWinRatePct?: number | null;
  outOfSampleAvgReturnPct?: number | null;
  outOfSampleAvgExcessReturnPct?: number | null;
  outOfSampleAvgMaxDrawdownPct?: number | null;
}

export interface PickerValidationRollingStatItem {
  templateId: string;
  templateName: string;
  ruleVersion: string;
  windowDays: number;
  rollingMonth: string;
  sampleStatus: string;
  rollingCount: number;
  rollingWinRatePct?: number | null;
  rollingAvgExcessReturnPct?: number | null;
  rollingAvgMaxDrawdownPct?: number | null;
}

export interface PickerValidationStatsResponse {
  windowDays: number;
  benchmarkCode: string;
  outOfSampleByTemplate: PickerValidationHoldoutStatItem[];
  rollingMonthlyByTemplate: PickerValidationRollingStatItem[];
}

export interface PickerRiskStatItem {
  templateId: string;
  templateName: string;
  ruleVersion: string;
  windowDays: number;
  sampleStatus: string;
  sampleCount: number;
  avgReturnPct?: number | null;
  avgExcessReturnPct?: number | null;
  avgMaxDrawdownPct?: number | null;
  avgMfePct?: number | null;
  avgMaePct?: number | null;
  profitFactor?: number | null;
  returnDrawdownRatio?: number | null;
  returnPctP25?: number | null;
  returnPctP50?: number | null;
  returnPctP75?: number | null;
  excessReturnPctP25?: number | null;
  excessReturnPctP50?: number | null;
  excessReturnPctP75?: number | null;
  maxDrawdownPctP25?: number | null;
  maxDrawdownPctP50?: number | null;
  maxDrawdownPctP75?: number | null;
  mfePctP25?: number | null;
  mfePctP50?: number | null;
  mfePctP75?: number | null;
  maePctP25?: number | null;
  maePctP50?: number | null;
  maePctP75?: number | null;
}

export interface PickerRiskStatsResponse {
  windowDays: number;
  benchmarkCode: string;
  items: PickerRiskStatItem[];
}
