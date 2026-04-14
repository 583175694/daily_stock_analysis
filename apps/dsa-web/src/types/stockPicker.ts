export interface PickerTemplateItem {
  templateId: string;
  name: string;
  description: string;
  focus: string;
  riskLevel: string;
  style: string;
  scoringNotes: string[];
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
  market: string;
  stockCount: number;
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
  fallbackCount: number;
  explainedCount: number;
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
  boardNames: string[];
  newsBriefs: PickerNewsBrief[];
  explanationSummary?: string | null;
  explanationRationale: string[];
  explanationRisks: string[];
  explanationWatchpoints: string[];
  technicalSnapshot: Record<string, unknown>;
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
