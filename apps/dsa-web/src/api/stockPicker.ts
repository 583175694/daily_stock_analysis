import apiClient from './index';
import { toCamelCase } from './utils';
import type {
  PickerRunRequest,
  PickerRunResponse,
  PickerSectorItem,
  PickerTaskDetail,
  PickerTaskItem,
  PickerTemplateItem,
  PickerTemplateStatsResponse,
  PickerUniverseItem,
} from '../types/stockPicker';

export const stockPickerApi = {
  async getTemplates(): Promise<PickerTemplateItem[]> {
    const response = await apiClient.get<Record<string, unknown>>('/api/v1/picker/templates');
    const data = toCamelCase<{ items?: PickerTemplateItem[] }>(response.data);
    return data.items ?? [];
  },

  async getUniverses(): Promise<PickerUniverseItem[]> {
    const response = await apiClient.get<Record<string, unknown>>('/api/v1/picker/universes');
    const data = toCamelCase<{ items?: PickerUniverseItem[] }>(response.data);
    return data.items ?? [];
  },

  async getSectors(): Promise<PickerSectorItem[]> {
    const response = await apiClient.get<Record<string, unknown>>('/api/v1/picker/sectors');
    const data = toCamelCase<{ items?: PickerSectorItem[] }>(response.data);
    return data.items ?? [];
  },

  async run(payload: PickerRunRequest): Promise<PickerRunResponse> {
    const response = await apiClient.post<Record<string, unknown>>('/api/v1/picker/run', {
      template_id: payload.templateId,
      template_overrides: payload.templateOverrides ?? {},
      universe_id: payload.universeId,
      mode: payload.mode ?? 'watchlist',
      sector_ids: payload.sectorIds ?? [],
      limit: payload.limit ?? 20,
      ai_top_k: payload.aiTopK ?? 5,
      force_refresh: payload.forceRefresh ?? false,
      notify: payload.notify ?? false,
    });
    return toCamelCase<PickerRunResponse>(response.data);
  },

  async listTasks(limit = 20): Promise<PickerTaskItem[]> {
    const response = await apiClient.get<Record<string, unknown>>('/api/v1/picker/tasks', {
      params: { limit },
    });
    const data = toCamelCase<{ items?: PickerTaskItem[] }>(response.data);
    return data.items ?? [];
  },

  async getTask(taskId: string): Promise<PickerTaskDetail> {
    const response = await apiClient.get<Record<string, unknown>>(`/api/v1/picker/tasks/${encodeURIComponent(taskId)}`);
    return toCamelCase<PickerTaskDetail>(response.data);
  },

  async getTemplateStats(windowDays = 10): Promise<PickerTemplateStatsResponse> {
    const response = await apiClient.get<Record<string, unknown>>('/api/v1/picker/stats/templates', {
      params: { window_days: windowDays },
    });
    return toCamelCase<PickerTemplateStatsResponse>(response.data);
  },
};
