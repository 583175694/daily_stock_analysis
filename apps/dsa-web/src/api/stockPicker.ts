import apiClient from './index';
import { toCamelCase } from './utils';
import type {
  PickerRunRequest,
  PickerRunResponse,
  PickerTaskDetail,
  PickerTaskItem,
  PickerTemplateItem,
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

  async run(payload: PickerRunRequest): Promise<PickerRunResponse> {
    const response = await apiClient.post<Record<string, unknown>>('/api/v1/picker/run', {
      template_id: payload.templateId,
      template_overrides: payload.templateOverrides ?? {},
      universe_id: payload.universeId,
      limit: payload.limit ?? 20,
      force_refresh: payload.forceRefresh ?? false,
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
};
