import {
  TablesResponse,
  PartitionsResponse,
  PaginationParams, TableInfo,
} from "./types.ts";

// Default to localhost in development can be overridden by environment variable
const API_BASE_URL = import.meta.env.VITE_API_URL || "http://localhost:8080";

interface ApiResponse<T> {
  data?: T;
  error?: string;
}

class ApiService {
  private readonly baseUrl: string;

  constructor(baseUrl: string = API_BASE_URL) {
    this.baseUrl = baseUrl;
  }

  private async fetchWithError<T>(
    endpoint: string,
    options: RequestInit = {}
  ): Promise<ApiResponse<T>> {
    try {
      const response = await fetch(`${this.baseUrl}${endpoint}`, {
        ...options,
        headers: {
          "Content-Type": "application/json",
          ...options.headers,
        },
      });

      if (!response.ok) {
        const error = await response.json().catch(() => ({
          message: "An unknown error occurred",
        }));
        return { error: error.message };
      }

      const data = await response.json();
      return { data };
    } catch (error) {
      return {
        error:
          error instanceof Error ? error.message : "An unknown error occurred",
      };
    }
  }

  async getTables(): Promise<ApiResponse<TableInfo[]>> {
    const response = await this.fetchWithError<TablesResponse>("/api/tables");
    if (response.data) {
      return { data: response.data.tables };
    }
    return { error: response.error };
  }

  async getPartitions(
    tableName: string,
    schema: string,
    pagination?: PaginationParams
  ): Promise<ApiResponse<PartitionsResponse>> {
    let endpoint = `/api/partitions?table=${tableName}&schema=${schema}`;
    if (pagination) {
      endpoint += `&limit=${pagination.limit}&offset=${pagination.offset}`;
    }
    return this.fetchWithError<PartitionsResponse>(endpoint);
  }
}

export const apiService = new ApiService();
