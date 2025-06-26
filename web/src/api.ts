import { Partition } from "./types.ts";

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

  async getTables(): Promise<ApiResponse<string[]>> {
    return this.fetchWithError<string[]>("/api/tables");
  }

  async getPartitions(tableName: string): Promise<ApiResponse<Partition[]>> {
    return this.fetchWithError<Partition[]>(
      `/api/partitions?table=${tableName}`
    );
  }
}

export const apiService = new ApiService();
