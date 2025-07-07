import {
  TableInfo,
  Partition,
  ParentTableInfo,
  PaginationParams,
} from "./types";

interface TablesResponse {
  tables: TableInfo[];
}

interface PartitionsResponse {
  partitions: Partition[];
  parent_table?: ParentTableInfo;
}

class ApiService {
  private baseUrl: string;

  constructor() {
    this.baseUrl = "/api";
  }

  async getTables(): Promise<{ data?: TableInfo[]; error?: string }> {
    try {
      const response = await fetch(`${this.baseUrl}/tables`);
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }1
      const data: TablesResponse = await response.json();
      return { data: data.tables };
    } catch (error) {
      return {
        error:
          error instanceof Error ? error.message : "Failed to fetch tables",
      };
    }
  }

  async getPartitions(
    schema: string,
    table: string,
    { limit, offset }: PaginationParams
  ): Promise<{
    data?: { partitions: Partition[]; parent_table?: ParentTableInfo };
    error?: string;
  }> {
    try {
      const params = new URLSearchParams({
        schema,
        table,
        limit: limit.toString(),
        offset: offset.toString(),
      });

      const response = await fetch(`${this.baseUrl}/partitions?${params}`);
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      const data: PartitionsResponse = await response.json();
      return { data };
    } catch (error) {
      return {
        error:
          error instanceof Error ? error.message : "Failed to fetch partitions",
      };
    }
  }
}

export const apiService = new ApiService();
