export interface TableInfo {
  name: string;
  schema: string;
}

export interface Partition {
  name: string;
  size: string;
  rows: number;
  created: string;
  size_bytes: number;
  total_count: number;
}

export interface ParentTableInfo {
  name: string;
  total_size: string;
  total_rows: number;
  partition_count: number;
  total_size_bytes: number;
}

export interface PaginationParams {
  limit: number;
  offset: number;
}
