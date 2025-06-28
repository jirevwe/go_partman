export interface Partition {
  name: string;
  size: string;
  rows: number;
  range: string;
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

export interface TableInfo {
  name: string;
  schema: string;
}

export interface TablesResponse {
  tables: TableInfo[];
}

export interface PartitionsResponse {
  partitions: Partition[];
  parent_table?: ParentTableInfo;
}

export interface PaginationParams {
  limit: number;
  offset: number;
}
