export interface Partition {
  name: string;
  size: string;
  rows: number;
  range: string;
  created: string;
  size_bytes: number;
}

export interface ParentTableInfo {
  name: string;
  total_size: string;
  total_rows: number;
  partition_count: number;
  total_size_bytes: number;
}

export interface TablesResponse {
  tables: string[];
}

export interface PartitionsResponse {
  partitions: Partition[];
  parent_table?: ParentTableInfo;
}
