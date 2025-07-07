import { useState, useEffect } from 'react';
import { Database, Table2, BarChart3, HardDrive, Calendar } from 'lucide-react';
import { apiService } from './api';
import { Partition, ParentTableInfo, TableInfo } from "./types";

export default function App() {
  const [selectedTable, setSelectedTable] = useState<TableInfo | null>(null);
  const [tables, setTables] = useState<TableInfo[]>([]);
  const [partitions, setPartitions] = useState<Partition[]>([]);
  const [parentTableInfo, setParentTableInfo] = useState<ParentTableInfo | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [currentPage, setCurrentPage] = useState(1);
  const [totalPages, setTotalPages] = useState(1);
  const itemsPerPage = 10;

  // Fetch tables on component mount
  useEffect(() => {
    fetchTables();
  }, []);

  // Fetch partitions when selected table changes
  useEffect(() => {
    if (selectedTable) {
      fetchPartitions(selectedTable, 1);
    }
  }, [selectedTable]);

  const fetchTables = async () => {
    try {
      setLoading(true);
      const { data, error } = await apiService.getTables();
      if (error) {
        throw new Error(error);
      }
      if (data) {
        setTables(data);
        if (data.length > 0) {
          setSelectedTable(data[0]);
        }
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch tables');
    } finally {
      setLoading(false);
    }
  };

  const fetchPartitions = async (table: TableInfo, page: number) => {
    try {
      setLoading(true);
      const offset = (page - 1) * itemsPerPage;
      const { data, error } = await apiService.getPartitions(table.schema, table.name, {
        limit: itemsPerPage,
        offset,
      });

      if (error) {
        throw new Error(error);
      }

      if (data) {
        setPartitions(data.partitions);
        setParentTableInfo(data.parent_table || null);
        if (data.partitions.length > 0) {
          setTotalPages(Math.ceil(data.partitions[0].total_count / itemsPerPage));
        }
        setCurrentPage(page);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch partitions');
    } finally {
      setLoading(false);
    }
  };

  const handleTableSelect = (table: TableInfo) => {
    setSelectedTable(table);
    setCurrentPage(1);
  };

  const handlePageChange = (page: number) => {
    if (selectedTable) {
      fetchPartitions(selectedTable, page);
    }
  };

  const formatBytes = (bytes: number): string => {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  const formatNumber = (num: number): string => {
    // Display "0" for negative numbers (which indicate empty/unanalyzed tables in PostgreSQL)
    if (num < 0) {
      return "0";
    }
    return new Intl.NumberFormat().format(num);
  };

  if (error) {
    return (
      <div className="min-h-screen bg-red-50 flex items-center justify-center">
        <div className="text-red-600 text-center">
          <h2 className="text-2xl font-semibold mb-2">Error</h2>
          <p>{error}</p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-50">
      {/* Header */}
      <header className="bg-white border-b border-gray-200">
        <div className="px-6 py-4">
          <div className="flex items-center space-x-3">
            <Database className="h-6 w-6 text-indigo-600" />
            <h1 className="text-xl font-semibold text-gray-900">Partition Manager</h1>
          </div>
        </div>
      </header>

      <div className="flex h-[calc(100vh-64px)]">
        {/* Sidebar */}
        <aside className="w-64 bg-white border-r border-gray-200 overflow-y-auto">
          <div className="p-4">
            <div className="flex items-center space-x-2 mb-4">
              <Table2 className="h-5 w-5 text-gray-500" />
              <h2 className="text-sm font-medium text-gray-700">Tables</h2>
            </div>
            <nav>
              {tables.map((table) => (
                <button
                  key={`${table.schema}.${table.name}`}
                  onClick={() => handleTableSelect(table)}
                  className={`w-full text-left px-3 py-2 text-sm rounded-md ${
                    selectedTable?.name === table.name
                      ? 'bg-indigo-50 text-indigo-700 font-medium'
                      : 'text-gray-700 hover:bg-gray-50'
                  }`}
                >
                  {table.schema}.{table.name}
                </button>
              ))}
            </nav>
          </div>
        </aside>

        <main className="flex-1 overflow-auto">
          <div className="p-6">

            {/* Parent Table Summary */}
            {parentTableInfo && (
              <div className="bg-white shadow-sm rounded-lg border border-gray-200 mb-6">
                <div className="px-6 py-4 border-b border-gray-200">
                  <h3 className="text-lg font-medium text-gray-900">Table Summary</h3>
                </div>
                <div className="px-6 py-4">
                  <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                    <div className="flex items-center space-x-3">
                      <HardDrive className="h-5 w-5 text-blue-500" />
                      <div>
                        <p className="text-sm font-medium text-gray-900">Total Size</p>
                        <p className="text-sm text-gray-500">{parentTableInfo.total_size}</p>
                      </div>
                    </div>
                    <div className="flex items-center space-x-3">
                      <BarChart3 className="h-5 w-5 text-green-500" />
                      <div>
                        <p className="text-sm font-medium text-gray-900">Total Rows</p>
                        <p className="text-sm text-gray-500">{formatNumber(parentTableInfo.total_rows)}</p>
                      </div>
                    </div>
                    <div className="flex items-center space-x-3">
                      <Table2 className="h-5 w-5 text-purple-500" />
                      <div>
                        <p className="text-sm font-medium text-gray-900">Partitions</p>
                        <p className="text-sm text-gray-500">{parentTableInfo.partition_count}</p>
                      </div>
                    </div>
                    <div className="flex items-center space-x-3">
                      <Calendar className="h-5 w-5 text-orange-500" />
                      <div>
                        <p className="text-sm font-medium text-gray-900">Avg Size/Partition</p>
                        <p className="text-sm text-gray-500">
                          {formatBytes(parentTableInfo.total_size_bytes / Math.max(parentTableInfo.partition_count, 1))}
                        </p>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            )}

            {/* Partitions table */}
            <div className="bg-white shadow-sm rounded-lg border border-gray-200">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Partition Name
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Rows
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Size
                    </th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {loading ? (
                    <tr>
                      <td colSpan={4} className="px-6 py-4 text-center text-gray-500">
                        Loading partitions...
                      </td>
                    </tr>
                  ) : partitions.length === 0 ? (
                    <tr>
                      <td colSpan={4} className="px-6 py-4 text-center text-gray-500">
                        No partitions found
                      </td>
                    </tr>
                  ) : (
                    partitions.map((partition) => (
                      <tr key={partition.name} className="hover:bg-gray-50">
                        {/* partition name */}
                        <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                          {partition.name}
                        </td>

                        {/* row count */}
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                          {formatNumber(partition.rows)}
                        </td>

                        {/* partition size */}
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                          <div>
                            <div>{partition.size}</div>
                            <div className="text-xs text-gray-400">
                              {formatBytes(partition.size_bytes)}
                            </div>
                          </div>
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
              {/* Pagination Controls */}
              {totalPages > 1 && (
                  <div className="bg-white px-4 py-3 flex items-center justify-between border-t border-gray-200 sm:px-6">
                    <div className="flex-1 flex justify-between sm:hidden">
                      <button
                          onClick={() => handlePageChange(currentPage - 1)}
                          disabled={currentPage === 1}
                          className={`relative inline-flex items-center px-4 py-2 border border-gray-300 text-sm font-medium rounded-md ${
                              currentPage === 1
                                  ? 'bg-gray-100 text-gray-400 cursor-not-allowed'
                                  : 'bg-white text-gray-700 hover:bg-gray-50'
                          }`}
                      >
                        Previous
                      </button>
                      <button
                          onClick={() => handlePageChange(currentPage + 1)}
                          disabled={currentPage === totalPages}
                          className={`ml-3 relative inline-flex items-center px-4 py-2 border border-gray-300 text-sm font-medium rounded-md ${
                              currentPage === totalPages
                                  ? 'bg-gray-100 text-gray-400 cursor-not-allowed'
                                  : 'bg-white text-gray-700 hover:bg-gray-50'
                          }`}
                      >
                        Next
                      </button>
                    </div>
                    <div className="hidden sm:flex-1 sm:flex sm:items-center sm:justify-between">
                      <div>
                        <p className="text-sm text-gray-700">
                          Showing <span className="font-medium">{(currentPage - 1) * itemsPerPage + 1}</span>{' '}
                          to{' '}
                          <span className="font-medium">
                            {Math.min(currentPage * itemsPerPage, partitions[0]?.total_count || 0)}
                          </span>{' '}
                          of <span className="font-medium">{partitions[0]?.total_count || 0}</span>{' '}
                          results
                        </p>
                      </div>
                      <div>
                        <nav className="relative z-0 inline-flex rounded-md shadow-sm -space-x-px">
                          <button
                              onClick={() => handlePageChange(currentPage - 1)}
                              disabled={currentPage === 1}
                              className={`relative inline-flex items-center px-2 py-2 rounded-l-md border border-gray-300 bg-white text-sm font-medium ${
                                  currentPage === 1
                                      ? 'text-gray-300 cursor-not-allowed'
                                      : 'text-gray-500 hover:bg-gray-50'
                              }`}
                          >
                            <span className="sr-only">Previous</span>
                            <svg
                                className="h-5 w-5"
                                xmlns="http://www.w3.org/2000/svg"
                                viewBox="0 0 20 20"
                                fill="currentColor"
                                aria-hidden="true"
                            >
                              <path
                                  fillRule="evenodd"
                                  d="M12.707 5.293a1 1 0 010 1.414L9.414 10l3.293 3.293a1 1 0 01-1.414 1.414l-4-4a1 1 0 010-1.414l4-4a1 1 0 011.414 0z"
                                  clipRule="evenodd"
                              />
                            </svg>
                          </button>
                          {Array.from({ length: totalPages }, (_, i) => i + 1).map((page) => (
                              <button
                                  key={page}
                                  onClick={() => handlePageChange(page)}
                                  className={`relative inline-flex items-center px-4 py-2 border text-sm font-medium ${
                                      currentPage === page
                                          ? 'z-10 bg-indigo-50 border-indigo-500 text-indigo-600'
                                          : 'bg-white border-gray-300 text-gray-500 hover:bg-gray-50'
                                  }`}
                              >
                                {page}
                              </button>
                          ))}
                          <button
                              onClick={() => handlePageChange(currentPage + 1)}
                              disabled={currentPage === totalPages}
                              className={`relative inline-flex items-center px-2 py-2 rounded-r-md border border-gray-300 bg-white text-sm font-medium ${
                                  currentPage === totalPages
                                      ? 'text-gray-300 cursor-not-allowed'
                                      : 'text-gray-500 hover:bg-gray-50'
                              }`}
                          >
                            <span className="sr-only">Next</span>
                            <svg
                                className="h-5 w-5"
                                xmlns="http://www.w3.org/2000/svg"
                                viewBox="0 0 20 20"
                                fill="currentColor"
                                aria-hidden="true"
                            >
                              <path
                                  fillRule="evenodd"
                                  d="M7.293 14.707a1 1 0 010-1.414L10.586 10 7.293 6.707a1 1 0 011.414-1.414l4 4a1 1 0 010 1.414l-4 4a1 1 0 01-1.414 0z"
                                  clipRule="evenodd"
                              />
                            </svg>
                          </button>
                        </nav>
                      </div>
                    </div>
                  </div>
              )}
            </div>
          </div>
        </main>
      </div>
    </div>
  );
}
