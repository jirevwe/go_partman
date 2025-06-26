import { useState, useEffect } from 'react';
import { Database, Table2, Settings2, BarChart3, HardDrive, Calendar } from 'lucide-react';
import { apiService } from './api';
import { Partition, ParentTableInfo } from "./types.ts";

// interface ApiError {
//   message: string;
// }

function App() {
  const [selectedTable, setSelectedTable] = useState<string>('');
  const [tables, setTables] = useState<string[]>([]);
  const [partitions, setPartitions] = useState<Partition[]>([]);
  const [parentTableInfo, setParentTableInfo] = useState<ParentTableInfo | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Fetch tables on component mount
  useEffect(() => {
    fetchTables();
  }, []);

  // Fetch partitions when selected table changes
  useEffect(() => {
    if (selectedTable) {
      fetchPartitions(selectedTable);
    }
  }, [selectedTable]);

  const fetchTables = async () => {
    try {
      const { data, error } = await apiService.getTables();
      if (error) {
        throw new Error(error);
      }
      if (data) {
        setTables(data);
        if (data.length > 0) {
          setSelectedTable(data[0]); // Select first table by default
        }
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch tables');
    } finally {
      setLoading(false);
    }
  };

  const fetchPartitions = async (tableName: string) => {
    try {
      setLoading(true);
      const { data, error } = await apiService.getPartitions(tableName);
      if (error) {
        throw new Error(error);
      }
      if (data) {
        setPartitions(data.partitions);
        setParentTableInfo(data.parent_table || null);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to fetch partitions');
    } finally {
      setLoading(false);
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
          <h2 className="text-lg font-semibold">Error</h2>
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
                  key={table}
                  onClick={() => setSelectedTable(table)}
                  className={`w-full text-left px-3 py-2 text-sm rounded-md ${
                    selectedTable === table
                      ? 'bg-indigo-50 text-indigo-700 font-medium'
                      : 'text-gray-700 hover:bg-gray-50'
                  }`}
                >
                  {table}
                </button>
              ))}
            </nav>
          </div>
        </aside>

        <main className="flex-1 overflow-auto">
          <div className="p-6">
            <div className="flex justify-between items-center mb-6">
              <div>
                <h2 className="text-lg font-semibold text-gray-900">
                  {loading ? 'Loading...' : `Partitions for ${selectedTable}`}
                </h2>
                <p className="text-sm text-gray-500 mt-1">
                  Manage and monitor your table partitions
                </p>
              </div>
              <div className="flex space-x-3">
                <button className="inline-flex items-center px-4 py-2 border border-gray-300 rounded-md shadow-sm text-sm font-medium text-gray-700 bg-white hover:bg-gray-50">
                  <Settings2 className="h-4 w-4 mr-2" />
                  Settings
                </button>
              </div>
            </div>

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
              <div className="px-6 py-4 border-b border-gray-200">
                <h3 className="text-lg font-medium text-gray-900">Partition Details</h3>
              </div>
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Partition Name
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Size
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Rows
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Range
                    </th>
                    <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                      Created
                    </th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {loading ? (
                    <tr>
                      <td colSpan={5} className="px-6 py-4 text-center text-gray-500">
                        Loading partitions...
                      </td>
                    </tr>
                  ) : partitions.length === 0 ? (
                    <tr>
                      <td colSpan={5} className="px-6 py-4 text-center text-gray-500">
                        No partitions found
                      </td>
                    </tr>
                  ) : (
                    partitions.map((partition) => (
                      <tr key={partition.name} className="hover:bg-gray-50">
                        <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                          {partition.name}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                          <div>
                            <div>{partition.size}</div>
                            <div className="text-xs text-gray-400">
                              {formatBytes(partition.size_bytes)}
                            </div>
                          </div>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                          {formatNumber(partition.rows)}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                          <div className="max-w-xs truncate" title={partition.range}>
                            {partition.range}
                          </div>
                        </td>
                        <td className="px-6 py-4 text-right whitespace-nowrap text-sm text-gray-500">
                          {partition.created}
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </div>
        </main>
      </div>
    </div>
  );
}

export default App;