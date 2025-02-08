import { useState } from 'react';
import { Database, Table2, Settings2 } from 'lucide-react';

interface Partition {
  name: string;
  table: string;
  size: string;
  rows: number;
  range: string;
  created: string;
}

function App() {
  const [selectedTable, setSelectedTable] = useState<string>('users');
  
  // Mock data - in a real app this would come from your database
  const tables = ['users', 'orders', 'products', 'analytics'];
  const partitions: Partition[] = [
    {
      name: 'users_2024_q1',
      table: 'users',
      size: '2.3 GB',
      rows: 1250000,
      range: '2024-01-01 to 2024-03-31',
      created: '2024-01-01'
    },
    {
      name: 'users_2024_q2',
      table: 'users',
      size: '1.8 GB',
      rows: 980000,
      range: '2024-04-01 to 2024-06-30',
      created: '2024-04-01'
    }
  ];

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
        <aside className="w-64 border-r border-gray-200 bg-white">
          <div className="p-4">
            <h2 className="text-sm font-medium text-gray-500">Tables</h2>
            <nav className="mt-4 space-y-1">
              {tables.map((table) => (
                <button
                  key={table}
                  onClick={() => setSelectedTable(table)}
                  className={`w-full flex items-center px-3 py-2 text-sm rounded-md ${
                    selectedTable === table
                      ? 'bg-indigo-50 text-indigo-700'
                      : 'text-gray-700 hover:bg-gray-50'
                  }`}
                >
                  <Table2 className="h-4 w-4 mr-2" />
                  {table}
                </button>
              ))}
            </nav>
          </div>
        </aside>

        {/* Main content */}
        <main className="flex-1 overflow-auto">
          <div className="p-6">
            <div className="flex justify-between items-center mb-6">
              <div>
                <h2 className="text-lg font-semibold text-gray-900">
                  Partitions for {selectedTable}
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
                {/*<button className="inline-flex items-center px-4 py-2 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-indigo-600 hover:bg-indigo-700">*/}
                {/*  <Plus className="h-4 w-4 mr-2" />*/}
                {/*  Add Partition*/}
                {/*</button>*/}
              </div>
            </div>

            {/* Partitions table */}
            <div className="bg-white shadow-sm rounded-lg border border-gray-200">
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
                    {/*<th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">*/}
                    {/*  Actions*/}
                    {/*</th>*/}
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {partitions
                    .filter((p) => p.table === selectedTable)
                    .map((partition) => (
                      <tr key={partition.name} className="hover:bg-gray-50">
                        <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                          {partition.name}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                          {partition.size}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                          {partition.rows.toLocaleString()}
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                          {partition.range}
                        </td>
                        <td className="px-6 py-4 text-right whitespace-nowrap text-sm text-gray-500">
                          {partition.created}
                        </td>
                        {/*<td className="px-6 py-4 whitespace-nowrap text-right text-sm font-medium">*/}
                        {/*  <button className="text-red-600 hover:text-red-900">*/}
                        {/*    <Trash2 className="h-4 w-4" />*/}
                        {/*  </button>*/}
                        {/*</td>*/}
                      </tr>
                    ))}
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