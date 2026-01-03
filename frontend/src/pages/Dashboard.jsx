import { useState } from 'react';
import { get } from 'aws-amplify/api';
import { RefreshCw } from 'lucide-react';
import clsx from 'clsx';

function Dashboard() {
    const [data, setData] = useState(null);
    const [error, setError] = useState(null);
    const [loading, setLoading] = useState(false);

    const fetchData = async () => {
        try {
            setLoading(true);
            setError(null);
            const restOperation = get({
                apiName: 'CurioApi',
                path: '/'
            });
            const response = await restOperation.response;
            const json = await response.body.json();
            setData(json);
        } catch (e) {
            console.log('GET call failed: ', e);
            setError(e.toString());
        } finally {
            setLoading(false);
        }
    };

    return (
        <>
            <header className="mb-8 flex justify-between items-center">
                <div>
                    <h1 className="text-2xl font-bold text-gray-900">Dashboard</h1>
                    <p className="text-gray-500 text-sm mt-1">Overview of system status and items</p>
                </div>

                <button
                    onClick={fetchData}
                    disabled={loading}
                    className={clsx(
                        "flex items-center gap-2 px-4 py-2 bg-white border border-gray-200 rounded-lg text-sm font-medium hover:bg-gray-50 text-gray-700 shadow-sm transition-all focus:ring-2 focus:ring-blue-100",
                        loading && "opacity-75 cursor-not-allowed"
                    )}
                >
                    <RefreshCw size={16} className={clsx(loading && "animate-spin")} />
                    Refresh Data
                </button>
            </header>

            {error && (
                <div className="bg-red-50 text-red-700 p-4 rounded-lg mb-6 border border-red-200 flex items-start gap-3">
                    <div className="mt-0.5">⚠️</div>
                    <div>
                        <h3 className="font-semibold">Error</h3>
                        <p className="text-sm">{error}</p>
                    </div>
                </div>
            )}

            <div className="bg-white border border-gray-200 rounded-xl shadow-sm overflow-hidden">
                <div className="px-6 py-4 border-b border-gray-100 bg-gray-50/50 flex justify-between items-center">
                    <h2 className="font-semibold text-gray-900">Database Items</h2>
                    <span className="bg-blue-100 text-blue-700 text-xs px-2.5 py-0.5 rounded-full font-medium">
                        {data ? (Array.isArray(data) ? data.length : '1') : 0} items
                    </span>
                </div>

                <div className="p-6">
                    {data ? (
                        <div className="overflow-x-auto">
                            {Array.isArray(data) && data.length > 0 ? (
                                <table className="min-w-full divide-y divide-gray-200">
                                    <thead className="bg-gray-50">
                                        <tr>
                                            {Object.keys(data[0]).map(key => (
                                                <th key={key} className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">{key}</th>
                                            ))}
                                        </tr>
                                    </thead>
                                    <tbody className="bg-white divide-y divide-gray-200">
                                        {data.map((item, i) => (
                                            <tr key={i} className="hover:bg-gray-50">
                                                {Object.values(item).map((val, j) => (
                                                    <td key={j} className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                                                        {typeof val === 'object' ? JSON.stringify(val) : val}
                                                    </td>
                                                ))}
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            ) : (
                                <pre className="bg-gray-900 text-gray-100 p-4 rounded-lg text-sm overflow-auto max-h-96 font-mono">
                                    {JSON.stringify(data, null, 2)}
                                </pre>
                            )}
                        </div>
                    ) : (
                        <div className="text-center py-12 text-gray-500">
                            <p className="mb-2 text-lg">No data loaded</p>
                            <p className="text-sm">Click "Refresh Data" to fetch items from the server.</p>
                        </div>
                    )}
                </div>
            </div>
        </>
    );
}

export default Dashboard;
