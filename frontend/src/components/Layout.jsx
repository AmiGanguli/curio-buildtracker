import { useState, useEffect } from 'react';
import { Link, useLocation, Outlet } from 'react-router-dom';
import { Home, List, LogOut, Terminal, ChevronUp, ChevronDown } from 'lucide-react';
import { Button, useAuthenticator } from '@aws-amplify/ui-react';
import clsx from 'clsx';

function Layout() {
    const { signOut, user } = useAuthenticator((context) => [context.user]);
    const location = useLocation();

    const [logs, setLogs] = useState([]);
    const [isConnected, setIsConnected] = useState(false);
    const [isPanelOpen, setIsPanelOpen] = useState(false);

    useEffect(() => {
        const wsUrl = import.meta.env.VITE_WEBSOCKET_URL;
        if (!wsUrl) return;

        let ws = null;
        let reconnectTimeout = null;

        const connect = () => {
            ws = new WebSocket(wsUrl);

            ws.onopen = () => {
                console.log("Connected to status stream");
                setIsConnected(true);
            };

            ws.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);
                    setLogs(prev => {
                        if (prev.length > 0) {
                            const last = prev[0];
                            // Simple deduplication based on key fields
                            if (last.taskId === data.taskId &&
                                last.state === data.state &&
                                last.message === data.message &&
                                last.timestamp === data.timestamp) {
                                return prev;
                            }
                        }
                        return [data, ...prev].slice(0, 100);
                    });
                } catch (e) {
                    console.error("Failed to parse log", e);
                }
            };

            ws.onclose = () => {
                console.log("Disconnected from status stream, reconnecting in 3s...");
                setIsConnected(false);
                reconnectTimeout = setTimeout(connect, 3000);
            };
        };

        connect();

        return () => {
            if (ws) ws.close();
            if (reconnectTimeout) clearTimeout(reconnectTimeout);
        };
    }, []);

    const navItems = [
        { path: '/', label: 'Dashboard', icon: Home },
        { path: '/inputs', label: 'Inputs', icon: List },
    ];

    return (
        <div className="flex min-h-screen bg-gray-50">
            {/* Sidebar */}
            <aside className="w-64 bg-white border-r border-gray-200 flex flex-col fixed h-full z-10">
                <div className="h-16 flex items-center px-6 border-b border-gray-100">
                    <span className="text-xl font-bold bg-clip-text text-transparent bg-gradient-to-r from-blue-600 to-indigo-600">
                        Curio Admin
                    </span>
                </div>

                <nav className="flex-1 p-4 space-y-2">
                    {navItems.map((item) => {
                        const Icon = item.icon;
                        const isActive = location.pathname === item.path;
                        return (
                            <Link
                                key={item.path}
                                to={item.path}
                                className={clsx(
                                    "flex items-center gap-3 px-4 py-3 rounded-lg text-sm font-medium transition-all duration-200",
                                    isActive
                                        ? "bg-blue-50 text-blue-700 shadow-sm ring-1 ring-blue-100"
                                        : "text-gray-600 hover:bg-gray-50 hover:text-gray-900"
                                )}
                            >
                                <Icon size={20} className={isActive ? "text-blue-600" : "text-gray-400 group-hover:text-gray-600"} />
                                {item.label}
                            </Link>
                        )
                    })}
                </nav>

                <div className="p-4 border-t border-gray-100 bg-gray-50/50">
                    <div className="flex items-center gap-3 px-2 mb-4">
                        <div className="w-8 h-8 rounded-full bg-indigo-100 flex items-center justify-center text-indigo-700 font-bold text-xs">
                            {user?.username?.substring(0, 2).toUpperCase()}
                        </div>
                        <div className="flex-1 min-w-0">
                            <p className="text-sm font-medium text-gray-900 truncate">{user?.username}</p>
                            <p className="text-xs text-gray-500 truncate">{user?.signInDetails?.loginId}</p>
                        </div>
                    </div>
                    <button
                        onClick={signOut}
                        className="flex w-full items-center gap-2 text-red-600 hover:bg-red-50 hover:text-red-700 px-4 py-2 rounded-lg text-sm font-medium transition-colors"
                    >
                        <LogOut size={16} />
                        Sign out
                    </button>
                </div>
            </aside>

            {/* Main Content */}
            <main className="flex-1 ml-64 p-8">
                <div className="max-w-7xl mx-auto animate-fadeIn">
                    <Outlet context={{ lastLog: logs.length > 0 ? logs[0] : null, isConnected }} />
                </div>
            </main>

            {/* Global Logs Panel */}
            <div className={clsx(
                "fixed bottom-0 right-0 left-64 bg-gray-900 border-t border-gray-800 transition-all duration-300 z-20 shadow-[0_-4px_6px_-1px_rgba(0,0,0,0.1)]",
                isPanelOpen ? "h-64" : "h-10"
            )}>
                <button
                    onClick={() => setIsPanelOpen(!isPanelOpen)}
                    className="w-full h-10 px-4 flex items-center justify-between text-gray-400 hover:text-gray-200 hover:bg-gray-800 transition-colors"
                >
                    <div className="flex items-center gap-2">
                        <Terminal size={14} />
                        <span className="text-xs font-semibold uppercase tracking-wider">System Logs</span>
                        <div className={`w-2 h-2 rounded-full ml-2 ${isConnected ? 'bg-green-400 animate-pulse' : 'bg-red-500'}`} title={isConnected ? "Connected" : "Disconnected"}></div>
                    </div>
                    {isPanelOpen ? <ChevronDown size={16} /> : <ChevronUp size={16} />}
                </button>

                {isPanelOpen && (
                    <div className="h-52 overflow-y-auto px-4 pb-4 font-mono text-xs space-y-1 scrollbar-thin scrollbar-thumb-gray-700 scrollbar-track-transparent">
                        <div className="flex justify-end mb-2 sticky top-0 bg-gray-900/90 py-1 backdrop-blur-sm">
                            <button onClick={(e) => { e.stopPropagation(); setLogs([]); }} className="text-gray-500 hover:text-red-400 transition-colors text-[10px] uppercase font-bold tracking-wider">
                                Clear Console
                            </button>
                        </div>
                        {logs.length === 0 ? (
                            <div className="text-gray-600 italic py-4 text-center">No recent logs...</div>
                        ) : (
                            logs.map((log, i) => (
                                <div key={i} className="flex gap-3 hover:bg-white/5 p-1 rounded transition-colors group">
                                    <span className="text-gray-500 shrink-0 tabular-nums select-none w-16">
                                        {new Date(log.timestamp || Date.now()).toLocaleTimeString()}
                                    </span>
                                    <span className={clsx(
                                        "font-bold w-12 shrink-0 select-none text-center rounded px-1",
                                        log.level === 'ERROR' ? "bg-red-900/30 text-red-400" : "bg-blue-900/30 text-blue-400"
                                    )}>
                                        {log.level || 'INFO'}
                                    </span>
                                    <span className="text-gray-300 break-all group-hover:text-white transition-colors flex-1 flex items-center gap-2">
                                        <span>{log.message}</span>
                                        {log.processed !== undefined && log.total !== undefined && Number(log.total) > 0 && log.jobType !== 'METRIC' && (
                                            <span className="text-xs text-gray-500 bg-gray-800 px-1.5 py-0.5 rounded border border-gray-700 font-mono">
                                                {log.processed}/{log.total} ({Math.round(Number(log.processed) / Number(log.total) * 100)}%)
                                            </span>
                                        )}
                                    </span>
                                    {log.taskId && (
                                        <span className="text-gray-600 text-[10px] shrink-0 select-none">
                                            {log.taskId}
                                        </span>
                                    )}
                                </div>
                            ))
                        )}
                    </div>
                )}
            </div>
        </div>
    );
}

export default Layout;
