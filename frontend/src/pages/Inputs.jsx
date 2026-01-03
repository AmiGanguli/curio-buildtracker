import { useState, useEffect } from 'react';
import { post, del, get } from 'aws-amplify/api';
import { fetchAuthSession } from 'aws-amplify/auth';
import { useOutletContext } from 'react-router-dom';
import { Play, Database, CheckCircle, AlertCircle, Trash2, FileJson, Loader2, RefreshCw, ChevronRight, ChevronDown, Folder, X, Code, Home, ChevronLeft, Eye } from 'lucide-react';
import clsx from 'clsx';
import { JsonRenderer } from '../components/JsonRenderer';


// --- Helper Components (Defined outside to prevent re-mounting) ---

const StatusBadge = ({ status, size = 'sm' }) => (
    <span className={clsx(
        "inline-flex font-semibold rounded-full items-center justify-center",
        size === 'xs' ? "px-1.5 py-0.5 text-[10px]" : "px-2 py-1 text-xs",
        status === 'OBSOLETE' ? "bg-red-100 text-red-800" :
            status === 'DIRTY' || status === 'MIXED' ? "bg-yellow-100 text-yellow-800" :
                "bg-green-100 text-green-800"
    )}>
        {status || 'Active'}
    </span>
);

const FileModal = ({ file, loading, onClose }) => {
    const [stack, setStack] = useState([]);
    const [viewMode, setViewMode] = useState('raw'); // 'raw', 'json', 'html', 'extracted'
    const [extractedText, setExtractedText] = useState(null);
    const [isExtracting, setIsExtracting] = useState(false);

    // Derived current view
    const current = stack[stack.length - 1];

    // Initialize Stack when file content loads
    useEffect(() => {
        if (file?.content) {
            setStack([{ name: 'Root', data: file.content }]);
            setExtractedText(null);
        } else {
            setStack([]);
        }
    }, [file?.content]);

    // Handle View Mode Switching
    const handleViewSwitch = async (mode) => {
        setViewMode(mode);
        if (mode === 'extracted' && !extractedText && file?.id) {
            setIsExtracting(true);
            try {
                const session = await fetchAuthSession();
                const token = session.tokens?.idToken?.toString();

                let pointer = '';
                // Construct JSON Pointer from stack (skipping Root)
                if (stack.length > 1) {
                    pointer = stack.slice(1).map(item => '/' + item.name).join('');
                }

                const query = new URLSearchParams({
                    id: file.id,
                    format: 'text'
                });
                if (pointer) {
                    query.append('pointer', pointer);
                }

                const api = await get({
                    apiName: 'CurioApi',
                    path: `/render?${query.toString()}`,
                    options: {
                        headers: { Authorization: token }
                    }
                }).response;

                const data = await api.body.json();
                setExtractedText(data.content);
            } catch (e) {
                console.error("Extraction failed", e);
                setExtractedText(`Error fetching extracted text: ${e.message}`);
            } finally {
                setIsExtracting(false);
            }
        }
    };

    // Auto-switch view mode on navigation
    useEffect(() => {
        if (!current) return;

        // Root Level: Use file extension hint
        if (stack.length === 1) {
            // ... (keep existing logic)
            if (file?.id?.endsWith('.json')) {
                setViewMode('json');
            } else {
                setViewMode('raw');
            }
        }
        // Drilled Level: Default based on data type
        else {
            if (typeof current.data === 'string') {
                setViewMode('raw');
            } else {
                setViewMode('json');
            }
        }
    }, [current, file?.id, stack.length]);

    const pushView = (key, value) => {
        setStack(prev => [...prev, { name: key, data: value }]);
        setExtractedText(null);
    };

    const popTo = (index) => {
        setStack(prev => prev.slice(0, index + 1));
        setExtractedText(null);
    };

    const goBack = () => {
        if (stack.length > 1) {
            setStack(prev => prev.slice(0, -1));
            setExtractedText(null);
        }
    };

    if (!file && !loading) return null;

    let contentToDisplay = current?.data;
    let jsonData = null;
    let isJsonError = false;

    if (viewMode === 'json') {
        if (typeof contentToDisplay === 'string') {
            try {
                jsonData = JSON.parse(contentToDisplay);
            } catch (e) {
                isJsonError = true;
            }
        } else if (typeof contentToDisplay === 'object' && contentToDisplay !== null) {
            jsonData = contentToDisplay;
        }
    }

    return (
        <div className="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4" onClick={onClose}>
            <div className="bg-white rounded-xl shadow-xl w-full max-w-4xl h-[80vh] flex flex-col" onClick={e => e.stopPropagation()}>

                {/* Header with Title and Breadcrumbs */}
                <div className="flex flex-col border-b border-gray-100">
                    <div className="flex items-center justify-between p-4 pb-2">
                        <div className="flex items-center gap-2">
                            <Code size={18} className="text-blue-500" />
                            <h3 className="font-semibold text-gray-900 truncate max-w-md" title={file?.id}>
                                {file?.id?.split('/').pop() || 'Loading...'}
                            </h3>
                        </div>
                        <div className="flex items-center gap-2">
                            {!loading && !file?.error && (
                                <div className="flex bg-gray-100 rounded-lg p-1">
                                    <button
                                        onClick={() => handleViewSwitch('raw')}
                                        className={clsx("px-3 py-1 text-xs font-medium rounded-md transition-colors", viewMode === 'raw' ? "bg-white shadow-sm text-gray-900" : "text-gray-500 hover:text-gray-700")}
                                    >
                                        Raw
                                    </button>
                                    <button
                                        onClick={() => handleViewSwitch('json')}
                                        className={clsx("px-3 py-1 text-xs font-medium rounded-md transition-colors", viewMode === 'json' ? "bg-white shadow-sm text-gray-900" : "text-gray-500 hover:text-gray-700")}
                                    >
                                        JSON
                                    </button>
                                    {typeof contentToDisplay === 'string' && (
                                        <>
                                            <button
                                                onClick={() => handleViewSwitch('html')}
                                                className={clsx("px-3 py-1 text-xs font-medium rounded-md transition-colors flex items-center gap-1", viewMode === 'html' ? "bg-white shadow-sm text-gray-900" : "text-gray-500 hover:text-gray-700")}
                                                title="Render safe HTML"
                                            >
                                                <Eye size={12} />
                                                Preview
                                            </button>
                                            <button
                                                onClick={() => handleViewSwitch('extracted')}
                                                className={clsx("px-3 py-1 text-xs font-medium rounded-md transition-colors flex items-center gap-1", viewMode === 'extracted' ? "bg-white shadow-sm text-gray-900" : "text-gray-500 hover:text-gray-700")}
                                                title="Backend Text Extraction"
                                            >
                                                <RefreshCw size={12} />
                                                Text Extract
                                            </button>
                                        </>
                                    )}
                                </div>
                            )}
                            <button onClick={onClose} className="p-1 hover:bg-gray-100 rounded-full text-gray-400 hover:text-gray-600 transition-colors">
                                <X size={20} />
                            </button>
                        </div>
                    </div>

                    {/* Breadcrumbs (Only show if stack has items) */}
                    {!loading && !file?.error && stack.length > 0 && (
                        <div className="flex items-center gap-2 px-4 pb-3 text-sm overflow-x-auto whitespace-nowrap scrollbar-hide">
                            {stack.length > 1 && (
                                <button onClick={goBack} className="p-1 hover:bg-gray-100 rounded text-gray-600 mr-2 shrink-0">
                                    <ChevronLeft size={16} />
                                </button>
                            )}
                            <div className="flex items-center">
                                {stack.map((view, i) => (
                                    <div key={i} className="flex items-center">
                                        {i > 0 && <ChevronRight size={14} className="text-gray-400 mx-1" />}
                                        <button
                                            onClick={() => popTo(i)}
                                            className={clsx(
                                                "px-2 py-0.5 rounded transition-colors text-gray-700 flex items-center gap-1",
                                                i === stack.length - 1 ? "font-bold text-blue-600 bg-blue-50" : "hover:bg-gray-100"
                                            )}
                                        >
                                            {i === 0 ? <Home size={14} /> : view.name}
                                        </button>
                                    </div>
                                ))}
                            </div>
                        </div>
                    )}
                </div>

                {/* Content Area */}
                <div className="flex-1 overflow-auto bg-gray-50 font-mono text-sm relative">
                    {loading ? (
                        <div className="flex flex-col items-center justify-center h-full text-gray-400">
                            <Loader2 size={32} className="animate-spin mb-2" />
                            <p>Fetching content...</p>
                        </div>
                    ) : (
                        <>
                            {viewMode === 'json' && !isJsonError && jsonData ? (
                                <div className="p-4 bg-white min-h-full">
                                    <JsonRenderer data={jsonData} onDrillDown={pushView} />
                                </div>
                            ) : viewMode === 'html' ? (
                                <div className="w-full h-full bg-white relative">
                                    <iframe
                                        srcDoc={(() => {
                                            if (typeof contentToDisplay !== 'string') return '';
                                            const txt = document.createElement('textarea');
                                            txt.innerHTML = contentToDisplay;
                                            return txt.value;
                                        })()}
                                        className="w-full h-full border-none"
                                        sandbox=""
                                        title="Safe Preview"
                                    />
                                </div>
                            ) : viewMode === 'extracted' ? (
                                <div className="p-4 bg-gray-50 min-h-full">
                                    {isExtracting ? (
                                        <div className="flex flex-col items-center justify-center p-8 text-gray-500">
                                            <Loader2 size={24} className="animate-spin mb-2" />
                                            <span>Extracting text via backend...</span>
                                        </div>
                                    ) : (
                                        <div className="bg-white p-4 rounded shadow-sm border border-gray-200">
                                            <div className="mb-2 text-xs font-semibold text-gray-500 uppercase tracking-wider flex items-center gap-2">
                                                <RefreshCw size={12} />
                                                Backend Text Extraction
                                            </div>
                                            <pre className="whitespace-pre-wrap break-all text-gray-800 font-mono text-sm">
                                                {extractedText || <span className="text-gray-400 italic">No extracted text returned.</span>}
                                            </pre>
                                        </div>
                                    )}
                                </div>
                            ) : (
                                <div className="p-4">
                                    {viewMode === 'json' && isJsonError && (
                                        <div className="mb-4 p-3 bg-yellow-50 text-yellow-800 text-xs rounded border border-yellow-200 flex items-center gap-2">
                                            <AlertCircle size={14} />
                                            <span>Could not parse content as JSON. Showing raw text.</span>
                                        </div>
                                    )}
                                    <pre className="whitespace-pre-wrap break-all text-gray-700">
                                        {file?.error ? (
                                            <span className="text-red-600">Error: {file.error}</span>
                                        ) : (
                                            (typeof contentToDisplay === 'string' ? contentToDisplay : JSON.stringify(contentToDisplay, null, 2)) || <span className="text-gray-400 italic">No content</span>
                                        )}
                                    </pre>
                                </div>
                            )}
                        </>
                    )}
                </div>

                <div className="p-3 border-t border-gray-100 bg-white rounded-b-xl flex justify-end">
                    <button onClick={onClose} className="px-4 py-2 text-sm font-medium text-gray-600 hover:bg-gray-50 rounded-lg">
                        Close
                    </button>
                </div>
            </div>
        </div>
    );
};

const LeafRow = ({ input, displayField, onView }) => {
    // Dynamic Label logic
    const fieldLabel = displayField ? displayField.charAt(0).toUpperCase() + displayField.slice(1) : '';
    const label = displayField && input.metadata?.[displayField]
        ? `${fieldLabel}: ${input.metadata[displayField]}`
        : input.id.split('/').pop();

    return (
        <div
            onClick={() => onView(input)}
            className="flex items-center gap-3 p-2 ml-6 hover:bg-gray-50 rounded text-sm group transition-colors cursor-pointer"
        >
            <FileJson size={14} className="text-gray-400 group-hover:text-blue-500 transition-colors" />
            <div className="flex-1 min-w-0 grid grid-cols-2 gap-4">
                <div className="truncate text-gray-600 font-mono text-xs group-hover:text-blue-600 transition-colors" title={input.etag}>
                    {label}
                </div>
                <div className="text-gray-400 text-xs text-right">
                    {new Date(input.last_seen || Date.now()).toLocaleString()}
                </div>
            </div>
            <StatusBadge status={input.status} size="xs" />
        </div>
    );
};

const LazyTreeNode = ({ node, level = 0, loadedNodes, loadNode, displayField, onView }) => {
    const [expanded, setExpanded] = useState(false);
    const hasChildren = node.type === 'GROUP';
    const children = loadedNodes[node.id];

    // Derive display label from ID (last part)
    const label = node.key || node.id.split('/').pop();

    const handleExpand = (e) => {
        e.stopPropagation();
        setExpanded(!expanded);
        if (!expanded && hasChildren) {
            loadNode(node.id);
        }
    };

    if (hasChildren) {
        return (
            <div className="ml-4">
                <div
                    onClick={handleExpand}
                    className="flex items-center gap-2 cursor-pointer hover:bg-gray-50 p-2 rounded select-none transition-colors border-l-2 border-transparent hover:border-gray-200"
                >
                    {expanded ? <ChevronDown size={14} className="text-gray-400" /> : <ChevronRight size={14} className="text-gray-400" />}
                    <Folder size={16} className="text-blue-400 fill-blue-50" />
                    <span className="font-medium text-sm text-gray-700">{label}</span>
                    <div className="text-xs text-gray-400 font-mono flex items-center gap-1">
                        <span>{node.count} items</span>
                    </div>
                </div>
                {expanded && (
                    <div className="border-l border-gray-100 ml-2 pl-2">
                        {children ? (
                            children.map((child, i) => (
                                child.type === 'GROUP' ? (
                                    <LazyTreeNode
                                        key={child.id || i}
                                        node={child}
                                        level={level + 1}
                                        loadedNodes={loadedNodes}
                                        loadNode={loadNode}
                                        displayField={displayField}
                                        onView={onView}
                                    />
                                ) : (
                                    <LeafRow key={child.id || i} input={child} displayField={displayField} onView={onView} />
                                )
                            ))
                        ) : (
                            <div className="p-2 text-gray-400 text-xs flex items-center gap-2">
                                <Loader2 size={12} className="animate-spin" /> Loading...
                            </div>
                        )}
                    </div>
                )}
            </div>
        );
    }
    return <LeafRow input={node} displayField={displayField} onView={onView} />;
};

function Inputs() {
    const { lastLog } = useOutletContext();

    const [loading, setLoading] = useState(false);
    const [status, setStatus] = useState(null);
    const [isOperator, setIsOperator] = useState(false);
    const [isAdmin, setIsAdmin] = useState(false);

    // System State
    const [systemStatus, setSystemStatus] = useState('IDLE');
    const [currentJob, setCurrentJob] = useState(null);
    const [inputs, setInputs] = useState([]);
    const [inputsLoading, setInputsLoading] = useState(true);

    // Config & Tree State
    const [config, setConfig] = useState([]);
    const [selectedInputType, setSelectedInputType] = useState(null);

    // File Viewer State
    const [viewFile, setViewFile] = useState(null);
    const [viewLoading, setViewLoading] = useState(false);

    useEffect(() => {
        checkPermission();
        fetchStatus();
        fetchConfig();
    }, []);

    // Listen for WebSocket Updates
    useEffect(() => {
        if (!lastLog) return;
        if (lastLog.jobType === 'METRIC') return;
        if (lastLog.jobType && lastLog.jobType.startsWith('CONTAINER')) return;

        if (lastLog.state === 'RUNNING' || lastLog.state === 'QUEUED') {
            if (systemStatus !== 'RUNNING') setSystemStatus('RUNNING');
            setCurrentJob({
                jobType: lastLog.jobType,
                processed: lastLog.processed,
                total: lastLog.total,
                message: lastLog.message
            });
        } else if (lastLog.state === 'COMPLETED') {
            if (systemStatus === 'RUNNING') {
                setSystemStatus('IDLE');
                setStatus({ type: 'success', message: 'Job completed successfully.' });
                // Do NOT call fetchStatus() here to avoid race condition with stale DB state
                // fetchStatus(); 

                // If inputs were purged or catalogued, reload inputs
                if (selectedInputType) fetchInputs(selectedInputType);
                setLoadedNodes({}); // Clear expanded nodes cache
            }
        } else if (lastLog.state === 'FAILED') {
            setSystemStatus('IDLE');
            setStatus({ type: 'error', message: 'Job failed: ' + lastLog.message });
        }
    }, [lastLog, systemStatus]);

    const checkPermission = async () => {
        try {
            const session = await fetchAuthSession();
            const groups = session.tokens?.accessToken.payload['cognito:groups'] || [];
            if (groups.includes('operator')) setIsOperator(true);
            if (groups.includes('admin')) setIsAdmin(true);
        } catch (e) { console.error(e); }
    };

    const fetchConfig = async () => {
        try {
            const session = await fetchAuthSession();
            const token = session.tokens?.idToken?.toString();
            if (!token) return;

            const response = await get({
                apiName: 'CurioApi',
                path: 'config',
                options: { headers: { Authorization: token } }
            }).response;

            const data = await response.body.json();
            setConfig(data);
            if (data.length > 0) setSelectedInputType(data[0].name);
        } catch (e) {
            console.error("Failed to fetch config", e);
        }
    };

    useEffect(() => {
        if (selectedInputType) {
            fetchInputs(selectedInputType);
        }
    }, [selectedInputType]);

    const fetchInputs = async (type) => {
        setInputsLoading(true);
        try {
            const session = await fetchAuthSession();
            const token = session.tokens?.idToken?.toString();
            if (!token) return;

            const parentId = `external/${type}`;
            const response = await get({
                apiName: 'CurioApi',
                path: `catalog?parentId=${encodeURIComponent(parentId)}`,
                options: { headers: { Authorization: token } }
            }).response;

            const data = await response.body.json();
            // If data is list (backward compat) or object depending on API?
            // API returns { items: [] } or just []?
            // stack.py for NO ID returns: { items: ... } wait... no.
            // stack.py returns `items` (list) directly?
            // Let's check stack.py return. 
            // It puts `items` in body... "body": json.dumps(items)
            // But if query_params are present, it does the same?
            // stack.py:
            // items = []
            // ... logic to fill items ...
            // return { body: json.dumps(items) }
            // So it returns an ARRAY.

            // Wait, helper loadNode used: data.items??
            // loadNode: const data = await response.body.json(); setLoadedNodes(..., data.items)
            // stack.py returns LIST.
            // I need to check loadNode again.
            // loadNode might be broken if API returns list.

            // Let's assume API returns LIST based on stack.py reading.
            // "items = [] ... items.append(...) ... body: json.dumps(items)"

            setInputs(Array.isArray(data) ? data : (data.items || []));
        } catch (e) {
            console.error("Failed to fetch inputs", e);
            setInputs([]);
        } finally {
            setInputsLoading(false);
        }
    };


    // Lazy Loading Logic
    const [loadedNodes, setLoadedNodes] = useState({}); // { [parentId]: [children] }

    const loadNode = async (parentId) => {
        if (loadedNodes[parentId]) return;

        try {
            const session = await fetchAuthSession();
            const token = session.tokens?.idToken?.toString();
            if (!token) return;

            const response = await get({
                apiName: 'CurioApi',
                path: `catalog?parentId=${encodeURIComponent(parentId)}`,
                options: { headers: { Authorization: token } }
            }).response;

            const data = await response.body.json();
            const items = Array.isArray(data) ? data : (data.items || []);
            setLoadedNodes(prev => ({ ...prev, [parentId]: items }));
        } catch (e) {
            console.error(`Failed to load children for ${parentId}`, e);
        }
    };

    const fetchStatus = async () => {
        try {
            const session = await fetchAuthSession();
            const token = session.tokens?.idToken?.toString();
            if (!token) return;

            const response = await get({
                apiName: 'CurioApi',
                path: 'catalog',
                options: { headers: { Authorization: token } }
            }).response;

            const data = await response.body.json();

            if (data.status === 'RUNNING') {
                setSystemStatus('RUNNING');
                setCurrentJob(data);
            } else {
                setSystemStatus('IDLE');
                setCurrentJob(null);
                // setInputs handled by fetchInputs now
            }
        } catch (e) {
            console.error("Failed to fetch status", e);
        }
    };

    const handleCatalogTags = async () => {
        setLoading(true); setStatus(null);
        try {
            const session = await fetchAuthSession();
            const token = session.tokens?.idToken?.toString();
            if (!token) throw new Error("No authentication token available");
            await post({ apiName: 'CurioApi', path: 'catalog', options: { headers: { Authorization: token } } }).response;
            setSystemStatus('RUNNING');
            setCurrentJob({ jobType: 'CATALOG', processed: 0, total: 0, message: 'Starting...' });
            setStatus({ type: 'success', message: 'Catalog job started.' });
        } catch (e) { setStatus({ type: 'error', message: e.message }); } finally { setLoading(false); }
    };

    const handleViewFile = async (input) => {
        setViewFile({ id: input.id }); // Show modal immediately with loading
        setViewLoading(true);

        try {
            const session = await fetchAuthSession();
            const token = session.tokens?.idToken?.toString();
            if (!token) return;

            const response = await get({
                apiName: 'CurioApi',
                path: `content?id=${encodeURIComponent(input.id)}`,
                options: { headers: { Authorization: token } }
            }).response;

            const data = await response.body.json();

            // Decode HTML entities for display?
            // Actually, <pre> will display entities literally if we don't be careful.
            // Backend returns HTML Escaped string: "&lt;xml..."
            // If we put that in children, React escapes it AGAIN.
            // So we get "&amp;lt;xml..." rendered as "&lt;xml..."
            // We want to render the TEXT content.
            // So we should decode it on the client, OR use dangerouslySetInnerHTML (unsafe).
            // Better: Backend sends Sanitized content, but maybe acts as plaintext?
            // If backend html.escapes, it's safe for browser to render as HTML.
            // But we are rendering inside <pre>.
            // If we render "&lt;" inside <pre>, it shows "<".
            // Wait.
            // Backend: html.escape("<") -> "&lt;"
            // React: <code>{"&lt;"}</code> -> Renders "&lt;" on screen.
            // We want it to render "<".
            // So we should use a temporary textarea to decode, or just trust backend didn't need to double-escape?
            // Actually, if we want to show CODE, we want the original content but strictly as text.
            // If I just display `data.content` in React `{data.content}`, React will escape it.
            // So if `data.content` is ALREADY escaped by backend, it gets double escaped.
            // Backend should probably just return the raw text if it's JSON?
            // JSON string is safe.
            // The constraint "backend must sanitize" implies returning safe HTML?
            // Or just ensure it doesn't execute script when viewed?
            // If we use <pre>{content}</pre>, React handles safety.
            // So logic:
            // 1. Backend: Read S3. Return JSON { content: "raw string" }.
            // 2. React: <pre>{content}</pre>. React escapes it. Safe.
            // BUT User requirement: "untrusted ... sanitized on backend".
            // This usually means backend ensures no malicious scripts.
            // html.escape is good for HTML contexts.
            // If I change backend to NOT escape, rely on React, it's safe.
            // If backend escapes, I need to unescape in React to show correct chars.
            // Let's assume backend DOES escape as per plan.
            // To display correctly in React: 
            // <div dangerouslySetInnerHTML={{ __html: data.content }} /> inside <pre>?
            // This treats the backend-sanitized string as HTML.
            // e.g. Backend: "&lt;script&gt;" -> safe text.
            // React: <div html="&lt;script&gt;"> -> Renders "<script>" visible to user, not executed.
            // This seems correct for "Backend Sanitization".

            setViewFile({ id: input.id, content: data.content, isHtml: true });
        } catch (e) {
            console.error("Failed to fetch content", e);
            setViewFile(prev => ({ ...prev, error: e.message }));
        } finally {
            setViewLoading(false);
        }
    };

    const handlePurgeInputs = async () => {
        if (!confirm("Are you sure?")) return;
        setLoading(true); setStatus(null);
        try {
            const session = await fetchAuthSession();
            const token = session.tokens?.idToken?.toString();
            if (!token) throw new Error("No authentication token available");
            await del({ apiName: 'CurioApi', path: 'catalog', options: { headers: { Authorization: token } } }).response;
            setSystemStatus('RUNNING');
            setCurrentJob({ jobType: 'PURGE', processed: 0, total: 0, message: 'Starting purge...' });
            setStatus({ type: 'success', message: 'Purge initiated.' });
        } catch (e) { setStatus({ type: 'error', message: e.message }); } finally { setLoading(false); }
    };

    // --- Tree View Helpers ---
    const aggregateStatus = (items) => {
        if (!items || items.length === 0) return 'UNKNOWN';
        const statuses = items.map(i => i.status);
        if (statuses.every(s => s === 'OBSOLETE')) return 'OBSOLETE';
        if (statuses.some(s => s === 'DIRTY')) return 'DIRTY';
        if (statuses.every(s => s === 'CLEAN' || !s)) return 'CLEAN';
        return 'MIXED';
    };




    // --- Render ---
    const currentConfig = config.find(c => c.name === selectedInputType);
    const displayField = currentConfig?.display_field;

    return (
        <>
            <header className="mb-8 flex justify-between items-center">
                <div>
                    <h1 className="text-2xl font-bold text-gray-900">Inputs</h1>
                    <p className="text-gray-500 text-sm mt-1">Manage system inputs and configurations</p>
                </div>
                <div className="flex gap-2">
                    <button onClick={() => { fetchStatus(); fetchConfig(); }} className="p-2 text-gray-400 hover:text-gray-600 rounded-lg hover:bg-gray-100"><RefreshCw size={18} className={clsx(inputsLoading && "animate-spin")} /></button>
                    {isAdmin && <button onClick={handlePurgeInputs} disabled={loading || systemStatus === 'RUNNING'} className={clsx("flex items-center gap-2 px-4 py-2 bg-red-600 text-white rounded-lg text-sm font-medium hover:bg-red-700", (loading || systemStatus === 'RUNNING') && "opacity-75 hidden")}><Trash2 size={16} />Purge</button>}
                    {isOperator && <button onClick={handleCatalogTags} disabled={loading || systemStatus === 'RUNNING'} className={clsx("flex items-center gap-2 px-4 py-2 bg-indigo-600 text-white rounded-lg text-sm font-medium hover:bg-indigo-700", (loading || systemStatus === 'RUNNING') && "opacity-75")}><Play size={16} />{systemStatus === 'RUNNING' ? 'Running...' : 'Catalog'}</button>}
                </div>
            </header>

            {status && (
                <div className={clsx("p-4 rounded-lg mb-6 border flex items-start gap-3", status.type === 'success' ? "bg-green-50 text-green-700 border-green-200" : "bg-red-50 text-red-700 border-red-200")}>
                    {status.type === 'success' ? <CheckCircle size={18} /> : <AlertCircle size={18} />}
                    <p className="text-sm font-medium">{status.message}</p>
                </div>
            )}

            {systemStatus === 'RUNNING' ? (
                <div className="bg-white border border-gray-200 rounded-xl shadow-sm p-6 mb-8">
                    <div className="flex items-center gap-4 mb-4">
                        <div className="w-10 h-10 rounded-full bg-blue-100 flex items-center justify-center text-blue-600"><Loader2 size={20} className="animate-spin" /></div>
                        <div><h3 className="text-lg font-medium text-gray-900">{currentJob?.jobType}</h3><p className="text-sm text-gray-500">{currentJob?.message}</p></div>
                    </div>
                    {currentJob && currentJob.total > 0 && (
                        <div className="w-full bg-gray-100 rounded-full h-2 overflow-hidden">
                            <div className="bg-blue-600 h-2 rounded-full transition-all duration-500" style={{ width: `${(currentJob.processed / currentJob.total) * 100}%` }}></div>
                        </div>
                    )}
                </div>
            ) : (
                <div className="bg-white border border-gray-200 rounded-xl shadow-sm min-h-[400px] flex flex-col">
                    {config.length > 0 && (
                        <div className="flex border-b border-gray-200 px-6 pt-4 gap-6">
                            {config.map(c => (
                                <button
                                    key={c.name}
                                    onClick={() => setSelectedInputType(c.name)}
                                    className={clsx(
                                        "pb-4 text-sm font-medium transition-colors relative",
                                        selectedInputType === c.name ? "text-blue-600" : "text-gray-500 hover:text-gray-700"
                                    )}
                                >
                                    {c.name}
                                    {selectedInputType === c.name && <div className="absolute bottom-0 left-0 right-0 h-0.5 bg-blue-600 rounded-t-full" />}
                                </button>
                            ))}
                        </div>
                    )}

                    <div className="p-6 overflow-y-auto flex-1">
                        {!selectedInputType ? (
                            inputs.length === 0 ? (
                                <div className="text-center text-gray-500 py-12">
                                    <Database className="mx-auto mb-3 text-gray-300" size={48} />
                                    <p>No inputs found. Run catalog.</p>
                                </div>
                            ) : (
                                <div className="space-y-1">
                                    {inputs.map(input => (
                                        <LeafRow key={input.id} input={input} onView={handleViewFile} />
                                    ))}
                                </div>
                            )
                        ) : !currentConfig ? (
                            <div className="text-center text-gray-500 py-12">No config found for type.</div>
                        ) : (
                            inputs.length === 0 ? (
                                <div className="text-center text-gray-500 py-8 italic">No inputs found for {selectedInputType}</div>
                            ) : (
                                <div className="space-y-1">
                                    {inputs.filter(i => i.id.includes(selectedInputType) && i.type === 'GROUP').map((node, i) => (
                                        <LazyTreeNode
                                            key={node.id}
                                            node={node}
                                            loadedNodes={loadedNodes}
                                            loadNode={loadNode}
                                            displayField={displayField}
                                            onView={handleViewFile}
                                        />
                                    ))}
                                    {/* Handle root items that are not groups but match type (unlikely for harvest_jobs but possible) */}
                                    {inputs.filter(i => i.id.includes(selectedInputType) && i.type !== 'GROUP').map(input => (
                                        <LeafRow key={input.id} input={input} displayField={displayField} onView={handleViewFile} />
                                    ))}
                                </div>
                            )
                        )}
                    </div>
                    <div className="bg-gray-50 px-6 py-3 border-t border-gray-200 text-xs text-gray-500 flex justify-between">
                        <span>{selectedInputType ? `Showing ${selectedInputType}` : 'All Inputs'}</span>
                        <span>Total: {inputs.length}</span>
                    </div>
                </div>
            )}

            {viewFile && (
                <FileModal
                    file={viewFile}
                    loading={viewLoading}
                    onClose={() => setViewFile(null)}
                />
            )}
        </>
    );
}
export default Inputs;
