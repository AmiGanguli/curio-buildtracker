import React from 'react';

export const JsonRenderer = ({ data, onDrillDown }) => {

    // Helper to determine if value is complex (object/array)
    const isComplex = (v) => v !== null && typeof v === 'object';

    // Helper for simple rendering in list/dict view
    const renderSimple = (v) => {
        if (v === null) return <span className="text-gray-400">null</span>;
        if (typeof v === 'boolean') return <span className="text-purple-600">{v.toString()}</span>;
        if (typeof v === 'number') return <span className="text-orange-600">{v}</span>;
        if (typeof v === 'string') return <span className="text-green-700">"{v}"</span>;
        return String(v);
    };

    const value = data;

    if (value === null) return <span className="text-gray-400">null</span>;
    if (typeof value === 'boolean') return <span className="text-purple-600">{value.toString()}</span>;
    if (typeof value === 'number') return <span className="text-orange-600">{value}</span>;
    if (typeof value === 'string') {
        // Strings are now drill-down targets too (if caller allows it, but here we just show content)
        // If this component is used to render the "current view", and the current view IS a string,
        // it means we drilled down into it.
        return (
            <pre className="whitespace-pre-wrap break-all text-gray-700 bg-gray-50 p-2 rounded border border-gray-100 font-mono text-sm">
                {value}
            </pre>
        );
    }

    if (Array.isArray(value)) {
        if (value.length === 0) return <span className="text-gray-400">[]</span>;
        return (
            <div className="space-y-1">
                {value.map((item, index) => (
                    <div key={index} className="flex gap-2 group">
                        <span className="text-gray-400 font-mono w-8 text-right shrink-0 select-none">[{index}]</span>
                        <div className="flex-1">
                            {isComplex(item) ? (
                                <button
                                    onClick={() => onDrillDown(`[${index}]`, item)}
                                    className="text-blue-600 hover:underline font-mono text-left"
                                >
                                    {Array.isArray(item) ? `Array(${item.length})` : 'Object {...}'}
                                </button>
                            ) : (
                                // Start of Deep Navigation for strings
                                typeof item === 'string' ? (
                                    <button
                                        onClick={() => onDrillDown(`[${index}]`, item)}
                                        className="text-green-700 hover:underline font-mono text-left break-all"
                                    >
                                        "{item}"
                                    </button>
                                ) : (
                                    <span className="font-mono">{renderSimple(item)}</span>
                                )
                            )}
                        </div>
                    </div>
                ))}
            </div>
        );
    }

    if (typeof value === 'object') {
        const keys = Object.keys(value);
        if (keys.length === 0) return <span className="text-gray-400">{'{}'}</span>;
        return (
            <div className="space-y-1">
                {keys.map((key) => {
                    const item = value[key];
                    return (
                        <div key={key} className="flex gap-2 group">
                            <span className="text-purple-700 font-semibold font-mono shrink-0">{key}:</span>
                            <div className="flex-1 truncate">
                                {isComplex(item) ? (
                                    <button
                                        onClick={() => onDrillDown(key, item)}
                                        className="text-blue-600 hover:underline font-mono text-left"
                                    >
                                        {Array.isArray(item) ? `Array(${item.length})` : 'Object {...}'}
                                    </button>
                                ) : (
                                    // Start of Deep Navigation for strings
                                    typeof item === 'string' ? (
                                        <button
                                            onClick={() => onDrillDown(key, item)}
                                            className="text-green-700 hover:underline font-mono text-left truncate block w-full"
                                            title={item}
                                        >
                                            "{item}"
                                        </button>
                                    ) : (
                                        <span className="font-mono">{renderSimple(item)}</span>
                                    )
                                )}
                            </div>
                        </div>
                    );
                })}
            </div>
        );
    }

    return <span>{String(value)}</span>;
};
