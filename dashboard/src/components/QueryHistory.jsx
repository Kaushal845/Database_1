import React, { useState, useEffect } from 'react';
import axios from 'axios';

function QueryHistory({ apiBase }) {
    const [history, setHistory] = useState([]);
    const [total, setTotal] = useState(0);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        fetchHistory();
        const interval = setInterval(fetchHistory, 5000);
        return () => clearInterval(interval);
    }, []);

    const fetchHistory = async () => {
        try {
            const response = await axios.get(`${apiBase}/api/dashboard/query-history?limit=200`);
            setHistory(response.data.history || []);
            setTotal(response.data.total || 0);
            setError(null);
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    const clearHistory = async () => {
        try {
            await axios.delete(`${apiBase}/api/dashboard/query-history`);
            setHistory([]);
            setTotal(0);
        } catch (err) {
            setError(err.message);
        }
    };

    const formatTimestamp = (ts) => {
        if (!ts) return 'N/A';
        try {
            return new Date(ts).toLocaleString();
        } catch {
            return ts;
        }
    };

    const formatFilters = (filters) => {
        if (!filters || Object.keys(filters).length === 0) return '—';
        return JSON.stringify(filters);
    };

    const getLatencyColor = (ms) => {
        if (ms < 50) return '#28a745';
        if (ms < 200) return '#ff9800';
        return '#c62828';
    };

    // Compute summary stats
    const successCount = history.filter(h => h.success).length;
    const failCount = history.filter(h => !h.success).length;
    const avgLatency = history.length > 0
        ? (history.reduce((sum, h) => sum + (h.latency_ms || 0), 0) / history.length).toFixed(1)
        : 0;
    const opCounts = {};
    history.forEach(h => {
        opCounts[h.operation] = (opCounts[h.operation] || 0) + 1;
    });

    if (loading) return <div className="loading">Loading query history...</div>;
    if (error) return <div className="error">Error: {error}</div>;

    return (
        <div>
            {/* Summary Stats */}
            <div className="card">
                <h2>Query Execution History</h2>
                <p style={{ color: '#666', marginBottom: '15px' }}>
                    History of all logical queries executed through the query interface. Latency includes
                    metadata routing, backend queries, and result merging.
                </p>
                <div className="stats-grid">
                    <div className="stat-card">
                        <div className="stat-label">Total Queries</div>
                        <div className="stat-value">{total}</div>
                    </div>
                    <div className="stat-card" style={{ background: 'linear-gradient(135deg, #43a047 0%, #2e7d32 100%)' }}>
                        <div className="stat-label">Successful</div>
                        <div className="stat-value">{successCount}</div>
                    </div>
                    <div className="stat-card" style={{ background: failCount > 0 ? 'linear-gradient(135deg, #e53935 0%, #c62828 100%)' : undefined }}>
                        <div className="stat-label">Failed</div>
                        <div className="stat-value">{failCount}</div>
                    </div>
                    <div className="stat-card" style={{ background: 'linear-gradient(135deg, #1e88e5 0%, #1565c0 100%)' }}>
                        <div className="stat-label">Avg Latency</div>
                        <div className="stat-value">{avgLatency} ms</div>
                    </div>
                </div>
            </div>

            {/* Operation breakdown */}
            {Object.keys(opCounts).length > 0 && (
                <div className="card">
                    <h2>Operations Breakdown</h2>
                    <div style={{ display: 'flex', gap: '15px', flexWrap: 'wrap' }}>
                        {Object.entries(opCounts).map(([op, count]) => (
                            <div key={op} style={{
                                padding: '10px 20px',
                                backgroundColor: op === 'read' ? '#e3f2fd' : op === 'insert' ? '#e8f5e9' : op === 'update' ? '#fff3e0' : '#ffebee',
                                borderRadius: '8px',
                                fontWeight: 'bold',
                                fontSize: '14px',
                            }}>
                                {op.toUpperCase()}: {count}
                            </div>
                        ))}
                    </div>
                </div>
            )}

            {/* History Table */}
            <div className="card">
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '15px' }}>
                    <h2 style={{ borderBottom: 'none', paddingBottom: 0, marginBottom: 0 }}>
                        Recent Queries ({history.length})
                    </h2>
                    {history.length > 0 && (
                        <button onClick={clearHistory} style={{ background: '#c62828', fontSize: '12px', padding: '6px 14px' }}>
                            Clear History
                        </button>
                    )}
                </div>

                {history.length === 0 ? (
                    <p style={{ textAlign: 'center', color: '#666', padding: '40px' }}>
                        No queries executed yet. Use the Query Builder tab to run queries.
                    </p>
                ) : (
                    <div style={{ overflowX: 'auto' }}>
                        <table>
                            <thead>
                                <tr>
                                    <th>Time</th>
                                    <th>Operation</th>
                                    <th>Filters</th>
                                    <th>Latency</th>
                                    <th>Status</th>
                                    <th>Records</th>
                                </tr>
                            </thead>
                            <tbody>
                                {history.map((entry, idx) => (
                                    <tr key={idx} style={{ opacity: entry.success ? 1 : 0.8 }}>
                                        <td style={{ fontSize: '12px', whiteSpace: 'nowrap' }}>
                                            {formatTimestamp(entry.timestamp)}
                                        </td>
                                        <td>
                                            <span style={{
                                                padding: '3px 8px',
                                                borderRadius: '4px',
                                                fontSize: '11px',
                                                fontWeight: 'bold',
                                                color: 'white',
                                                backgroundColor:
                                                    entry.operation === 'read' ? '#1976d2' :
                                                        entry.operation === 'insert' ? '#388e3c' :
                                                            entry.operation === 'update' ? '#f57c00' :
                                                                entry.operation === 'delete' ? '#c62828' : '#666'
                                            }}>
                                                {entry.operation?.toUpperCase()}
                                            </span>
                                        </td>
                                        <td style={{ fontSize: '12px', fontFamily: 'monospace', maxWidth: '250px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                                            {formatFilters(entry.filters)}
                                        </td>
                                        <td style={{ fontWeight: 'bold', color: getLatencyColor(entry.latency_ms) }}>
                                            {entry.latency_ms?.toFixed(1)} ms
                                        </td>
                                        <td>
                                            {entry.success ? (
                                                <span style={{ color: '#2e7d32', fontWeight: 'bold' }}>✓ OK</span>
                                            ) : (
                                                <span style={{ color: '#c62828', fontWeight: 'bold' }} title={entry.error}>✗ FAIL</span>
                                            )}
                                        </td>
                                        <td style={{ textAlign: 'center' }}>
                                            {entry.record_count ?? '—'}
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                )}
            </div>
        </div>
    );
}

export default QueryHistory;
