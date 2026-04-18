import React, { useState, useEffect } from 'react';
import axios from 'axios';

function PerformanceView({ apiBase }) {
    const [benchmarks, setBenchmarks] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        fetchBenchmarks();
    }, []);

    const fetchBenchmarks = async () => {
        try {
            const response = await axios.get(`${apiBase}/api/dashboard/benchmark-results`);
            setBenchmarks(response.data.benchmarks || {});
            setError(null);
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    };

    // Pure CSS bar chart renderer
    const BarChart = ({ items, valueKey, labelKey, unit, maxValue, color }) => {
        const maxLabelChars = Math.max(...items.map(i => String(i[labelKey] ?? '').length), 10);
        const LABEL_COL_WIDTH = Math.min(240, Math.max(150, maxLabelChars * 7 + 18));
        const max = maxValue || Math.max(...items.map(i => i[valueKey] || 0)) * 1.1 || 1;
        return (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                {items.map((item, idx) => (
                    <div key={idx} style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                        <div
                            title={String(item[labelKey] ?? '')}
                            style={{
                                flex: `0 0 ${LABEL_COL_WIDTH}px`,
                                width: `${LABEL_COL_WIDTH}px`,
                                minWidth: `${LABEL_COL_WIDTH}px`,
                                maxWidth: `${LABEL_COL_WIDTH}px`,
                                fontSize: '13px',
                                fontWeight: 'bold',
                                textAlign: 'left',
                                whiteSpace: 'nowrap',
                                overflow: 'hidden',
                                textOverflow: 'ellipsis',
                            }}
                        >
                            {item[labelKey]}
                        </div>
                        <div style={{ flex: 1, height: '28px', backgroundColor: '#e0e0e0', borderRadius: '4px', overflow: 'hidden', position: 'relative' }}>
                            <div style={{
                                height: '100%',
                                width: `${Math.min(((item[valueKey] || 0) / max) * 100, 100)}%`,
                                background: color || `linear-gradient(90deg, #667eea 0%, #764ba2 100%)`,
                                borderRadius: '4px',
                                transition: 'width 0.5s ease',
                                display: 'flex',
                                alignItems: 'center',
                                justifyContent: 'flex-end',
                                paddingRight: '8px',
                            }}>
                                <span style={{ color: 'white', fontSize: '11px', fontWeight: 'bold', whiteSpace: 'nowrap' }}>
                                    {typeof item[valueKey] === 'number' ? item[valueKey].toFixed(2) : item[valueKey]} {unit}
                                </span>
                            </div>
                        </div>
                    </div>
                ))}
            </div>
        );
    };

    // SVG Line chart renderer 
    const LineChart = ({ data, xKey, yKey, xLabel, yLabel }) => {
        if (!data || data.length === 0) return null;
        const maxY = Math.max(...data.map(d => d[yKey])) * 1.15 || 1;
        const chartHeight = 200;
        const chartWidth = 500;
        const points = data.map((d, i) => ({
            x: (i / Math.max(data.length - 1, 1)) * chartWidth,
            y: chartHeight - (d[yKey] / maxY) * chartHeight,
            ...d,
        }));

        return (
            <div style={{ display: 'flex', justifyContent: 'center', margin: '20px 0' }}>
                <svg width={chartWidth + 80} height={chartHeight + 60} style={{ overflow: 'visible' }}>
                    {/* Y axis grid lines */}
                    {[0, 0.25, 0.5, 0.75, 1].map(frac => (
                        <g key={frac}>
                            <line x1={60} y1={20 + chartHeight * (1 - frac)} x2={60 + chartWidth} y2={20 + chartHeight * (1 - frac)}
                                stroke="#e0e0e0" strokeDasharray="4" />
                            <text x={55} y={25 + chartHeight * (1 - frac)} textAnchor="end" fontSize="11" fill="#666">
                                {Math.round(maxY * frac)}
                            </text>
                        </g>
                    ))}
                    {/* Line */}
                    <path d={`M ${points.map(p => `${p.x + 60} ${p.y + 20}`).join(' L ')}`}
                        fill="none" stroke="#1976d2" strokeWidth="3" />
                    {/* Points */}
                    {points.map((p, i) => (
                        <g key={i}>
                            <circle cx={p.x + 60} cy={p.y + 20} r="6" fill="#1976d2" />
                            <text x={p.x + 60} y={p.y + 8} textAnchor="middle" fontSize="11" fill="#333" fontWeight="bold">
                                {p[yKey]}
                            </text>
                            <text x={p.x + 60} y={chartHeight + 40} textAnchor="middle" fontSize="11" fill="#666">
                                {p[xKey]}
                            </text>
                        </g>
                    ))}
                    {/* Axis labels */}
                    <text x={60 + chartWidth / 2} y={chartHeight + 58} textAnchor="middle" fontSize="12" fill="#333" fontWeight="bold">
                        {xLabel}
                    </text>
                    <text x={15} y={20 + chartHeight / 2} textAnchor="middle" fontSize="12" fill="#333" fontWeight="bold"
                        transform={`rotate(-90, 15, ${20 + chartHeight / 2})`}>
                        {yLabel}
                    </text>
                </svg>
            </div>
        );
    };

    // Section heading style
    const sectionHeadingStyle = {
        background: 'linear-gradient(135deg, #1a237e 0%, #283593 100%)',
        color: 'white',
        padding: '20px 25px',
        borderRadius: '8px',
        marginBottom: '0',
        marginTop: '20px',
    };

    const sectionSubtextStyle = {
        color: 'rgba(255,255,255,0.85)', fontSize: '14px', marginTop: '5px', marginBottom: 0
    };

    if (loading) return <div className="loading">Loading benchmark results...</div>;
    if (error) return <div className="error">Error: {error}</div>;

    const hasPerformance = benchmarks?.performance && !benchmarks.performance.error;
    const hasComparative = benchmarks?.comparative && !benchmarks.comparative.error;
    const hasIngestion = benchmarks?.ingestion && !benchmarks.ingestion.error;
    const hasAnyData = hasPerformance || hasComparative || hasIngestion;

    return (
        <div>
            {/* Introduction */}
            <div className="card">
                <h2>Performance & Comparative Analysis</h2>
                <p style={{ color: '#666', marginBottom: '15px' }}>
                    This tab presents the results of the performance evaluation (Section 3) and comparative
                    analysis (Section 4) experiments for the hybrid SQL/MongoDB framework. The benchmarks
                    measure the impact of the logical abstraction layer on system performance and compare it
                    against direct database access.
                </p>
                {!hasAnyData && (
                    <div style={{ textAlign: 'center', padding: '40px', color: '#666' }}>
                        <p style={{ fontSize: '18px', marginBottom: '10px' }}>No benchmark results available yet.</p>
                        <p>Run the benchmarks to generate data:</p>
                        <pre style={{ backgroundColor: '#f5f5f5', padding: '15px', borderRadius: '4px', textAlign: 'left', display: 'inline-block', marginTop: '10px' }}>
                            {`.venv\\Scripts\\python.exe performance_benchmark.py
.venv\\Scripts\\python.exe comparative_benchmark.py`}
                        </pre>
                    </div>
                )}
            </div>

            {/* ================================================================== */}
            {/* SECTION 3: PERFORMANCE ANALYSIS                                     */}
            {/* ================================================================== */}
            {hasPerformance && (
                <>
                    <div style={sectionHeadingStyle}>
                        <h2 style={{ margin: 0, color: 'white', fontSize: '22px' }}>
                            Section 3: Performance Analysis
                        </h2>
                        <p style={sectionSubtextStyle}>
                            Evaluation of the hybrid framework's performance across data ingestion,
                            query response time, metadata lookup, transaction coordination, data distribution,
                            and throughput scaling.
                        </p>
                    </div>

                    {/* 3.1 Ingestion Latency */}
                    {benchmarks.performance.ingestion && (
                        <div className="card">
                            <h2>3.1 Data Ingestion Latency</h2>
                            <p style={{ color: '#666', marginBottom: '15px' }}>
                                Time to ingest records through the full pipeline: type detection, placement heuristics,
                                normalization, and dual-backend writes (SQL + MongoDB).
                            </p>
                            <BarChart
                                items={benchmarks.performance.ingestion}
                                valueKey="avg_ms"
                                labelKey="label"
                                unit="ms/record"
                                color="linear-gradient(90deg, #1e88e5 0%, #1565c0 100%)"
                            />
                            <div style={{ overflowX: 'auto', marginTop: '20px' }}>
                                <table>
                                    <thead>
                                        <tr>
                                            <th>Batch Size</th>
                                            <th>Records</th>
                                            <th>Total (ms)</th>
                                            <th>Avg (ms/record)</th>
                                            <th>Throughput (rec/s)</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {benchmarks.performance.ingestion.map((row, i) => (
                                            <tr key={i}>
                                                <td><strong>{row.label}</strong></td>
                                                <td>{row.count}</td>
                                                <td>{row.total_ms?.toFixed(1)}</td>
                                                <td style={{ fontWeight: 'bold' }}>{row.avg_ms?.toFixed(2)}</td>
                                                <td>{row.throughput?.toFixed(1)}</td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    )}

                    {/* 3.2 Query Response Time */}
                    {benchmarks.performance.queries && (
                        <div className="card">
                            <h2>3.2 Logical Query Response Time</h2>
                            <p style={{ color: '#666', marginBottom: '15px' }}>
                                Latency for logical read, update, and delete operations through the framework's query engine.
                                Each operation involves metadata routing, multi-backend execution, and result merging.
                            </p>
                            <BarChart
                                items={benchmarks.performance.queries}
                                valueKey="avg_ms"
                                labelKey="label"
                                unit="ms"
                                color="linear-gradient(90deg, #43a047 0%, #2e7d32 100%)"
                            />
                            <div style={{ overflowX: 'auto', marginTop: '20px' }}>
                                <table>
                                    <thead>
                                        <tr>
                                            <th>Operation</th>
                                            <th>Iterations</th>
                                            <th>Avg (ms)</th>
                                            <th>Min (ms)</th>
                                            <th>Max (ms)</th>
                                            <th>P95 (ms)</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {benchmarks.performance.queries.map((row, i) => (
                                            <tr key={i}>
                                                <td><strong>{row.label}</strong></td>
                                                <td>{row.iterations}</td>
                                                <td style={{ fontWeight: 'bold' }}>{row.avg_ms?.toFixed(2)}</td>
                                                <td>{row.min_ms?.toFixed(2)}</td>
                                                <td>{row.max_ms?.toFixed(2)}</td>
                                                <td>{row.p95_ms?.toFixed(2)}</td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    )}

                    {/* 3.3 Metadata Lookup Overhead */}
                    {benchmarks.performance.metadata && (
                        <div className="card">
                            <h2>3.3 Metadata Lookup Overhead</h2>
                            <p style={{ color: '#666', marginBottom: '15px' }}>
                                Time for in-memory metadata lookups that drive every query operation: field mapping resolution
                                and placement decision lookup. These are sub-millisecond, contributing negligible overhead.
                            </p>
                            <BarChart
                                items={benchmarks.performance.metadata}
                                valueKey="avg_ms"
                                labelKey="label"
                                unit="ms"
                                color="linear-gradient(90deg, #ef6c00 0%, #e65100 100%)"
                            />
                            <div style={{ overflowX: 'auto', marginTop: '20px' }}>
                                <table>
                                    <thead>
                                        <tr>
                                            <th>Operation</th>
                                            <th>Iterations</th>
                                            <th>Avg (ms)</th>
                                            <th>Min (ms)</th>
                                            <th>Max (ms)</th>
                                            <th>P95 (ms)</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {benchmarks.performance.metadata.map((row, i) => (
                                            <tr key={i}>
                                                <td><strong>{row.label}</strong></td>
                                                <td>{row.iterations}</td>
                                                <td style={{ fontWeight: 'bold' }}>{row.avg_ms?.toFixed(4)}</td>
                                                <td>{row.min_ms?.toFixed(4)}</td>
                                                <td>{row.max_ms?.toFixed(4)}</td>
                                                <td>{row.p95_ms?.toFixed(4)}</td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    )}

                    {/* 3.4 Transaction Coordination Overhead */}
                    {benchmarks.performance.transactions && (
                        <div className="card">
                            <h2>3.4 Transaction Coordination Overhead</h2>
                            <p style={{ color: '#666', marginBottom: '15px' }}>
                                Overhead of 2-phase commit (begin &rarr; prepare &rarr; commit) across SQL and MongoDB
                                versus direct operations without transactional coordination.
                            </p>
                            <BarChart
                                items={benchmarks.performance.transactions}
                                valueKey="avg_ms"
                                labelKey="label"
                                unit="ms"
                                color="linear-gradient(90deg, #8e24aa 0%, #6a1b9a 100%)"
                            />
                        </div>
                    )}

                    {/* 3.5 Data Distribution Across Backends */}
                    {benchmarks.performance.data_distribution && (
                        <div className="card">
                            <h2>3.5 Distribution of Data Across Storage Backends</h2>
                            <p style={{ color: '#666', marginBottom: '15px' }}>
                                How the framework automatically distributes fields and records across SQL and MongoDB
                                based on metadata-driven placement heuristics (structured &rarr; SQL, nested/complex &rarr; MongoDB).
                            </p>
                            <div className="stats-grid">
                                <div className="stat-card" style={{ borderLeft: '4px solid #1976d2' }}>
                                    <div className="stat-label">SQL Fields</div>
                                    <div className="stat-value">{benchmarks.performance.data_distribution.field_distribution.sql}</div>
                                </div>
                                <div className="stat-card" style={{ borderLeft: '4px solid #388e3c' }}>
                                    <div className="stat-label">MongoDB Fields</div>
                                    <div className="stat-value">{benchmarks.performance.data_distribution.field_distribution.mongodb}</div>
                                </div>
                                <div className="stat-card" style={{ borderLeft: '4px solid #f57c00' }}>
                                    <div className="stat-label">Both Backends</div>
                                    <div className="stat-value">{benchmarks.performance.data_distribution.field_distribution.both}</div>
                                </div>
                                <div className="stat-card" style={{ borderLeft: '4px solid #7b1fa2' }}>
                                    <div className="stat-label">Buffer</div>
                                    <div className="stat-value">{benchmarks.performance.data_distribution.field_distribution.buffer}</div>
                                </div>
                            </div>

                            <BarChart
                                items={[
                                    { label: 'SQL', count: benchmarks.performance.data_distribution.field_distribution.sql },
                                    { label: 'MongoDB', count: benchmarks.performance.data_distribution.field_distribution.mongodb },
                                    { label: 'Both', count: benchmarks.performance.data_distribution.field_distribution.both },
                                    { label: 'Buffer', count: benchmarks.performance.data_distribution.field_distribution.buffer },
                                ]}
                                valueKey="count"
                                labelKey="label"
                                unit="fields"
                                color="linear-gradient(90deg, #26c6da 0%, #00838f 100%)"
                            />

                            <div style={{ overflowX: 'auto', marginTop: '20px' }}>
                                <h3>Record Distribution</h3>
                                <table>
                                    <thead>
                                        <tr><th>Storage</th><th>Record Count</th></tr>
                                    </thead>
                                    <tbody>
                                        <tr><td><strong>SQL (main table)</strong></td><td>{benchmarks.performance.data_distribution.record_counts.sql_main_table}</td></tr>
                                        <tr><td><strong>MongoDB (main collection)</strong></td><td>{benchmarks.performance.data_distribution.record_counts.mongodb_main_collection}</td></tr>
                                        <tr><td><strong>Buffer (pending)</strong></td><td>{benchmarks.performance.data_distribution.record_counts.buffer_collection}</td></tr>
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    )}

                    {/* 3.6 Throughput Scaling */}
                    {benchmarks.performance.throughput_scaling && (
                        <div className="card">
                            <h2>3.6 Throughput (Operations Per Second)</h2>
                            <p style={{ color: '#666', marginBottom: '15px' }}>
                                Read throughput measured under increasing workload sizes. Consistent ops/s values
                                indicate the framework scales linearly for read operations.
                            </p>
                            <LineChart
                                data={benchmarks.performance.throughput_scaling.map(d => ({
                                    ...d,
                                    xLabel: `${d.workload_ops} ops`,
                                }))}
                                xKey="xLabel"
                                yKey="throughput_ops_per_s"
                                xLabel="Workload Size"
                                yLabel="ops/s"
                            />
                            <div style={{ overflowX: 'auto', marginTop: '10px' }}>
                                <table>
                                    <thead>
                                        <tr><th>Workload (ops)</th><th>Elapsed (s)</th><th>Throughput (ops/s)</th></tr>
                                    </thead>
                                    <tbody>
                                        {benchmarks.performance.throughput_scaling.map((row, i) => (
                                            <tr key={i}>
                                                <td><strong>{row.workload_ops}</strong></td>
                                                <td>{row.elapsed_s?.toFixed(3)}</td>
                                                <td style={{ fontWeight: 'bold', color: '#1976d2' }}>{row.throughput_ops_per_s?.toFixed(1)}</td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    )}

                    {/* 3.7 Performance Analysis Summary */}
                    <div className="card">
                        <h2>3.7 Analysis: How the Abstraction Layer Affects Performance</h2>
                        <div style={{ lineHeight: '1.8', fontSize: '14px' }}>
                            <p><strong>Ingestion:</strong> Per-record latency is dominated by the framework's multi-step pipeline: type detection, placement heuristics, normalization of nested entities, and dual-backend writes. This is inherently slower than a raw single-backend INSERT, but provides automatic schema evolution and intelligent data routing.</p>
                            <p><strong>Queries:</strong> Read operations through the logical layer involve metadata routing, parallel SQL/MongoDB execution, and result merging. The sub-5ms average latency is acceptable for most applications and provides the key benefit of backend-agnostic data access.</p>
                            <p><strong>Metadata Lookups:</strong> Field mapping and placement decisions are served from in-memory dictionaries in sub-microsecond time, contributing negligible overhead to each operation.</p>
                            <p><strong>Transactions:</strong> The 2-phase commit protocol adds coordination overhead (SQL savepoints + MongoDB temp collection writes) but ensures ACID atomicity across both backends, which would require complex custom implementation with direct access.</p>
                            <p><strong>Data Distribution:</strong> The framework intelligently routes structured, scalar fields to SQL and complex/nested fields to MongoDB, demonstrating effective use of each backend's strengths.</p>
                            <p><strong>Throughput:</strong> Read throughput remains consistent as workload scales, indicating the framework's metadata-driven routing does not degrade under increasing load.</p>
                        </div>
                    </div>
                </>
            )}

            {/* ================================================================== */}
            {/* SECTION 4: COMPARATIVE ANALYSIS                                     */}
            {/* ================================================================== */}
            {hasComparative && (
                <>
                    <div style={{ ...sectionHeadingStyle, background: 'linear-gradient(135deg, #b71c1c 0%, #c62828 100%)' }}>
                        <h2 style={{ margin: 0, color: 'white', fontSize: '22px' }}>
                            Section 4: Comparative Analysis
                        </h2>
                        <p style={sectionSubtextStyle}>
                            Comparison of the hybrid framework's logical abstraction layer versus direct SQL and MongoDB
                            access. Measures query latency, update latency, and query processing overhead.
                        </p>
                    </div>

                    {/* 4.1-4.N Comparison Bar Charts */}
                    {benchmarks.comparative.comparisons && benchmarks.comparative.comparisons.map((comp, idx) => (
                        <div className="card" key={idx}>
                            <h2>4.{idx + 1} {comp.test_name}</h2>
                            <p style={{ color: '#666', marginBottom: '15px' }}>
                                {idx === 0 && 'Retrieving user records through the logical query interface vs direct SQL SELECT and MongoDB find.'}
                                {idx === 1 && 'Accessing nested documents (orders + profile) using the framework vs direct MongoDB queries with projection.'}
                                {idx === 2 && 'Updating records across multiple entities through the framework vs direct SQL UPDATE and MongoDB $set.'}
                                {idx === 3 && 'Inserting new records through the full ingestion pipeline vs direct single-backend INSERT operations.'}
                            </p>
                            <BarChart
                                items={comp.results}
                                valueKey="avg_ms"
                                labelKey="method"
                                unit="ms"
                            />
                            {comp.overhead_pct !== undefined && (
                                <div style={{
                                    marginTop: '10px', padding: '8px 15px', borderRadius: '4px',
                                    backgroundColor: comp.overhead_pct < 50 ? '#e8f5e9' : comp.overhead_pct < 200 ? '#fff3e0' : '#ffebee',
                                    display: 'inline-block', fontSize: '13px'
                                }}>
                                    Framework overhead vs combined direct access: <strong>{comp.overhead_pct?.toFixed(1)}%</strong>
                                </div>
                            )}

                            {/* Detailed table per comparison */}
                            <div style={{ overflowX: 'auto', marginTop: '15px' }}>
                                <table>
                                    <thead>
                                        <tr>
                                            <th>Method</th>
                                            <th>Avg (ms)</th>
                                            <th>Min (ms)</th>
                                            <th>Max (ms)</th>
                                            <th>P95 (ms)</th>
                                            <th>Iterations</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {comp.results.map((r, ri) => (
                                            <tr key={ri}>
                                                <td><strong>{r.method}</strong></td>
                                                <td style={{ fontWeight: 'bold' }}>{r.avg_ms?.toFixed(3)}</td>
                                                <td>{r.min_ms?.toFixed(3)}</td>
                                                <td>{r.max_ms?.toFixed(3)}</td>
                                                <td>{r.p95_ms?.toFixed(3)}</td>
                                                <td>{r.iterations}</td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    ))}

                    {/* 4.N+1 Summary Table */}
                    {benchmarks.comparative.summary && (
                        <div className="card">
                            <h2>4.{(benchmarks.comparative.comparisons?.length || 0) + 1} Summary: Performance Metrics</h2>
                            <p style={{ color: '#666', marginBottom: '15px' }}>
                                Consolidated comparison of query latency, update latency, and processing overhead
                                across all experiments.
                            </p>
                            <div style={{ overflowX: 'auto' }}>
                                <table>
                                    <thead>
                                        <tr>
                                            <th>Metric</th>
                                            <th>Framework (ms)</th>
                                            <th>Direct SQL (ms)</th>
                                            <th>Direct MongoDB (ms)</th>
                                            <th>Overhead</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {benchmarks.comparative.summary.map((row, i) => (
                                            <tr key={i}>
                                                <td><strong>{row.metric}</strong></td>
                                                <td>{row.framework_ms?.toFixed(2)}</td>
                                                <td>{row.direct_sql_ms != null ? row.direct_sql_ms.toFixed(2) : 'N/A'}</td>
                                                <td>{row.direct_mongo_ms?.toFixed(2)}</td>
                                                <td style={{ fontWeight: 'bold', color: row.overhead_pct < 50 ? '#2e7d32' : row.overhead_pct < 200 ? '#f57c00' : '#c62828' }}>
                                                    {row.overhead_pct?.toFixed(1)}%
                                                </td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    )}

                    {/* 4.N+2 Discussion */}
                    <div className="card">
                        <h2>4.{(benchmarks.comparative.comparisons?.length || 0) + 2} Discussion: Overhead vs Value</h2>
                        <div style={{ lineHeight: '1.8', fontSize: '14px' }}>
                            <h3 style={{ color: '#c62828', marginTop: '10px' }}>Where the abstraction adds overhead</h3>
                            <ul>
                                <li><strong>Read operations:</strong> The framework performs metadata lookups, routes queries to both SQL and MongoDB, and merges results. This adds measurable latency compared to a single direct query.</li>
                                <li><strong>Ingestion:</strong> The framework runs type detection, placement heuristics, normalization, and dual-backend writes — significantly slower than a single raw INSERT.</li>
                                <li><strong>Updates:</strong> The framework routes fields to the correct backend based on metadata, updating SQL and MongoDB separately. Direct updates on a single backend are naturally faster.</li>
                            </ul>

                            <h3 style={{ color: '#2e7d32', marginTop: '20px' }}>Where the abstraction provides value</h3>
                            <ul>
                                <li><strong>Unified access:</strong> Users query a single logical interface without knowing which backend stores each field.</li>
                                <li><strong>Automatic schema evolution:</strong> New fields are automatically detected, classified, and routed to the optimal backend.</li>
                                <li><strong>Nested entity management:</strong> Arrays are normalized into child tables, embedded documents stay in MongoDB — all transparently.</li>
                                <li><strong>ACID guarantees:</strong> The 2-phase commit ensures atomic operations across both backends.</li>
                                <li><strong>Backend-agnostic applications:</strong> Application code doesn't change if data moves between backends.</li>
                            </ul>

                            <h3 style={{ marginTop: '20px' }}>When to use the framework vs direct access</h3>
                            <div style={{ overflowX: 'auto' }}>
                                <table>
                                    <thead><tr><th>Scenario</th><th>Recommended Approach</th></tr></thead>
                                    <tbody>
                                        <tr><td>Schema-flexible, evolving data</td><td><strong>Framework</strong></td></tr>
                                        <tr><td>High-throughput, fixed-schema writes</td><td><strong>Direct backend</strong></td></tr>
                                        <tr><td>Cross-backend queries (SQL + MongoDB)</td><td><strong>Framework</strong></td></tr>
                                        <tr><td>Single-backend, latency-critical reads</td><td><strong>Direct backend</strong></td></tr>
                                        <tr><td>Need ACID across SQL + MongoDB</td><td><strong>Framework</strong></td></tr>
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    </div>
                </>
            )}

            {/* Legacy Ingestion Benchmark (if present and no performance data) */}
            {hasIngestion && !hasPerformance && benchmarks.ingestion.results && (
                <div className="card">
                    <h2>Ingestion Throughput (Batch Size Scaling)</h2>
                    <BarChart
                        items={benchmarks.ingestion.results.map(r => ({
                            label: `${r.batch_size} records`,
                            avg_ms: r.per_record_ms || 0,
                        }))}
                        valueKey="avg_ms"
                        labelKey="label"
                        unit="ms/record"
                        color="linear-gradient(90deg, #ff9800 0%, #f57c00 100%)"
                    />
                </div>
            )}
        </div>
    );
}

export default PerformanceView;
