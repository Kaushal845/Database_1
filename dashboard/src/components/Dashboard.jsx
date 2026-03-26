import React, { useState, useEffect } from 'react';
import axios from 'axios';

function Dashboard({ apiBase }) {
  const [summary, setSummary] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    fetchSummary();
    const interval = setInterval(fetchSummary, 5000);
    return () => clearInterval(interval);
  }, []);

  const fetchSummary = async () => {
    try {
      const response = await axios.get(`${apiBase}/api/dashboard/summary`);
      setSummary(response.data.summary);
      setError(null);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  if (loading) return <div className="loading">Loading dashboard...</div>;
  if (error) return <div className="error">Error: {error}</div>;
  if (!summary) return <div className="loading">No data available</div>;

  return (
    <div>
      <div className="card">
        <h2>Database Records</h2>
        <div className="stats-grid">
          <div className="stat-card">
            <div className="stat-label">Total Records</div>
            <div className="stat-value">{summary.total_records.total}</div>
          </div>
          <div className="stat-card buffer">
            <div className="stat-label">Buffered Records</div>
            <div className="stat-value">{summary.total_records.buffered}</div>
          </div>
          <div className="stat-card">
            <div className="stat-label">Total Processed</div>
            <div className="stat-value">{summary.total_records.total_processed}</div>
          </div>
        </div>
      </div>

      <div className="card">
        <h2>Field Information</h2>
        <div className="stats-grid">
          <div className="stat-card">
            <div className="stat-label">Total Fields Discovered</div>
            <div className="stat-value">{summary.field_count.total_fields}</div>
          </div>
          <div className="stat-card">
            <div className="stat-label">Mapped Fields</div>
            <div className="stat-value">{summary.field_count.mapped_fields}</div>
          </div>
        </div>
      </div>

      <div className="card">
        <h2>Database Objects</h2>
        <div className="stats-grid">
          <div className="stat-card">
            <div className="stat-label">Total Database Tables</div>
            <div className="stat-value">{summary.database_objects.total_tables}</div>
          </div>
        </div>
      </div>

      <div className="card">
        <h2>Pipeline Statistics</h2>
        <table>
          <tbody>
            <tr>
              <td><strong>Total Processed</strong></td>
              <td>{summary.pipeline_stats.total_processed}</td>
            </tr>
            <tr>
              <td><strong>Total Inserts</strong></td>
              <td>{summary.pipeline_stats.total_inserts}</td>
            </tr>
            <tr>
              <td><strong>Buffer Inserts</strong></td>
              <td>{summary.pipeline_stats.buffer_inserts}</td>
            </tr>
            <tr>
              <td><strong>Errors</strong></td>
              <td style={{ color: summary.pipeline_stats.errors > 0 ? '#c62828' : 'inherit' }}>
                {summary.pipeline_stats.errors}
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default Dashboard;
