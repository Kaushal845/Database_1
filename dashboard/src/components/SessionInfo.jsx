import React, { useState, useEffect } from 'react';
import axios from 'axios';

function SessionInfo({ apiBase }) {
  const [session, setSession] = useState(null);
  const [transactionStatus, setTransactionStatus] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 5000); // Refresh every 5 seconds
    return () => clearInterval(interval);
  }, []);

  const fetchData = async () => {
    try {
      const [sessionResponse, txResponse] = await Promise.all([
        axios.get(`${apiBase}/api/dashboard/session`),
        axios.get(`${apiBase}/api/dashboard/transaction-status`)
      ]);

      setSession(sessionResponse.data.session);
      setTransactionStatus(txResponse.data.transaction_system);
      setError(null);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const formatTimestamp = (timestamp) => {
    if (!timestamp) return 'N/A';
    try {
      const date = new Date(timestamp);
      return date.toLocaleString();
    } catch {
      return timestamp;
    }
  };

  if (loading) return <div className="loading">Loading session information...</div>;
  if (error) return <div className="error">Error: {error}</div>;
  if (!session) return <div className="loading">No session data available</div>;

  return (
    <div>
      {transactionStatus && (
        <div className="card">
          <h2>Transaction System Status</h2>
          <div style={{
            padding: '20px',
            backgroundColor: transactionStatus.enabled ? '#e8f5e9' : '#fff3e0',
            borderRadius: '8px',
            marginBottom: '20px'
          }}>
            <div style={{
              display: 'flex',
              alignItems: 'center',
              gap: '10px',
              marginBottom: '10px'
            }}>
              <span style={{
                fontSize: '24px',
                fontWeight: 'bold',
                color: transactionStatus.enabled ? '#2e7d32' : '#f57c00'
              }}>
                {transactionStatus.enabled ? '✓ ACID TRANSACTIONS ENABLED' : '⚠ TRANSACTIONS DISABLED'}
              </span>
            </div>
            <p style={{ margin: 0, color: '#666', fontSize: '14px' }}>
              {transactionStatus.message}
            </p>
            {transactionStatus.enabled && (
              <div style={{ marginTop: '15px' }}>
                <strong>Guarantees:</strong>
                <ul style={{ marginTop: '8px', marginLeft: '20px', fontSize: '14px' }}>
                  <li>All CRUD operations are atomic across all storage backends</li>
                  <li>Automatic rollback on any failure - no partial writes</li>
                  <li>Full data consistency maintained across the system</li>
                </ul>
              </div>
            )}
            {transactionStatus.active_transactions > 0 && (
              <div style={{ marginTop: '15px', padding: '10px', backgroundColor: '#fff', borderRadius: '4px' }}>
                <strong>Active Transactions:</strong> {transactionStatus.active_transactions}
              </div>
            )}
          </div>
        </div>
      )}

      <div className="card">
        <h2>Active Session Information</h2>

        <div className="stats-grid">
          <div className="stat-card">
            <div className="stat-label">Session Duration</div>
            <div className="stat-value" style={{ fontSize: '28px' }}>{session.duration || 'N/A'}</div>
          </div>
          <div className="stat-card">
            <div className="stat-label">Records Processed</div>
            <div className="stat-value">{session.activity?.total_records || 0}</div>
          </div>
          <div className="stat-card">
            <div className="stat-label">Fields Discovered</div>
            <div className="stat-value">{session.activity?.total_fields_discovered || 0}</div>
          </div>
          <div className="stat-card">
            <div className="stat-label">Entities Discovered</div>
            <div className="stat-value">{session.activity?.total_entities_discovered || 0}</div>
          </div>
        </div>
      </div>

      <div className="card">
        <h2>Session Timeline</h2>
        <table>
          <tbody>
            <tr>
              <td><strong>Session Started</strong></td>
              <td>{formatTimestamp(session.session_start)}</td>
            </tr>
            <tr>
              <td><strong>Last Activity</strong></td>
              <td>{formatTimestamp(session.last_updated)}</td>
            </tr>
            <tr>
              <td><strong>Duration</strong></td>
              <td>{session.duration || 'N/A'}</td>
            </tr>
          </tbody>
        </table>
      </div>

      <div className="card">
        <h2>Activity Summary</h2>
        <table>
          <tbody>
            <tr>
              <td><strong>Total Records Processed</strong></td>
              <td>{session.activity?.total_records || 0}</td>
            </tr>
            <tr>
              <td><strong>Unique Fields Discovered</strong></td>
              <td>{session.activity?.total_fields_discovered || 0}</td>
            </tr>
            <tr>
              <td><strong>Logical Entities Identified</strong></td>
              <td>{session.activity?.total_entities_discovered || 0}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default SessionInfo;
