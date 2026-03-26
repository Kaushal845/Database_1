import React, { useState, useEffect } from 'react';
import axios from 'axios';

function FieldPlacements({ apiBase }) {
  const [placements, setPlacements] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    fetchPlacements();
  }, []);

  const fetchPlacements = async () => {
    try {
      const response = await axios.get(`${apiBase}/api/dashboard/field-placements`);
      setPlacements(response.data.placements);
      setError(null);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  if (loading) return <div className="loading">Loading field information...</div>;
  if (error) return <div className="error">Error: {error}</div>;

  return (
    <div className="card">
      <h2>Field Information ({placements.length} fields)</h2>

      {placements.length === 0 ? (
        <p style={{ textAlign: 'center', color: '#666', padding: '40px' }}>
          No fields found
        </p>
      ) : (
        <div style={{ overflowX: 'auto' }}>
          <table>
            <thead>
              <tr>
                <th>Field Name</th>
                <th>Status</th>
                <th>Count</th>
                <th>Details</th>
              </tr>
            </thead>
            <tbody>
              {placements.map(placement => (
                <tr key={placement.field_name}>
                  <td><strong>{placement.field_name}</strong></td>
                  <td>
                    <span className={`badge ${placement.status === 'final' ? 'badge-sql' : 'badge-buffer'}`}>
                      {placement.status}
                    </span>
                  </td>
                  <td>{placement.statistics.count}</td>
                  <td>
                    <details>
                      <summary style={{ cursor: 'pointer', color: '#007bff' }}>View</summary>
                      <div style={{ marginTop: '10px', fontSize: '12px' }}>
                        <div><strong>First Seen:</strong> {placement.statistics.first_seen || 'N/A'}</div>
                        <div><strong>Last Seen:</strong> {placement.statistics.last_seen || 'N/A'}</div>

                        {placement.statistics.semantic_types && Object.keys(placement.statistics.semantic_types).length > 0 && (
                          <div style={{ marginTop: '8px' }}>
                            <strong>Semantic Types:</strong>
                            <ul style={{ marginLeft: '20px', marginTop: '4px' }}>
                              {Object.entries(placement.statistics.semantic_types).map(([type, count]) => (
                                <li key={type}>{type}: {count}</li>
                              ))}
                            </ul>
                          </div>
                        )}
                      </div>
                    </details>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

export default FieldPlacements;
