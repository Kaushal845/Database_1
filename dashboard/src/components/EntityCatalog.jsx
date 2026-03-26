import React, { useState, useEffect } from 'react';
import axios from 'axios';

function EntityCatalog({ apiBase }) {
  const [entities, setEntities] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [filter, setFilter] = useState('all');

  useEffect(() => {
    fetchEntities();
  }, []);

  const fetchEntities = async () => {
    try {
      const response = await axios.get(`${apiBase}/api/dashboard/entities`);
      setEntities(response.data.entities);
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

  const filteredEntities = filter === 'all'
    ? entities
    : entities.filter(e => e.type === filter);

  if (loading) return <div className="loading">Loading entity catalog...</div>;
  if (error) return <div className="error">Error: {error}</div>;

  return (
    <div>
      <div className="card">
        <h2>Entity Catalog ({entities.length} logical entities)</h2>
        <p style={{ color: '#666', marginBottom: '20px' }}>
          Logical entities discovered from your data. These represent structured objects
          and repeating groups that have been automatically identified and organized.
        </p>

        <div style={{ marginBottom: '20px' }}>
          <label>Filter by type:</label>
          <select value={filter} onChange={(e) => setFilter(e.target.value)}>
            <option value="all">All Entity Types</option>
            <option value="repeating">Repeating Groups</option>
            <option value="nested">Nested Objects</option>
          </select>
        </div>

        {filteredEntities.length === 0 ? (
          <p style={{ textAlign: 'center', color: '#666', padding: '40px' }}>
            No entities found for selected filter
          </p>
        ) : (
          <div style={{ overflowX: 'auto' }}>
            <table>
              <thead>
                <tr>
                  <th>Entity Name</th>
                  <th>Type</th>
                  <th>Instances</th>
                  <th>Fields</th>
                  <th>Schema</th>
                </tr>
              </thead>
              <tbody>
                {filteredEntities.map(entity => (
                  <tr key={entity.name}>
                    <td><strong>{entity.name}</strong></td>
                    <td>
                      <span className={`badge ${entity.type === 'repeating' ? 'badge-sql' : 'badge-mongodb'}`}>
                        {entity.type}
                      </span>
                    </td>
                    <td>{entity.instance_count}</td>
                    <td>{entity.fields.length} fields</td>
                    <td>
                      <details>
                        <summary style={{ cursor: 'pointer', color: '#007bff' }}>View Schema</summary>
                        <div style={{ marginTop: '10px', fontSize: '12px' }}>
                          <div style={{ marginBottom: '10px' }}>
                            <strong>Registered:</strong> {formatTimestamp(entity.registered_at)}
                          </div>
                          <div>
                            <strong>Fields ({entity.fields.length}):</strong>
                            <ul style={{ marginLeft: '20px', marginTop: '8px' }}>
                              {entity.fields.map(field => (
                                <li key={field} style={{ margin: '4px 0' }}>
                                  <code style={{
                                    backgroundColor: '#f0f0f0',
                                    padding: '2px 6px',
                                    borderRadius: '3px',
                                    fontSize: '11px'
                                  }}>
                                    {field}
                                  </code>
                                </li>
                              ))}
                            </ul>
                          </div>
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

      <div className="card">
        <h2>Entity Type Summary</h2>
        <div className="stats-grid">
          <div className="stat-card">
            <div className="stat-label">Total Entities</div>
            <div className="stat-value">{entities.length}</div>
          </div>
          <div className="stat-card sql">
            <div className="stat-label">Repeating Groups</div>
            <div className="stat-value">
              {entities.filter(e => e.type === 'repeating').length}
            </div>
          </div>
          <div className="stat-card mongo">
            <div className="stat-label">Nested Objects</div>
            <div className="stat-value">
              {entities.filter(e => e.type === 'nested').length}
            </div>
          </div>
          <div className="stat-card">
            <div className="stat-label">Total Instances</div>
            <div className="stat-value">
              {entities.reduce((sum, e) => sum + (e.instance_count || 0), 0)}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

export default EntityCatalog;
