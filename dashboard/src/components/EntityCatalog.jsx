import React, { useState, useEffect } from 'react';
import axios from 'axios';

function EntityCatalog({ apiBase }) {
  const [entities, setEntities] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [filter, setFilter] = useState('all');
  const [expandedEntity, setExpandedEntity] = useState(null);
  const [expandedField, setExpandedField] = useState(null);

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

  const formatValue = (value) => {
    if (value === null || value === undefined) return 'N/A';
    if (typeof value === 'object') {
      return JSON.stringify(value).substring(0, 50) + (JSON.stringify(value).length > 50 ? '...' : '');
    }
    return String(value).substring(0, 100) + (String(value).length > 100 ? '...' : '');
  };

  const getTypeColor = (type) => {
    const colors = {
      'string': '#0066cc',
      'number': '#009933',
      'boolean': '#ff6600',
      'array': '#990099',
      'object': '#cc0000',
      'datetime': '#006666',
      'date': '#006666',
      'uuid': '#663300',
      'unknown': '#666666'
    };
    return colors[type] || '#666666';
  };

  const getCardinalityBadge = (cardinality) => {
    if (cardinality === 'required') {
      return <span style={{ color: '#009933', fontWeight: 'bold', fontSize: '12px' }}>✓ required</span>;
    }
    return <span style={{ color: '#ff6600', fontWeight: 'bold', fontSize: '12px' }}>◇ optional</span>;
  };

  const filteredEntities = filter === 'all'
    ? entities
    : entities.filter(e => e.type === filter);

  if (loading) return <div className="loading">Loading entity catalog...</div>;
  if (error) return <div className="error">Error: {error}</div>;

  return (
    <div>
      {/* Header Section */}
      <div className="card">
        <h2>Entity Catalog</h2>
        <p style={{ color: '#666', marginBottom: '20px' }}>
          Logical entities discovered from your data. These represent structured objects
          and repeating groups with complete schema information including field types, coverage, and relationships.
        </p>

        <div style={{ marginBottom: '20px' }}>
          <label htmlFor="entity-filter">Filter by type:</label>
          <select 
            id="entity-filter"
            value={filter} 
            onChange={(e) => setFilter(e.target.value)}
            style={{ marginLeft: '10px', padding: '8px', borderRadius: '4px' }}
          >
            <option value="all">All Entity Types</option>
            <option value="repeating">Repeating Groups</option>
            <option value="nested">Nested Objects</option>
          </select>
        </div>
      </div>

      {/* Summary Stats */}
      <div className="card">
        <div className="stats-grid">
          <div className="stat-card">
            <div className="stat-label">Total Entities</div>
            <div className="stat-value">{entities.length}</div>
          </div>
          <div className="stat-card" style={{ borderLeft: '4px solid #007bff' }}>
            <div className="stat-label">Repeating Groups</div>
            <div className="stat-value">
              {entities.filter(e => e.type === 'repeating').length}
            </div>
          </div>
          <div className="stat-card" style={{ borderLeft: '4px solid #28a745' }}>
            <div className="stat-label">Nested Objects</div>
            <div className="stat-value">
              {entities.filter(e => e.type === 'nested').length}
            </div>
          </div>
          <div className="stat-card">
            <div className="stat-label">Total Instances</div>
            <div className="stat-value">
              {entities.reduce((sum, e) => sum + (e.instance_count || 0), 0).toLocaleString()}
            </div>
          </div>
        </div>
      </div>

      {/* Entity Details */}
      {filteredEntities.length === 0 ? (
        <div className="card">
          <p style={{ textAlign: 'center', color: '#666', padding: '40px' }}>
            No entities found for selected filter
          </p>
        </div>
      ) : (
        <div>
          {filteredEntities.map(entity => (
            <div key={entity.name} className="card" style={{ marginBottom: '20px' }}>
              {/* Entity Header */}
              <div
                onClick={() => setExpandedEntity(expandedEntity === entity.name ? null : entity.name)}
                style={{
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'center',
                  cursor: 'pointer',
                  paddingBottom: '15px',
                  borderBottom: expandedEntity === entity.name ? '2px solid #007bff' : 'none'
                }}
              >
                <div style={{ flex: 1 }}>
                  <h3 style={{ margin: '0 0 8px 0', display: 'flex', alignItems: 'center', gap: '10px' }}>
                    <span>{entity.name}</span>
                    <span style={{
                      backgroundColor: entity.type === 'repeating' ? '#007bff' : '#28a745',
                      color: 'white',
                      padding: '2px 8px',
                      borderRadius: '12px',
                      fontSize: '12px',
                      fontWeight: 'normal'
                    }}>
                      {entity.type}
                    </span>
                  </h3>
                  <div style={{ fontSize: '14px', color: '#666', display: 'flex', gap: '20px' }}>
                    <span><strong>{entity.instance_count?.toLocaleString() || 0}</strong> instances</span>
                    <span><strong>{entity.field_count}</strong> fields</span>
                    {entity.relationships?.parent && (
                      <span style={{ color: '#0066cc' }}>Parent: <strong>{entity.relationships.parent}</strong></span>
                    )}
                    {entity.relationships?.children?.length > 0 && (
                      <span style={{ color: '#009933' }}>Contains: <strong>{entity.relationships.children.length}</strong> child {entity.relationships.children.length > 1 ? 'entities' : 'entity'}</span>
                    )}
                  </div>
                </div>
                <div style={{ fontSize: '20px', color: '#666' }}>
                  {expandedEntity === entity.name ? '▼' : '▶'}
                </div>
              </div>

              {/* Entity Details Section */}
              {expandedEntity === entity.name && (
                <div style={{ paddingTop: '15px' }}>
                  {/* Fields Table */}
                  <div style={{ marginBottom: '20px' }}>
                    <h4 style={{ marginTop: '0', marginBottom: '12px' }}>Fields ({entity.field_count})</h4>
                    <div style={{ overflowX: 'auto' }}>
                      <table style={{ width: '100%', fontSize: '13px', borderCollapse: 'collapse' }}>
                        <thead>
                          <tr style={{ backgroundColor: '#f5f5f5', borderBottom: '2px solid #ddd' }}>
                            <th style={{ textAlign: 'left', padding: '10px', fontWeight: 'bold' }}>Field Name</th>
                            <th style={{ textAlign: 'left', padding: '10px', fontWeight: 'bold' }}>Type</th>
                            <th style={{ textAlign: 'center', padding: '10px', fontWeight: 'bold' }}>Cardinality</th>
                            <th style={{ textAlign: 'center', padding: '10px', fontWeight: 'bold' }}>Coverage</th>
                            <th style={{ textAlign: 'center', padding: '10px', fontWeight: 'bold' }}>Instances</th>
                            <th style={{ textAlign: 'left', padding: '10px', fontWeight: 'bold' }}>Sample Value</th>
                          </tr>
                        </thead>
                        <tbody>
                          {entity.fields && entity.fields.length > 0 ? (
                            entity.fields.map((field, idx) => (
                              <tr
                                key={field.name || idx}
                                style={{
                                  borderBottom: '1px solid #eee',
                                  backgroundColor: idx % 2 === 0 ? '#fafafa' : 'white',
                                  cursor: 'pointer',
                                  transition: 'background-color 0.2s'
                                }}
                                onClick={() => setExpandedField(expandedField === `${entity.name}-${field.name}` ? null : `${entity.name}-${field.name}`)}
                                onMouseEnter={(e) => e.currentTarget.style.backgroundColor = '#f0f0f0'}
                                onMouseLeave={(e) => e.currentTarget.style.backgroundColor = idx % 2 === 0 ? '#fafafa' : 'white'}
                              >
                                <td style={{ padding: '10px', fontFamily: 'monospace', fontWeight: 'bold' }}>
                                  {field.name}
                                </td>
                                <td style={{ padding: '10px' }}>
                                  <span style={{
                                    backgroundColor: getTypeColor(field.type),
                                    color: 'white',
                                    padding: '2px 8px',
                                    borderRadius: '3px',
                                    fontSize: '11px',
                                    fontWeight: 'bold'
                                  }}>
                                    {field.type}
                                  </span>
                                </td>
                                <td style={{ padding: '10px', textAlign: 'center' }}>
                                  {getCardinalityBadge(field.cardinality)}
                                </td>
                                <td style={{ padding: '10px', textAlign: 'center' }}>
                                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px', justifyContent: 'center' }}>
                                    <div style={{
                                      width: '60px',
                                      height: '20px',
                                      backgroundColor: '#e0e0e0',
                                      borderRadius: '3px',
                                      overflow: 'hidden'
                                    }}>
                                      <div style={{
                                        height: '100%',
                                        width: `${field.coverage}%`,
                                        backgroundColor: field.coverage === 100 ? '#28a745' : '#ff9800',
                                        transition: 'width 0.3s'
                                      }} />
                                    </div>
                                    <span style={{ fontSize: '12px', fontWeight: 'bold', minWidth: '35px' }}>
                                      {field.coverage}%
                                    </span>
                                  </div>
                                </td>
                                <td style={{ padding: '10px', textAlign: 'center', fontWeight: 'bold' }}>
                                  {field.instances}
                                </td>
                                <td style={{ padding: '10px' }}>
                                  <code style={{
                                    backgroundColor: '#f0f0f0',
                                    padding: '3px 6px',
                                    borderRadius: '3px',
                                    fontSize: '11px',
                                    maxWidth: '200px',
                                    display: 'inline-block',
                                    overflow: 'hidden',
                                    textOverflow: 'ellipsis',
                                    whiteSpace: 'nowrap'
                                  }}>
                                    {formatValue(field.sample_value)}
                                  </code>
                                </td>
                              </tr>
                            ))
                          ) : (
                            <tr>
                              <td colSpan="6" style={{ padding: '20px', textAlign: 'center', color: '#999' }}>
                                No fields information available
                              </td>
                            </tr>
                          )}
                        </tbody>
                      </table>
                    </div>
                  </div>

                  {/* Metadata Section */}
                  <div style={{
                    backgroundColor: '#f9f9f9',
                    padding: '15px',
                    borderRadius: '4px',
                    borderLeft: '4px solid #007bff'
                  }}>
                    <h4 style={{ marginTop: '0', marginBottom: '12px' }}>Metadata</h4>
                    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '15px', fontSize: '13px' }}>
                      <div>
                        <strong>Registered:</strong>
                        <div style={{ color: '#666', marginTop: '4px' }}>
                          {formatTimestamp(entity.registered_at)}
                        </div>
                      </div>
                      <div>
                        <strong>Entity Type:</strong>
                        <div style={{ color: '#666', marginTop: '4px' }}>
                          {entity.type === 'repeating' ? 'Repeating Group' : 'Nested Object'}
                        </div>
                      </div>
                      {entity.relationships?.parent && (
                        <div>
                          <strong>Parent Entity:</strong>
                          <div style={{ color: '#0066cc', marginTop: '4px', fontWeight: 'bold' }}>
                            {entity.relationships.parent}
                          </div>
                        </div>
                      )}
                      {entity.relationships?.children?.length > 0 && (
                        <div>
                          <strong>Child Entities:</strong>
                          <div style={{ color: '#009933', marginTop: '4px' }}>
                            {entity.relationships.children.join(', ')}
                          </div>
                        </div>
                      )}
                      <div>
                        <strong>Total Instances:</strong>
                        <div style={{ color: '#666', marginTop: '4px' }}>
                          {entity.instance_count?.toLocaleString() || 0}
                        </div>
                      </div>
                      <div>
                        <strong>Data Completeness:</strong>
                        <div style={{ color: '#666', marginTop: '4px' }}>
                          Fields available across all instances
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              )}
            </div>
          ))}
        </div>
      )}

    </div>
  );
}

export default EntityCatalog;
