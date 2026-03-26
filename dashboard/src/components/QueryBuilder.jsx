import React, { useState } from 'react';
import axios from 'axios';

function QueryBuilder({ apiBase }) {
  const [operation, setOperation] = useState('read');
  const [fields, setFields] = useState('');
  const [filters, setFilters] = useState('');
  const [data, setData] = useState('');
  const [result, setResult] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const executeQuery = async () => {
    try {
      setLoading(true);
      setError(null);

      const request = {
        operation: operation
      };

      // Parse fields
      if (fields.trim()) {
        request.fields = fields.split(',').map(f => f.trim()).filter(Boolean);
      }

      // Parse filters
      if (filters.trim()) {
        try {
          request.filters = JSON.parse(filters);
        } catch (e) {
          setError('Invalid JSON in filters field');
          setLoading(false);
          return;
        }
      }

      // Parse data for insert/update
      if ((operation === 'insert' || operation === 'update') && data.trim()) {
        try {
          request.data = JSON.parse(data);
        } catch (e) {
          setError('Invalid JSON in data field');
          setLoading(false);
          return;
        }
      }

      const response = await axios.post(`${apiBase}/api/query`, request);
      setResult(response.data);
    } catch (err) {
      setError(err.response?.data?.detail || err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div>
      <div className="card">
        <h2>Query Builder</h2>

        <div className="query-builder">
          <div className="form-group">
            <label>Operation:</label>
            <select value={operation} onChange={(e) => setOperation(e.target.value)}>
              <option value="read">Read</option>
              <option value="insert">Insert</option>
              <option value="update">Update</option>
              <option value="delete">Delete</option>
            </select>
          </div>

          {operation === 'read' && (
            <div className="form-group">
              <label>Fields (comma-separated, leave empty for all):</label>
              <input
                type="text"
                value={fields}
                onChange={(e) => setFields(e.target.value)}
                placeholder="username, email, age"
              />
            </div>
          )}

          {(operation === 'read' || operation === 'update' || operation === 'delete') && (
            <div className="form-group">
              <label>Filters (JSON format):</label>
              <textarea
                value={filters}
                onChange={(e) => setFilters(e.target.value)}
                placeholder='{"username": "user1"}'
                rows="3"
              />
            </div>
          )}

          {(operation === 'insert' || operation === 'update') && (
            <div className="form-group">
              <label>Data (JSON format):</label>
              <textarea
                value={data}
                onChange={(e) => setData(e.target.value)}
                placeholder='{"username": "newuser", "email": "user@example.com", "age": 25}'
                rows="6"
              />
            </div>
          )}

          <button onClick={executeQuery} disabled={loading}>
            {loading ? 'Executing...' : `Execute ${operation.charAt(0).toUpperCase() + operation.slice(1)}`}
          </button>
        </div>
      </div>

      {error && (
        <div className="error">
          <strong>Error:</strong> {error}
        </div>
      )}

      {result && (
        <div className="card">
          <h2>Query Results</h2>

          {result.result.success ? (
            <div className="success">
              Operation completed successfully
            </div>
          ) : (
            <div className="error">
              Operation failed: {result.result.error}
            </div>
          )}

          <div style={{ marginTop: '20px' }}>
            <h3>Response Details</h3>
            {result.result.count !== undefined && (
              <p><strong>Records returned:</strong> {result.result.count}</p>
            )}
            {result.result.inserted !== undefined && (
              <p><strong>Records inserted:</strong> {result.result.inserted}</p>
            )}
            {result.result.deleted !== undefined && (
              <p><strong>Records deleted:</strong> {result.result.deleted}</p>
            )}
          </div>

          {result.result.records && result.result.records.length > 0 && (
            <div style={{ marginTop: '20px' }}>
              <h3>Records ({result.result.records.length})</h3>
              <div className="json-viewer">
                <pre>{JSON.stringify(result.result.records, null, 2)}</pre>
              </div>
            </div>
          )}

          <details style={{ marginTop: '20px' }}>
            <summary style={{ cursor: 'pointer', color: '#007bff', fontWeight: 'bold' }}>
              View Full Response
            </summary>
            <div className="json-viewer">
              <pre>{JSON.stringify(result, null, 2)}</pre>
            </div>
          </details>
        </div>
      )}
    </div>
  );
}

export default QueryBuilder;
