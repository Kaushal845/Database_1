import React, { useState, useEffect } from 'react';
import axios from 'axios';

function Records({ apiBase }) {
  const [records, setRecords] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [filterInput, setFilterInput] = useState('');
  const [filter, setFilter] = useState('');
  const [pagination, setPagination] = useState({
    limit: 50,
    offset: 0,
    has_more: false,
    total_returned: 0
  });

  useEffect(() => {
    const timer = setTimeout(() => {
      setFilter(filterInput.trim());
      setPagination((prev) => ({ ...prev, offset: 0 }));
    }, 300);

    return () => clearTimeout(timer);
  }, [filterInput]);

  useEffect(() => {
    const controller = new AbortController();

    const fetchRecords = async () => {
      try {
        setLoading(true);
        const params = {
          limit: pagination.limit,
          offset: pagination.offset
        };
        if (filter) {
          params.username = filter;
        }

        const response = await axios.get(`${apiBase}/api/dashboard/records`, {
          params,
          signal: controller.signal
        });

        const pageInfo = response.data.pagination || {};
        setRecords(response.data.records || []);
        setPagination((prev) => ({
          ...prev,
          has_more: Boolean(pageInfo.has_more),
          total_returned: pageInfo.total_returned || 0
        }));
        setError(null);
      } catch (err) {
        if (err.name !== 'CanceledError' && err.code !== 'ERR_CANCELED') {
          setError(err.message);
        }
      } finally {
        setLoading(false);
      }
    };

    fetchRecords();

    return () => controller.abort();
  }, [apiBase, pagination.limit, pagination.offset, filter]);

  const handleNextPage = () => {
    setPagination(prev => ({ ...prev, offset: prev.offset + prev.limit }));
  };

  const handlePrevPage = () => {
    setPagination(prev => ({ ...prev, offset: Math.max(0, prev.offset - prev.limit) }));
  };

  const handleLimitChange = (newLimit) => {
    setPagination(prev => ({ ...prev, limit: newLimit, offset: 0 }));
  };

  const currentPage = Math.floor(pagination.offset / pagination.limit) + 1;

  const renderValue = (value) => {
    if (value === null || value === undefined) return <span style={{ color: '#999' }}>null</span>;
    if (typeof value === 'object') return <pre style={{ margin: 0 }}>{JSON.stringify(value, null, 2)}</pre>;
    return String(value);
  };

  if (loading && records.length === 0) return <div className="loading">Loading records...</div>;
  if (error) return <div className="error">Error: {error}</div>;

  return (
    <div className="card records-page">
      <h2>Ingested Records</h2>

      <div style={{ marginBottom: '20px', display: 'flex', gap: '20px', alignItems: 'center', flexWrap: 'wrap' }}>
        <div style={{ flex: '1', minWidth: '200px' }}>
          <label>Search username prefix:</label>
          <input
            type="text"
            value={filterInput}
            onChange={(e) => {
              setFilterInput(e.target.value);
            }}
            placeholder="Type beginning of username..."
          />
        </div>
        <div>
          <label style={{ marginRight: '10px' }}>Records per page:</label>
          <select
            value={pagination.limit}
            onChange={(e) => handleLimitChange(Number(e.target.value))}
            style={{ padding: '8px', borderRadius: '4px', border: '1px solid #ccc' }}
          >
            <option value={50}>50</option>
            <option value={100}>100</option>
            <option value={200}>200</option>
            <option value={500}>500</option>
            <option value={1000}>1000</option>
          </select>
        </div>
      </div>

      {records.length === 0 ? (
        <p style={{ textAlign: 'center', color: '#666', padding: '40px' }}>
          No records found
        </p>
      ) : (
        <>
          <div style={{ overflowX: 'auto' }}>
            <table>
              <thead>
                <tr>
                  <th>Username</th>
                  <th>Ingested At</th>
                  <th>Timestamp</th>
                  <th>Fields</th>
                </tr>
              </thead>
              <tbody>
                {records.map((record, idx) => (
                  <tr key={record.sys_ingested_at || idx}>
                    <td><strong>{record.username}</strong></td>
                    <td style={{ fontSize: '12px' }}>{record.sys_ingested_at}</td>
                    <td style={{ fontSize: '12px' }}>{record.t_stamp}</td>
                    <td>
                      <details>
                        <summary style={{ cursor: 'pointer', color: '#007bff' }}>
                          View {Object.keys(record).length} fields
                        </summary>
                        <div className="json-viewer">
                          {Object.entries(record).map(([key, value]) => (
                            <div key={key} style={{ marginBottom: '8px' }}>
                              <strong>{key}:</strong> {renderValue(value)}
                            </div>
                          ))}
                        </div>
                      </details>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="pagination">
            <div className="pagination-info">
              Showing {pagination.offset + 1} - {pagination.offset + records.length}
            </div>
            <div className="pagination-buttons">
              <button
                onClick={handlePrevPage}
                disabled={pagination.offset === 0}
              >
                Previous
              </button>
              <span className="page-number">Page {currentPage}</span>
              <button
                onClick={handleNextPage}
                disabled={!pagination.has_more}
              >
                Next
              </button>
            </div>
          </div>
        </>
      )}

      {loading && <div style={{ textAlign: 'center', padding: '20px', color: '#666' }}>Loading...</div>}
    </div>
  );
}

export default Records;
