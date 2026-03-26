import React, { useState, useEffect } from 'react';
import axios from 'axios';
import Dashboard from './components/Dashboard';
import Records from './components/Records';
import QueryBuilder from './components/QueryBuilder';
import FieldPlacements from './components/FieldPlacements';
import SessionInfo from './components/SessionInfo';
import EntityCatalog from './components/EntityCatalog';

const API_BASE = 'http://localhost:8000';

function App() {
  const [activeTab, setActiveTab] = useState('dashboard');
  const [apiStatus, setApiStatus] = useState('checking');

  useEffect(() => {
    checkApiStatus();
  }, []);

  const checkApiStatus = async () => {
    try {
      await axios.get(`${API_BASE}/health`);
      setApiStatus('connected');
    } catch (error) {
      setApiStatus('disconnected');
    }
  };

  return (
    <div className="container">
      <h1 style={{
        textAlign: 'center',
        margin: '30px 0',
        color: '#333',
        fontSize: '36px'
      }}>
        Hybrid Database Dashboard
      </h1>

      {apiStatus === 'disconnected' && (
        <div className="error">
          Cannot connect to API server at {API_BASE}. Please ensure the dashboard API is running on port 8000.
        </div>
      )}

      <div className="tabs">
        <button
          className={`tab ${activeTab === 'dashboard' ? 'active' : ''}`}
          onClick={() => setActiveTab('dashboard')}
        >
          Overview
        </button>
        <button
          className={`tab ${activeTab === 'records' ? 'active' : ''}`}
          onClick={() => setActiveTab('records')}
        >
          Records
        </button>
        <button
          className={`tab ${activeTab === 'entities' ? 'active' : ''}`}
          onClick={() => setActiveTab('entities')}
        >
          Entities
        </button>
        <button
          className={`tab ${activeTab === 'query' ? 'active' : ''}`}
          onClick={() => setActiveTab('query')}
        >
          Query Builder
        </button>
        <button
          className={`tab ${activeTab === 'placements' ? 'active' : ''}`}
          onClick={() => setActiveTab('placements')}
        >
          Fields
        </button>
        <button
          className={`tab ${activeTab === 'session' ? 'active' : ''}`}
          onClick={() => setActiveTab('session')}
        >
          Session
        </button>
      </div>

      {activeTab === 'dashboard' && <Dashboard apiBase={API_BASE} />}
      {activeTab === 'records' && <Records apiBase={API_BASE} />}
      {activeTab === 'entities' && <EntityCatalog apiBase={API_BASE} />}
      {activeTab === 'query' && <QueryBuilder apiBase={API_BASE} />}
      {activeTab === 'placements' && <FieldPlacements apiBase={API_BASE} />}
      {activeTab === 'session' && <SessionInfo apiBase={API_BASE} />}
    </div>
  );
}

export default App;
