'use client';

import { useState, useEffect } from 'react';
import { searchVespa } from '../actions';

export default function RealTimeSearch() {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<any[]>([]);
  const [notifications, setNotifications] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    const eventSource = new EventSource('http://localhost:8090/events');

    eventSource.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        const message = `[SSE Alert] Document ${data.id || 'item'} status: ${data.status || 'updated'}`;
        setNotifications((prev) => [message, ...prev.slice(0, 4)]);
      } catch (e) {
        setNotifications((prev) => [`[SSE Alert] Raw message: ${event.data}`, ...prev.slice(0, 4)]);
      }
    };

    eventSource.onerror = (error) => {
      console.error('SSE Error:', error);
    };

    return () => {
      eventSource.close();
    };
  }, []);

  const handleSearch = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!query.trim()) return;

    setLoading(true);
    try {
      const hits = await searchVespa(query);
      setResults(hits);
    } catch (err) {
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{ fontFamily: 'sans-serif', maxWidth: '800px', margin: '2rem auto', padding: '1rem' }}>
      <h2>🚀 Real-Time Vespa + Kafka + SSE Search</h2>

      {/* Live Notifications Banner */}
      <div style={{ backgroundColor: '#f0f9ff', border: '1px solid #bae6fd', padding: '1rem', borderRadius: '8px', marginBottom: '1.5rem' }}>
        <h3>⚡ Live SSE Notifications</h3>
        {notifications.length === 0 ? (
          <p style={{ color: '#666' }}>Listening for live Kafka -&gt; Vespa indexing events...</p>
        ) : (
          <ul style={{ paddingLeft: '1.2rem', margin: 0 }}>
            {notifications.map((note, idx) => (
              <li key={idx} style={{ color: '#0369a1', fontWeight: 'bold' }}>{note}</li>
            ))}
          </ul>
        )}
      </div>

      {/* Search Form */}
      <form onSubmit={handleSearch} style={{ display: 'flex', gap: '0.5rem', marginBottom: '1.5rem' }}>
        <input
          type="text"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="Search Vespa index..."
          style={{ flex: 1, padding: '0.75rem', borderRadius: '6px', border: '1px solid #ccc' }}
        />
        <button
          type="submit"
          disabled={loading}
          style={{ padding: '0.75rem 1.5rem', borderRadius: '6px', backgroundColor: '#0284c7', color: 'white', border: 'none', cursor: 'pointer' }}
        >
          {loading ? 'Searching...' : 'Search'}
        </button>
      </form>

      {/* Results */}
      <div>
        <h3>Search Results ({results.length})</h3>
        {results.map((hit, i) => (
          <div key={i} style={{ border: '1px solid #e2e8f0', padding: '1rem', borderRadius: '6px', marginBottom: '0.5rem' }}>
            <h4>{hit.fields?.title || hit.id}</h4>
            <p style={{ color: '#64748b' }}>ID: {hit.id} | Relevance: {hit.relevance}</p>
          </div>
        ))}
      </div>
    </div>
  );
}
