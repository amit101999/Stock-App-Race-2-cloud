import React from 'react';
import { Package, Eye } from 'lucide-react';
import './HoldingsList.css';

const HoldingsList = ({ holdings, loading, onViewStock }) => {
  const formatNumber = (value) => {
    if (!value && value !== 0) return '0';
    return new Intl.NumberFormat('en-IN').format(value);
  };

  if (loading) {
    return (
      <div className="holdings-list">
        <div className="loading-container">
          <div className="spinner"></div>
          <p>Loading holdings...</p>
        </div>
      </div>
    );
  }

  if (!holdings || holdings.length === 0) {
    return (
      <div className="holdings-list">
        <div className="no-holdings">
          <Package size={48} />
          <h3>No Holdings Found</h3>
          <p>No stock holdings found for the selected client and date range.</p>
        </div>
      </div>
    );
  }

  return (
    <div className="holdings-list">
      <div className="holdings-header">
        <h2>Stock Holdings ({holdings.length})</h2>
        <p className="holdings-subtitle">Click "View" to see detailed transaction history</p>
      </div>

      <div className="holdings-grid">
        {holdings.map((holding, index) => (
          <div key={index} className="holding-card">
            <div className="holding-card-header">
              <div className="stock-info">
                <h3 className="stock-name">{holding.stockName}</h3>
                {holding.stockCode && (
                  <p className="stock-code">({holding.stockCode})</p>
                )}
              </div>
            </div>

            <div className="holding-card-body">
              <div className="holding-metric">
                <span className="metric-label">Current Holding:</span>
                <span className={`metric-value ${holding.currentHolding <= 0 ? 'zero-holding' : ''}`}>
                  {formatNumber(holding.currentHolding)}
                </span>
                {holding.currentHolding <= 0 && (
                  <span className="fully-sold-badge">Fully Sold</span>
                )}
              </div>
            </div>

            <div className="holding-card-footer">
              <button
                className="view-button"
                onClick={() => onViewStock(holding)}
              >
                <Eye size={16} />
                View Details
              </button>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

export default HoldingsList;

