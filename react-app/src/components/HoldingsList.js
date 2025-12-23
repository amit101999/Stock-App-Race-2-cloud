import React from 'react';
import { Package, Eye } from 'lucide-react';
import './HoldingsList.css';

const HoldingsList = ({ holdings, loading, onViewStock }) => {
  const truncateTo2Decimals = (num) => {
    if (typeof num !== 'number') {
      const parsed = Number(num);
      if (Number.isNaN(parsed)) return num;
      num = parsed;
    }
    return Math.trunc(num * 100) / 100;
  };

  const formatNumber = (value) => {
    if (!value && value !== 0) return '0';
    const truncated = truncateTo2Decimals(value);
    return new Intl.NumberFormat('en-IN', {
      maximumFractionDigits: 2,
      minimumFractionDigits: 0,
    }).format(truncated);
  };

  const formatCurrency = (value) => {
    if (!value && value !== 0) return '₹0';
    const truncated = truncateTo2Decimals(value);
    return new Intl.NumberFormat('en-IN', {
      style: 'currency',
      currency: 'INR',
      maximumFractionDigits: 2,
      minimumFractionDigits: 0,
    }).format(truncated);
  };

  // Only show active holdings where currentHolding > 0
  const activeHoldings = Array.isArray(holdings)
    ? holdings.filter((h) => (Number(h.currentHolding) || 0) > 0)
    : [];

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

  if (!activeHoldings || activeHoldings.length === 0) {
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
        <h2>Active Stock Holdings ({activeHoldings.length})</h2>
        <p className="holdings-subtitle">
          Showing only stocks with current holdings greater than 0. Click "View" to see detailed
          transaction history.
        </p>
      </div>

      <div className="holdings-table-wrapper">
        <table className="holdings-table">
          <thead>
            <tr>
              <th>Stock Name</th>
              <th>Code</th>
              <th>Current Holding</th>
              <th>Holding Value</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody>
            {activeHoldings.map((holding, index) => (
              <tr key={index}>
                <td>
                  <div className="stock-info">
                    <span className="stock-name">{holding.stockName}</span>
                  </div>
                </td>
                <td className="stock-code-cell">
                  {holding.stockCode ? <span className="stock-code">({holding.stockCode})</span> : '-'}
                </td>
                <td className="number-cell">
                  {formatNumber(holding.currentHolding)}
                </td>
                <td className="number-cell">
                  {formatCurrency(holding.holdingValue || 0)}
                </td>
                <td>
                  <button
                    className="view-button"
                    onClick={() => onViewStock(holding)}
                  >
                    <Eye size={16} />
                    View Details
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

export default HoldingsList;

