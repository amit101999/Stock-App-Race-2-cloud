import React from 'react';
import { TrendingUp, TrendingDown, Calendar, DollarSign, Package, FileText } from 'lucide-react';
import './ClientTradeDetails.css';

const ClientTradeDetails = ({ trades, loading, clientId, endDate }) => {
  const truncateTo2Decimals = (num) => {
    if (typeof num !== 'number') {
      const parsed = Number(num);
      if (Number.isNaN(parsed)) return num;
      num = parsed;
    }
    return Math.trunc(num * 100) / 100;
  };

  const formatCurrency = (value) => {
    if (!value && value !== 0) return '-';
    const truncated = truncateTo2Decimals(value);
    return new Intl.NumberFormat('en-IN', {
      style: 'currency',
      currency: 'INR',
      maximumFractionDigits: 2,
      minimumFractionDigits: 0,
    }).format(truncated);
  };

  const formatDate = (dateString) => {
    if (!dateString) return '-';
    try {
      const date = new Date(dateString);
      return date.toLocaleDateString('en-IN', {
        year: 'numeric',
        month: 'short',
        day: 'numeric',
      });
    } catch {
      return dateString;
    }
  };

  const formatNumber = (value) => {
    if (!value && value !== 0) return '-';
    // Truncate to 2 decimals without rounding.
    const truncated = truncateTo2Decimals(value);
    return new Intl.NumberFormat('en-IN', {
      maximumFractionDigits: 2,
      minimumFractionDigits: 0,
    }).format(truncated);
  };

  // Calculate summary statistics
  const calculateSummary = () => {
    if (!trades || trades.length === 0) {
      return {
        totalBuy: 0,
        totalSell: 0,
        totalBuyQty: 0,
        totalSellQty: 0,
        totalBuyAmount: 0,
        totalSellAmount: 0,
        netAmount: 0,
        uniqueStocks: 0,
      };
    }

    const buyTrades = trades.filter(t => 
      t.tranType && String(t.tranType).toUpperCase().startsWith('B')
    );
    const sellTrades = trades.filter(t => 
      t.tranType && String(t.tranType).toUpperCase().startsWith('S')
    );

    const totalBuyQty = buyTrades.reduce((sum, t) => sum + (Number(t.qty) || 0), 0);
    const totalSellQty = sellTrades.reduce((sum, t) => sum + (Number(t.qty) || 0), 0);
    const totalBuyAmount = buyTrades.reduce((sum, t) => sum + (Number(t.netAmount) || 0), 0);
    const totalSellAmount = sellTrades.reduce((sum, t) => sum + (Number(t.netAmount) || 0), 0);
    const uniqueStocks = new Set(trades.map(t => t.securityName).filter(Boolean)).size;

    return {
      totalBuy: buyTrades.length,
      totalSell: sellTrades.length,
      totalBuyQty,
      totalSellQty,
      totalBuyAmount,
      totalSellAmount,
      netAmount: totalSellAmount - totalBuyAmount,
      uniqueStocks,
    };
  };

  const summary = calculateSummary();

  if (loading) {
    return (
      <div className="client-trade-details">
        <div className="loading-container">
          <div className="spinner"></div>
          <p>Loading trade details...</p>
        </div>
      </div>
    );
  }

  if (!trades || trades.length === 0) {
    const noTradesMessage = clientId && endDate 
      ? `No trade records found for client ID ${clientId} up to ${formatDate(endDate)}.`
      : clientId
      ? `No trade records found for client ID ${clientId}.`
      : endDate
      ? `No trade records found up to ${formatDate(endDate)}.`
      : 'No trade records found.';
    
    return (
      <div className="client-trade-details">
        <div className="no-trades">
          <FileText size={48} />
          <h3>No Trades Found</h3>
          <p>{noTradesMessage}</p>
        </div>
      </div>
    );
  }

  return (
    <div className="client-trade-details">
      {/* Summary Cards */}
      <div className="trade-summary">
        <div className="summary-card buy-card">
          <div className="summary-icon buy-icon">
            <TrendingUp size={24} />
          </div>
          <div className="summary-content">
            <h4>Buy Trades</h4>
            <p className="summary-value">{summary.totalBuy}</p>
            <p className="summary-detail">
              Qty: {formatNumber(summary.totalBuyQty)} | 
              Amount: {formatCurrency(summary.totalBuyAmount)}
            </p>
          </div>
        </div>

        <div className="summary-card sell-card">
          <div className="summary-icon sell-icon">
            <TrendingDown size={24} />
          </div>
          <div className="summary-content">
            <h4>Sell Trades</h4>
            <p className="summary-value">{summary.totalSell}</p>
            <p className="summary-detail">
              Qty: {formatNumber(summary.totalSellQty)} | 
              Amount: {formatCurrency(summary.totalSellAmount)}
            </p>
          </div>
        </div>

        <div className="summary-card net-card">
          <div className="summary-icon net-icon">
            <DollarSign size={24} />
          </div>
          <div className="summary-content">
            <h4>Net Amount</h4>
            <p className={`summary-value ${summary.netAmount >= 0 ? 'positive' : 'negative'}`}>
              {formatCurrency(summary.netAmount)}
            </p>
            <p className="summary-detail">
              {summary.netAmount >= 0 ? 'Profit' : 'Loss'}
            </p>
          </div>
        </div>

        <div className="summary-card stocks-card">
          <div className="summary-icon stocks-icon">
            <Package size={24} />
          </div>
          <div className="summary-content">
            <h4>Unique Stocks</h4>
            <p className="summary-value">{summary.uniqueStocks}</p>
            <p className="summary-detail">Different securities traded</p>
          </div>
        </div>
      </div>

      {/* Trades Table */}
      <div className="trades-table-container">
        <h3 className="table-title">
          <FileText size={20} />
          All Trades ({trades.length})
        </h3>
        <div className="table-wrapper">
          <table className="client-trades-table">
            <thead>
              <tr>
                <th>Date</th>
                <th>Type</th>
                <th>Stock Name</th>
                <th>Exchange</th>
                <th>Quantity</th>
                <th>Price</th>
                <th>Total Price</th>
                <th>Net Amount</th>
                <th>Account Code</th>
              </tr>
            </thead>
            <tbody>
              {trades.map((trade, index) => {
                const isBuy = trade.tranType && String(trade.tranType).toUpperCase().startsWith('B');
                return (
                  <tr key={index} className={isBuy ? 'buy-row' : 'sell-row'}>
                    <td>
                      <div className="date-cell">
                        <Calendar size={14} />
                        {formatDate(trade.trandate)}
                      </div>
                    </td>
                    <td>
                      <span className={`trade-type-badge ${isBuy ? 'buy' : 'sell'}`}>
                        {isBuy ? 'BUY' : 'SELL'}
                      </span>
                    </td>
                    <td className="stock-name-cell">
                      <strong>{trade.securityName || '-'}</strong>
                      {trade.securityCode && (
                        <span className="stock-code">({trade.securityCode})</span>
                      )}
                    </td>
                    <td>{trade.exchg || '-'}</td>
                    <td className="number-cell">{formatNumber(trade.qty)}</td>
                    <td className="number-cell">{formatCurrency(trade.rate)}</td>
                    <td className="number-cell">
                      {formatCurrency((Number(trade.qty) || 0) * (Number(trade.rate) || 0))}
                    </td>
                    <td className={`number-cell ${isBuy ? 'buy-amount' : 'sell-amount'}`}>
                      {formatCurrency(trade.netAmount)}
                    </td>
                    <td>{trade.wsAccountCode || '-'}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
};

export default ClientTradeDetails;

