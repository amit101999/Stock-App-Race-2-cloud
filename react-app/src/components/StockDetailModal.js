import React, { useState, useEffect } from 'react';
import { X, TrendingUp, TrendingDown, DollarSign, Package, Calendar } from 'lucide-react';
import { tradesAPI } from '../services/api';
import './StockDetailModal.css';

const StockDetailModal = ({ isOpen, onClose, stock, clientId, endDate }) => {
  const [transactions, setTransactions] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (isOpen && stock && clientId) {
      fetchTransactions();
    } else {
      setTransactions([]);
      setError(null);
    }
  }, [isOpen, stock, clientId, endDate]);

  const fetchTransactions = async () => {
    if (!stock || !clientId) return;

    try {
      setLoading(true);
      setError(null);
      const response = await tradesAPI.getStockTransactionHistory(
        clientId,
        stock.stockName,
        endDate
      );
      const data = response?.data?.data || [];
      console.log(`[StockDetailModal] Fetched ${data.length} transactions`);
      console.log(`[StockDetailModal] Transaction types:`, data.map(t => t.tranType).filter(Boolean));
      setTransactions(data);
    } catch (err) {
      console.error('Error fetching transactions:', err);
      setError(err.response?.data?.message || err.message || 'Failed to fetch transactions');
      setTransactions([]);
    } finally {
      setLoading(false);
    }
  };

  const formatCurrency = (value) => {
    if (!value && value !== 0) return '₹0';
    return new Intl.NumberFormat('en-IN', {
      style: 'currency',
      currency: 'INR',
      maximumFractionDigits: 2,
    }).format(value);
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
    if (!value && value !== 0) return '0';
    return new Intl.NumberFormat('en-IN').format(value);
  };

  if (!isOpen || !stock) return null;

  // Calculate summary from transactions
  // Handle multiple buy and sell transactions - sum all of them
  // First, log all transaction types to debug
  if (transactions.length > 0) {
    const allTypes = transactions.map(t => ({
      raw: t.tranType,
      upper: t.tranType ? String(t.tranType).toUpperCase().trim() : 'NULL',
      qty: t.qty,
      netAmount: t.netAmount
    }));
    console.log('[StockDetailModal] All transaction types received:', allTypes);
  }

  const buyTransactions = transactions.filter(t => {
    const type = t.tranType ? String(t.tranType).toUpperCase().trim() : '';
    const isBuy = type.startsWith('B') || type === 'BUY' || type === 'PURCHASE' || type.includes('BUY');
    return isBuy;
  });
  
  const sellTransactions = transactions.filter(t => {
    const type = t.tranType ? String(t.tranType).toUpperCase().trim() : '';
    const isSell = type.startsWith('S') || type === 'SELL' || type === 'SALE' || type.includes('SELL');
    return isSell;
  });

  console.log('[StockDetailModal] Filtered transactions:', {
    total: transactions.length,
    buyCount: buyTransactions.length,
    sellCount: sellTransactions.length,
    buyTypes: buyTransactions.map(t => t.tranType),
    sellTypes: sellTransactions.map(t => t.tranType)
  });

  // Sum all buy transactions (handle both positive and negative amounts)
  const totalBuyQty = buyTransactions.reduce((sum, t) => {
    const qty = Math.abs(Number(t.qty) || 0);
    return sum + qty;
  }, 0);
  
  const totalBuyAmount = buyTransactions.reduce((sum, t) => {
    const amount = Number(t.netAmount) || 0;
    // For buy transactions, use absolute value (some systems store as negative)
    const absAmount = Math.abs(amount);
    console.log(`[StockDetailModal] Buy transaction: type=${t.tranType}, qty=${t.qty}, netAmount=${t.netAmount}, absAmount=${absAmount}`);
    return sum + absAmount;
  }, 0);

  // Sum all sell transactions (handle both positive and negative amounts)
  const totalSellQty = sellTransactions.reduce((sum, t) => {
    const qty = Math.abs(Number(t.qty) || 0);
    return sum + qty;
  }, 0);
  
  const totalSellAmount = sellTransactions.reduce((sum, t) => {
    const amount = Number(t.netAmount) || 0;
    // For sell transactions, use absolute value (some systems store as negative)
    const absAmount = Math.abs(amount);
    console.log(`[StockDetailModal] Sell transaction: type=${t.tranType}, qty=${t.qty}, netAmount=${t.netAmount}, absAmount=${absAmount}`);
    return sum + absAmount;
  }, 0);

  console.log('[StockDetailModal] Calculated totals:', {
    totalBuyQty,
    totalBuyAmount,
    totalSellQty,
    totalSellAmount
  });

  const currentHolding = totalBuyQty - totalSellQty;
  
  // Profit/Loss = Total Money Received from Sells - Total Money Spent on Buys
  // Positive = Profit, Negative = Loss
  const profit = totalSellAmount - totalBuyAmount;

  // Use calculated values from transactions (more accurate) if transactions are loaded
  // Otherwise fallback to backend values
  // This ensures consistency between cards and transaction table
  const hasTransactions = transactions.length > 0;
  const displayBuyQty = hasTransactions ? totalBuyQty : (stock.totalBuyQty || 0);
  const displaySellQty = hasTransactions ? totalSellQty : (stock.totalSellQty || 0);
  const displayBuyAmount = hasTransactions ? totalBuyAmount : (stock.totalBuyAmount || 0);
  const displaySellAmount = hasTransactions ? totalSellAmount : (stock.totalSellAmount || 0);
  const displayCurrentHolding = hasTransactions ? currentHolding : (stock.currentHolding || 0);
  const displayProfit = hasTransactions ? profit : (stock.profit || 0);

  // Debug logging
  if (hasTransactions) {
    console.log('[StockDetailModal] Transaction Analysis:', {
      totalTransactions: transactions.length,
      buyTransactions: buyTransactions.length,
      sellTransactions: sellTransactions.length,
      buyTransactionsDetails: buyTransactions.map(t => ({
        type: t.tranType,
        qty: t.qty,
        netAmount: t.netAmount
      })),
      sellTransactionsDetails: sellTransactions.map(t => ({
        type: t.tranType,
        qty: t.qty,
        netAmount: t.netAmount
      })),
      calculated: {
        buyQty: totalBuyQty,
        sellQty: totalSellQty,
        buyAmount: totalBuyAmount,
        sellAmount: totalSellAmount,
        currentHolding: currentHolding,
        profit: profit,
        profitFormula: `${totalSellAmount} - ${totalBuyAmount} = ${profit}`
      }
    });
  }

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-content" onClick={(e) => e.stopPropagation()}>
        {/* Modal Header */}
        <div className="modal-header">
          <div className="modal-title-section">
            <h2>{stock.stockName}</h2>
            {stock.stockCode && <p className="modal-subtitle">({stock.stockCode})</p>}
          </div>
          <button className="modal-close-button" onClick={onClose}>
            <X size={24} />
          </button>
        </div>

        {/* Summary Cards */}
        <div className="summary-cards-grid">
          <div className="summary-card">
            <div className="summary-card-icon current-holding">
              <Package size={24} />
            </div>
            <div className="summary-card-content">
              <h4>Current Holding</h4>
              <p className="summary-value">{formatNumber(displayCurrentHolding)}</p>
              <p className="summary-label">Remaining Quantity</p>
            </div>
          </div>

          <div className="summary-card">
            <div className="summary-card-icon total-buy">
              <TrendingUp size={24} />
            </div>
            <div className="summary-card-content">
              <h4>Total Buy</h4>
              <p className="summary-value">{formatNumber(displayBuyQty)}</p>
              <p className="summary-label">{formatCurrency(displayBuyAmount)}</p>
            </div>
          </div>

          <div className="summary-card">
            <div className="summary-card-icon total-sell">
              <TrendingDown size={24} />
            </div>
            <div className="summary-card-content">
              <h4>Total Sell</h4>
              <p className="summary-value">{formatNumber(displaySellQty)}</p>
              <p className="summary-label">{formatCurrency(displaySellAmount)}</p>
            </div>
          </div>

          <div className="summary-card">
            <div className={`summary-card-icon ${displayProfit >= 0 ? 'profit' : 'loss'}`}>
              <DollarSign size={24} />
            </div>
            <div className="summary-card-content">
              <h4>Profit / Loss</h4>
              <p className={`summary-value ${displayProfit >= 0 ? 'positive' : 'negative'}`}>
                {formatCurrency(displayProfit)}
              </p>
              <p className="summary-label">
                {displayProfit >= 0 ? 'Profit' : 'Loss'}
              </p>
            </div>
          </div>
        </div>

        {/* Transactions Table */}
        <div className="transactions-section">
          <h3 className="transactions-title">Transaction History</h3>
          
          {loading ? (
            <div className="loading-container">
              <div className="spinner"></div>
              <p>Loading transactions...</p>
            </div>
          ) : error ? (
            <div className="error-container">
              <p>{error}</p>
              <button onClick={fetchTransactions} className="retry-button">
                Retry
              </button>
            </div>
          ) : transactions.length === 0 ? (
            <div className="no-transactions">
              <p>No transactions found for this stock.</p>
            </div>
          ) : (
            <div className="table-wrapper">
              <table className="transactions-table">
                <thead>
                  <tr>
                    <th>DATE</th>
                    <th>TYPE</th>
                    <th>STOCK NAME</th>
                    <th>EXCHANGE</th>
                    <th>QUANTITY</th>
                    <th>PRICE</th>
                    <th>TOTAL PRICE</th>
                    <th>NET AMOUNT</th>
                    <th>ACCOUNT CODE</th>
                  </tr>
                </thead>
                <tbody>
                  {transactions.map((transaction, index) => {
                    const tranType = transaction.tranType ? String(transaction.tranType).toUpperCase().trim() : '';
                    const isBuy = tranType.startsWith('B') || tranType === 'BUY' || tranType === 'PURCHASE' || tranType.includes('BUY');
                    const isSell = tranType.startsWith('S') || tranType === 'SELL' || tranType === 'SALE' || tranType.includes('SELL');
                    // If type is not clearly identified, check the displayed type in table
                    const displayType = isBuy ? 'BUY' : (isSell ? 'SELL' : (tranType || 'UNKNOWN'));
                    const totalPrice = (Number(transaction.qty) || 0) * (Number(transaction.rate) || 0);
                    
                    return (
                      <tr key={index} className={isBuy ? 'buy-row' : 'sell-row'}>
                        <td>
                          <div className="date-cell">
                            <Calendar size={14} />
                            {formatDate(transaction.trandate)}
                          </div>
                        </td>
                        <td>
                          <span className={`trade-type-badge ${isBuy ? 'buy' : 'sell'}`}>
                            {displayType}
                          </span>
                        </td>
                        <td className="stock-name-cell">
                          <strong>{transaction.securityName || '-'}</strong>
                          {transaction.securityCode && (
                            <span className="stock-code">({transaction.securityCode})</span>
                          )}
                        </td>
                        <td>{transaction.exchg || '-'}</td>
                        <td className="number-cell">{formatNumber(transaction.qty)}</td>
                        <td className="number-cell">{formatCurrency(transaction.rate)}</td>
                        <td className="number-cell">{formatCurrency(totalPrice)}</td>
                        <td className={`number-cell ${isBuy ? 'buy-amount' : 'sell-amount'}`}>
                          {formatCurrency(transaction.netAmount)}
                        </td>
                        <td>{transaction.wsAccountCode || '-'}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default StockDetailModal;

