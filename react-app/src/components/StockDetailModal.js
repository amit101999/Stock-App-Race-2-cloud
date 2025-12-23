import React, { useState, useEffect, useCallback } from 'react';
import { X, TrendingUp, TrendingDown, DollarSign, Package, Calendar, BarChart3, Gift } from 'lucide-react';
import { tradesAPI } from '../services/api';
import './StockDetailModal.css';

const StockDetailModal = ({ isOpen, onClose, stock, clientId, endDate }) => {
  const [transactions, setTransactions] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const fetchTransactions = useCallback(async () => {
    if (!stock || !clientId) return;

    try {
      setLoading(true);
      setError(null);
      const response = await tradesAPI.getStockTransactionHistory(
        clientId,
        stock.stockName,
        endDate,
        stock.stockCode // Pass stockCode to filter by Security_code
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
  }, [stock, clientId, endDate]);

  useEffect(() => {
    if (isOpen && stock && clientId) {
      fetchTransactions();
    } else {
      setTransactions([]);
      setError(null);
    }
  }, [isOpen, stock, clientId, fetchTransactions]);

  // Helper function to truncate to 2 decimal places (no rounding)
  const truncateTo2Decimals = (value) => {
    if (!value && value !== 0) return 0;
    return Math.floor(value * 100) / 100;
  };

  const formatCurrency = (value) => {
    if (!value && value !== 0) return '₹0.00';
    // Truncate to 2 decimal places (no rounding)
    const truncated = truncateTo2Decimals(value);
    return new Intl.NumberFormat('en-IN', {
      style: 'currency',
      currency: 'INR',
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
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
    if (!value && value !== 0) return '0';
    return new Intl.NumberFormat('en-IN').format(value);
  };

  // Calculate summary from transactions
  // Initialize all variables to ensure they're always defined
  // Note: All dividend-related transactions are excluded from holdings calculation
  const buyTransactions = Array.isArray(transactions) ? transactions.filter(t => {
    const type = t.tranType ? String(t.tranType).toUpperCase().trim() : '';
    // Include Buy transactions and additional types that add to holdings
    const isBuy = type.startsWith('B') || type === 'BUY' || type === 'PURCHASE' || type.includes('BUY');
    const isSQB = type === 'SQB'; // Sell Quantity Buy - treated as buy
    const isOPI = type === 'OPI'; // Opening Position In - treated as buy
    // Exclude all dividend-related transactions from holdings calculation
    const isDividend = type === 'DIO' || 
                       type === 'DIVIDEND' || 
                       type === 'DIVIDEND REINVEST' || 
                       type === 'DIVIDEND REINVESTMENT' ||
                       type === 'DIVIDEND RECEIVED' ||
                       type.startsWith('DIVIDEND') ||
                       type.includes('DIVIDEND');
    return (isBuy || isSQB || isOPI) && !isDividend;
  }) : [];
  
  const sellTransactions = Array.isArray(transactions) ? transactions.filter(t => {
    const type = t.tranType ? String(t.tranType).toUpperCase().trim() : '';
    // Include Sell transactions and additional types that reduce holdings
    const isSell = type.startsWith('S') || type === 'SELL' || type === 'SALE' || type.includes('SELL');
    const isSQS = type === 'SQS'; // Sell Quantity Sell - treated as sell
    const isOPO = type === 'OPO'; // Opening Position Out - treated as sell
    const isNF = type === 'NF-' || type.startsWith('NF-'); // NF- transaction type - treated as sell
    return isSell || isSQS || isOPO || isNF;
  }) : [];

  // Sum all buy transactions (handle both positive and negative amounts)
  const totalBuyQty = buyTransactions.reduce((sum, t) => {
    const qty = Math.abs(Number(t.qty) || 0);
    return sum + qty;
  }, 0);
  
  const totalBuyAmount = buyTransactions.reduce((sum, t) => {
    const amount = Number(t.netAmount) || 0;
    // For buy transactions, use absolute value (some systems store as negative)
    const absAmount = Math.abs(amount);
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
    return sum + absAmount;
  }, 0);

  const currentHolding = totalBuyQty - totalSellQty;
  
  // Calculate Weighted Average Buy Price
  // Formula: Sum of (Quantity × Price) for all buy transactions / Total Buy Quantity
  // Use netrate if available, fallback to rate
  const weightedAverageBuyPrice = totalBuyQty > 0 
    ? buyTransactions.reduce((sum, t) => {
        const qty = Math.abs(Number(t.qty) || 0);
        // Prioritize netrate, fallback to rate
        const netrate = Number(t.netrate) || Number(t.netRate) || Number(t.NETRATE) || 0;
        const rate = Number(t.rate) || 0;
        const price = netrate > 0 ? netrate : rate;
        // If still 0, calculate from netAmount as last resort
        const finalPrice = price > 0 ? price : (qty > 0 && t.netAmount && Math.abs(t.netAmount) > 0 ? Math.abs(t.netAmount) / qty : 0);
        return sum + (qty * finalPrice);
      }, 0) / totalBuyQty
    : 0;
  
  // Profit/Loss = Total Money Received from Sells - Total Money Spent on Buys
  // Positive = Profit, Negative = Loss
  // If there are no sell transactions, profit/loss should be 0 (unrealized)
  const profit = totalSellQty > 0 ? (totalSellAmount - totalBuyAmount) : 0;

  if (!isOpen || !stock) return null;

  // Use calculated values from transactions (more accurate) if transactions are loaded
  // Otherwise fallback to backend values
  // This ensures consistency between cards and transaction table
  const hasTransactions = Array.isArray(transactions) && transactions.length > 0;
  const displayBuyQty = hasTransactions ? totalBuyQty : (stock?.totalBuyQty || 0);
  const displaySellQty = hasTransactions ? totalSellQty : (stock?.totalSellQty || 0);
  const displayBuyAmount = hasTransactions ? totalBuyAmount : (stock?.totalBuyAmount || 0);
  const displaySellAmount = hasTransactions ? totalSellAmount : (stock?.totalSellAmount || 0);
  const displayCurrentHolding = hasTransactions ? currentHolding : (stock?.currentHolding || 0);
  const displayProfit = hasTransactions ? profit : (stock?.profit || 0);
  const displayWeightedAvgBuyPrice = hasTransactions ? weightedAverageBuyPrice : (stock?.weightedAverageBuyPrice || 0);

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
            <div className="summary-card-top">
            <div className="summary-card-icon current-holding">
              <Package size={24} />
            </div>
              <div className="summary-card-header">
              <h4>Current Holding</h4>
              <p className="summary-label">Remaining Quantity</p>
              </div>
            </div>
            <p className="summary-value">{formatNumber(displayCurrentHolding)}</p>
          </div>

          <div className="summary-card">
            <div className="summary-card-top">
            <div className="summary-card-icon total-buy">
              <TrendingUp size={24} />
            </div>
              <div className="summary-card-header">
              <h4>Total Buy</h4>
              <p className="summary-label">{formatCurrency(displayBuyAmount)}</p>
              </div>
            </div>
            <p className="summary-value">{formatNumber(displayBuyQty)}</p>
          </div>

          <div className="summary-card">
            <div className="summary-card-top">
            <div className="summary-card-icon total-sell">
              <TrendingDown size={24} />
            </div>
              <div className="summary-card-header">
              <h4>Total Sell</h4>
              <p className="summary-label">{formatCurrency(displaySellAmount)}</p>
              </div>
            </div>
            <p className="summary-value">{formatNumber(displaySellQty)}</p>
          </div>

          <div className="summary-card">
            <div className="summary-card-top">
            <div className={`summary-card-icon ${displayProfit >= 0 ? 'profit' : 'loss'}`}>
              <DollarSign size={24} />
            </div>
              <div className="summary-card-header">
              <h4>Profit / Loss</h4>
                <p className="summary-label">
                  {displayProfit >= 0 ? 'Profit' : 'Loss'}
                </p>
              </div>
            </div>
              <p className={`summary-value ${displayProfit >= 0 ? 'positive' : 'negative'}`}>
                {formatCurrency(displayProfit)}
              </p>
          </div>

          <div className="summary-card">
            <div className="summary-card-top">
              <div className="summary-card-icon weighted-avg">
                <BarChart3 size={24} />
              </div>
              <div className="summary-card-header">
                <h4>Weighted Avg Buy Price</h4>
              <p className="summary-label">
                  {displayBuyQty > 0 ? `Based on ${formatNumber(displayBuyQty)} shares` : 'No buy transactions'}
              </p>
              </div>
            </div>
            <p className="summary-value">
              {displayWeightedAvgBuyPrice > 0 ? formatCurrency(displayWeightedAvgBuyPrice) : '-'}
            </p>
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
          ) : transactions.filter((t) => {
            const type = t.tranType ? String(t.tranType).toUpperCase().trim() : '';
            const isBuy = type.startsWith('B') || type === 'BUY' || type === 'PURCHASE' || type.includes('BUY');
            const isSell = type.startsWith('S') || type === 'SELL' || type === 'SALE' || type.includes('SELL');
            const isBonus = type === 'BONUS' || t.isBonus === true;
            const isDividend = type === 'DIO' || 
                               type === 'DIVIDEND' || 
                               type === 'DIVIDEND REINVEST' || 
                               type === 'DIVIDEND REINVESTMENT' ||
                               type === 'DIVIDEND RECEIVED' ||
                               type.startsWith('DIVIDEND') ||
                               type.includes('DIVIDEND');
            return (isBuy || isSell || isBonus) && !isDividend;
          }).length === 0 ? (
            <div className="no-transactions">
              <p>No Buy, Sell, or Bonus transactions found for this stock.</p>
            </div>
          ) : (
            <div className="table-wrapper">
              <table className="transactions-table">
                <thead>
                  <tr>
                    <th>DATE</th>
                    <th>TYPE</th>
                    <th>STOCK NAME</th>
                    <th>QUANTITY</th>
                    <th>PRICE</th>
                    <th>TOTAL AMOUNT</th>
                    <th>HOLDING</th>
                    <th>WAP</th>
                    <th>AVG COST OF HOLDINGS</th>
                    <th>P/L</th>
                  </tr>
                </thead>
                <tbody>
                  {(() => {
                    // Filter transactions first
                    const filteredTransactions = transactions.filter((transaction) => {
                      // Filter to show Buy, Sell, Bonus, and additional transaction types (exclude dividends)
                      const type = transaction.tranType ? String(transaction.tranType).toUpperCase().trim() : '';
                      const isBuy = type.startsWith('B') || type === 'BUY' || type === 'PURCHASE' || type.includes('BUY');
                      const isSell = type.startsWith('S') || type === 'SELL' || type === 'SALE' || type.includes('SELL');
                      const isBonus = type === 'BONUS' || transaction.isBonus === true;
                      const isSQB = type === 'SQB'; // Sell Quantity Buy
                      const isSQS = type === 'SQS'; // Sell Quantity Sell
                      const isOPI = type === 'OPI'; // Opening Position In
                      const isOPO = type === 'OPO'; // Opening Position Out
                      const isNF = type === 'NF-' || type.startsWith('NF-'); // NF- transaction type
                      // Exclude all dividend-related transactions
                      const isDividend = type === 'DIO' || 
                                         type === 'DIVIDEND' || 
                                         type === 'DIVIDEND REINVEST' || 
                                         type === 'DIVIDEND REINVESTMENT' ||
                                         type === 'DIVIDEND RECEIVED' ||
                                         type.startsWith('DIVIDEND') ||
                                         type.includes('DIVIDEND');
                      return (isBuy || isSell || isBonus || isSQB || isSQS || isOPI || isOPO || isNF) && !isDividend;
                    });

                    // Calculate cumulative values chronologically
                    // Track lots (FIFO) for WAP:
                    // - BUY adds a lot with cost
                    // - BONUS adds a lot with zero cost
                    // - SELL consumes from oldest lots; WAP = totalCost / totalQty after adjustments
                    const lotQueue = []; // each: { qty, price }

                    // Map transactions with calculated values
                    return filteredTransactions.map((transaction, index) => {
                    const tranType = transaction.tranType ? String(transaction.tranType).toUpperCase().trim() : '';
                    const isBuy = tranType.startsWith('B') || tranType === 'BUY' || tranType === 'PURCHASE' || tranType.includes('BUY');
                    const isSell = tranType.startsWith('S') || tranType === 'SELL' || tranType === 'SALE' || tranType.includes('SELL');
                      const isBonus = tranType === 'BONUS' || transaction.isBonus === true;
                      const isSQB = tranType === 'SQB'; // Sell Quantity Buy - treated as buy
                      const isSQS = tranType === 'SQS'; // Sell Quantity Sell - treated as sell
                      const isOPI = tranType === 'OPI'; // Opening Position In - treated as buy
                      const isOPO = tranType === 'OPO'; // Opening Position Out - treated as sell
                      const isNF = tranType === 'NF-' || tranType.startsWith('NF-'); // NF- transaction type - treated as sell
                      // Determine if buy or sell for styling
                      const isBuyType = isBuy || isSQB || isOPI;
                      const isSellType = isSell || isSQS || isOPO || isNF;
                      // Display the actual transaction type
                      const displayType = tranType || 'UNKNOWN';
                      
                      // Use netrate for price calculation, fallback to rate if netrate is not available
                      const netrate = Number(transaction.netrate) || Number(transaction.netRate) || Number(transaction.NETRATE) || 0;
                      const rate = Number(transaction.rate) || 0;
                      // Prioritize netrate, fallback to rate, then calculate from netAmount as last resort
                      let price = netrate > 0 ? netrate : rate;
                      if (price === 0 && transaction.netAmount && Math.abs(transaction.netAmount) > 0 && transaction.qty) {
                        price = Math.abs(transaction.netAmount) / Math.abs(transaction.qty);
                      }
                      
                      // For bonus transactions, show 0 for price/total/net
                      const totalPrice = isBonus ? 0 : ((Number(transaction.qty) || 0) * price);
                      
                      // Bonus-specific formatting
                      const bonusQty = isBonus ? (Number(transaction.qty) || 0) : 0;
                      
                      // Calculate values AFTER this transaction
                      const qty = Math.abs(Number(transaction.qty) || 0);
                      
                      if (isBuyType && !isBonus) {
                        // BUY: add lot with cost
                        if (qty > 0) {
                          lotQueue.push({ qty, price });
                        }
                      } else if (isBonus) {
                        // BONUS: add zero-cost lot
                        if (bonusQty > 0) {
                          lotQueue.push({ qty: bonusQty, price: 0 });
                        }
                      } else if (isSellType) {
                        // SELL: consume from oldest lots (FIFO)
                        let remaining = qty;
                        while (remaining > 0 && lotQueue.length > 0) {
                          const lot = lotQueue[0];
                          if (lot.qty <= remaining) {
                            remaining -= lot.qty;
                            lotQueue.shift();
                          } else {
                            lot.qty -= remaining;
                            remaining = 0;
                          }
                        }
                      }
                      
                      // Recompute holding and cost from lotQueue
                      const holdingAfter = lotQueue.reduce((sum, lot) => sum + lot.qty, 0);
                      const totalCostBasis = lotQueue.reduce((sum, lot) => sum + (lot.qty * lot.price), 0);
                      const weightedAvgAfter = holdingAfter > 0 ? (totalCostBasis / holdingAfter) : 0;
                      
                      // Calculate Average Cost of Holdings directly: WAP × HOLDING
                      const averageCostOfHoldings = holdingAfter * weightedAvgAfter;
                      
                      // Use profitLoss from backend (FIFO-based, only for SELL transactions)
                      const profitLossAfter = (isSellType && transaction.profitLoss !== undefined)
                        ? transaction.profitLoss
                        : null;
                    
                    return (
                      <tr key={index} className={isBonus ? 'bonus-row' : (isBuyType ? 'buy-row' : (isSellType ? 'sell-row' : ''))}>
                        <td>
                          <div className="date-cell">
                            <Calendar size={14} />
                            {formatDate(transaction.trandate)}
                          </div>
                        </td>
                        <td>
                          <span className={`trade-type-badge ${isBonus ? 'bonus' : (isBuyType ? 'buy' : (isSellType ? 'sell' : ''))}`}>
                            {displayType}
                          </span>
                        </td>
                        <td className="stock-name-cell">
                          <strong>{transaction.securityName || '-'}</strong>
                          {transaction.securityCode && (
                            <span className="stock-code">({transaction.securityCode})</span>
                          )}
                        </td>
                        <td className={`number-cell ${isBonus ? 'bonus-qty' : ''}`}>
                          {isBonus ? (
                            <span className="bonus-quantity">
                              {bonusQty > 0 ? (
                                <strong className="bonus-qty-positive">+{formatNumber(bonusQty)}</strong>
                              ) : (
                                <>
                                  <strong className="bonus-qty-zero">{formatNumber(bonusQty)}</strong>
                                  <span className="bonus-zero-note">No holdings on Ex-Date</span>
                                </>
                              )}
                            </span>
                          ) : (
                            formatNumber(transaction.qty)
                          )}
                        </td>
                        <td className="number-cell">{isBonus ? formatCurrency(0) : formatCurrency(price)}</td>
                        <td className={`number-cell ${isBonus ? '' : (isBuyType ? 'buy-amount' : (isSellType ? 'sell-amount' : ''))}`}>
                          {isBonus ? formatCurrency(0) : formatCurrency((Number(transaction.qty) || 0) * price)}
                        </td>
                        <td className="number-cell">{formatNumber(holdingAfter)}</td>
                        <td className="number-cell">
                          {weightedAvgAfter > 0 ? formatCurrency(weightedAvgAfter) : '-'}
                        </td>
                        <td className="number-cell">
                          {averageCostOfHoldings > 0
                            ? formatCurrency(averageCostOfHoldings)
                            : '-'}
                        </td>
                        <td className={`number-cell ${profitLossAfter !== null ? (profitLossAfter >= 0 ? 'profit' : 'loss') : ''}`}>
                          {profitLossAfter !== null ? formatCurrency(profitLossAfter) : '-'}
                        </td>
                      </tr>
                    );
                  })})()}
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

