import React, { useState, useEffect } from 'react';
import { tradesAPI } from '../services/api';
import './TradesTable.css';

const TradesTable = ({ filters, onFilterChange }) => {
  const [trades, setTrades] = useState([]);
  const [loading, setLoading] = useState(true);
  const [pagination, setPagination] = useState({ page: 1, limit: 50, total: 0, pages: 0 });

  useEffect(() => {
    fetchTrades();
  }, [filters, pagination.page, pagination.limit]);

  const fetchTrades = async () => {
    try {
      setLoading(true);
      const response = await tradesAPI.getTrades({
        ...filters,
        page: pagination.page,
        limit: pagination.limit,
      });
      // Response shape from API adapter: { data: { data: rows, pagination: {...} } }
      const apiResponse = response?.data || {};
      const rows = apiResponse.data || [];
      const paginationData = apiResponse.pagination || {};
      
      console.log('Fetched trades:', { rowsCount: rows.length, paginationData });
      
      setTrades(rows);
      if (paginationData && paginationData.page) {
        setPagination(paginationData);
      } else if (paginationData.total !== undefined) {
        setPagination({
          ...pagination,
          total: paginationData.total,
          pages: paginationData.pages || Math.ceil((paginationData.total || 0) / (paginationData.limit || pagination.limit))
        });
      }
    } catch (error) {
      console.error('Error fetching trades:', error);
      setTrades([]);
    } finally {
      setLoading(false);
    }
  };

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
      return new Date(dateString).toLocaleDateString('en-IN', {
        year: 'numeric',
        month: 'short',
        day: 'numeric',
      });
    } catch (error) {
      return '-';
    }
  };

  const formatNumber = (value) => {
    if (value === null || value === undefined || value === '') return '-';
    const truncated = truncateTo2Decimals(value);
    return new Intl.NumberFormat('en-IN', {
      maximumFractionDigits: 2,
      minimumFractionDigits: 0,
    }).format(truncated);
  };

  const getStatusBadge = (status) => {
    const statusClasses = {
      COMPLETED: 'status-completed',
      PENDING: 'status-pending',
      CANCELLED: 'status-cancelled',
    };
    return <span className={`status-badge ${statusClasses[status]}`}>{status}</span>;
  };

  const getTradeTypeBadge = (type) => {
    if (!type || type === '-') return <span className="trade-type">-</span>;
    const normalizedType = type.toString().toLowerCase();
    const isBuy = normalizedType.includes('buy') || normalizedType.includes('purchase');
    return (
      <span className={`trade-type ${isBuy ? 'buy' : 'sell'}`}>
        {type}
      </span>
    );
  };

  if (loading) {
    return (
      <div className="table-loading">
        <div className="spinner"></div>
        <p>Loading trades...</p>
      </div>
    );
  }

  return (
    <div className="trades-table-container">
      <div className="table-header">
        <h2>Recent Trades</h2>
        <div className="table-info">
          Showing {trades.length} of {pagination.total} trades
        </div>
      </div>
      <div className="table-wrapper">
        <table className="trades-table">
          <thead>
            <tr>
              {/* Client Information */}
              <th>WS Client ID</th>
              <th>WS Account Code</th>
              
              {/* Transaction Dates */}
              <th>Transaction Date</th>
              <th>Set Date</th>
              
              {/* Transaction Details */}
              <th>Tran Type</th>
              <th>Tran Desc</th>
              
              {/* Security Information */}
              <th>Security Type</th>
              <th>Security Type Desc</th>
              <th>Detail Type Name</th>
              <th>ISIN</th>
              <th>Security Code</th>
              <th>Security Name</th>
              
              {/* Exchange & Broker */}
              <th>Exchange</th>
              <th>Broker Code</th>
              
              {/* Depository/Registrar */}
              <th>Depository/Registrar</th>
              <th>DPID/AMC</th>
              <th>DP Client ID/Folio</th>
              
              {/* Bank Information */}
              <th>Bank Code</th>
              <th>Bank AC ID</th>
              
              {/* Trade Details */}
              <th>Qty</th>
              <th>Rate</th>
              <th>Brokerage</th>
              <th>Service Tax</th>
              <th>Net Rate</th>
              <th>Net Amount</th>
              <th>STT</th>
              
              {/* Transfer Details */}
              <th>TRF Date</th>
              <th>TRF Rate</th>
              <th>TRF Amount</th>
              
              {/* Transaction Fees */}
              <th>Total Txn Fee</th>
              <th>Total Txn Fee STax</th>
              
              {/* Transaction Reference */}
              <th>Txn Ref No</th>
              <th>Desc Memo</th>
              
              {/* Payment Details */}
              <th>Cheque No</th>
              <th>Cheque Dtl</th>
              <th>Portfolio ID</th>
              <th>Delivery Date</th>
              <th>Payment Date</th>
              <th>Accrued Interest</th>
              
              {/* Issuer Information */}
              <th>Issuer</th>
              <th>Issuer Name</th>
              <th>TDS Amount</th>
              <th>Stamp Duty</th>
              <th>TPMSGain</th>
              
              {/* Relationship Manager */}
              <th>RM ID</th>
              <th>RM Name</th>
              
              {/* Advisor Information */}
              <th>Advisor ID</th>
              <th>Advisor Name</th>
              
              {/* Branch Information */}
              <th>Branch ID</th>
              <th>Branch Name</th>
              
              {/* Group Information */}
              <th>Group ID</th>
              <th>Group Name</th>
              
              {/* Owner Information */}
              <th>Owner ID</th>
              <th>Owner Name</th>
              <th>Wealth Advisor Name</th>
              
              {/* Scheme Information */}
              <th>Scheme ID</th>
              <th>Scheme Name</th>
            </tr>
          </thead>
          <tbody>
            {trades.length === 0 ? (
              <tr>
                <td colSpan="58" className="no-data">
                  No trades found. Import an Excel file to get started.
                </td>
              </tr>
            ) : (
              trades.map((trade, index) => (
                <tr key={index}>
                  {/* Client Information */}
                  <td>{trade.wsClientId || '-'}</td>
                  <td>{trade.wsAccountCode || '-'}</td>
                  
                  {/* Transaction Dates */}
                  <td>{trade.trandate ? formatDate(trade.trandate) : '-'}</td>
                  <td>{trade.setdate ? formatDate(trade.setdate) : '-'}</td>
                  
                  {/* Transaction Details */}
                  <td>{getTradeTypeBadge(trade.tranType || '-')}</td>
                  <td>{trade.tranDesc || '-'}</td>
                  
                  {/* Security Information */}
                  <td>{trade.securityType || '-'}</td>
                  <td>{trade.securityTypeDescription || '-'}</td>
                  <td>{trade.detailTypeName || '-'}</td>
                  <td>{trade.isin || '-'}</td>
                  <td>{trade.securityCode || '-'}</td>
                  <td>{trade.securityName || '-'}</td>
                  
                  {/* Exchange & Broker */}
                  <td><span className="exchange-badge">{trade.exchg || '-'}</span></td>
                  <td>{trade.brokerCode || '-'}</td>
                  
                  {/* Depository/Registrar */}
                  <td>{trade.depositoryRegistrar || '-'}</td>
                  <td>{trade.dpidAmc || '-'}</td>
                  <td>{trade.dpClientIdFolio || '-'}</td>
                  
                  {/* Bank Information */}
                  <td>{trade.bankCode || '-'}</td>
                  <td>{trade.bankAcid || '-'}</td>
                  
                  {/* Trade Details */}
                  <td>{formatNumber(trade.qty)}</td>
                  <td>{trade.rate ? formatCurrency(trade.rate) : '-'}</td>
                  <td>{trade.brokerage ? formatCurrency(trade.brokerage) : '-'}</td>
                  <td>{trade.serviceTax ? formatCurrency(trade.serviceTax) : '-'}</td>
                  <td>{trade.netRate ? formatCurrency(trade.netRate) : '-'}</td>
                  <td className="total-value">{trade.netAmount ? formatCurrency(trade.netAmount) : '-'}</td>
                  <td>{trade.stt ? formatCurrency(trade.stt) : '-'}</td>
                  
                  {/* Transfer Details */}
                  <td>{trade.trfdate ? formatDate(trade.trfdate) : '-'}</td>
                  <td>{trade.trfrate ? formatCurrency(trade.trfrate) : '-'}</td>
                  <td>{trade.trfamt ? formatCurrency(trade.trfamt) : '-'}</td>
                  
                  {/* Transaction Fees */}
                  <td>{trade.totalTrxnFee ? formatCurrency(trade.totalTrxnFee) : '-'}</td>
                  <td>{trade.totalTrxnFeeStax ? formatCurrency(trade.totalTrxnFeeStax) : '-'}</td>
                  
                  {/* Transaction Reference */}
                  <td>{trade.txnRefNo || '-'}</td>
                  <td>{trade.descMemo || '-'}</td>
                  
                  {/* Payment Details */}
                  <td>{trade.chequeNo || '-'}</td>
                  <td>{trade.chequeDtl || '-'}</td>
                  <td>{trade.portfolioId || '-'}</td>
                  <td>{trade.deliveryDate ? formatDate(trade.deliveryDate) : '-'}</td>
                  <td>{trade.paymentDate ? formatDate(trade.paymentDate) : '-'}</td>
                  <td>{trade.accruedInterest ? formatCurrency(trade.accruedInterest) : '-'}</td>
                  
                  {/* Issuer Information */}
                  <td>{trade.issuer || '-'}</td>
                  <td>{trade.issuerName || '-'}</td>
                  <td>{trade.tdsAmount ? formatCurrency(trade.tdsAmount) : '-'}</td>
                  <td>{trade.stampDuty ? formatCurrency(trade.stampDuty) : '-'}</td>
                  <td>{trade.tpmsgain ? formatCurrency(trade.tpmsgain) : '-'}</td>
                  
                  {/* Relationship Manager */}
                  <td>{trade.rmid || '-'}</td>
                  <td>{trade.rmname || '-'}</td>
                  
                  {/* Advisor Information */}
                  <td>{trade.advisorId || '-'}</td>
                  <td>{trade.advisorName || '-'}</td>
                  
                  {/* Branch Information */}
                  <td>{trade.branchId || '-'}</td>
                  <td>{trade.branchName || '-'}</td>
                  
                  {/* Group Information */}
                  <td>{trade.groupId || '-'}</td>
                  <td>{trade.groupName || '-'}</td>
                  
                  {/* Owner Information */}
                  <td>{trade.ownerId || '-'}</td>
                  <td>{trade.ownerName || '-'}</td>
                  <td>{trade.wealthAdvisorName || '-'}</td>
                  
                  {/* Scheme Information */}
                  <td>{trade.schemeId || '-'}</td>
                  <td>{trade.schemeName || '-'}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
      <div className="pagination">
        <div className="page-controls">
          <button
            onClick={() => setPagination({ ...pagination, page: 1 })}
            disabled={pagination.page <= 1}
          >
            « First
          </button>
          <button
            onClick={() => setPagination({ ...pagination, page: Math.max(1, pagination.page - 1) })}
            disabled={pagination.page <= 1}
          >
            ‹ Prev
          </button>
          <span>
            Page {pagination.page} of {Math.max(1, pagination.pages || Math.ceil((pagination.total || 0) / (pagination.limit || 50)))}
          </span>
          <button
            onClick={() =>
              setPagination({
                ...pagination,
                page: Math.min(
                  (pagination.pages || Math.ceil((pagination.total || 0) / (pagination.limit || 50))),
                  pagination.page + 1
                ),
              })
            }
            disabled={
              pagination.page >=
              (pagination.pages || Math.ceil((pagination.total || 0) / (pagination.limit || 50)))
            }
          >
            Next ›
          </button>
          <button
            onClick={() =>
              setPagination({
                ...pagination,
                page: pagination.pages || Math.ceil((pagination.total || 0) / (pagination.limit || 50)),
              })
            }
            disabled={
              pagination.page >=
              (pagination.pages || Math.ceil((pagination.total || 0) / (pagination.limit || 50)))
            }
          >
            Last »
          </button>
        </div>
        <div className="page-size">
          <label htmlFor="page-size-select">Rows per page:</label>
          <select
            id="page-size-select"
            value={pagination.limit}
            onChange={(e) =>
              setPagination({ ...pagination, page: 1, limit: parseInt(e.target.value, 10) })
            }
          >
            <option value={25}>25</option>
            <option value={50}>50</option>
            <option value={100}>100</option>
            <option value={200}>200</option>
          </select>
        </div>
      </div>
    </div>
  );
};

export default TradesTable;

