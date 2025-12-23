import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { tradesAPI } from '../services/api';
import { ArrowLeft, Filter, XCircle } from 'lucide-react';
import SearchableSelect from '../components/SearchableSelect';
import HoldingsList from '../components/HoldingsList';
import StockDetailModal from '../components/StockDetailModal';
import './Analytics.css';

const Analytics = () => {
  const navigate = useNavigate();
  const [filters, setFilters] = useState({});
  const [accountCodes, setAccountCodes] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [accountCodeToClientId, setAccountCodeToClientId] = useState({}); // Cache for account code -> client ID mapping
  const [holdings, setHoldings] = useState([]);
  const [holdingsLoading, setHoldingsLoading] = useState(false);
  const [holdingsError, setHoldingsError] = useState(null);
  const [selectedStock, setSelectedStock] = useState(null);
  const [isModalOpen, setIsModalOpen] = useState(false);

  useEffect(() => {
    fetchFilterOptions();
  }, []);


  useEffect(() => {
    // Fetch stocks for selected client only
    if (filters.customerId) {
      fetchHoldingsSummary();
    } else {
      // Clear holdings when no client is selected
      setHoldings([]);
    }
  }, [filters.customerId, filters.endDate]);

  const fetchFilterOptions = async () => {
    try {
      setLoading(true);
      setError(null);
      console.log('[Analytics] Fetching account codes...');
      const startTime = Date.now();
      
      const accountCodesRes = await tradesAPI.getAccountCodes();
      
      console.log('[Analytics] Raw API response:', accountCodesRes);
      
      // Handle both direct array response and wrapped response
      const accountCodesData = accountCodesRes?.data?.data || accountCodesRes?.data || [];
      
      console.log('[Analytics] Extracted accountCodesData:', accountCodesData);
      console.log('[Analytics] Is array?', Array.isArray(accountCodesData));
      console.log('[Analytics] Length:', accountCodesData?.length);
      
      const sortedAccountCodes = Array.isArray(accountCodesData) 
        ? [...new Set(accountCodesData)].sort() 
        : [];
      
      const duration = Date.now() - startTime;
      console.log(`[Analytics] Loaded ${sortedAccountCodes.length} account codes in ${duration}ms`);
      console.log(`[Analytics] First 5 codes:`, sortedAccountCodes.slice(0, 5));
      console.log(`[Analytics] Last 5 codes:`, sortedAccountCodes.slice(-5));
      
      if (sortedAccountCodes.length === 0) {
        setError('No account codes found. Please check your database connection.');
      }
      
      setAccountCodes(sortedAccountCodes);
    } catch (error) {
      console.error('[Analytics] Error fetching filter options:', error);
      console.error('[Analytics] Error response:', error.response);
      console.error('[Analytics] Error details:', error.response?.data || error.message);
      
      const errorMessage = error.response?.data?.message || error.message || 'Failed to fetch account codes';
      setError(`Error: ${errorMessage}. Please check your backend server and database connection.`);
      setAccountCodes([]);
    } finally {
      setLoading(false);
    }
  };

  const handleAccountCodeChange = async (accountCode) => {
    // Clear stock filter when account code changes
    if (!accountCode) {
      setFilters({ 
        ...filters, 
        customerId: undefined,
        accountCode: undefined,
        stockName: undefined
      });
      setHoldings([]);
      return;
    }

    // Check if we already have the client ID cached
    let clientId = accountCodeToClientId[accountCode];
    
    if (!clientId) {
      // Fetch client ID from account code
      try {
        console.log(`[Analytics] Resolving client ID for account code: ${accountCode}`);
        const clientIdRes = await tradesAPI.getClientIdByAccountCode(accountCode);
        clientId = clientIdRes?.data?.clientId;
        
        if (!clientId) {
          console.error(`[Analytics] No client ID found for account code: ${accountCode}`);
          setError(`No client ID found for account code: ${accountCode}`);
          return;
        }
        
        // Cache the mapping
        setAccountCodeToClientId(prev => ({
          ...prev,
          [accountCode]: clientId
        }));
        
        console.log(`[Analytics] Resolved client ID ${clientId} for account code ${accountCode}`);
      } catch (error) {
        console.error(`[Analytics] Error resolving client ID for account code ${accountCode}:`, error);
        setError(`Failed to resolve client ID for account code: ${accountCode}`);
        return;
      }
    }

    // Update filters with both account code and client ID
    setFilters({ 
      ...filters, 
      customerId: clientId,
      accountCode: accountCode,
      stockName: undefined // Clear stock when account code changes
    });
  };


  const handleDateChange = (value) => {
    setFilters({ 
      ...filters, 
      endDate: value || undefined 
    });
  };

  const clearFilters = () => {
    const newFilters = { ...filters };
    delete newFilters.customerId;
    delete newFilters.accountCode;
    delete newFilters.endDate;
    setFilters(newFilters);
    setHoldings([]);
  };

  const fetchHoldingsSummary = async () => {
    // Allow fetching with just date filter (no client ID required)
    if (!filters.customerId && !filters.endDate) {
      setHoldings([]);
      return;
    }

    try {
      setHoldingsLoading(true);
      setHoldingsError(null);
      
      console.log('[Analytics] Fetching holdings summary...');
      console.log('[Analytics] Filters:', { 
        customerId: filters.customerId, 
        endDate: filters.endDate
      });
      
      const response = await tradesAPI.getHoldingsSummary(
        filters.customerId,
        filters.endDate
      );
      
      const holdingsData = response?.data?.data || [];
      
      console.log(`[Analytics] Fetched ${holdingsData.length} holdings`);
      
      setHoldings(holdingsData);
    } catch (error) {
      console.error('[Analytics] Error fetching holdings:', error);
      console.error('[Analytics] Error response:', error.response);
      console.error('[Analytics] Error data:', error.response?.data);
      setHoldingsError(error.response?.data?.message || error.message || 'Failed to fetch holdings');
      setHoldings([]);
    } finally {
      setHoldingsLoading(false);
    }
  };

  const handleViewStock = (stock) => {
    setSelectedStock(stock);
    setIsModalOpen(true);
  };

  const handleCloseModal = () => {
    setIsModalOpen(false);
    setSelectedStock(null);
  };

  const hasActiveFilters = filters.accountCode || filters.endDate;

  const renderAccountCodeField = () => {
    if (error) {
      return (
        <div className="filter-inline-wrapper">
          <label htmlFor="analytics-account-code" className="filter-input-label">
            Account Code
          </label>
          <div className="filter-select" style={{ borderColor: '#e53e3e', color: '#e53e3e' }}>
            Error loading account codes
          </div>
          <button 
            onClick={fetchFilterOptions}
            className="retry-btn"
          >
            Retry
          </button>
        </div>
      );
    }

    return (
      <div className="filter-inline-wrapper">
        <label htmlFor="analytics-account-code" className="filter-input-label">
          Account Code
        </label>
        <SearchableSelect
          id="analytics-account-code"
          label=""
          value={filters.accountCode || ''}
          onChange={handleAccountCodeChange}
          options={accountCodes}
          placeholder={loading ? 'Loading account codes...' : 'All Account Codes'}
          searchPlaceholder="Search account code..."
          description=""
          disabled={loading}
          countText={
            loading
              ? 'Loading account codes...'
              : accountCodes.length > 0
                ? `${accountCodes.length} account code${accountCodes.length !== 1 ? 's' : ''} available`
                : 'No account codes found'
          }
        />
      </div>
    );
  };

  return (
    <div className="analytics-page">
      <div className="analytics-page-header">
        <button 
          className="back-to-dashboard-btn"
          onClick={() => navigate('/')}
        >
          <ArrowLeft size={20} />
          Back to Dashboard
        </button>
        <div className="analytics-page-title">
          <Filter size={24} />
          <h1>Analytics Filters</h1>
        </div>
      </div>

      <div className="analytics-page-content">
          <div className="analytics-filters-container">
            <div className="filter-section">
              <div className="filter-label-wrapper">
                <Filter size={18} />
                <label className="filter-label">Filters</label>
              </div>

              {/* Filters Row */}
              <div className="filters-row">
                {/* Account Code Filter */}
                <div className="filter-group-inline">
                {renderAccountCodeField()}
                </div>

                {/* Date Filter */}
                <div className="filter-group-inline">
                  <div className="filter-inline-wrapper">
                    <label htmlFor="analytics-date" className="filter-input-label">
                      Filter by Date
                    </label>
                    <input
                      id="analytics-date"
                      type="date"
                      value={filters.endDate || ''}
                      onChange={(e) => handleDateChange(e.target.value)}
                      className="filter-input"
                      placeholder="Select date"
                    />
                  </div>
                </div>

                {/* Clear Button */}
                {hasActiveFilters && (
                  <div className="filter-group-inline clear-btn-wrapper">
                    <div className="filter-inline-wrapper">
                      <label className="filter-input-label" style={{ visibility: 'hidden' }}>
                        Clear
                      </label>
                      <button 
                        className="clear-filters-btn-inline"
                        onClick={clearFilters}
                      >
                        <XCircle size={16} />
                        Clear All
                      </button>
                    </div>
                  </div>
                )}
              </div>

              {/* Active Filters Display */}
              {hasActiveFilters && (
                <div className="active-filters">
                  <h4 className="active-filters-title">Active Filters:</h4>
                  <div className="active-filters-list">
                    {filters.accountCode && (
                      <span className="active-filter-badge">
                        Account Code: {filters.accountCode}
                      </span>
                    )}
                    {filters.endDate && (
                      <span className="active-filter-badge">
                        Date: {new Date(filters.endDate).toLocaleDateString('en-IN')}
                      </span>
                    )}
                  </div>
                </div>
              )}
            </div>
          </div>

        {/* Holdings List - Show when client is selected */}
        {filters.customerId ? (
          <div className="holdings-section">
            {holdingsError ? (
              <div className="holdings-error">
                <p style={{ color: '#e53e3e', margin: '20px 0' }}>
                  Error loading holdings: {holdingsError}
                </p>
                <button 
                  onClick={fetchHoldingsSummary}
                  style={{
                    padding: '8px 16px',
                    background: '#667eea',
                    color: 'white',
                    border: 'none',
                    borderRadius: '6px',
                    cursor: 'pointer'
                  }}
                >
                  Retry
                </button>
              </div>
            ) : (
              <HoldingsList 
                holdings={holdings}
                loading={holdingsLoading}
                onViewStock={handleViewStock}
              />
            )}
          </div>
        ) : null}

        {/* Stock Detail Modal */}
        <StockDetailModal
          isOpen={isModalOpen}
          onClose={handleCloseModal}
          stock={selectedStock}
          clientId={filters.customerId}
          endDate={filters.endDate}
        />
      </div>
    </div>
  );
};

export default Analytics;

