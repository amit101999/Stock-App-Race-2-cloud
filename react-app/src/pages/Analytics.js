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
  const [clientIds, setClientIds] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [holdings, setHoldings] = useState([]);
  const [holdingsLoading, setHoldingsLoading] = useState(false);
  const [holdingsError, setHoldingsError] = useState(null);
  const [selectedStock, setSelectedStock] = useState(null);
  const [isModalOpen, setIsModalOpen] = useState(false);

  useEffect(() => {
    fetchFilterOptions();
  }, []);


  useEffect(() => {
    // Fetch holdings when client ID is selected, or when date is selected (even without client ID)
    if (filters.customerId || filters.endDate) {
      fetchHoldingsSummary();
    } else {
      setHoldings([]);
      setHoldingsError(null);
    }
  }, [filters.customerId, filters.endDate]);

  const fetchFilterOptions = async () => {
    try {
      setLoading(true);
      setError(null);
      console.log('[Analytics] Fetching client IDs...');
      const startTime = Date.now();
      
      const clientIdsRes = await tradesAPI.getClientIds();
      
      console.log('[Analytics] Raw API response:', clientIdsRes);
      
      // Handle both direct array response and wrapped response
      const clientIdsData = clientIdsRes?.data?.data || clientIdsRes?.data || [];
      
      console.log('[Analytics] Extracted clientIdsData:', clientIdsData);
      console.log('[Analytics] Is array?', Array.isArray(clientIdsData));
      console.log('[Analytics] Length:', clientIdsData?.length);
      
      const sortedClientIds = Array.isArray(clientIdsData) 
        ? [...new Set(clientIdsData)].sort() 
        : [];
      
      const duration = Date.now() - startTime;
      console.log(`[Analytics] Loaded ${sortedClientIds.length} client IDs in ${duration}ms`);
      console.log(`[Analytics] First 5 IDs:`, sortedClientIds.slice(0, 5));
      console.log(`[Analytics] Last 5 IDs:`, sortedClientIds.slice(-5));
      
      if (sortedClientIds.length === 0) {
        setError('No client IDs found. Please check your database connection.');
      }
      
      setClientIds(sortedClientIds);
    } catch (error) {
      console.error('[Analytics] Error fetching filter options:', error);
      console.error('[Analytics] Error response:', error.response);
      console.error('[Analytics] Error details:', error.response?.data || error.message);
      
      const errorMessage = error.response?.data?.message || error.message || 'Failed to fetch client IDs';
      setError(`Error: ${errorMessage}. Please check your backend server and database connection.`);
      setClientIds([]);
    } finally {
      setLoading(false);
    }
  };

  const handleClientIdChange = (value) => {
    // Clear stock filter when client changes
    setFilters({ 
      ...filters, 
      customerId: value || undefined,
      stockName: undefined // Clear stock when client changes
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

  const hasActiveFilters = filters.customerId || filters.endDate;

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
        {loading ? (
          <div className="loading-container">
            <div className="spinner"></div>
            <p>Loading filters...</p>
          </div>
        ) : (
          <div className="analytics-filters-container">
            <div className="filter-section">
              <div className="filter-label-wrapper">
                <Filter size={18} />
                <label className="filter-label">Filters</label>
              </div>

              {/* Filters Row */}
              <div className="filters-row">
                {/* Client ID Filter */}
                <div className="filter-group-inline">
                  {loading ? (
                    <div className="filter-inline-wrapper">
                      <label htmlFor="analytics-client-id" className="filter-input-label">
                        Client ID
                      </label>
                      <div className="filter-select" style={{ opacity: 0.6, cursor: 'not-allowed' }}>
                        Loading client IDs...
                      </div>
                    </div>
                  ) : error ? (
                    <div className="filter-inline-wrapper">
                      <label htmlFor="analytics-client-id" className="filter-input-label">
                        Client ID
                      </label>
                      <div className="filter-select" style={{ borderColor: '#e53e3e', color: '#e53e3e' }}>
                        Error loading client IDs
                      </div>
                      <button 
                        onClick={fetchFilterOptions}
                        className="retry-btn"
                      >
                        Retry
                      </button>
                    </div>
                  ) : (
                    <div className="filter-inline-wrapper">
                      <label htmlFor="analytics-client-id" className="filter-input-label">
                        Client ID
                      </label>
                      <SearchableSelect
                        id="analytics-client-id"
                        label=""
                        value={filters.customerId || ''}
                        onChange={handleClientIdChange}
                        options={clientIds}
                        placeholder="All Clients"
                        searchPlaceholder="Search client ID..."
                        description=""
                        countText={clientIds.length > 0 ? `${clientIds.length} client${clientIds.length !== 1 ? 's' : ''} available` : 'No client IDs found'}
                      />
                    </div>
                  )}
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
                    {filters.customerId && (
                      <span className="active-filter-badge">
                        Client ID: {filters.customerId}
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
        )}

        {/* Holdings List */}
        {(filters.customerId || filters.endDate) && (
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
        )}

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

