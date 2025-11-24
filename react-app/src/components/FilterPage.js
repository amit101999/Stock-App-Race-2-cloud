import React from 'react';
import { X, Filter, XCircle } from 'lucide-react';
import './FilterPage.css';

const FilterPage = ({ 
  isOpen,
  onClose,
  filters, 
  onFilterChange, 
  clientIds = []
}) => {
  // If used as modal, check isOpen. If used as page component, always show.
  if (isOpen !== undefined && !isOpen) return null;

  const handleClientIdChange = (value) => {
    onFilterChange({ 
      ...filters, 
      customerId: value || undefined 
    });
  };

  const handleDateChange = (value) => {
    onFilterChange({ 
      ...filters, 
      endDate: value || undefined 
    });
  };

  const clearFilters = () => {
    const newFilters = { ...filters };
    delete newFilters.customerId;
    delete newFilters.endDate;
    onFilterChange(newFilters);
  };

  const hasActiveFilters = filters.customerId || filters.endDate;

  return (
    <>
      {/* Overlay - Only show if onClose is provided (modal mode) */}
      {onClose && <div className="filter-page-overlay" onClick={onClose} />}

      {/* Filter Page */}
      <div className="filter-page">
        <div className="filter-page-header">
          <div className="filter-page-title">
            <Filter size={24} />
            <h2>Analytics Filters</h2>
          </div>
          {onClose && (
            <button 
              className="filter-page-close-btn"
              onClick={onClose}
              aria-label="Close filters"
            >
              <X size={20} />
            </button>
          )}
        </div>

        <div className="filter-page-content">
          <div className="filter-section">
            <div className="filter-label-wrapper">
              <Filter size={18} />
              <label className="filter-label">Filters</label>
            </div>

            {/* Client ID Filter */}
            <div className="filter-group">
              <label htmlFor="filter-client-id" className="filter-input-label">
                Client ID
              </label>
              <select
                id="filter-client-id"
                value={filters.customerId || ''}
                onChange={(e) => handleClientIdChange(e.target.value)}
                className="filter-select"
              >
                <option value="">All Clients</option>
                {clientIds.map((clientId) => (
                  <option key={clientId} value={clientId}>
                    {clientId}
                  </option>
                ))}
              </select>
              <p className="filter-description">
                Select a client ID to view all stock details for that specific client
              </p>
            </div>

            {/* Date Filter */}
            <div className="filter-group">
              <label htmlFor="filter-date" className="filter-input-label">
                Filter by Date
              </label>
              <input
                id="filter-date"
                type="date"
                value={filters.endDate || ''}
                onChange={(e) => handleDateChange(e.target.value)}
                className="filter-input"
                placeholder="Select date"
              />
              <p className="filter-description">
                Select a date to show all stock sell and purchase transactions up to and including this date
              </p>
            </div>

            {/* Clear Button */}
            {hasActiveFilters && (
              <button 
                className="clear-filters-btn"
                onClick={clearFilters}
              >
                <XCircle size={16} />
                Clear All Filters
              </button>
            )}

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
      </div>
    </>
  );
};

export default FilterPage;

