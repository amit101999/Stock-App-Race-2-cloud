import React from 'react';
import { Filter, X } from 'lucide-react';
import SearchableSelect from './SearchableSelect';
import './FilterBar.css';

const FilterBar = ({ filters, onFilterChange, stocks = [], exchanges = [], transactionTypes = [], clientIds = [] }) => {
  const handleFilterChange = (key, value) => {
    onFilterChange({ ...filters, [key]: value || undefined });
  };

  const clearFilters = () => {
    onFilterChange({});
  };

  const hasActiveFilters = Object.values(filters).some((value) => value);

  return (
    <div className="filter-bar">
      <div className="filter-header">
        <div className="filter-title">
          <Filter size={20} />
          <h3>Filters</h3>
        </div>
        {hasActiveFilters && (
          <button className="clear-filters" onClick={clearFilters}>
            <X size={16} />
            Clear All
          </button>
        )}
      </div>
      <div className="filter-grid">
        <div className="filter-group">
          <SearchableSelect
            id="filter-stock-symbol"
            label="Search Security"
            value={filters.stockSymbol || ''}
            onChange={(value) => handleFilterChange('stockSymbol', value)}
            options={stocks}
            placeholder="All Securities"
            searchPlaceholder="Search security..."
            description=""
            countText={stocks.length > 0 ? `${stocks.length} securities available` : 'No securities found'}
          />
        </div>

        <div className="filter-group">
          <SearchableSelect
            id="filter-exchange"
            label="Exchange"
            value={filters.exchange || ''}
            onChange={(value) => handleFilterChange('exchange', value)}
            options={exchanges}
            placeholder="All Exchanges"
            searchPlaceholder="Search exchange..."
            description=""
            countText={exchanges.length > 0 ? `${exchanges.length} exchanges available` : 'No exchanges found'}
          />
        </div>

        <div className="filter-group">
          <SearchableSelect
            id="filter-transaction-type"
            label="Transaction Type"
            value={filters.tradeType || ''}
            onChange={(value) => handleFilterChange('tradeType', value)}
            options={transactionTypes}
            placeholder="All Types"
            searchPlaceholder="Search type..."
            description=""
            countText={transactionTypes.length > 0 ? `${transactionTypes.length} types available` : 'No types found'}
          />
        </div>

        <div className="filter-group">
          <SearchableSelect
            id="filter-client-id"
            label="Client ID"
            value={filters.customerId || ''}
            onChange={(value) => handleFilterChange('customerId', value)}
            options={clientIds}
            placeholder="All Clients"
            searchPlaceholder="Search client ID..."
            description=""
            countText={clientIds.length > 0 ? `${clientIds.length} clients available` : 'No clients found'}
          />
        </div>

        <div className="filter-group">
          <label>Start Date</label>
          <input
            type="date"
            value={filters.startDate || ''}
            onChange={(e) => handleFilterChange('startDate', e.target.value)}
            className="filter-input"
          />
        </div>

        <div className="filter-group">
          <label>End Date</label>
          <input
            type="date"
            value={filters.endDate || ''}
            onChange={(e) => handleFilterChange('endDate', e.target.value)}
            className="filter-input"
          />
        </div>
      </div>
    </div>
  );
};

export default FilterBar;

