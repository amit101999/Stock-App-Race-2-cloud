import React, { useState, useEffect } from 'react';
import SearchableSelect from '../components/SearchableSelect';
import { tradesAPI } from '../services/api';
import './Split.css';

const Split = () => {
  const [form, setForm] = useState({
    securityName: '',
    securityCode: '',
    shareUnit: '',
    allottedUnit: '',
    recordDate: '',
  });
  const [gridRow, setGridRow] = useState({
    clientId: '',
    stockName: '',
    securityCode: '',
    shareUnit: '',
    allottedUnit: '',
    currentQty: '',
    newQty: '',
    correctedQty: '',
  });
  const [gridRows, setGridRows] = useState([]);
  const [custodianFile, setCustodianFile] = useState(null);
  const [securityOptions, setSecurityOptions] = useState([]);
  const [isLoadingOptions, setIsLoadingOptions] = useState(false);
  const [isFormSaved, setIsFormSaved] = useState(false);
  const [currentPage, setCurrentPage] = useState(1);
  const [itemsPerPage] = useState(10);

  // Load security names for dropdown
  useEffect(() => {
    const loadSecurities = async () => {
      try {
        setIsLoadingOptions(true);
        const res = await tradesAPI.getStocks();
        const list = res?.data?.data || [];
        // Normalize to full display: prefer full name, append code if available
        const names = list
          .map((item) => {
            if (typeof item === 'string') return item;
            const name =
              item.securityName ||
              item.Security_Name ||
              item.name ||
              '';
            const code =
              item.securityCode ||
              item.Security_code ||
              item.Security_Code ||
              item.code ||
              '';
            if (name && code) return `${name} (${code})`;
            return name || code || '';
          })
          .filter(Boolean);
        setSecurityOptions(names);
      } catch (err) {
        console.error('Failed to load securities', err);
        setSecurityOptions([]);
      } finally {
        setIsLoadingOptions(false);
      }
    };
    loadSecurities();
  }, []);

  const handleChange = (e) => {
    const { name, value } = e.target;
    setForm((prev) => ({ ...prev, [name]: value }));
    // Mirror to grid row where applicable
    if (name === 'securityName') {
      setGridRow((prev) => ({ ...prev, stockName: value }));
    }
    if (name === 'securityCode') {
      setGridRow((prev) => ({ ...prev, securityCode: value }));
    }
    if (name === 'shareUnit') {
      setGridRow((prev) => ({ ...prev, shareUnit: value }));
    }
    if (name === 'allottedUnit') {
      setGridRow((prev) => ({ ...prev, allottedUnit: value }));
    }
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    console.log('Split form submitted', form);
    
    // Extract security name from dropdown value (might be "Name (Code)" format)
    let actualSecurityName = form.securityName;
    if (actualSecurityName && actualSecurityName.includes('(')) {
      // Extract just the name part before the parentheses
      actualSecurityName = actualSecurityName.split('(')[0].trim();
    }
    
    console.log('Fetching clients for:', { 
      securityName: actualSecurityName, 
      securityCode: form.securityCode 
    });
    
    try {
      const res = await tradesAPI.getClientsBySecurity({
        securityName: actualSecurityName,
        securityCode: form.securityCode,
      });
      
      console.log('Clients fetched:', res?.data?.data);
      
      const rows = (res?.data?.data || []).map((item) => ({
        clientId: String(item.clientId || ''),
        stockName: item.securityName || actualSecurityName || '',
        securityCode: item.securityCode || form.securityCode || '',
        shareUnit: form.shareUnit,
        allottedUnit: form.allottedUnit,
        currentQty: item.currentQty ?? '',
        newQty: '',
        correctedQty: '',
      }));
      
      console.log('Mapped rows:', rows);
      setGridRows(rows);
      setCurrentPage(1); // Reset to first page when new data is loaded
      
      if (rows.length === 0) {
        console.warn('No clients found for this security');
      }
    } catch (err) {
      console.error('Failed to fetch clients by security', err);
      setGridRows([]);
    }
    // Show the Split Allocation section after save
    setIsFormSaved(true);
  };

  // Pagination calculations
  const totalPages = Math.ceil((gridRows.length || 0) / itemsPerPage);
  const startIndex = (currentPage - 1) * itemsPerPage;
  const endIndex = startIndex + itemsPerPage;
  const paginatedRows = gridRows.length ? gridRows.slice(startIndex, endIndex) : [];

  const handlePageChange = (newPage) => {
    if (newPage >= 1 && newPage <= totalPages) {
      setCurrentPage(newPage);
      // Scroll to top of table when page changes
      const tableCard = document.querySelector('.split-table-card');
      if (tableCard) {
        tableCard.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }
    }
  };

  return (
    <div className="split-page">
      <div className="split-header">
        <h1>Split</h1>
      </div>

      <div className="split-card">
        <form onSubmit={handleSubmit} className="split-form">
          <div className="form-row horizontal-row">
            <div className="form-field-left">
              <SearchableSelect
                id="securityName"
                label="Security Name"
                value={form.securityName}
                onChange={(val) => setForm((prev) => ({ ...prev, securityName: val }))}
                options={securityOptions}
                placeholder={isLoadingOptions ? 'Loading...' : 'Select security'}
                searchPlaceholder="Search security..."
              />
            </div>
            <div className="form-field-right">
              <label htmlFor="securityCode">Security Code</label>
              <input
                id="securityCode"
                name="securityCode"
                type="text"
                value={form.securityCode}
                onChange={handleChange}
                placeholder="e.g., ACC"
                required
              />
            </div>
          </div>

          <div className="form-row horizontal-row">
            <div className="form-field">
              <label htmlFor="shareUnit">Share Unit</label>
              <input
                id="shareUnit"
                name="shareUnit"
                type="number"
                min="0"
                step="1"
                value={form.shareUnit}
                onChange={handleChange}
                placeholder="e.g., 1"
                required
              />
            </div>
            <div className="form-field">
              <label htmlFor="allottedUnit">Allotted Unit</label>
              <input
                id="allottedUnit"
                name="allottedUnit"
                type="number"
                min="0"
                step="1"
                value={form.allottedUnit}
                onChange={handleChange}
                placeholder="e.g., 2"
                required
              />
            </div>
            <div className="form-field-right">
              <label htmlFor="recordDate">Record Date</label>
              <input
                id="recordDate"
                name="recordDate"
                type="date"
                value={form.recordDate}
                onChange={handleChange}
                required
              />
            </div>
          </div>

          <div className="form-actions">
            <button type="submit" className="btn-primary">Save Split</button>
          </div>
        </form>
      </div>

      {isFormSaved && (
        <div className="split-table-card">
        <h2>Split Allocation</h2>
        <div className="table-form">
          <div className="table-row table-header">
            <div>Client ID</div>
            <div>Stock Name</div>
            <div>Security Code</div>
            <div>Share Unit</div>
            <div>Allotted Unit</div>
            <div>Current Qty</div>
            <div>Split Qty</div>
            <div>New Qty</div>
            <div>Corrected Qty</div>
          </div>
          <div className="table-rows">
            {paginatedRows.length > 0 ? (
              paginatedRows.map((row, idx) => {
                // Calculate actual index in full array for correct updates
                const actualIndex = startIndex + idx;
                // Calculate Split Qty: (Current Qty / Share Unit) * Allotted Unit
                const currentQty = Number(row.currentQty) || 0;
                const shareUnit = Number(row.shareUnit) || 1;
                const allottedUnit = Number(row.allottedUnit) || 0;
                const splitQty = shareUnit > 0 
                  ? Math.floor((currentQty / shareUnit) * allottedUnit)
                  : 0;
                // Calculate New Qty: Current Qty + Split Qty
                const newQty = currentQty + splitQty;
                
                return (
                  <div className="table-row" key={`${row.clientId}-${actualIndex}`}>
                    <input
                      type="text"
                      value={row.clientId}
                      readOnly
                      placeholder="Client ID"
                    />
                    <input
                      type="text"
                      value={row.stockName}
                      readOnly
                      placeholder="Stock Name"
                    />
                    <input
                      type="text"
                      value={row.securityCode}
                      readOnly
                      placeholder="Code"
                    />
                    <input
                      type="number"
                      min="0"
                      step="1"
                      value={row.shareUnit}
                      readOnly
                      placeholder="Share Unit"
                    />
                    <input
                      type="number"
                      min="0"
                      step="1"
                      value={row.allottedUnit}
                      readOnly
                      placeholder="Allotted Unit"
                    />
                    <input
                      type="number"
                      min="0"
                      step="1"
                      value={row.currentQty}
                      readOnly
                      placeholder="Current Qty"
                    />
                    <input
                      type="number"
                      min="0"
                      step="1"
                      value={splitQty}
                      readOnly
                      placeholder="Split Qty"
                    />
                    <input
                      type="number"
                      min="0"
                      step="1"
                      value={newQty}
                      readOnly
                      placeholder="New Qty"
                    />
                    <input
                      type="number"
                      min="0"
                      step="1"
                      value={row.correctedQty || ''}
                      onChange={(e) => setGridRows((prev) => {
                        const next = [...prev];
                        next[actualIndex] = { ...row, correctedQty: e.target.value };
                        return next;
                      })}
                      placeholder="Corrected Qty"
                    />
                  </div>
                );
              })
            ) : (
              <div className="no-data-message">
                <p>No clients found for this security</p>
              </div>
            )}
          </div>
          
          {/* Pagination Controls */}
          {gridRows.length > 0 && (
            <div className="pagination-controls">
              <div className="pagination-info">
                Showing {startIndex + 1} to {Math.min(endIndex, gridRows.length)} of {gridRows.length} clients
              </div>
              <div className="pagination-buttons">
                <button
                  type="button"
                  className="pagination-btn"
                  onClick={() => handlePageChange(currentPage - 1)}
                  disabled={currentPage === 1}
                >
                  Previous
                </button>
                <div className="pagination-numbers">
                  {Array.from({ length: totalPages }, (_, i) => i + 1).map((page) => {
                    // Show first page, last page, current page, and pages around current
                    if (
                      page === 1 ||
                      page === totalPages ||
                      (page >= currentPage - 1 && page <= currentPage + 1)
                    ) {
                      return (
                        <button
                          key={page}
                          type="button"
                          className={`pagination-btn ${currentPage === page ? 'active' : ''}`}
                          onClick={() => handlePageChange(page)}
                        >
                          {page}
                        </button>
                      );
                    } else if (
                      page === currentPage - 2 ||
                      page === currentPage + 2
                    ) {
                      return <span key={page} className="pagination-ellipsis">...</span>;
                    }
                    return null;
                  })}
                </div>
                <button
                  type="button"
                  className="pagination-btn"
                  onClick={() => handlePageChange(currentPage + 1)}
                  disabled={currentPage === totalPages}
                >
                  Next
                </button>
              </div>
            </div>
          )}
        </div>

        <div className="custodian-upload">
          <label htmlFor="custodianFile">Custodian File</label>
          <input
            id="custodianFile"
            type="file"
            onChange={(e) => setCustodianFile(e.target.files?.[0] || null)}
          />
          {custodianFile && <p className="file-name">{custodianFile.name}</p>}
        </div>
      </div>
      )}
    </div>
  );
};

export default Split;

