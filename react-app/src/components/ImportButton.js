import React, { useState, useRef } from 'react';
import { Upload, CheckCircle, XCircle, Loader } from 'lucide-react';
import { tradesAPI } from '../services/api';
import './ImportButton.css';

const ImportButton = ({ onImportSuccess, onProgressUpdate }) => {
  const [isImporting, setIsImporting] = useState(false);
  const [importStatus, setImportStatus] = useState(null);
  const [error, setError] = useState(null);
  const [progress, setProgress] = useState(null);
  const fileInputRef = useRef(null);
  const progressIntervalRef = useRef(null);
  const importIdRef = useRef(null);

  const handleFileSelect = async (event) => {
    const file = event.target.files[0];
    if (!file) return;

    // Validate file type (Excel or CSV)
    const isExcel = (
      file.name.endsWith('.xlsx') ||
      file.name.endsWith('.xls') ||
      file.name.endsWith('.xlsb') ||
      file.type === 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' ||
      file.type === 'application/vnd.ms-excel' ||
      file.type === 'application/vnd.ms-excel.sheet.binary.macroEnabled.12'
    );
    const isCSV = (
      file.name.endsWith('.csv') ||
      file.type === 'text/csv' ||
      file.type === 'application/csv' ||
      file.type === 'text/plain'
    );

    if (!isExcel && !isCSV) {
      setError('Please select a valid Excel file (.xlsx, .xls, .xlsb) or CSV file (.csv)');
      setImportStatus(null);
      return;
    }

    // Validate file size (200MB limit)
    if (file.size > 200 * 1024 * 1024) {
      setError('File size must be less than 200MB');
      setImportStatus(null);
      return;
    }

    await importFile(file);
  };

  const importFile = async (file) => {
    setIsImporting(true);
    setError(null);
    setImportStatus(null);
    setProgress(null);
    importIdRef.current = null;

    // Clear any existing progress interval
    if (progressIntervalRef.current) {
      clearInterval(progressIntervalRef.current);
      progressIntervalRef.current = null;
    }

    try {
      const formData = new FormData();
      formData.append('file', file);

      // Start polling for progress
      const startProgressPolling = (importId) => {
        progressIntervalRef.current = setInterval(async () => {
          try {
            const progressResponse = await tradesAPI.getImportProgress(importId);
            if (progressResponse.data.success) {
              const progressData = progressResponse.data.progress;
              setProgress(progressData);
              
              // Notify parent component about progress update
              if (onProgressUpdate) {
                onProgressUpdate(progressData);
              }
              
              // Stop polling if completed or error
              if (progressData.stage === 'completed' || 
                  progressData.stage === 'error') {
                clearInterval(progressIntervalRef.current);
                progressIntervalRef.current = null;
                // Clear progress in parent after completion
                if (onProgressUpdate) {
                  setTimeout(() => onProgressUpdate(null), 2000);
                }
              }
            }
          } catch (err) {
            // Progress endpoint might not be available yet, ignore
            console.log('Progress polling error (ignored):', err.message);
          }
        }, 500); // Poll every 500ms
      };

      // Determine if file is CSV or Excel
      const isCSV = file.name.endsWith('.csv') || 
                    file.type === 'text/csv' || 
                    file.type === 'application/csv' ||
                    file.type === 'text/plain';
      
      const response = isCSV 
        ? await tradesAPI.importCSV(formData)
        : await tradesAPI.importExcel(formData);

      if (response.data.success) {
        // Start polling if importId is provided
        if (response.data.importId) {
          importIdRef.current = response.data.importId;
          // Initialize progress immediately
          if (onProgressUpdate) {
            onProgressUpdate({
              stage: 'uploading',
              progress: 5,
              message: 'File uploaded, starting import...',
              totalRows: 0,
              processedRows: 0,
              imported: 0,
              errors: 0
            });
          }
          startProgressPolling(response.data.importId);
        }

        setImportStatus({
          success: true,
          message: response.data.message,
          imported: response.data.imported,
          totalRows: response.data.totalRows,
          errors: response.data.errors,
        });

        // Call success callback to refresh data
        if (onImportSuccess) {
          setTimeout(() => {
            onImportSuccess();
          }, 1000);
        }

        // Clear status after 5 seconds
        setTimeout(() => {
          setImportStatus(null);
          setProgress(null);
          if (onProgressUpdate) {
            onProgressUpdate(null);
          }
        }, 5000);
      } else {
        setError(response.data.error || 'Failed to import file');
      }
    } catch (err) {
      console.error('Import error:', err);
      
      // If we have an importId, try to get final progress
      if (err.response?.data?.importId) {
        importIdRef.current = err.response.data.importId;
        try {
          const progressResponse = await tradesAPI.getImportProgress(err.response.data.importId);
          if (progressResponse.data.success) {
            setProgress(progressResponse.data.progress);
          }
        } catch (progressErr) {
          // Ignore progress errors
        }
      }
      
      setError(
        err.response?.data?.error ||
          err.message ||
          'Failed to import file. Please check the file format and try again.'
      );
    } finally {
      setIsImporting(false);
      // Reset file input
      if (fileInputRef.current) {
        fileInputRef.current.value = '';
      }
      // Clear progress after a delay
      setTimeout(() => {
        setProgress(null);
        if (onProgressUpdate) {
          onProgressUpdate(null);
        }
      }, 3000);
    }
  };

  const handleButtonClick = () => {
    fileInputRef.current?.click();
  };

  return (
    <div className="import-button-container">
      <input
        ref={fileInputRef}
        type="file"
        accept=".xlsx,.xls,.xlsb,.csv,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/vnd.ms-excel,application/vnd.ms-excel.sheet.binary.macroEnabled.12,text/csv,application/csv,text/plain"
        onChange={handleFileSelect}
        style={{ display: 'none' }}
      />
      <button
        className="import-button"
        onClick={handleButtonClick}
        disabled={isImporting}
      >
        {isImporting ? (
          <>
            <Loader className="import-icon spinning" />
            <span>Importing...</span>
            {progress && (
              <span className="progress-text">{progress.progress}%</span>
            )}
          </>
        ) : (
          <>
            <Upload className="import-icon" />
            <span>Import File (Excel/CSV)</span>
          </>
        )}
      </button>

      {/* Progress Bar */}
      {isImporting && progress && (
        <div className="progress-container">
          <div className="progress-bar-wrapper">
            <div 
              className="progress-bar-fill" 
              style={{ width: `${progress.progress}%` }}
            />
          </div>
          <div className="progress-info">
            <span className="progress-message">{progress.message}</span>
            {progress.totalRows > 0 && (
              <span className="progress-stats">
                {progress.processedRows}/{progress.totalRows} rows
                {progress.imported > 0 && ` • ${progress.imported} imported`}
              </span>
            )}
          </div>
        </div>
      )}

      {importStatus && (
        <div className={`import-status ${importStatus.success ? 'success' : 'error'}`}>
          {importStatus.success ? (
            <CheckCircle className="status-icon" />
          ) : (
            <XCircle className="status-icon" />
          )}
          <div className="status-content">
            <p className="status-message">{importStatus.message}</p>
            {importStatus.imported && (
              <p className="status-details">
                Imported {importStatus.imported} of {importStatus.totalRows} rows
              </p>
            )}
            {importStatus.errors && importStatus.errors.length > 0 && (
              <div className="status-errors">
                <p className="errors-title">Some rows had errors:</p>
                <ul>
                  {importStatus.errors.slice(0, 5).map((err, idx) => (
                    <li key={idx}>{err}</li>
                  ))}
                </ul>
              </div>
            )}
          </div>
        </div>
      )}

      {error && (
        <div className="import-status error">
          <XCircle className="status-icon" />
          <div className="status-content">
            <p className="status-message">{error}</p>
          </div>
          <button
            className="close-error"
            onClick={() => setError(null)}
            aria-label="Close error"
          >
            ×
          </button>
        </div>
      )}
    </div>
  );
};

export default ImportButton;

