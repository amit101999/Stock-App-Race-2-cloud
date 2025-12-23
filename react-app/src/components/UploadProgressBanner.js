import React from 'react';
import { Database, Loader } from 'lucide-react';
import './UploadProgressBanner.css';

const UploadProgressBanner = ({ progress }) => {
  if (!progress || progress.stage === 'completed' || progress.stage === 'error') {
    return null;
  }

  const formatNumber = (num) => {
    if (!num && num !== 0) return '0';
    return new Intl.NumberFormat('en-IN').format(num);
  };

  const getStageMessage = () => {
    switch (progress.stage) {
      case 'uploading':
        return 'File uploading...';
      case 'parsing':
        return 'File parsing...';
      case 'processing':
        return 'Data processing...';
      case 'inserting':
        return 'Data Uploading to Catalyst Server...';
      default:
        return 'Processing...';
    }
  };

  return (
    <div className="upload-progress-banner">
      <div className="banner-content">
        <div className="banner-left">
          <div className="banner-icon-wrapper">
            <Database className="banner-icon" />
            <Loader className="banner-spinner" />
          </div>
          <div className="banner-text">
            <h3 className="banner-title">Catalyst Server Data Upload</h3>
            <p className="banner-message">{getStageMessage()}</p>
          </div>
        </div>
        <div className="banner-right">
          <div className="banner-stats">
            {progress.imported > 0 && (
              <div className="stat-item">
                <span className="stat-label">Uploaded:</span>
                <span className="stat-value">{formatNumber(progress.imported)} records</span>
              </div>
            )}
            {progress.totalRows > 0 && (
              <div className="stat-item">
                <span className="stat-label">Total:</span>
                <span className="stat-value">{formatNumber(progress.totalRows)} rows</span>
              </div>
            )}
            {progress.processedRows > 0 && (
              <div className="stat-item">
                <span className="stat-label">Processed:</span>
                <span className="stat-value">{formatNumber(progress.processedRows)}</span>
              </div>
            )}
          </div>
          <div className="banner-progress-wrapper">
            <div className="banner-progress-bar">
              <div
                className="banner-progress-fill"
                style={{ width: `${progress.progress || 0}%` }}
              />
            </div>
            <span className="banner-progress-percent">
              {progress.progress !== undefined && progress.progress !== null
                ? `${progress.progress}%`
                : '0%'}
            </span>
          </div>
        </div>
      </div>
    </div>
  );
};

export default UploadProgressBanner;

