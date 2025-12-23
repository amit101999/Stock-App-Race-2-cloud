import React, { useState, useEffect, useCallback, useRef } from 'react';
import { tradesAPI } from '../services/api';
import StatCard from '../components/StatCard';
import Charts from '../components/Charts';
import FilterBar from '../components/FilterBar';
import TradesTable from '../components/TradesTable';
import ImportButton from '../components/ImportButton';
import UploadProgressBanner from '../components/UploadProgressBanner';
import { TrendingUp, DollarSign, Activity, BarChart3, Gift, Loader } from 'lucide-react';

const Dashboard = () => {
  const [stats, setStats] = useState({});
  const [stocks, setStocks] = useState([]);
  const [exchanges, setExchanges] = useState([]);
  const [transactionTypes, setTransactionTypes] = useState([]);
  const [accountCodes, setAccountCodes] = useState([]);
  const [accountCodeToClientId, setAccountCodeToClientId] = useState({}); // Cache for account code -> client ID mapping
  const [filters, setFilters] = useState({});
  
  // Handle filter change with Account Code to Client ID resolution
  const handleFilterChange = async (newFilters) => {
    // If accountCode is being set, resolve Client ID
    if (newFilters.accountCode !== undefined && newFilters.accountCode !== filters.accountCode) {
      const accountCode = newFilters.accountCode;
      
      if (!accountCode) {
        // Account code cleared, clear client ID too
        setFilters({ ...newFilters, customerId: undefined, accountCode: undefined });
        return;
      }
      
      // Check cache first
      let clientId = accountCodeToClientId[accountCode];
      
      if (!clientId) {
        // Fetch client ID from account code
        try {
          console.log(`[Dashboard] Resolving client ID for account code: ${accountCode}`);
          const clientIdRes = await tradesAPI.getClientIdByAccountCode(accountCode);
          clientId = clientIdRes?.data?.clientId;
          
          if (clientId) {
            // Cache the mapping
            setAccountCodeToClientId(prev => ({
              ...prev,
              [accountCode]: clientId
            }));
            console.log(`[Dashboard] Resolved client ID ${clientId} for account code ${accountCode}`);
          }
        } catch (error) {
          console.error(`[Dashboard] Error resolving client ID for account code ${accountCode}:`, error);
        }
      }
      
      // Update filters with resolved client ID
      setFilters({ 
        ...newFilters, 
        customerId: clientId || undefined,
        accountCode: accountCode
      });
    } else {
      // No account code change, just update filters normally
      setFilters(newFilters);
    }
  };
  const [loading, setLoading] = useState(true);
  const [uploadProgress, setUploadProgress] = useState(null);
  const [isUploadingBonus, setIsUploadingBonus] = useState(false);
  const bonusFileInputRef = useRef(null);

  const fetchStats = useCallback(async () => {
    setLoading(true);
    try {
      const response = await tradesAPI.getStats();
      const serverStats = response?.data?.data ?? response?.data ?? {};
      if (serverStats && serverStats.overall) {
        if (!serverStats.overall.totalTrades || serverStats.overall.totalTrades === 0) {
          try {
            const listRes = await tradesAPI.getTrades({
              page: 1,
              limit: 1,
            });
            const pagination = listRes?.data?.pagination || {};
            setStats({
              ...serverStats,
              overall: {
                ...serverStats.overall,
                totalTrades: pagination.total || 0,
              },
            });
          } catch {
            setStats(serverStats);
          }
        } else {
          setStats(serverStats);
        }
        return;
      }
      try {
        const listRes = await tradesAPI.getTrades({
          page: 1,
          limit: 1,
        });
        const pagination = listRes?.data?.pagination || {};
        setStats({
          overall: {
            totalTrades: pagination.total || 0,
            totalNetAmount: 0,
            avgTradeValue: 0,
            buyTrades: 0,
            sellTrades: 0,
            completedTrades: 0,
          },
          topStocks: [],
          exchangeStats: [],
          dailyVolume: [],
        });
      } catch (fallbackErr) {
        setStats({ overall: {} });
      }
    } catch (error) {
      console.error('Error fetching stats:', error);
      try {
        const listRes = await tradesAPI.getTrades({
          page: 1,
          limit: 1,
        });
        const pagination = listRes?.data?.pagination || {};
        setStats({
          overall: {
            totalTrades: pagination.total || 0,
            totalNetAmount: 0,
            avgTradeValue: 0,
            buyTrades: 0,
            sellTrades: 0,
            completedTrades: 0,
          },
          topStocks: [],
          exchangeStats: [],
          dailyVolume: [],
        });
      } catch (e) {
        // swallow
      }
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchStats();
    fetchFilterOptions();
  }, [fetchStats]);

  const fetchFilterOptions = async () => {
    try {
      const [stocksRes, exchangesRes, transactionTypesRes, accountCodesRes] = await Promise.all([
        tradesAPI.getStocks(),
        tradesAPI.getExchanges(),
        tradesAPI.getTransactionTypes(),
        tradesAPI.getAccountCodes()
      ]);
      setStocks(stocksRes.data.data || []);
      setExchanges(exchangesRes.data.data || []);
      setTransactionTypes(transactionTypesRes.data.data || []);
      setAccountCodes(accountCodesRes.data.data || []);
    } catch (error) {
      console.error('Error fetching filter options:', error);
    }
  };

  const handleImportSuccess = async () => {
    setLoading(true);
    await Promise.all([fetchStats(), fetchFilterOptions()]);
    setLoading(false);
  };

  const handleBonusFileSelect = async (event) => {
    const file = event.target.files[0];
    if (!file) return;

    // Validate file type (Excel only for bonuses)
    const isExcel = (
      file.name.endsWith('.xlsx') ||
      file.name.endsWith('.xls') ||
      file.name.endsWith('.xlsb') ||
      file.type === 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' ||
      file.type === 'application/vnd.ms-excel' ||
      file.type === 'application/vnd.ms-excel.sheet.binary.macroEnabled.12'
    );

    if (!isExcel) {
      alert('Please select a valid Excel file (.xlsx, .xls, .xlsb)');
      return;
    }

    // Validate file size (200MB limit)
    if (file.size > 200 * 1024 * 1024) {
      alert('File size must be less than 200MB');
      return;
    }

    await uploadBonusFile(file);
  };

  const uploadBonusFile = async (file) => {
    setIsUploadingBonus(true);

    try {
      const formData = new FormData();
      formData.append('file', file);

      const response = await tradesAPI.importBonus(formData);

      if (response.data.success) {
        // Start polling for progress
        const importId = response.data.importId;
        if (importId) {
          const progressInterval = setInterval(async () => {
            try {
              const progressResponse = await tradesAPI.getImportProgress(importId);
              if (progressResponse.data.success) {
                const progressData = progressResponse.data.progress;
                setUploadProgress(progressData);
                
                // Stop polling if completed or error
                if (progressData.stage === 'completed' || progressData.stage === 'error') {
                  clearInterval(progressInterval);
                  setIsUploadingBonus(false);
                  // Clear progress after 2 seconds
                  setTimeout(() => {
                    setUploadProgress(null);
                  }, 2000);
                  
                  // Refresh data on success
                  if (progressData.stage === 'completed') {
                    await handleImportSuccess();
                  }
                }
              }
            } catch (err) {
              console.log('Progress polling error (ignored):', err.message);
            }
          }, 500); // Poll every 500ms

          // Initialize progress immediately
          setUploadProgress({
            stage: 'uploading',
            progress: 5,
            message: 'File uploaded, starting bonus import...',
            totalRows: 0,
            processedRows: 0,
            imported: 0,
            errors: 0
          });
        }
      }
    } catch (error) {
      console.error('Bonus upload error:', error);
      alert(error.response?.data?.error || error.message || 'Failed to upload bonus file');
      setIsUploadingBonus(false);
    } finally {
      // Reset file input
      if (bonusFileInputRef.current) {
        bonusFileInputRef.current.value = '';
      }
    }
  };

  const handleBonusButtonClick = () => {
    bonusFileInputRef.current?.click();
  };

  const formatCurrency = (value) => {
    if (!value) return '₹0';
    return new Intl.NumberFormat('en-IN', {
      style: 'currency',
      currency: 'INR',
      maximumFractionDigits: 0,
    }).format(value);
  };

  const formatNumber = (value) => {
    if (!value) return '0';
    return new Intl.NumberFormat('en-IN').format(value);
  };

  const { overall = {} } = stats;

  return (
    <div className="dashboard-page">
      {/* MongoDB Upload Progress Banner */}
      <UploadProgressBanner progress={uploadProgress} />
      
      <header className="app-header" style={{ marginTop: uploadProgress ? '60px' : '0' }}>
        <div className="header-content">
          <div className="header-left">
            <div className="header-title">
              <BarChart3 size={32} />
              <h1>Stock Portfolio Dashboard</h1>
            </div>
            <p className="header-subtitle">Real-time stock trading analytics and insights</p>
          </div>
          <div className="header-right">
            <div className="header-buttons">
              <ImportButton 
                onImportSuccess={handleImportSuccess}
                onProgressUpdate={setUploadProgress}
              />
              <input
                ref={bonusFileInputRef}
                type="file"
                accept=".xlsx,.xls,.xlsb,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/vnd.ms-excel,application/vnd.ms-excel.sheet.binary.macroEnabled.12"
                onChange={handleBonusFileSelect}
                style={{ display: 'none' }}
              />
              <button 
                className="upload-bonuses-button"
                onClick={handleBonusButtonClick}
                disabled={isUploadingBonus}
              >
                {isUploadingBonus ? (
                  <>
                    <Loader className="spinning" size={20} />
                    <span>Uploading...</span>
                  </>
                ) : (
                  <>
                    <Gift size={20} />
                    <span>Upload Bonuses</span>
                  </>
                )}
              </button>
            </div>
          </div>
        </div>
      </header>

      <main className="app-main">
        {loading ? (
          <div className="loading-container">
            <div className="spinner"></div>
            <p>Loading dashboard...</p>
          </div>
        ) : (
          <>
            {/* Statistics Cards */}
            <div className="stats-grid">
              <StatCard
                title="Total Trades"
                value={formatNumber(overall.totalTrades)}
                subtitle="All time trades"
                icon={Activity}
              />
              <StatCard
                title="Total Value"
                value={formatCurrency(overall.totalNetAmount)}
                subtitle={`Avg: ${formatCurrency(overall.avgTradeValue)}`}
                icon={DollarSign}
              />
              <StatCard
                title="Buy Trades"
                value={formatNumber(overall.buyTrades)}
                subtitle={`${overall.totalTrades ? ((overall.buyTrades / overall.totalTrades) * 100).toFixed(1) : 0}% of total`}
                icon={TrendingUp}
                trend="up"
              />
              <StatCard
                title="Sell Trades"
                value={formatNumber(overall.sellTrades)}
                subtitle={`${overall.totalTrades ? ((overall.sellTrades / overall.totalTrades) * 100).toFixed(1) : 0}% of total`}
                icon={TrendingUp}
                trend="down"
              />
              <StatCard
                title="Completed"
                value={formatNumber(overall.completedTrades)}
                subtitle={`${overall.totalTrades ? ((overall.completedTrades / overall.totalTrades) * 100).toFixed(1) : 0}% success rate`}
                icon={Activity}
                trend="up"
              />
            </div>

            {/* Charts */}
            <Charts stats={stats} />

            {/* Filters */}
            <FilterBar 
              filters={filters} 
              onFilterChange={handleFilterChange} 
              stocks={stocks}
              exchanges={exchanges}
              transactionTypes={transactionTypes}
              accountCodes={accountCodes}
            />

            {/* Trades Table */}
            <TradesTable filters={filters} onFilterChange={setFilters} />
          </>
        )}
      </main>
    </div>
  );
};

export default Dashboard;

