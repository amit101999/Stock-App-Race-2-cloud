import React, { useState, useEffect } from 'react';
import { tradesAPI } from '../services/api';
import StatCard from '../components/StatCard';
import Charts from '../components/Charts';
import FilterBar from '../components/FilterBar';
import TradesTable from '../components/TradesTable';
import ImportButton from '../components/ImportButton';
import UploadProgressBanner from '../components/UploadProgressBanner';
import { TrendingUp, DollarSign, Activity, BarChart3 } from 'lucide-react';

const Dashboard = () => {
  const [stats, setStats] = useState({});
  const [stocks, setStocks] = useState([]);
  const [exchanges, setExchanges] = useState([]);
  const [transactionTypes, setTransactionTypes] = useState([]);
  const [clientIds, setClientIds] = useState([]);
  const [filters, setFilters] = useState({});
  const [loading, setLoading] = useState(true);
  const [uploadProgress, setUploadProgress] = useState(null);

  useEffect(() => {
    fetchStats();
    fetchFilterOptions();
  }, []);

  useEffect(() => {
    fetchStats();
  }, [filters]);

  const fetchStats = async () => {
    try {
      const response = await tradesAPI.getStats({
        stockSymbol: filters.stockSymbol,
        exchange: filters.exchange,
        tradeType: filters.tradeType,
        customerId: filters.customerId,
        startDate: filters.startDate,
        endDate: filters.endDate,
      });
      const serverStats = response?.data?.data || {};
      if (serverStats && serverStats.overall) {
        if (!serverStats.overall.totalTrades || serverStats.overall.totalTrades === 0) {
          try {
            const listRes = await tradesAPI.getTrades({
              page: 1,
              limit: 1,
              ...filters,
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
          ...filters,
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
          ...filters,
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
  };

  const fetchFilterOptions = async () => {
    try {
      const [stocksRes, exchangesRes, transactionTypesRes, clientIdsRes] = await Promise.all([
        tradesAPI.getStocks(),
        tradesAPI.getExchanges(),
        tradesAPI.getTransactionTypes(),
        tradesAPI.getClientIds()
      ]);
      setStocks(stocksRes.data.data || []);
      setExchanges(exchangesRes.data.data || []);
      setTransactionTypes(transactionTypesRes.data.data || []);
      setClientIds(clientIdsRes.data.data || []);
    } catch (error) {
      console.error('Error fetching filter options:', error);
    }
  };

  const handleImportSuccess = async () => {
    setLoading(true);
    await Promise.all([fetchStats(), fetchFilterOptions()]);
    setLoading(false);
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
            <ImportButton 
              onImportSuccess={handleImportSuccess}
              onProgressUpdate={setUploadProgress}
            />
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
              onFilterChange={setFilters} 
              stocks={stocks}
              exchanges={exchanges}
              transactionTypes={transactionTypes}
              clientIds={clientIds}
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

