import React from 'react';
import {
  LineChart,
  Line,
  BarChart,
  Bar,
  PieChart,
  Pie,
  Cell,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts';
import './Charts.css';

const COLORS = ['#667eea', '#f093fb', '#4facfe', '#43e97b', '#fa709a', '#fee140', '#764ba2', '#f5576c'];

const formatCompactNumber = (value, isCurrency = false) => {
  if (!value) return isCurrency ? '₹0' : '0';
  const formatter = new Intl.NumberFormat('en-IN', {
    notation: 'compact',
    maximumFractionDigits: 1,
    ...(isCurrency
      ? { style: 'currency', currency: 'INR' }
      : {}),
  });
  return formatter.format(value);
};

const Charts = ({ stats }) => {
  const { overall = {}, topStocks = [], exchangeStats = [], dailyVolume = [] } = stats;

  // Debug logging
  console.log('[Charts] Stats received:', {
    overall,
    topStocksCount: topStocks.length,
    exchangeStatsCount: exchangeStats.length,
    dailyVolumeCount: dailyVolume.length,
    topStocks: topStocks.slice(0, 3),
    exchangeStats: exchangeStats.slice(0, 3),
    dailyVolume: dailyVolume.slice(0, 3)
  });

  // Prepare data for pie chart (exchange distribution)
  const exchangeData = (exchangeStats || [])
    .map((item) => ({
      name: (item._id || item.name || '').trim(),
      value: item.count || item.value || 0,
      totalValue: item.totalValue || 0,
    }))
    .filter(item => {
      if (!item.name) return false;
      const normalized = item.name.toLowerCase();
      if (normalized === 'unknown' || normalized === 'other') return false;
      return item.value > 0 || item.totalValue > 0;
    });

  // Prepare data for top stocks bar chart
  const topStocksData = (topStocks || []).map((item) => ({
    name: item._id || item.name || 'Unknown',
    trades: item.tradeCount || item.trades || 0,
    value: item.totalValue || item.value || 0,
    quantity: item.totalQuantity || item.quantity || 0,
  })).filter(item => item.value > 0 || item.trades > 0);

  // Prepare yearly volume data (using dailyVolume key from backend but contains yearly data)
  const dailyData = (dailyVolume || [])
    .map((item) => ({
      date: item._id || item.date || '',
      trades: item.count || item.trades || 0,
      value: item.totalValue || item.value || 0,
      buyTrades: item.buyTrades || 0,
      sellTrades: item.sellTrades || 0,
    }))
    .filter(item => item.date); // Filter out items without dates

  const exchangeValueTotal = exchangeData.reduce((sum, item) => sum + (item.totalValue || 0), 0);

  const renderEmptyState = (message) => (
    <div className="chart-empty-state">
      <div className="chart-empty-icon">📊</div>
      <p>{message}</p>
      <span>Import or filter trades to unlock this insight.</span>
    </div>
  );

  return (
    <div className="charts-container">
      <div className="charts-grid">
        {/* Yearly Trade Volume Line Chart */}
        <div className="chart-card">
          <div className="chart-card-header">
            <div>
              <h3>Yearly Trade Volume (Buy/Sell Breakdown)</h3>
              <p>Track how buying vs selling evolved year over year.</p>
            </div>
          </div>
          {dailyData.length === 0 ? (
            renderEmptyState('No yearly volume data available yet.')
          ) : (
            <ResponsiveContainer width="100%" height={260}>
            <LineChart data={dailyData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
              <XAxis
                dataKey="date"
                stroke="#718096"
                tick={{ fontSize: 12 }}
                label={{ value: 'Year', position: 'insideBottom', offset: -5 }}
              />
              <YAxis 
                stroke="#718096" 
                tick={{ fontSize: 12 }}
                tickFormatter={(value) => {
                  if (value >= 1000000) {
                    return `${(value / 1000000).toFixed(1)}M`;
                  } else if (value >= 1000) {
                    return `${(value / 1000).toFixed(1)}K`;
                  }
                  return value.toString();
                }}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: 'white',
                  border: '1px solid #e2e8f0',
                  borderRadius: '8px',
                }}
                formatter={(value, name) => {
                  if (typeof value === 'number') {
                    return [new Intl.NumberFormat('en-IN').format(value), name];
                  }
                  return [value, name];
                }}
              />
              <Line
                type="monotone"
                dataKey="trades"
                stroke="#667eea"
                strokeWidth={3}
                name="Number of Trades"
                dot={{ fill: '#667eea', r: 4 }}
              />
              <Line
                type="monotone"
                dataKey="buyTrades"
                stroke="#4facfe"
                strokeWidth={3}
                name="Buy Trades"
                dot={{ fill: '#4facfe', r: 4 }}
              />
              <Line
                type="monotone"
                dataKey="sellTrades"
                stroke="#43e97b"
                strokeWidth={3}
                name="Sell Trades"
                dot={{ fill: '#43e97b', r: 4 }}
              />
              <Line
                type="monotone"
                dataKey="value"
                stroke="#f093fb"
                strokeWidth={3}
                name="Total Value (₹)"
                dot={{ fill: '#f093fb', r: 4 }}
              />
            </LineChart>
          </ResponsiveContainer>
          )}
        </div>

        {/* Top Stocks Bar Chart */}
        <div className="chart-card">
          <div className="chart-card-header">
            <div>
              <h3>Top 10 Stocks by Trade Value</h3>
              <p>Where most of the capital is concentrated.</p>
            </div>
          </div>
          {topStocksData.length === 0 ? (
            renderEmptyState('No stock data available.')
          ) : (
            <ResponsiveContainer width="100%" height={260}>
            <BarChart data={topStocksData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
              <XAxis
                dataKey="name"
                stroke="#718096"
                tick={{ fontSize: 12 }}
                angle={-45}
                textAnchor="end"
                height={80}
              />
              <YAxis 
                stroke="#718096" 
                tick={{ fontSize: 12 }}
                tickFormatter={(value) => {
                  if (value >= 10000000) {
                    return `₹${(value / 10000000).toFixed(1)}Cr`;
                  } else if (value >= 100000) {
                    return `₹${(value / 100000).toFixed(1)}L`;
                  } else if (value >= 1000) {
                    return `₹${(value / 1000).toFixed(1)}K`;
                  }
                  return `₹${value}`;
                }}
              />
              <Tooltip
                contentStyle={{
                  backgroundColor: 'white',
                  border: '1px solid #e2e8f0',
                  borderRadius: '8px',
                }}
                formatter={(value) => {
                  if (typeof value === 'number') {
                    return new Intl.NumberFormat('en-IN', {
                      style: 'currency',
                      currency: 'INR',
                      maximumFractionDigits: 0,
                    }).format(value);
                  }
                  return value;
                }}
              />
              <Bar dataKey="value" fill="#667eea" name="Total Value (₹)" radius={[8, 8, 0, 0]} />
              <Bar dataKey="trades" fill="#4facfe" name="Number of Trades" radius={[8, 8, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
          )}
        </div>

        {/* Exchange Distribution Pie Chart */}
        <div className="chart-card">
          <div className="chart-card-header">
            <div>
              <h3>Exchange Distribution</h3>
              <p>Understand where your trades are executed.</p>
            </div>
          </div>
          {exchangeData.length === 0 ? (
            renderEmptyState('No exchange data available.')
          ) : (
            <ResponsiveContainer width="100%" height={260}>
              <PieChart>
                <Pie
                  data={exchangeData}
                  cx="50%"
                  cy="50%"
                  labelLine={false}
                  outerRadius={100}
                  fill="#8884d8"
                  dataKey="totalValue"
                >
                  {exchangeData.map((entry, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip
                  contentStyle={{
                    backgroundColor: 'white',
                    border: '1px solid #e2e8f0',
                    borderRadius: '8px',
                  }}
                  formatter={(value, name, payload) => {
                    const percent = payload && payload.percent ? (payload.percent * 100).toFixed(2) : '0.00';
                    return [`${formatCompactNumber(value, true)} (${percent}%)`, payload?.payload?.name || name];
                  }}
                />
              </PieChart>
            </ResponsiveContainer>
          )}
        </div>
      </div>
    </div>
  );
};

export default Charts;

