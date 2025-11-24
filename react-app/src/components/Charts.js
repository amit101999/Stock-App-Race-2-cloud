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
  Legend,
  ResponsiveContainer,
} from 'recharts';
import './Charts.css';

const COLORS = ['#667eea', '#f093fb', '#4facfe', '#43e97b', '#fa709a', '#fee140', '#764ba2', '#f5576c'];

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
  const exchangeData = (exchangeStats || []).map((item) => ({
    name: item._id || item.name || 'Unknown',
    value: item.count || item.value || 0,
    totalValue: item.totalValue || 0,
  })).filter(item => item.value > 0);

  // Prepare data for top stocks bar chart
  const topStocksData = (topStocks || []).map((item) => ({
    name: item._id || item.name || 'Unknown',
    trades: item.tradeCount || item.trades || 0,
    value: item.totalValue || item.value || 0,
    quantity: item.totalQuantity || item.quantity || 0,
  })).filter(item => item.value > 0 || item.trades > 0);

  // Prepare daily volume data
  const dailyData = (dailyVolume || [])
    .slice() // Create a copy before reversing
    .reverse()
    .map((item) => ({
      date: item._id || item.date || '',
      trades: item.count || item.trades || 0,
      value: item.totalValue || item.value || 0,
      buyTrades: item.buyTrades || 0,
      sellTrades: item.sellTrades || 0,
    }))
    .filter(item => item.date); // Filter out items without dates

  return (
    <div className="charts-container">
      <div className="charts-grid">
        {/* Daily Trade Volume Line Chart */}
        <div className="chart-card">
          <h3>Daily Trade Volume (Last 30 Days)</h3>
          {dailyData.length === 0 ? (
            <div style={{ height: 300, display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#718096' }}>
              No daily volume data available
            </div>
          ) : (
            <ResponsiveContainer width="100%" height={300}>
            <LineChart data={dailyData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
              <XAxis
                dataKey="date"
                stroke="#718096"
                tick={{ fontSize: 12 }}
                angle={-45}
                textAnchor="end"
                height={80}
              />
              <YAxis stroke="#718096" tick={{ fontSize: 12 }} />
              <Tooltip
                contentStyle={{
                  backgroundColor: 'white',
                  border: '1px solid #e2e8f0',
                  borderRadius: '8px',
                }}
              />
              <Legend />
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
          <h3>Top 10 Stocks by Trade Value</h3>
          {topStocksData.length === 0 ? (
            <div style={{ height: 300, display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#718096' }}>
              No stock data available
            </div>
          ) : (
            <ResponsiveContainer width="100%" height={300}>
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
              <YAxis stroke="#718096" tick={{ fontSize: 12 }} />
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
              <Legend />
              <Bar dataKey="value" fill="#667eea" name="Total Value (₹)" radius={[8, 8, 0, 0]} />
              <Bar dataKey="trades" fill="#4facfe" name="Number of Trades" radius={[8, 8, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
          )}
        </div>

        {/* Exchange Distribution Pie Chart */}
        <div className="chart-card">
          <h3>Exchange Distribution</h3>
          {exchangeData.length === 0 ? (
            <div style={{ height: 300, display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#718096' }}>
              No exchange data available
            </div>
          ) : (
            <ResponsiveContainer width="100%" height={300}>
              <PieChart>
                <Pie
                  data={exchangeData}
                  cx="50%"
                  cy="50%"
                  labelLine={false}
                  label={({ name, percent }) => `${name}: ${(percent * 100).toFixed(0)}%`}
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
                  formatter={(value) => {
                    return new Intl.NumberFormat('en-IN', {
                      style: 'currency',
                      currency: 'INR',
                      maximumFractionDigits: 0,
                    }).format(value);
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

