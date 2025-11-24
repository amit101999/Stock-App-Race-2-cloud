import React from 'react';
import { TrendingUp, TrendingDown, DollarSign, Activity } from 'lucide-react';
import './StatCard.css';

const StatCard = ({ title, value, subtitle, icon: Icon, trend, trendValue }) => {
  const getIcon = () => {
    if (Icon) return <Icon className="stat-icon" />;
    switch (title) {
      case 'Total Trades':
        return <Activity className="stat-icon" />;
      case 'Total Value':
        return <DollarSign className="stat-icon" />;
      default:
        return <Activity className="stat-icon" />;
    }
  };

  return (
    <div className="stat-card">
      <div className="stat-header">
        <div className="stat-icon-wrapper">
          {getIcon()}
        </div>
        <div className="stat-content">
          <h3 className="stat-title">{title}</h3>
          <p className="stat-value">{value}</p>
          {subtitle && <p className="stat-subtitle">{subtitle}</p>}
          {trend && trendValue && (
            <div className={`stat-trend ${trend}`}>
              {trend === 'up' ? (
                <TrendingUp size={16} />
              ) : (
                <TrendingDown size={16} />
              )}
              <span>{trendValue}</span>
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default StatCard;

