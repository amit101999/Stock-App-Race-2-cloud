import React from 'react';
import { HashRouter, Routes, Route } from 'react-router-dom';
import Navigation from './components/Navigation';
import Dashboard from './pages/Dashboard';
import Analytics from './pages/Analytics';
import './App.css';

function App() {
  return (
    <HashRouter>
      <div className="App">
        {/* Navigation Sidebar - Always Visible */}
        <Navigation />

        {/* Main Content Wrapper */}
        <div className="app-content-wrapper">
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/analytics" element={<Analytics />} />
          </Routes>
        </div>
      </div>
    </HashRouter>
  );
}

export default App;
