import React from 'react';
import { HashRouter, Routes, Route } from 'react-router-dom';
import Navigation from './components/Navigation';
import Dashboard from './pages/Dashboard';
import Analytics from './pages/Analytics';
import Bonus from './pages/Bonus';
import Split from './pages/Split';
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
            <Route path="/bonus" element={<Bonus />} />
            <Route path="/split" element={<Split />} />
          </Routes>
        </div>
      </div>
    </HashRouter>
  );
}

export default App;
