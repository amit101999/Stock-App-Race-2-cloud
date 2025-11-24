import React, { useState, useRef, useEffect } from 'react';
import { ChevronDown, X, Search } from 'lucide-react';
import './SearchableSelect.css';

const SearchableSelect = ({
  id,
  label,
  value,
  onChange,
  options = [],
  placeholder = 'Select...',
  searchPlaceholder = 'Search...',
  description,
  countText
}) => {
  const [isOpen, setIsOpen] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');
  const [highlightedIndex, setHighlightedIndex] = useState(-1);
  const dropdownRef = useRef(null);
  const searchInputRef = useRef(null);

  // Filter options based on search term
  const filteredOptions = options.filter(option =>
    String(option).toLowerCase().includes(searchTerm.toLowerCase())
  );

  // Close dropdown when clicking outside
  useEffect(() => {
    const handleClickOutside = (event) => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target)) {
        setIsOpen(false);
        setSearchTerm('');
        setHighlightedIndex(-1);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
    };
  }, []);

  // Focus search input when dropdown opens
  useEffect(() => {
    if (isOpen && searchInputRef.current) {
      searchInputRef.current.focus();
    }
  }, [isOpen]);

  // Handle keyboard navigation
  const handleKeyDown = (e) => {
    if (!isOpen) {
      if (e.key === 'Enter' || e.key === 'ArrowDown') {
        e.preventDefault();
        setIsOpen(true);
      }
      return;
    }

    switch (e.key) {
      case 'ArrowDown':
        e.preventDefault();
        setHighlightedIndex(prev =>
          prev < filteredOptions.length - 1 ? prev + 1 : prev
        );
        break;
      case 'ArrowUp':
        e.preventDefault();
        setHighlightedIndex(prev => (prev > 0 ? prev - 1 : -1));
        break;
      case 'Enter':
        e.preventDefault();
        if (highlightedIndex >= 0 && filteredOptions[highlightedIndex]) {
          handleSelect(filteredOptions[highlightedIndex]);
        }
        break;
      case 'Escape':
        e.preventDefault();
        setIsOpen(false);
        setSearchTerm('');
        setHighlightedIndex(-1);
        break;
    }
  };

  const handleSelect = (selectedValue) => {
    onChange(selectedValue);
    setIsOpen(false);
    setSearchTerm('');
    setHighlightedIndex(-1);
  };

  const handleClear = (e) => {
    e.stopPropagation();
    onChange('');
    setSearchTerm('');
  };

  const selectedLabel = value || placeholder;
  const displayValue = value || '';

  return (
    <div className="searchable-select-wrapper">
      {label && (
        <label htmlFor={id} className="searchable-select-label">
          {label}
        </label>
      )}
      
      <div className="searchable-select-container" ref={dropdownRef}>
        <div
          className={`searchable-select-trigger ${isOpen ? 'open' : ''}`}
          onClick={() => setIsOpen(!isOpen)}
          onKeyDown={handleKeyDown}
          tabIndex={0}
          role="combobox"
          aria-expanded={isOpen}
          aria-haspopup="listbox"
        >
          <div className="searchable-select-trigger-content">
            <Search size={18} className="search-icon-trigger" />
            <span className={!value ? 'placeholder' : ''}>
              {selectedLabel}
            </span>
          </div>
          <div className="searchable-select-actions">
            {value && (
              <button
                type="button"
                className="clear-button"
                onClick={handleClear}
                aria-label="Clear selection"
              >
                <X size={16} />
              </button>
            )}
            <ChevronDown 
              size={18} 
              className={`chevron ${isOpen ? 'open' : ''}`}
            />
          </div>
        </div>

        {isOpen && (
          <div className="searchable-select-dropdown">
            <div className="searchable-select-search">
              <input
                ref={searchInputRef}
                type="text"
                className="searchable-select-input"
                placeholder={searchPlaceholder}
                value={searchTerm}
                onChange={(e) => {
                  setSearchTerm(e.target.value);
                  setHighlightedIndex(-1);
                }}
                onKeyDown={handleKeyDown}
              />
            </div>

            <div className="searchable-select-options" role="listbox">
              {options.length === 0 ? (
                <div className="searchable-select-no-results">
                  No options available
                </div>
              ) : filteredOptions.length === 0 ? (
                <div className="searchable-select-no-results">
                  No results found for "{searchTerm}"
                </div>
              ) : (
                filteredOptions.map((option, index) => (
                  <div
                    key={option}
                    className={`searchable-select-option ${
                      value === option ? 'selected' : ''
                    } ${highlightedIndex === index ? 'highlighted' : ''}`}
                    onClick={() => handleSelect(option)}
                    onMouseEnter={() => setHighlightedIndex(index)}
                    role="option"
                    aria-selected={value === option}
                  >
                    {option}
                  </div>
                ))
              )}
            </div>

            {filteredOptions.length > 0 && (
              <div className="searchable-select-footer">
                {filteredOptions.length} of {options.length} {options.length === 1 ? 'option' : 'options'}
              </div>
            )}
          </div>
        )}
      </div>

      {countText && (
        <p className="filter-count">{countText}</p>
      )}

      {description && (
        <p className="filter-description">{description}</p>
      )}
    </div>
  );
};

export default SearchableSelect;

