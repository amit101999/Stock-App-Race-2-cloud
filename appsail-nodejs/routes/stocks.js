'use strict';

const express = require('express');
const router = express.Router();
const stocksController = require('../controllers/stocksController');

// GET /api/stocks -> list with filters + pagination
router.get('/', stocksController.listStocks);

// Stats and meta endpoints
router.get('/stats/summary', stocksController.getStats);
router.get('/meta/exchanges', stocksController.getExchanges);
router.get('/meta/transaction-types', stocksController.getTransactionTypes);
router.get('/meta/client-ids', stocksController.getClientIds);
router.get('/meta/account-codes', stocksController.getAccountCodes);
router.get('/meta/client-id-by-account-code', stocksController.getClientIdByAccountCode);
router.get('/meta/symbols', stocksController.getSymbols);
router.get('/meta/stocks-by-client', stocksController.getStocksByClientId);
router.get('/meta/clients-by-security', stocksController.getClientsBySecurityHoldings);

// Holdings endpoints
router.get('/holdings/summary', stocksController.getHoldingsSummary);
router.get('/holdings/clients-cumulative', stocksController.getClientsWithCumulativeHoldings);
router.get('/holdings/:stockName/transactions', stocksController.getStockTransactionHistory);

// Cost management endpoints
router.get('/holdings/weighted-average-cost', stocksController.getWeightedAverageCost);

// Test endpoint to check for specific bonus
router.get('/check-bonus', stocksController.checkBonus);

// Export transactions and bonuses to Excel
router.get('/export/excel', stocksController.exportClientTransactionsToExcel);

// GET /api/stocks/:id -> fetch single row by ROWID
// Keep this last so it doesn't intercept meta routes like /meta/client-ids
router.get('/:id', stocksController.getStockById);

module.exports = router;


