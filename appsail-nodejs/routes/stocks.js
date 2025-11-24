'use strict';

const express = require('express');
const router = express.Router();
const stocksController = require('../controllers/stocksController');

// GET /api/stocks -> list with filters + pagination
router.get('/', stocksController.listStocks);

// GET /api/stocks/:id -> fetch single row by ROWID
router.get('/:id', stocksController.getStockById);

// Stats and meta endpoints
router.get('/stats/summary', stocksController.getStats);
router.get('/meta/exchanges', stocksController.getExchanges);
router.get('/meta/transaction-types', stocksController.getTransactionTypes);
router.get('/meta/client-ids', stocksController.getClientIds);
router.get('/meta/symbols', stocksController.getSymbols);
router.get('/meta/stocks-by-client', stocksController.getStocksByClientId);

// Holdings endpoints
router.get('/holdings/summary', stocksController.getHoldingsSummary);
router.get('/holdings/:stockName/transactions', stocksController.getStockTransactionHistory);

module.exports = router;


