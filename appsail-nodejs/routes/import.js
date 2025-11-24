'use strict';

const express = require('express');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const router = express.Router();
const importController = require('../controllers/importController');

// Configure multer for Excel files (memory storage - smaller files)
const excelStorage = multer.memoryStorage();
const excelUpload = multer({
	storage: excelStorage,
	limits: {
		fileSize: 200 * 1024 * 1024 // 200MB limit
	},
	fileFilter: (req, file, cb) => {
		// Accept Excel files
		const isExcel = (
			file.mimetype === 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' ||
			file.mimetype === 'application/vnd.ms-excel' ||
			file.mimetype === 'application/vnd.ms-excel.sheet.binary.macroEnabled.12' ||
			file.originalname.endsWith('.xlsx') ||
			file.originalname.endsWith('.xls') ||
			file.originalname.endsWith('.xlsb')
		);
		
		if (isExcel) {
			cb(null, true);
		} else {
			cb(new Error('Only Excel (.xlsx, .xls, .xlsb) files are allowed'), false);
		}
	}
});

// Configure multer for CSV files (disk storage - large files)
const csvStorage = multer.diskStorage({
	destination: (req, file, cb) => {
		// Create temp directory if it doesn't exist
		const tempDir = path.join(process.cwd(), 'temp');
		if (!fs.existsSync(tempDir)) {
			fs.mkdirSync(tempDir, { recursive: true });
		}
		cb(null, tempDir);
	},
	filename: (req, file, cb) => {
		// Generate unique filename with timestamp
		const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1E9);
		const ext = path.extname(file.originalname);
		cb(null, `csv-${uniqueSuffix}${ext}`);
	}
});

const csvUpload = multer({
	storage: csvStorage,
	limits: {
		fileSize: 200 * 1024 * 1024 // 200MB limit
	},
	fileFilter: (req, file, cb) => {
		// Accept CSV files
		const isCSV = (
			file.mimetype === 'text/csv' ||
			file.mimetype === 'application/csv' ||
			file.mimetype === 'text/plain' ||
			file.originalname.endsWith('.csv')
		);
		
		if (isCSV) {
			cb(null, true);
		} else {
			cb(new Error('Only CSV (.csv) files are allowed'), false);
		}
	}
});

// POST /api/import/excel -> import Excel file
router.post('/excel', excelUpload.single('file'), importController.importExcel);

// POST /api/import/csv -> import CSV file (streaming, chunked processing)
router.post('/csv', csvUpload.single('file'), importController.importCSV);

// GET /api/import/progress/:id -> current progress
router.get('/progress/:id', importController.getImportProgress);

// GET /api/import/test -> test database insertion with dummy data
router.get('/test', importController.testInsert);

module.exports = router;

