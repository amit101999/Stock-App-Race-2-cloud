'use strict';

const XLSX = require('xlsx');
const csv = require('csv-parser');
const fs = require('fs');
const path = require('path');
const { Readable } = require('stream');
const catalyst = require('zcatalyst-sdk-node');
const DEFAULT_TABLE = 'Transaction';

// In-memory progress tracker (per runtime instance)
const IMPORT_PROGRESS = new Map();

function newImportId() {
	return Date.now().toString(36) + Math.random().toString(36).slice(2, 8);
}

// Allowed columns as per Data Store schema (underscore-separated)
const ALLOWED_COLUMNS = new Set([
	'WS_client_id','WS_Account_code','TRANDATE','SETDATE','Tran_Type','Tran_Desc',
	'Security_Type','Security_Type_Description','DETAILTYPENAME','ISIN','Security_code',
	'Security_Name','EXCHG','BROKERCODE','Depository_Registrar','DPID_AMC','Dp_Client_id_Folio',
	'BANKCODE','BANKACID','QTY','RATE','BROKERAGE','SERVICETAX','NETRATE','Net_Amount','STT',
	'TRFDATE','TRFRATE','TRFAMT','TOTAL_TRXNFEE','TOTAL_TRXNFEE_STAX','Txn_Ref_No','DESCMEMO',
	'CHEQUENO','CHEQUEDTL','PORTFOLIOID','DELIVERYDATE','PAYMENTDATE','ACCRUEDINTEREST','ISSUER',
	'ISSUERNAME','TDSAMOUNT','STAMPDUTY','TPMSGAIN','RMID','RMNAME','ADVISORID','ADVISORNAME',
	'BRANCHID','BRANCHNAME','GROUPID','GROUPNAME','OWNERID','OWNERNAME','WEALTHADVISOR_NAME',
	'SCHEMEID','SCHEMENAME'
]);

// Columns that must be numeric
const NUMERIC_COLUMNS = new Set([
	'QTY','RATE','BROKERAGE','SERVICETAX','NETRATE','Net_Amount','STT','TRFRATE','TRFAMT',
	'TOTAL_TRXNFEE','TOTAL_TRXNFEE_STAX','TDSAMOUNT','STAMPDUTY','TPMSGAIN','ACCRUEDINTEREST'
]);

// Columns that must be integers (not floats)
const INTEGER_COLUMNS = new Set([
	'QTY', 'WS_client_id', 'WS_Account_code', 'Security_code', 'BROKERCODE', 'BANKCODE',
	'PORTFOLIOID', 'RMID', 'ADVISORID', 'BRANCHID', 'GROUPID', 'OWNERID', 'SCHEMEID'
]);

// Map Excel column names (with spaces/special chars) to database field names (with underscores)
const COLUMN_MAPPING = {
	'WS client id': 'WS_client_id',
	'WS Client id': 'WS_client_id',
	'WS_client_id': 'WS_client_id',
	'WS Account code': 'WS_Account_code',
	'WS_Account_code': 'WS_Account_code',
	'TRANDATE': 'TRANDATE',
	'TRANDATE': 'TRANDATE',
	'SETDATE': 'SETDATE',
	'Set Date': 'SETDATE',
	'Tran Type': 'Tran_Type',
	'Tran_Type': 'Tran_Type',
	'Tran Desc': 'Tran_Desc',
	'Tran_Desc': 'Tran_Desc',
	'Security Type': 'Security_Type',
	'Security_Type': 'Security_Type',
	'Security Type Description': 'Security_Type_Description',
	'Security_Type_Description': 'Security_Type_Description',
	'DETAILTYPENAME': 'DETAILTYPENAME',
	'Detail Type Name': 'DETAILTYPENAME',
	'ISIN': 'ISIN',
	'Security code': 'Security_code',
	'Security_code': 'Security_code',
	'Security Name': 'Security_Name',
	'Security_Name': 'Security_Name',
	'EXCHG': 'EXCHG',
	'Exchange': 'EXCHG',
	'BROKERCODE': 'BROKERCODE',
	'Broker Code': 'BROKERCODE',
	'Depository/Registrar': 'Depository_Registrar',
	'Depositoy/Registrar': 'Depository_Registrar',
	'DPID/AMC': 'DPID_AMC',
	'Dp Client id/Folio': 'Dp_Client_id_Folio',
	'DP Client id/Folio': 'Dp_Client_id_Folio',
	'BANKCODE': 'BANKCODE',
	'Bank Code': 'BANKCODE',
	'BANKACID': 'BANKACID',
	'Bank AC ID': 'BANKACID',
	'QTY': 'QTY',
	'Quantity': 'QTY',
	'RATE': 'RATE',
	'Rate': 'RATE',
	'BROKERAGE': 'BROKERAGE',
	'Brokerage': 'BROKERAGE',
	'SERVICETAX': 'SERVICETAX',
	'Service Tax': 'SERVICETAX',
	'NETRATE': 'NETRATE',
	'Net Rate': 'NETRATE',
	'Net Amount': 'Net_Amount',
	'NET_Amount': 'Net_Amount',
	'STT': 'STT',
	'TRFDATE': 'TRFDATE',
	'TRF Date': 'TRFDATE',
	'TRFRATE': 'TRFRATE',
	'TRF Rate': 'TRFRATE',
	'TRFAMT': 'TRFAMT',
	'TRF Amount': 'TRFAMT',
	'TOTAL_TRXNFEE': 'TOTAL_TRXNFEE',
	'Total Txn Fee': 'TOTAL_TRXNFEE',
	'TOTAL_TRXNFEE_STAX': 'TOTAL_TRXNFEE_STAX',
	'Total Txn Fee STax': 'TOTAL_TRXNFEE_STAX',
	'Txn Ref No': 'Txn_Ref_No',
	'TXN_Ref_No': 'Txn_Ref_No',
	'DESCMEMO': 'DESCMEMO',
	'Desc Memo': 'DESCMEMO',
	'CHEQUENO': 'CHEQUENO',
	'Cheque No': 'CHEQUENO',
	'CHEQUEDTL': 'CHEQUEDTL',
	'Cheque Dtl': 'CHEQUEDTL',
	'PORTFOLIOID': 'PORTFOLIOID',
	'Portfolio ID': 'PORTFOLIOID',
	'DELIVERYDATE': 'DELIVERYDATE',
	'Delivery Date': 'DELIVERYDATE',
	'PAYMENTDATE': 'PAYMENTDATE',
	'Payment Date': 'PAYMENTDATE',
	'ACCRUEDINTEREST': 'ACCRUEDINTEREST',
	'Accrued Interest': 'ACCRUEDINTEREST',
	'ISSUER': 'ISSUER',
	'Issuer': 'ISSUER',
	'ISSUERNAME': 'ISSUERNAME',
	'Issuer Name': 'ISSUERNAME',
	'TDSAMOUNT': 'TDSAMOUNT',
	'TDS Amount': 'TDSAMOUNT',
	'STAMPDUTY': 'STAMPDUTY',
	'Stamp Duty': 'STAMPDUTY',
	'TPMSGAIN': 'TPMSGAIN',
	'TPMSGain': 'TPMSGAIN',
	'RMID': 'RMID',
	'RM ID': 'RMID',
	'RMNAME': 'RMNAME',
	'RM Name': 'RMNAME',
	'ADVISORID': 'ADVISORID',
	'Advisor ID': 'ADVISORID',
	'ADVISORNAME': 'ADVISORNAME',
	'Advisor Name': 'ADVISORNAME',
	'BRANCHID': 'BRANCHID',
	'Branch ID': 'BRANCHID',
	'BRANCHNAME': 'BRANCHNAME',
	'Branch Name': 'BRANCHNAME',
	'GROUPID': 'GROUPID',
	'Group ID': 'GROUPID',
	'GROUPNAME': 'GROUPNAME',
	'Group Name': 'GROUPNAME',
	'OWNERID': 'OWNERID',
	'Owner ID': 'OWNERID',
	'OWNERNAME': 'OWNERNAME',
	'Owner Name': 'OWNERNAME',
	'WEALTHADVISOR NAME': 'WEALTHADVISOR_NAME',
	'Wealth Advisor Name': 'WEALTHADVISOR_NAME',
	'SCHEMEID': 'SCHEMEID',
	'Scheme ID': 'SCHEMEID',
	'SCHEMENAME': 'SCHEMENAME',
	'Scheme Name': 'SCHEMENAME'
};

// Canonicalize helper to match headers regardless of spaces, dots, slashes or case
function canonicalize(name) {
	return String(name || '')
		.toLowerCase()
		.replace(/[^a-z0-9]/g, ''); // remove all non-alphanumerics
}

// Precompute canonical -> schema column map
const CANONICAL_TO_SCHEMA = (() => {
	const map = {};
	ALLOWED_COLUMNS.forEach(col => {
		map[canonicalize(col)] = col;
	});
	return map;
})();

function normalizeColumnName(excelColName) {
	if (!excelColName) return null;
	const trimmed = String(excelColName).trim();

	// 1) Direct explicit overrides
	if (COLUMN_MAPPING[trimmed]) return COLUMN_MAPPING[trimmed];

	// 2) Replace common separators with underscore and test direct match
	const underscored = trimmed.replace(/[.\s\/-]+/g, '_');
	if (ALLOWED_COLUMNS.has(underscored)) return underscored;

	// 3) Canonical fuzzy match to schema
	const canon = canonicalize(trimmed);
	if (CANONICAL_TO_SCHEMA[canon]) return CANONICAL_TO_SCHEMA[canon];

	// 4) Fallback to underscored (will be flagged unknown later if not allowed)
	return underscored;
}

function coerceValueForColumn(column, value) {
	if (value === null || value === '') {
		// For numeric columns (both integers and doubles), return 0/0.0 instead of null
		// Catalyst requires valid numeric values, not null
		if (NUMERIC_COLUMNS.has(column)) {
			// Integer columns get 0, double columns get 0.0 (same in JS, but explicit)
			return INTEGER_COLUMNS.has(column) ? 0 : 0.0;
		}
		// For non-numeric integer columns, return 0
		if (INTEGER_COLUMNS.has(column)) {
			return 0;
		}
		return null;
	}
	
	if (NUMERIC_COLUMNS.has(column)) {
		const num = Number(value);
		if (isNaN(num)) {
			// For invalid numeric values, return 0/0.0 instead of null
			// Catalyst requires valid numeric values
			return INTEGER_COLUMNS.has(column) ? 0 : 0.0;
		}
		// For integer columns, ensure it's an integer (not float)
		if (INTEGER_COLUMNS.has(column)) {
			return Math.floor(num); // Convert to integer (rounds down)
		}
		// For double columns, return the number as-is (can be decimal)
		return num;
	}
	
	// For non-numeric integer columns (like WS_client_id), try to parse as integer
	if (INTEGER_COLUMNS.has(column)) {
		const num = Number(value);
		if (isNaN(num)) {
			return 0; // Default to 0 if can't parse
		}
		return Math.floor(num); // Ensure integer
	}
	
	return value;
}

function parseExcelFile(buffer) {
	try {
		const workbook = XLSX.read(buffer, { type: 'buffer' });
		const firstSheetName = workbook.SheetNames[0];
		const worksheet = workbook.Sheets[firstSheetName];
		const jsonData = XLSX.utils.sheet_to_json(worksheet, { raw: false, defval: null });
		return jsonData;
	} catch (err) {
		throw new Error(`Failed to parse Excel file: ${err.message}`);
	}
}

function mapRowToDatabaseFormat(excelRow) {
	const dbRow = {};
	// date columns as per schema
	const dateColumns = new Set([
		'TRANDATE','SETDATE','DELIVERYDATE','PAYMENTDATE','TRFDATE'
	]);

	// Track unknown columns for diagnostics
	const unknownColumns = [];

	Object.keys(excelRow).forEach(excelKey => {
		const dbKey = normalizeColumnName(excelKey);
		if (dbKey) {
			let value = excelRow[excelKey];
			// Convert empty strings to null
			if (value === '' || value === undefined) {
				value = null;
			}
			// Normalize dates to YYYY-MM-DD
			if (value && dateColumns.has(dbKey)) {
				// Handle Excel date or string dates
				const d = new Date(value);
				if (!isNaN(d.getTime())) {
					const yyyy = d.getFullYear();
					const mm = String(d.getMonth() + 1).padStart(2, '0');
					const dd = String(d.getDate()).padStart(2, '0');
					value = `${yyyy}-${mm}-${dd}`;
				}
			}
			// Keep numbers as numbers, others as strings/null
			if (ALLOWED_COLUMNS.has(dbKey)) {
				dbRow[dbKey] = coerceValueForColumn(dbKey, value);
			} else {
				unknownColumns.push(dbKey);
			}
		}
	});

	// Attach list of unknowns for later debugging (not inserted)
	if (unknownColumns.length) {
		dbRow.__unknown = unknownColumns;
	}
	return dbRow;
}

exports.importExcel = async (req, res) => {
	try {
		const app = req.catalystApp;
		if (!app) {
			return res.status(500).json({ 
				success: false, 
				error: 'Catalyst app context missing' 
			});
		}

		if (!req.file) {
			return res.status(400).json({ 
				success: false, 
				error: 'No file uploaded' 
			});
		}

		// Store file buffer before async context (req.file.buffer might not be available later)
		const fileBuffer = Buffer.from(req.file.buffer);
		const fileName = req.file.originalname;
		const fileSize = req.file.size;

		// Start async import and return importId immediately
		const importId = newImportId();
		IMPORT_PROGRESS.set(importId, {
			stage: 'parsing',
			progress: 5,
			message: 'Parsing Excel...',
			totalRows: 0,
			processedRows: 0,
			imported: 0,
			errors: 0,
			errorDetails: []
		});

		// Kick off background processing
		setImmediate(async () => {
			const tableName = DEFAULT_TABLE;
			const progress = IMPORT_PROGRESS.get(importId);
			try {
				// Re-initialize Catalyst app in async context
				const appAsync = catalyst.initialize(req);
				if (!appAsync) {
					throw new Error('Failed to initialize Catalyst app');
				}

				// Parse Excel using stored buffer
				let excelRows;
				try {
					excelRows = parseExcelFile(fileBuffer);
				} catch (parseErr) {
					console.error(`[Import ${importId}] Parse error:`, parseErr);
					throw new Error(`Failed to parse Excel: ${parseErr.message}`);
				}
				if (!excelRows || excelRows.length === 0) {
					throw new Error('Excel file is empty or has no data rows');
				}

				progress.totalRows = excelRows.length;
				progress.stage = 'mapping';
				progress.progress = 15;
				progress.message = 'Mapping columns...';

				// Map rows
				const mappedRows = excelRows.map((row, idx) => {
					try {
						return mapRowToDatabaseFormat(row);
					} catch (mapErr) {
						console.error(`[Import ${importId}] Error mapping row ${idx + 1}:`, mapErr);
						return null;
					}
				}).filter(row => {
					if (!row) return false;
					const values = Object.entries(row)
						.filter(([k]) => k !== '__unknown')
						.map(([, v]) => v);
					return values.some(val => val !== null && val !== '');
				});

				if (mappedRows.length === 0) {
					throw new Error('No valid rows found after mapping. Check column headers match schema.');
				}

				// Unknown sample, remove helper
				const unknownSample = new Set();
				mappedRows.forEach((r, idx) => {
					if (idx < 5 && Array.isArray(r.__unknown)) {
						r.__unknown.forEach(c => unknownSample.add(c));
					}
					if ('__unknown' in r) delete r.__unknown;
				});

				if (unknownSample.size > 0) {
					console.warn(`[Import ${importId}] Unknown columns detected:`, Array.from(unknownSample));
				}

				// Insert
				progress.stage = 'inserting';
				progress.progress = 25;
				progress.message = 'Inserting into Data Store...';

				const datastore = appAsync.datastore();
				const table = datastore.table(tableName);
				let totalInserted = 0;
				let errorCount = 0;
				const errorMessages = [];
				const BATCH_SIZE = 200; // Zoho Catalyst limit: maximum 200 rows per batch operation

				const insertBatch = async (batch) => {
					if (typeof table.insertRows === 'function') {
						await table.insertRows(batch);
					} else if (typeof table.bulkWriteRows === 'function') {
						await table.bulkWriteRows(batch);
					} else if (typeof table.insertRow === 'function') {
						for (const row of batch) {
							await table.insertRow(row);
						}
					} else {
						throw new Error('No suitable insert method available');
					}
				};

				for (let i = 0; i < mappedRows.length; i += BATCH_SIZE) {
					const batch = mappedRows.slice(i, i + BATCH_SIZE);
					const batchNum = Math.floor(i / BATCH_SIZE) + 1;
					try {
						await insertBatch(batch);
						totalInserted += batch.length;
					} catch (batchErr) {
						console.error(`[Import ${importId}] Batch ${batchNum} failed:`, batchErr.message);
						// Try per-row fallback
						for (let j = 0; j < batch.length; j++) {
							const row = batch[j];
							try {
								if (typeof table.insertRow === 'function') {
									await table.insertRow(row);
									totalInserted++;
								} else if (typeof table.bulkWriteRows === 'function') {
									await table.bulkWriteRows([row]);
									totalInserted++;
								} else {
									throw new Error('No per-row insert available');
								}
							} catch (rowErr) {
								errorCount++;
								const errMsg = `Row ${i + j + 1}: ${rowErr.message}`;
								errorMessages.push(errMsg);
								if (errorMessages.length <= 10) {
									console.error(`[Import ${importId}] ${errMsg}`);
								}
							}
						}
					}
					progress.processedRows = Math.min(i + batch.length, mappedRows.length);
					progress.imported = totalInserted;
					progress.errors = errorCount;
					progress.errorDetails = errorMessages.slice(0, 10);
					progress.progress = Math.min(95, Math.round((progress.processedRows / mappedRows.length) * 90) + 5);
					progress.message = `Inserted ${totalInserted}/${mappedRows.length} rows...`;
					IMPORT_PROGRESS.set(importId, progress);
				}

				console.log(`[Import ${importId}] Import completed: ${totalInserted} inserted, ${errorCount} errors`);
				progress.stage = 'completed';
				progress.progress = 100;
				progress.message = `Imported ${totalInserted} of ${mappedRows.length} rows${errorCount > 0 ? ` (${errorCount} errors)` : ''}`;
				progress.unknownColumns = Array.from(unknownSample);
			} catch (err) {
				console.error(`[Import ${importId}] Fatal error:`, err);
				progress.stage = 'error';
				progress.message = err.message || 'Import failed';
				progress.errorDetails = [err.toString()];
				if (err.stack) {
					console.error(`[Import ${importId}] Stack:`, err.stack);
				}
			}
			IMPORT_PROGRESS.set(importId, progress);
		});

		return res.status(200).json({
			success: true,
			importId,
			message: 'Import started'
		});

	} catch (err) {
		console.error('Import error:', err);
		return res.status(500).json({ 
			success: false, 
			error: `Failed to import Excel file: ${err.message}` 
		});
	}
};

// CSV Import with streaming and chunked processing
exports.importCSV = async (req, res) => {
	try {
		const app = req.catalystApp;
		if (!app) {
			return res.status(500).json({ 
				success: false, 
				error: 'Catalyst app context missing' 
			});
		}

		if (!req.file) {
			return res.status(400).json({ 
				success: false, 
				error: 'No file uploaded' 
			});
		}

		// Get file path from disk storage (req.file.path) or buffer from memory storage
		const filePath = req.file.path; // Disk storage provides path
		const fileName = req.file.originalname;
		const fileSize = req.file.size;

		if (!filePath) {
			return res.status(400).json({ 
				success: false, 
				error: 'File path not available. File must be uploaded using disk storage.' 
			});
		}

		// Start async import and return importId immediately
		const importId = newImportId();
		IMPORT_PROGRESS.set(importId, {
			stage: 'parsing',
			progress: 5,
			message: 'Starting CSV import...',
			totalRows: 0,
			processedRows: 0,
			imported: 0,
			errors: 0,
			errorDetails: []
		});

		// Kick off background processing
		setImmediate(async () => {
			const tableName = DEFAULT_TABLE;
			const progress = IMPORT_PROGRESS.get(importId);
			const CHUNK_SIZE = 10000; // Process 10,000 rows at a time
			const BATCH_SIZE = 200; // Insert 200 rows per batch (Catalyst limit)

			try {
				// Re-initialize Catalyst app in async context
				const appAsync = catalyst.initialize(req);
				if (!appAsync) {
					throw new Error('Failed to initialize Catalyst app');
				}

				const datastore = appAsync.datastore();
				const table = datastore.table(tableName);

				// Create readable stream from file path (disk storage)
				if (!fs.existsSync(filePath)) {
					throw new Error(`File not found at path: ${filePath}`);
				}
				const csvStream = fs.createReadStream(filePath, { encoding: 'utf8' });

				let totalRows = 0;
				let processedRows = 0;
				let totalInserted = 0;
				let errorCount = 0;
				const errorMessages = [];
				let headers = null;
				let currentChunk = [];
				let rowIndex = 0;
				let isProcessingChunk = false;

				// Track unknown columns
				const unknownSample = new Set();

				progress.stage = 'parsing';
				progress.progress = 10;
				progress.message = 'Parsing CSV headers...';

				// Parse CSV with streaming
				return new Promise((resolve, reject) => {
					const parser = csv({
						skipEmptyLines: true,
						skipLinesWithError: false
					});

					csvStream
						.pipe(parser)
						.on('headers', (headerList) => {
							headers = headerList;
							progress.message = 'Processing CSV rows...';
						})
						.on('data', async (row) => {
							// Wait if chunk is being processed
							if (isProcessingChunk) {
								parser.pause();
								// Wait a bit and resume
								setTimeout(() => {
									if (!isProcessingChunk) {
										parser.resume();
									}
								}, 100);
								return;
							}

							try {
								totalRows++;
								rowIndex++;

								// Map row to database format
								const mappedRow = mapRowToDatabaseFormat(row);
								
								// Track unknown columns from first few rows
								if (rowIndex <= 5 && mappedRow.__unknown) {
									mappedRow.__unknown.forEach(col => unknownSample.add(col));
								}
								if ('__unknown' in mappedRow) {
									delete mappedRow.__unknown;
								}

								// Check if row has any data
								const hasData = Object.values(mappedRow).some(val => val !== null && val !== '');
								if (!hasData) {
									return; // Skip empty rows
								}

								currentChunk.push(mappedRow);

								// When chunk is full, process it
								if (currentChunk.length >= CHUNK_SIZE) {
									parser.pause();
									isProcessingChunk = true;

									try {
										const chunkToProcess = [...currentChunk];
										currentChunk = [];

										// Re-initialize Catalyst app before processing chunk to get fresh token
										let freshApp;
										try {
											freshApp = catalyst.initialize(req);
											if (!freshApp) {
												throw new Error('Failed to re-initialize Catalyst app');
											}
										} catch (initErr) {
											console.error(`[Import ${importId}] Failed to re-initialize app:`, initErr);
											// Continue with existing app, but log warning
										}

										// Use fresh app if available, otherwise use existing
										const appToUse = freshApp || appAsync;
										const freshDatastore = appToUse.datastore();
										const freshTable = freshDatastore.table(tableName);

										const result = await processChunk(
											chunkToProcess,
											freshTable,
											BATCH_SIZE,
											importId,
											processedRows
										);

										totalInserted += result.inserted;
										errorCount += result.errors.length;
										errorMessages.push(...result.errors.slice(0, 10 - errorMessages.length));
										processedRows += chunkToProcess.length;

										// Clear chunk from memory
										chunkToProcess.length = 0;
										
										// Update progress
										progress.processedRows = processedRows;
										progress.imported = totalInserted;
										progress.errors = errorCount;
										progress.errorDetails = errorMessages.slice(0, 10);
										progress.progress = Math.min(95, Math.round((processedRows / Math.max(totalRows, 1)) * 90) + 10);
										progress.message = `Processed ${processedRows} rows, inserted ${totalInserted}...`;
										IMPORT_PROGRESS.set(importId, progress);
									} catch (chunkErr) {
										console.error(`[Import ${importId}] Chunk processing error:`, chunkErr);
										errorCount += chunkToProcess.length; // Count all rows in failed chunk as errors
										if (errorMessages.length < 10) {
											errorMessages.push(`Chunk error at row ${processedRows + 1}: ${chunkErr.message}`);
										}
										// Don't stop - continue processing next chunk
										processedRows += chunkToProcess.length; // Still count as processed
									} finally {
										isProcessingChunk = false;
										// Always resume parser to continue processing
										try {
										parser.resume();
										} catch (resumeErr) {
											console.error(`[Import ${importId}] Error resuming parser:`, resumeErr);
										}
									}
								}
							} catch (rowErr) {
								console.error(`[Import ${importId}] Row ${rowIndex} error:`, rowErr);
								errorCount++;
								if (errorMessages.length < 10) {
									errorMessages.push(`Row ${rowIndex}: ${rowErr.message}`);
								}
								// Continue processing even if row fails - don't stop the stream
							}
						})
						.on('end', async () => {
							try {
								// Process remaining rows in chunk
								if (currentChunk.length > 0) {
									// Re-initialize Catalyst app before final chunk
									let freshApp;
									try {
										freshApp = catalyst.initialize(req);
										if (!freshApp) {
											throw new Error('Failed to re-initialize Catalyst app');
										}
									} catch (initErr) {
										console.error(`[Import ${importId}] Failed to re-initialize app:`, initErr);
									}

									// Use fresh app if available, otherwise use existing
									const appToUse = freshApp || appAsync;
									const freshDatastore = appToUse.datastore();
									const freshTable = freshDatastore.table(tableName);

									const result = await processChunk(
										currentChunk,
										freshTable,
										BATCH_SIZE,
										importId,
										processedRows
									);
									totalInserted += result.inserted;
									errorCount += result.errors.length;
									errorMessages.push(...result.errors.slice(0, 10 - errorMessages.length));
									processedRows += currentChunk.length;
									currentChunk = [];
								}

								// Final update
								progress.totalRows = totalRows;
								progress.processedRows = processedRows;
								progress.imported = totalInserted;
								progress.errors = errorCount;
								progress.errorDetails = errorMessages.slice(0, 10);
								progress.stage = 'completed';
								progress.progress = 100;
								progress.message = `Imported ${totalInserted} of ${totalRows} rows${errorCount > 0 ? ` (${errorCount} errors)` : ''}`;
								progress.unknownColumns = Array.from(unknownSample);
								IMPORT_PROGRESS.set(importId, progress);

								console.log(`[Import ${importId}] CSV import completed: ${totalInserted} inserted, ${errorCount} errors out of ${totalRows} total rows`);
								
								// Clean up: Delete temporary file
								try {
									if (fs.existsSync(filePath)) {
										fs.unlinkSync(filePath);
									}
								} catch (cleanupErr) {
									console.warn(`[Import ${importId}] Failed to delete temp file: ${cleanupErr.message}`);
								}
								
								resolve();
							} catch (finalErr) {
								console.error(`[Import ${importId}] Final chunk error:`, finalErr);
								
								// Clean up on error
								try {
									if (fs.existsSync(filePath)) {
										fs.unlinkSync(filePath);
									}
								} catch (cleanupErr) {
									console.warn(`[Import ${importId}] Failed to delete temp file on error: ${cleanupErr.message}`);
								}
								
								reject(finalErr);
							}
						})
						.on('error', (err) => {
							console.error(`[Import ${importId}] CSV stream error:`, err);
							progress.stage = 'error';
							progress.message = `CSV parsing error: ${err.message}`;
							progress.errorDetails = [err.toString()];
							IMPORT_PROGRESS.set(importId, progress);
							
							// Clean up on error
							try {
								if (fs.existsSync(filePath)) {
									fs.unlinkSync(filePath);
								}
							} catch (cleanupErr) {
								console.warn(`[Import ${importId}] Failed to delete temp file on stream error: ${cleanupErr.message}`);
							}
							
							reject(err);
						});
				});
			} catch (err) {
				console.error(`[Import ${importId}] Fatal error:`, err);
				progress.stage = 'error';
				progress.message = err.message || 'Import failed';
				progress.errorDetails = [err.toString()];
				if (err.stack) {
					console.error(`[Import ${importId}] Stack:`, err.stack);
				}
				IMPORT_PROGRESS.set(importId, progress);
				
				// Clean up on fatal error
				try {
					if (filePath && fs.existsSync(filePath)) {
						fs.unlinkSync(filePath);
					}
				} catch (cleanupErr) {
					console.warn(`[Import ${importId}] Failed to delete temp file after fatal error: ${cleanupErr.message}`);
				}
			}
		});

		return res.status(200).json({
			success: true,
			importId,
			message: 'CSV import started'
		});

	} catch (err) {
		console.error('CSV import error:', err);
		return res.status(500).json({ 
			success: false, 
			error: `Failed to start CSV import: ${err.message}` 
		});
	}
};

// Helper function to process a chunk of rows
async function processChunk(chunk, table, batchSize, importId, startRowIndex) {
	let totalInserted = 0;
	const errors = [];

	// Validate and clean rows before processing
	const cleanedChunk = chunk.map((row, idx) => {
		try {
			// Ensure all numeric columns (integers and doubles) have valid values
			NUMERIC_COLUMNS.forEach(col => {
				if (col in row) {
					const val = row[col];
					if (val === null || val === '' || val === undefined) {
						// Default to 0 for integers, 0.0 for doubles
						row[col] = INTEGER_COLUMNS.has(col) ? 0 : 0.0;
					} else {
						const num = Number(val);
						if (isNaN(num)) {
							// Default to 0/0.0 if invalid
							row[col] = INTEGER_COLUMNS.has(col) ? 0 : 0.0;
						} else {
							// For integers, ensure it's an integer; for doubles, keep as decimal
							row[col] = INTEGER_COLUMNS.has(col) ? Math.floor(num) : num;
						}
					}
				}
			});
			
			// Also handle non-numeric integer columns
			INTEGER_COLUMNS.forEach(col => {
				if (!NUMERIC_COLUMNS.has(col) && col in row) {
					const val = row[col];
					if (val === null || val === '' || val === undefined) {
						row[col] = 0;
					} else {
						const num = Number(val);
						if (isNaN(num)) {
							row[col] = 0;
						} else {
							row[col] = Math.floor(num);
						}
					}
				}
			});
			
			return row;
		} catch (cleanErr) {
			console.error(`[Import ${importId}] Error cleaning row ${startRowIndex + idx + 1}:`, cleanErr);
			return null; // Mark as invalid
		}
	}).filter(row => row !== null); // Remove invalid rows

	if (cleanedChunk.length === 0) {
		console.warn(`[Import ${importId}] No valid rows in chunk after cleaning`);
		return { inserted: 0, errors: [`Chunk at row ${startRowIndex + 1}: All rows invalid after cleaning`] };
	}

	const insertBatch = async (batch) => {
		if (typeof table.insertRows === 'function') {
			await table.insertRows(batch);
		} else if (typeof table.bulkWriteRows === 'function') {
			await table.bulkWriteRows(batch);
		} else if (typeof table.insertRow === 'function') {
			for (const row of batch) {
				await table.insertRow(row);
			}
		} else {
			throw new Error('No suitable insert method available');
		}
	};

	// Insert in batches
	for (let i = 0; i < cleanedChunk.length; i += batchSize) {
		const batch = cleanedChunk.slice(i, i + batchSize);
		const batchNum = Math.floor(i / batchSize) + 1;
		try {
			await insertBatch(batch);
			totalInserted += batch.length;
		} catch (batchErr) {
			console.error(`[Import ${importId}] Batch ${batchNum} failed:`, batchErr.message);
			// Try per-row fallback with better error handling
			for (let j = 0; j < batch.length; j++) {
				const row = batch[j];
				try {
					// Double-check all numeric fields before inserting
					NUMERIC_COLUMNS.forEach(col => {
						if (col in row) {
							const val = row[col];
							if (val === null || val === undefined || isNaN(Number(val))) {
								// Default to 0/0.0 for invalid numeric values
								row[col] = INTEGER_COLUMNS.has(col) ? 0 : 0.0;
							} else {
								// Ensure integers are integers, doubles can be decimals
								if (INTEGER_COLUMNS.has(col)) {
									row[col] = Math.floor(Number(val));
								}
							}
						}
					});
					
					if (typeof table.insertRow === 'function') {
						await table.insertRow(row);
						totalInserted++;
					} else if (typeof table.bulkWriteRows === 'function') {
						await table.bulkWriteRows([row]);
						totalInserted++;
					}
				} catch (rowErr) {
					const errMsg = `Row ${startRowIndex + i + j + 1}: ${rowErr.message}`;
					errors.push(errMsg);
					if (errors.length <= 10) {
						console.error(`[Import ${importId}] ${errMsg}`);
					}
				}
			}
		}
	}

	return { inserted: totalInserted, errors };
}

exports.getImportProgress = async (req, res) => {
	const { id } = req.params;
	const state = IMPORT_PROGRESS.get(id);
	if (!state) {
		return res.status(404).json({ success: false, error: 'Import not found' });
	}
	// Prevent caching so clients don't get 304 Not Modified
	res.set('Cache-Control', 'no-store, no-cache, must-revalidate, proxy-revalidate');
	res.set('Pragma', 'no-cache');
	res.set('Expires', '0');
	return res.status(200).json({ success: true, progress: state });
};

// Test endpoint to insert dummy data and verify database insertion works
exports.testInsert = async (req, res) => {
	try {
		const app = req.catalystApp;
		if (!app) {
			return res.status(500).json({ 
				success: false, 
				error: 'Catalyst app context missing' 
			});
		}

		const tableName = DEFAULT_TABLE;
		
		// Create a simple dummy row with required fields
		const dummyRow = {
			WS_client_id: 'TEST_' + Date.now(),
			WS_Account_code: 'TEST_ACCOUNT',
			TRANDATE: new Date().toISOString().split('T')[0], // YYYY-MM-DD format
			SETDATE: new Date().toISOString().split('T')[0],
			Tran_Type: 'Buy',
			Tran_Desc: 'Test Import',
			Security_Type: 'Equity',
			Security_Name: 'TEST_STOCK',
			Security_code: 'TEST',
			EXCHG: 'NSE',
			QTY: 10,
			RATE: 100.50,
			Net_Amount: 1005.00
		};

		// Get Data Store instance and table
		const datastore = app.datastore();
		const table = datastore.table(tableName);

		console.log('Attempting to insert dummy row:', dummyRow);

		// Try to insert the row
		let insertResult;
		try {
			// Try insertRow first (most common method)
			if (typeof table.insertRow === 'function') {
				insertResult = await table.insertRow(dummyRow);
				console.log('insertRow result:', insertResult);
			} else if (typeof table.insertRows === 'function') {
				insertResult = await table.insertRows([dummyRow]);
				console.log('insertRows result:', insertResult);
			} else if (typeof table.bulkWriteRows === 'function') {
				insertResult = await table.bulkWriteRows([dummyRow]);
				console.log('bulkWriteRows result:', insertResult);
			} else {
				return res.status(500).json({ 
					success: false, 
					error: 'No insert method found on table object. Available methods: ' + Object.keys(table).join(', ')
				});
			}
		} catch (insertErr) {
			console.error('Insert error details:', insertErr);
			return res.status(500).json({ 
				success: false, 
				error: `Insert failed: ${insertErr.message}`,
				details: insertErr.toString(),
				stack: insertErr.stack
			});
		}

		// Verify by querying the inserted row
		const zcql = app.zcql();
		const verifyQuery = `SELECT * FROM ${tableName} WHERE WS_client_id = ? ORDER BY CREATEDTIME DESC LIMIT 1`;
		const verifyResult = await zcql.executeZCQLQuery(verifyQuery, [dummyRow.WS_client_id]);

		return res.status(200).json({
			success: true,
			message: 'Dummy data inserted successfully',
			insertResult: insertResult,
			insertedRow: dummyRow,
			verified: verifyResult && verifyResult.length > 0 ? verifyResult[0] : null,
			verificationCount: verifyResult ? verifyResult.length : 0
		});

	} catch (err) {
		console.error('Test insert error:', err);
		return res.status(500).json({ 
			success: false, 
			error: `Test insert failed: ${err.message}`,
			details: err.toString(),
			stack: err.stack
		});
	}
};

// Bonus table columns
const BONUS_TABLE = 'Bonus';
const BONUS_COLUMNS = new Set([
	'Security-Code', 'Company-Name', 'Series', 'BonusShare', 
	'Ex-Date', 'Record-Date', 'ClientId'
]);

// Parse date from Excel format (e.g., "08-Dec-25" -> "2025-12-08")
function parseBonusDate(dateStr) {
	if (!dateStr || dateStr === '' || dateStr === null) {
		return null;
	}
	
	// Try to parse as Excel date number first
	if (typeof dateStr === 'number') {
		// Excel date serial number (days since 1900-01-01)
		const excelEpoch = new Date(1899, 11, 30); // Excel epoch is 1900-01-01, but JS Date is 0-indexed month
		const date = new Date(excelEpoch.getTime() + dateStr * 24 * 60 * 60 * 1000);
		return date.toISOString().split('T')[0]; // Return YYYY-MM-DD format
	}
	
	// Try parsing as string date (e.g., "08-Dec-25")
	const dateStrTrimmed = String(dateStr).trim();
	
	// Pattern: DD-MMM-YY or DD-MMM-YYYY
	const datePatterns = [
		/^(\d{1,2})[-/](\w{3})[-/](\d{2,4})$/i, // DD-MMM-YY or DD-MMM-YYYY
		/^(\d{1,2})[-/](\d{1,2})[-/](\d{2,4})$/i // DD-MM-YY or DD-MM-YYYY
	];
	
	for (const pattern of datePatterns) {
		const match = dateStrTrimmed.match(pattern);
		if (match) {
			let day, month, year;
			
			if (match[2].length === 3) {
				// MMM format (e.g., "Dec")
				day = parseInt(match[1], 10);
				const monthNames = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 
					'jul', 'aug', 'sep', 'oct', 'nov', 'dec'];
				const monthIndex = monthNames.indexOf(match[2].toLowerCase());
				if (monthIndex === -1) {
					continue; // Try next pattern
				}
				month = monthIndex + 1;
				year = parseInt(match[3], 10);
				// Handle 2-digit year
				if (year < 100) {
					year = year < 50 ? 2000 + year : 1900 + year;
				}
			} else {
				// Numeric month format
				day = parseInt(match[1], 10);
				month = parseInt(match[2], 10);
				year = parseInt(match[3], 10);
				if (year < 100) {
					year = year < 50 ? 2000 + year : 1900 + year;
				}
			}
			
			// Validate date
			const date = new Date(year, month - 1, day);
			if (date.getFullYear() === year && date.getMonth() === month - 1 && date.getDate() === day) {
				return `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
			}
		}
	}
	
	// Try standard Date parsing as fallback
	try {
		const date = new Date(dateStrTrimmed);
		if (!isNaN(date.getTime())) {
			return date.toISOString().split('T')[0];
		}
	} catch (e) {
		// Ignore parse errors
	}
	
	return null;
}

// Map bonus Excel row to database format
function mapBonusRowToDatabaseFormat(excelRow) {
	const dbRow = {};
	
	// Map each column
	for (const [excelKey, value] of Object.entries(excelRow)) {
		const normalizedKey = String(excelKey).trim();
		
		// Direct mapping (case-insensitive, handle variations)
		let dbKey = null;
		if (normalizedKey.toLowerCase() === 'security-code' || normalizedKey === 'Security-Code') {
			dbKey = 'Security-Code';
		} else if (normalizedKey.toLowerCase() === 'company-name' || normalizedKey === 'Company-Name') {
			dbKey = 'Company-Name';
		} else if (normalizedKey.toLowerCase() === 'series' || normalizedKey === 'Series') {
			dbKey = 'Series';
		} else if (normalizedKey.toLowerCase() === 'bonusshare' || normalizedKey === 'BonusShare' || normalizedKey === 'bonus-share' || normalizedKey === 'Bonus-Share') {
			dbKey = 'BonusShare';
		} else if (normalizedKey.toLowerCase() === 'ex-date' || normalizedKey === 'Ex-Date') {
			dbKey = 'Ex-Date';
		} else if (normalizedKey.toLowerCase() === 'record-date' || normalizedKey === 'Record-Date') {
			dbKey = 'Record-Date';
		} else if (normalizedKey.toLowerCase() === 'clientid' || normalizedKey === 'ClientId' || normalizedKey === 'Client-ID' || normalizedKey === 'Client-ID') {
			dbKey = 'ClientId';
		}
		
		if (dbKey && BONUS_COLUMNS.has(dbKey)) {
			// Handle date columns
			if (dbKey === 'Ex-Date' || dbKey === 'Record-Date') {
				dbRow[dbKey] = parseBonusDate(value);
			}
			// Handle integer columns
			else if (dbKey === 'BonusShare' || dbKey === 'ClientId') {
				const num = Number(value);
				if (dbKey === 'ClientId') {
					if (!isNaN(num)) {
						// Keep as string to avoid numeric formatting issues in varchar column
						dbRow[dbKey] = String(Math.floor(num));
					} else if (value !== null && value !== undefined && value !== '') {
						dbRow[dbKey] = String(value).trim();
					} else {
						dbRow[dbKey] = null;
					}
				} else {
					// BonusShare: total bonus quantity (integer)
					dbRow[dbKey] = isNaN(num) ? 0 : Math.floor(num);
				}
			}
			// Handle string columns
			else {
				dbRow[dbKey] = value !== null && value !== undefined ? String(value).trim() : null;
			}
		}
	}
	
	return dbRow;
}

// Import Bonus Excel file
exports.importBonus = async (req, res) => {
	try {
		const app = req.catalystApp;
		if (!app) {
			return res.status(500).json({ 
				success: false, 
				error: 'Catalyst app context missing' 
			});
		}

		if (!req.file) {
			return res.status(400).json({ 
				success: false, 
				error: 'No file uploaded' 
			});
		}

		// Store file buffer before async context
		const fileBuffer = Buffer.from(req.file.buffer);
		const fileName = req.file.originalname;
		const fileSize = req.file.size;

		// Start async import and return importId immediately
		const importId = newImportId();

		IMPORT_PROGRESS.set(importId, {
			stage: 'parsing',
			progress: 5,
			message: 'Parsing Bonus Excel...',
			totalRows: 0,
			processedRows: 0,
			imported: 0,
			errors: 0,
			errorDetails: []
		});

		// Kick off background processing
		setImmediate(async () => {
			const tableName = BONUS_TABLE;
			const progress = IMPORT_PROGRESS.get(importId);
			try {
				// Re-initialize Catalyst app in async context
				const appAsync = catalyst.initialize(req);
				if (!appAsync) {
					throw new Error('Failed to initialize Catalyst app');
				}

				// Parse Excel using stored buffer
				let excelRows;
				try {
					excelRows = parseExcelFile(fileBuffer);
				} catch (parseErr) {
					console.error(`[Bonus Import ${importId}] Parse error:`, parseErr);
					throw new Error(`Failed to parse Excel: ${parseErr.message}`);
				}
				if (!excelRows || excelRows.length === 0) {
					throw new Error('Excel file is empty or has no data rows');
				}

				progress.totalRows = excelRows.length;
				progress.stage = 'mapping';
				progress.progress = 15;
				progress.message = 'Mapping bonus columns...';

				// Map rows
				const mappedRows = excelRows.map((row, idx) => {
					try {
						return mapBonusRowToDatabaseFormat(row);
					} catch (mapErr) {
						console.error(`[Bonus Import ${importId}] Error mapping row ${idx + 1}:`, mapErr);
						return null;
					}
				}).filter(row => {
					if (!row) return false;
					// Filter out rows with no essential data
					const hasData = Object.values(row).some(val => val !== null && val !== '');
					return hasData;
				});

				if (mappedRows.length === 0) {
					throw new Error('No valid rows found after mapping. Check column headers match schema.');
				}

				// Insert
				progress.stage = 'inserting';
				progress.progress = 25;
				progress.message = 'Inserting into Bonus table...';

				const datastore = appAsync.datastore();
				const table = datastore.table(tableName);
				let totalInserted = 0;
				let errorCount = 0;
				const errorMessages = [];
				const BATCH_SIZE = 200; // Zoho Catalyst limit

				const insertBatch = async (batch) => {
					if (typeof table.insertRows === 'function') {
						await table.insertRows(batch);
					} else if (typeof table.bulkWriteRows === 'function') {
						await table.bulkWriteRows(batch);
					} else if (typeof table.insertRow === 'function') {
						for (const row of batch) {
							await table.insertRow(row);
						}
					} else {
						throw new Error('No suitable insert method available');
					}
				};

				for (let i = 0; i < mappedRows.length; i += BATCH_SIZE) {
					const batch = mappedRows.slice(i, i + BATCH_SIZE);
					const batchNum = Math.floor(i / BATCH_SIZE) + 1;
					try {
						await insertBatch(batch);
						totalInserted += batch.length;
					} catch (batchErr) {
						console.error(`[Bonus Import ${importId}] Batch ${batchNum} failed:`, batchErr.message);
						// Try per-row fallback
						for (let j = 0; j < batch.length; j++) {
							const row = batch[j];
							try {
								if (typeof table.insertRow === 'function') {
									await table.insertRow(row);
									totalInserted++;
								} else if (typeof table.bulkWriteRows === 'function') {
									await table.bulkWriteRows([row]);
									totalInserted++;
								} else {
									throw new Error('No per-row insert available');
								}
							} catch (rowErr) {
								errorCount++;
								const errMsg = `Row ${i + j + 1}: ${rowErr.message}`;
								errorMessages.push(errMsg);
								if (errorMessages.length <= 10) {
									console.error(`[Bonus Import ${importId}] ${errMsg}`);
								}
							}
						}
					}
					progress.processedRows = Math.min(i + batch.length, mappedRows.length);
					progress.imported = totalInserted;
					progress.errors = errorCount;
					progress.errorDetails = errorMessages.slice(0, 10);
					progress.progress = Math.min(95, Math.round((progress.processedRows / mappedRows.length) * 90) + 5);
					progress.message = `Inserted ${totalInserted}/${mappedRows.length} bonus records...`;
					IMPORT_PROGRESS.set(importId, progress);
				}

				console.log(`[Bonus Import ${importId}] Import completed: ${totalInserted} inserted, ${errorCount} errors`);
				progress.stage = 'completed';
				progress.progress = 100;
				progress.message = `Imported ${totalInserted} of ${mappedRows.length} bonus records${errorCount > 0 ? ` (${errorCount} errors)` : ''}`;
			} catch (err) {
				console.error(`[Bonus Import ${importId}] Fatal error:`, err);
				progress.stage = 'error';
				progress.message = err.message || 'Bonus import failed';
				progress.errorDetails = [err.toString()];
				if (err.stack) {
					console.error(`[Bonus Import ${importId}] Stack:`, err.stack);
				}
			}
			IMPORT_PROGRESS.set(importId, progress);
		});

		return res.status(200).json({
			success: true,
			importId,
			message: 'Bonus import started'
		});

	} catch (err) {
		console.error('Bonus import error:', err);
		return res.status(500).json({ 
			success: false, 
			error: `Failed to import Bonus Excel file: ${err.message}` 
		});
	}
};

// Seed bonus data from Stocks-bouns.txt file
exports.seedBonus = async (req, res) => {
	console.log('[Seed Bonus] Route called');
	
	try {
		const app = req.catalystApp;
		if (!app) {
			console.error('[Seed Bonus] Catalyst app context missing');
			return res.status(500).json({ 
				success: false,
				message: "Catalyst app context missing" 
			});
		}

		console.log('[Seed Bonus] Catalyst app initialized, starting background process');

		// Return immediately and process in background
		res.status(202).json({
			success: true,
			message: 'Bonus seed process started. This may take several minutes. Check server logs for progress.',
			status: 'processing'
		});

		// Run seed in background
		setImmediate(async () => {
			try {
				// Re-initialize Catalyst app in async context
				const appAsync = catalyst.initialize(req);
				if (!appAsync) {
					throw new Error('Failed to initialize Catalyst app');
				}

				console.log('[Seed Bonus] Starting seed process...');
				
				const fs = require('fs');
				const path = require('path');

				const BONUS_TABLE = 'Bonus';
				const CLIENT_IDS_TABLE = 'clientIds';
				const BATCH_SIZE = 200;

				// Parse date function
				function parseDate(dateStr) {
					if (!dateStr || dateStr === '' || dateStr === null) {
						return null;
					}
					const dateStrTrimmed = String(dateStr).trim();
					const datePattern = /^(\d{1,2})[-/](\d{1,2})[-/](\d{2,4})$/;
					const match = dateStrTrimmed.match(datePattern);
					if (match) {
						const day = parseInt(match[1], 10);
						const month = parseInt(match[2], 10);
						let year = parseInt(match[3], 10);
						if (year < 100) {
							year = year < 50 ? 2000 + year : 1900 + year;
						}
						const date = new Date(year, month - 1, day);
						if (date.getFullYear() === year && date.getMonth() === month - 1 && date.getDate() === day) {
							return `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
						}
					}
					return null;
				}

				// Find file
				const possiblePaths = [
					path.join(__dirname, '..', 'react-app', 'Stocks-bouns.txt'), // Stocks-app/appsail-nodejs/../react-app/Stocks-bouns.txt
					path.join(__dirname, '..', '..', 'react-app', 'Stocks-bouns.txt'), // If __dirname is deeper
					path.join(process.cwd(), 'Stocks-app', 'react-app', 'Stocks-bouns.txt'),
					path.join(process.cwd(), 'react-app', 'Stocks-bouns.txt'),
					path.join(process.cwd(), 'Stocks-bouns.txt'),
					path.join(__dirname, '..', 'Stocks-bouns.txt') // Fallback
				];
				
				let filePath = null;
				for (const p of possiblePaths) {
					if (fs.existsSync(p)) {
						filePath = p;
						break;
					}
				}
				
				if (!filePath) {
					throw new Error(`File not found. Tried: ${possiblePaths.join(', ')}`);
				}

				console.log(`[Seed Bonus] Using file: ${filePath}`);

				// Parse file
				const content = fs.readFileSync(filePath, 'utf-8');
				const lines = content.split('\n').map(line => line.trim()).filter(line => line);
				
				if (lines.length < 2) {
					throw new Error('File must have at least a header row and one data row');
				}
				
				const headers = lines[0].split('\t').map(h => h.trim());
				// Use new field names only
				const wsAccountCodeIdx = headers.findIndex(h => h.toLowerCase().trim() === 'wsaccountcode');
				const securityCodeIdx = headers.findIndex(h => h.toLowerCase().trim() === 'securitycode');
				const exDateIdx = headers.findIndex(h => h.toLowerCase().trim() === 'exdate');
				const companyNameIdx = headers.findIndex(h => h.toLowerCase().trim() === 'companyname');
				const exchgIdx = headers.findIndex(h => h.toLowerCase() === 'exchg');
				const schemeNameIdx = headers.findIndex(h => h.toLowerCase() === 'schemename');
				const bonusShareIdx = headers.findIndex(h => h.toLowerCase() === 'bonusshare');
				
				if (wsAccountCodeIdx === -1 || securityCodeIdx === -1 || exDateIdx === -1 || 
					companyNameIdx === -1 || bonusShareIdx === -1) {
					throw new Error(`Required columns not found in header. Found: ${headers.join(', ')}`);
				}

				const bonusRows = [];
				for (let i = 1; i < lines.length; i++) {
					const values = lines[i].split('\t');
					if (values.length < headers.length) continue;
					
					const wsAccountCode = values[wsAccountCodeIdx]?.trim() || '';
					const securityCode = values[securityCodeIdx]?.trim() || '';
					const exDateStr = values[exDateIdx]?.trim() || '';
					const companyName = values[companyNameIdx]?.trim() || '';
					const exchg = values[exchgIdx]?.trim() || '';
					const schemeName = values[schemeNameIdx]?.trim() || '';
					const bonusShareStr = values[bonusShareIdx]?.trim() || '0';
					
					if (!wsAccountCode && !securityCode && !companyName) continue;
					
					bonusRows.push({
						'wsAccountCode': wsAccountCode,
						'SecurityCode': securityCode,
						'ExDate': parseDate(exDateStr),
						'CompanyName': companyName,
						'EXCHG': exchg || null,
						'SCHEMENAME': schemeName || null,
						'BonusShare': parseInt(bonusShareStr, 10) || 0,
						'ClientId': null
					});
				}

				console.log(`[Seed Bonus] Parsed ${bonusRows.length} bonus rows`);

				// Load clientIds mapping
				const zcql = appAsync.zcql();
				const mapping = new Map();
				const batchSize = 250;
				let offset = 0;
				let hasMore = true;
				
				console.log('[Seed Bonus] Loading clientIds mapping...');
				while (hasMore) {
					const query = `SELECT * FROM ${CLIENT_IDS_TABLE} WHERE ${CLIENT_IDS_TABLE}.ws_account_code IS NOT NULL LIMIT ${batchSize} OFFSET ${offset}`;
					const rows = await zcql.executeZCQLQuery(query, []);
					
					if (!rows || rows.length === 0) {
						hasMore = false;
						break;
					}
					
					rows.forEach((row) => {
						const r = row.clientIds || row[CLIENT_IDS_TABLE] || row;
						const wsAccountCode = r.ws_account_code || r.WS_Account_code || r['ws_account_code'];
						const clientId = r.clientId || r.ClientId || r.client_id;
						
						if (wsAccountCode && clientId) {
							const accountCode = String(wsAccountCode).trim();
							const clientIdNum = Number(clientId);
							if (!isNaN(clientIdNum)) {
								mapping.set(accountCode, clientIdNum);
							}
						}
					});
					
					if (rows.length < batchSize) {
						hasMore = false;
					} else {
						offset += batchSize;
						if (offset > 100000) hasMore = false;
					}
				}

				console.log(`[Seed Bonus] Loaded ${mapping.size} client ID mappings`);

				// Match ClientId
				let matchedCount = 0;
				let unmatchedCount = 0;
				for (const row of bonusRows) {
					const accountCode = row['wsAccountCode'];
					const clientId = mapping.get(accountCode);
					if (clientId) {
						row['ClientId'] = clientId;
						matchedCount++;
					} else {
						unmatchedCount++;
						if (unmatchedCount <= 10) {
							console.warn(`[Seed Bonus] No ClientId found for account code: ${accountCode}`);
						}
					}
				}

				console.log(`[Seed Bonus] Matched ClientId for ${matchedCount} rows, ${unmatchedCount} unmatched`);

				// Insert rows
				const datastore = appAsync.datastore();
				const table = datastore.table(BONUS_TABLE);
				let totalInserted = 0;
				let totalErrors = 0;

				console.log('[Seed Bonus] Inserting bonus rows...');
				for (let i = 0; i < bonusRows.length; i += BATCH_SIZE) {
					const batch = bonusRows.slice(i, i + BATCH_SIZE);
					const batchNum = Math.floor(i / BATCH_SIZE) + 1;
					const totalBatches = Math.ceil(bonusRows.length / BATCH_SIZE);
					
					try {
						if (typeof table.insertRows === 'function') {
							await table.insertRows(batch);
							totalInserted += batch.length;
						} else if (typeof table.bulkWriteRows === 'function') {
							await table.bulkWriteRows(batch);
							totalInserted += batch.length;
						} else {
							for (const row of batch) {
								try {
									await table.insertRow(row);
									totalInserted++;
								} catch (err) {
									totalErrors++;
									console.error(`[Seed Bonus] Error inserting row:`, err.message);
								}
							}
						}
						console.log(`[Seed Bonus] Inserted batch ${batchNum}/${totalBatches} (${totalInserted} total)`);
					} catch (err) {
						console.error(`[Seed Bonus] Error inserting batch ${batchNum}:`, err.message);
						totalErrors += batch.length;
					}
				}

				// Update existing rows
				console.log('[Seed Bonus] Updating existing bonus rows...');
				let totalUpdated = 0;
				let updateErrors = 0;
				offset = 0;
				hasMore = true;

				while (hasMore) {
					const query = `SELECT * FROM ${BONUS_TABLE} WHERE ${BONUS_TABLE}.wsAccountCode IS NOT NULL LIMIT ${batchSize} OFFSET ${offset}`;
					const rows = await zcql.executeZCQLQuery(query, []);
					
					if (!rows || rows.length === 0) {
						hasMore = false;
						break;
					}
					
					for (const row of rows) {
						const r = row.Bonus || row[BONUS_TABLE] || row;
						const rowId = r.ROWID || r.rowid;
						const wsAccountCode = r.wsAccountCode || r['wsAccountCode'];
						
						if (rowId && wsAccountCode) {
							const accountCode = String(wsAccountCode).trim();
							const clientId = mapping.get(accountCode);
							if (clientId && (!r.ClientId || r.ClientId === null)) {
								try {
									await table.updateRow({
										ROWID: rowId,
										ClientId: clientId
									});
									totalUpdated++;
								} catch (err) {
									updateErrors++;
									console.error(`[Seed Bonus] Error updating row ${rowId}:`, err.message);
								}
							}
						}
					}
					
					if (rows.length < batchSize) {
						hasMore = false;
					} else {
						offset += batchSize;
						if (offset > 100000) hasMore = false;
					}
				}

				console.log('[Seed Bonus] ===== COMPLETED =====');
				console.log(`[Seed Bonus] Total rows parsed: ${bonusRows.length}`);
				console.log(`[Seed Bonus] Rows with matched ClientId: ${matchedCount}`);
				console.log(`[Seed Bonus] Rows without matching ClientId: ${unmatchedCount}`);
				console.log(`[Seed Bonus] Rows inserted: ${totalInserted}`);
				console.log(`[Seed Bonus] Insert errors: ${totalErrors}`);
				console.log(`[Seed Bonus] Existing rows updated: ${totalUpdated}`);
				console.log(`[Seed Bonus] Update errors: ${updateErrors}`);
				console.log('[Seed Bonus] =======================');

			} catch (err) {
				console.error('[Seed Bonus] Fatal error:', err);
				console.error('[Seed Bonus] Stack:', err.stack);
			}
		});

	} catch (err) {
		console.error('[Seed Bonus] Controller error:', err);
		return res.status(500).json({
			success: false,
			message: 'Failed to start seed process',
			error: err.message
		});
	}
};

// Seed Security_List data from security.txt file
exports.seedSecurityList = async (req, res) => {
	console.log('[Seed Security List] Route called');
	try {
		const app = req.catalystApp;
		if (!app) {
			console.error('[Seed Security List] Catalyst app context missing');
			return res.status(500).json({
				success: false,
				message: "Catalyst app context missing"
			});
		}

		console.log('[Seed Security List] Catalyst app initialized, starting background process');

		res.status(202).json({
			success: true,
			message: 'Security List seed process started. This may take several minutes. Check server logs for progress.',
			status: 'processing'
		});

		setImmediate(async () => {
			try {
				const appAsync = catalyst.initialize(req);
				if (!appAsync) {
					throw new Error('Failed to initialize Catalyst app');
				}

				console.log('[Seed Security List] Starting seed process...');

				const fs = require('fs');
				const path = require('path');

				const SECURITY_LIST_TABLE = 'Security_List';
				const BATCH_SIZE = 200;

				// Find file
				const possiblePaths = [
					path.join(__dirname, '..', 'react-app', 'security.txt'),
					path.join(__dirname, '..', '..', 'react-app', 'security.txt'),
					path.join(process.cwd(), 'Stocks-app', 'react-app', 'security.txt'),
					path.join(process.cwd(), 'react-app', 'security.txt'),
					path.join(process.cwd(), 'security.txt'),
					path.join(__dirname, '..', 'security.txt')
				];

				let filePath = null;
				for (const p of possiblePaths) {
					if (fs.existsSync(p)) {
						filePath = p;
						break;
					}
				}

				if (!filePath) {
					throw new Error(`File not found. Tried: ${possiblePaths.join(', ')}`);
				}

				console.log(`[Seed Security List] Found file at: ${filePath}`);

				// Parse file (tab-separated)
				const content = fs.readFileSync(filePath, 'utf-8');
				const lines = content.split('\n').map(line => line.trim()).filter(line => line);

				if (lines.length < 2) {
					throw new Error('File must have at least a header row and one data row');
				}

				// Parse header
				const headers = lines[0].split('\t').map(h => h.trim());
				const securityCodeIdx = headers.findIndex(h => 
					h.toLowerCase() === 'security_code' || 
					h.toLowerCase() === 'security-code' ||
					h.toLowerCase() === 'securitycode'
				);
				const securityNameIdx = headers.findIndex(h => 
					h.toLowerCase() === 'security_name' || 
					h.toLowerCase() === 'security-name' ||
					h.toLowerCase() === 'securityname'
				);

				if (securityCodeIdx === -1 || securityNameIdx === -1) {
					throw new Error(`Required columns not found. Expected: Security_Code, Security_Name. Found headers: ${headers.join(', ')}`);
				}

				console.log(`[Seed Security List] Headers found: ${headers.join(', ')}`);
				console.log(`[Seed Security List] Security_Code index: ${securityCodeIdx}, Security_Name index: ${securityNameIdx}`);

				// Parse data rows
				const securityRows = [];
				for (let i = 1; i < lines.length; i++) {
					const values = lines[i].split('\t');
					if (values.length < headers.length) {
						console.warn(`[Seed Security List] Skipping row ${i + 1}: insufficient columns (expected ${headers.length}, got ${values.length})`);
						continue;
					}

					const securityCode = (values[securityCodeIdx] || '').trim();
					const securityName = (values[securityNameIdx] || '').trim();

					if (!securityCode || !securityName) {
						console.warn(`[Seed Security List] Skipping row ${i + 1}: missing Security_Code or Security_Name`);
						continue;
					}

					securityRows.push({
						Security_Code: securityCode,
						Security_Name: securityName
					});
				}

				console.log(`[Seed Security List] Parsed ${securityRows.length} security rows from file.`);

				if (securityRows.length === 0) {
					console.warn('[Seed Security List] No security data found in the file after parsing.');
					return;
				}

				// Insert security rows
				const datastore = appAsync.datastore();
				const securityTable = datastore.table(SECURITY_LIST_TABLE);
				let totalInserted = 0;
				let totalSkipped = 0;
				let totalErrors = 0;
				const errors = [];

				for (let i = 0; i < securityRows.length; i += BATCH_SIZE) {
					const batch = securityRows.slice(i, i + BATCH_SIZE);
					const batchNum = Math.floor(i / BATCH_SIZE) + 1;
					try {
						await securityTable.insertRows(batch);
						totalInserted += batch.length;
						console.log(`[Seed Security List] Inserted batch ${batchNum}: ${batch.length} rows. Total inserted: ${totalInserted}`);
					} catch (insertErr) {
						console.error(`[Seed Security List] Error inserting batch ${batchNum}:`, insertErr.message);
						// Attempt single row insertion to identify duplicates vs other errors
						for (const row of batch) {
							try {
								await securityTable.insertRow(row);
								totalInserted++;
							} catch (singleInsertErr) {
								if (singleInsertErr.message && (
									singleInsertErr.message.includes('unique') ||
									singleInsertErr.message.includes('duplicate') ||
									singleInsertErr.message.includes('UNIQUE constraint')
								)) {
									totalSkipped++;
									// console.log(`[Seed Security List] Skipped duplicate security: ${row.Security_Code}`);
								} else {
									totalErrors++;
									errors.push({ row: row, error: singleInsertErr.message });
									console.error(`[Seed Security List] Error inserting single security row (${row.Security_Code}):`, singleInsertErr.message);
								}
							}
						}
					}
				}

				console.log(`[Seed Security List] Seed process completed.`);
				console.log(`[Seed Security List] Total rows processed from file: ${securityRows.length}`);
				console.log(`[Seed Security List] Total security rows inserted: ${totalInserted}`);
				console.log(`[Seed Security List] Total security rows skipped (duplicates): ${totalSkipped}`);
				console.log(`[Seed Security List] Total security rows with errors: ${totalErrors}`);
				if (errors.length > 0) {
					console.error('[Seed Security List] Sample errors:', errors.slice(0, 5));
				}

			} catch (err) {
				console.error('[Seed Security List] Background process error:', err);
				console.error('[Seed Security List] Stack:', err.stack);
			}
		});

	} catch (err) {
		console.error('[Seed Security List] Route handler error:', err);
		return res.status(500).json({
			success: false,
			message: 'Failed to start seed process',
			error: err.message
		});
	}
};

