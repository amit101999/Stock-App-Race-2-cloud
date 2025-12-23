const catalyst = require('zcatalyst-sdk-node');
const fs = require('fs');
const path = require('path');

// Initialize Catalyst app
const app = catalyst.initialize({
  type: catalyst.type.web
});

// Configuration
const BONUS_TABLE = 'Bonus';
const CLIENT_IDS_TABLE = 'clientIds';
const BATCH_SIZE = 200; // Zoho Catalyst batch limit

// Parse date from DD-MM-YYYY format
function parseDate(dateStr) {
  if (!dateStr || dateStr === '' || dateStr === null) {
    return null;
  }
  
  const dateStrTrimmed = String(dateStr).trim();
  
  // Pattern: DD-MM-YYYY
  const datePattern = /^(\d{1,2})[-/](\d{1,2})[-/](\d{2,4})$/;
  const match = dateStrTrimmed.match(datePattern);
  
  if (match) {
    const day = parseInt(match[1], 10);
    const month = parseInt(match[2], 10);
    let year = parseInt(match[3], 10);
    
    // Handle 2-digit year
    if (year < 100) {
      year = year < 50 ? 2000 + year : 1900 + year;
    }
    
    // Validate date
    const date = new Date(year, month - 1, day);
    if (date.getFullYear() === year && date.getMonth() === month - 1 && date.getDate() === day) {
      return `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
    }
  }
  
  return null;
}

// Parse tab-separated file
function parseBonusFile(filePath) {
  const content = fs.readFileSync(filePath, 'utf-8');
  const lines = content.split('\n').map(line => line.trim()).filter(line => line);
  
  if (lines.length < 2) {
    throw new Error('File must have at least a header row and one data row');
  }
  
  // Parse header
  const headers = lines[0].split('\t').map(h => h.trim());
  console.log('Headers found:', headers);
  
  // Find column indices
  const wsAccountCodeIdx = headers.findIndex(h => 
    h.toLowerCase() === 'ws_account_code' || h.toLowerCase() === 'ws account code'
  );
  const securityCodeIdx = headers.findIndex(h => 
    h.toLowerCase() === 'security-code' || h.toLowerCase() === 'security code'
  );
  const exDateIdx = headers.findIndex(h => {
    const lower = h.toLowerCase().trim();
    return (lower.includes('ex') && lower.includes('date')) || 
           lower === 'ex -date' || 
           lower === 'ex-date';
  });
  const companyNameIdx = headers.findIndex(h => 
    h.toLowerCase() === 'company-name' || h.toLowerCase() === 'company name'
  );
  const exchgIdx = headers.findIndex(h => 
    h.toLowerCase() === 'exchg' || h.toLowerCase() === 'exchange'
  );
  const schemeNameIdx = headers.findIndex(h => 
    h.toLowerCase() === 'schemename' || h.toLowerCase() === 'scheme name'
  );
  const bonusShareIdx = headers.findIndex(h => 
    h.toLowerCase() === 'bonusshare' || h.toLowerCase() === 'bonus share'
  );
  
  console.log('Column indices:', {
    wsAccountCode: wsAccountCodeIdx,
    securityCode: securityCodeIdx,
    exDate: exDateIdx,
    companyName: companyNameIdx,
    exchg: exchgIdx,
    schemeName: schemeNameIdx,
    bonusShare: bonusShareIdx
  });
  
  if (wsAccountCodeIdx === -1 || securityCodeIdx === -1 || exDateIdx === -1 || 
      companyNameIdx === -1 || bonusShareIdx === -1) {
    throw new Error('Required columns not found in header');
  }
  
  // Parse data rows
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const values = lines[i].split('\t');
    if (values.length < headers.length) {
      console.warn(`Row ${i + 1} has fewer columns than headers, skipping`);
      continue;
    }
    
    const wsAccountCode = values[wsAccountCodeIdx]?.trim() || '';
    const securityCode = values[securityCodeIdx]?.trim() || '';
    const exDateStr = values[exDateIdx]?.trim() || '';
    const companyName = values[companyNameIdx]?.trim() || '';
    const exchg = values[exchgIdx]?.trim() || '';
    const schemeName = values[schemeNameIdx]?.trim() || '';
    const bonusShareStr = values[bonusShareIdx]?.trim() || '0';
    
    // Skip empty rows
    if (!wsAccountCode && !securityCode && !companyName) {
      continue;
    }
    
    const exDate = parseDate(exDateStr);
    const bonusShare = parseInt(bonusShareStr, 10) || 0;
    
    rows.push({
      'ws_account_code': wsAccountCode, // Store temporarily for matching
      'Security-Code': securityCode,
      'Ex-Date': exDate,
      'Company-Name': companyName,
      'EXCHG': exchg || null,
      'SCHEMENAME': schemeName || null,
      'BonusShare': bonusShare,
      'ClientId': null // Will be updated later
    });
  }
  
  return rows;
}

// Load clientIds mapping (ws_account_code -> clientId)
async function loadClientIdsMapping() {
  const zcql = app.zcql();
  const mapping = new Map();
  
  const batchSize = 250;
  let offset = 0;
  let hasMore = true;
  
  console.log('Loading clientIds mapping...');
  
  while (hasMore) {
    const query = `SELECT * FROM ${CLIENT_IDS_TABLE} WHERE ${CLIENT_IDS_TABLE}.ws_account_code IS NOT NULL LIMIT ${batchSize} OFFSET ${offset}`;
    
    try {
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
      
      console.log(`Loaded ${mapping.size} mappings so far (batch at offset ${offset})`);
      
      if (rows.length < batchSize) {
        hasMore = false;
      } else {
        offset += batchSize;
        if (offset > 100000) {
          console.warn('Reached safety limit, stopping');
          hasMore = false;
        }
      }
    } catch (err) {
      console.error(`Error loading clientIds at offset ${offset}:`, err);
      hasMore = false;
    }
  }
  
  console.log(`Total clientIds mappings loaded: ${mapping.size}`);
  return mapping;
}

// Insert bonus rows in batches
async function insertBonusRows(rows) {
  const datastore = app.datastore();
  const table = datastore.table(BONUS_TABLE);
  
  let totalInserted = 0;
  let totalErrors = 0;
  const errors = [];
  
  for (let i = 0; i < rows.length; i += BATCH_SIZE) {
    const batch = rows.slice(i, i + BATCH_SIZE);
    const batchNum = Math.floor(i / BATCH_SIZE) + 1;
    const totalBatches = Math.ceil(rows.length / BATCH_SIZE);
    
    console.log(`Inserting batch ${batchNum}/${totalBatches} (${batch.length} rows)...`);
    
    try {
      let insertResult;
      if (typeof table.insertRows === 'function') {
        insertResult = await table.insertRows(batch);
      } else if (typeof table.bulkWriteRows === 'function') {
        insertResult = await table.bulkWriteRows(batch);
      } else {
        // Fallback to individual inserts
        for (const row of batch) {
          try {
            await table.insertRow(row);
            totalInserted++;
          } catch (err) {
            totalErrors++;
            errors.push({ row, error: err.message });
          }
        }
        continue;
      }
      
      totalInserted += batch.length;
      console.log(`Batch ${batchNum} inserted successfully`);
    } catch (err) {
      console.error(`Error inserting batch ${batchNum}:`, err);
      totalErrors += batch.length;
      errors.push({ batch: batchNum, error: err.message });
    }
  }
  
  return { totalInserted, totalErrors, errors };
}

// Update ClientId in Bonus table based on ws_account_code
async function updateClientIds(clientIdsMapping) {
  const zcql = app.zcql();
  const datastore = app.datastore();
  const table = datastore.table(BONUS_TABLE);
  
  let totalUpdated = 0;
  let totalErrors = 0;
  let totalSkipped = 0;
  
  console.log(`Updating ClientId for ${clientIdsMapping.size} account codes...`);
  
  // Process in batches - query all bonus rows with ws_account_code
  const batchSize = 250;
  let offset = 0;
  let hasMore = true;
  
  while (hasMore) {
    try {
      // Query bonus rows that have ws_account_code but no ClientId or ClientId is null
      const query = `SELECT * FROM ${BONUS_TABLE} WHERE ${BONUS_TABLE}.ws_account_code IS NOT NULL LIMIT ${batchSize} OFFSET ${offset}`;
      const rows = await zcql.executeZCQLQuery(query, []);
      
      if (!rows || rows.length === 0) {
        hasMore = false;
        break;
      }
      
      for (const row of rows) {
        const r = row.Bonus || row[BONUS_TABLE] || row;
        const rowId = r.ROWID || r.rowid;
        const wsAccountCode = r.ws_account_code || r['ws_account_code'] || r.WS_Account_code;
        
        if (!rowId || !wsAccountCode) {
          totalSkipped++;
          continue;
        }
        
        const accountCode = String(wsAccountCode).trim();
        const clientId = clientIdsMapping.get(accountCode);
        
        if (clientId) {
          try {
            await table.updateRow({
              ROWID: rowId,
              ClientId: clientId
            });
            totalUpdated++;
          } catch (updateErr) {
            console.error(`Error updating row ${rowId} (account: ${accountCode}):`, updateErr.message);
            totalErrors++;
          }
        } else {
          totalSkipped++;
          console.warn(`No ClientId found for account code: ${accountCode}`);
        }
      }
      
      console.log(`Processed ${offset + rows.length} rows, updated ${totalUpdated} so far...`);
      
      if (rows.length < batchSize) {
        hasMore = false;
      } else {
        offset += batchSize;
        if (offset > 100000) {
          console.warn('Reached safety limit, stopping');
          hasMore = false;
        }
      }
    } catch (err) {
      console.error(`Error processing batch at offset ${offset}:`, err);
      hasMore = false;
    }
  }
  
  return { totalUpdated, totalErrors, totalSkipped };
}

// Main function
async function seedBonus() {
  try {
    console.log('Starting bonus seed script...');
    
    // Read and parse file
    // Try multiple possible paths
    const possiblePaths = [
      path.join(__dirname, 'Stocks-bouns.txt'),
      path.join(__dirname, '..', 'Stocks-bouns.txt'),
      path.join(process.cwd(), 'Stocks-bouns.txt'),
      path.join(process.cwd(), 'Stocks-app', 'Stocks-bouns.txt')
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
    
    console.log(`Using file: ${filePath}`);
    
    console.log('Parsing bonus file...');
    const bonusRows = parseBonusFile(filePath);
    console.log(`Parsed ${bonusRows.length} bonus rows`);
    
    if (bonusRows.length === 0) {
      throw new Error('No bonus rows found in file');
    }
    
    // Load clientIds mapping
    const clientIdsMapping = await loadClientIdsMapping();
    console.log(`Loaded ${clientIdsMapping.size} client ID mappings`);
    
    // Match ClientId before insertion for efficiency
    console.log('Matching ClientId for bonus rows...');
    let matchedCount = 0;
    let unmatchedCount = 0;
    
    for (const row of bonusRows) {
      const accountCode = row['ws_account_code'];
      const clientId = clientIdsMapping.get(accountCode);
      if (clientId) {
        row['ClientId'] = clientId;
        matchedCount++;
      } else {
        row['ClientId'] = null;
        unmatchedCount++;
        if (unmatchedCount <= 10) {
          console.warn(`No ClientId found for account code: ${accountCode}`);
        }
      }
    }
    
    console.log(`Matched ClientId for ${matchedCount} rows, ${unmatchedCount} unmatched`);
    
    // Insert bonus rows (now with ClientId already set)
    console.log('Inserting bonus rows...');
    const insertResult = await insertBonusRows(bonusRows);
    console.log(`Inserted ${insertResult.totalInserted} rows, ${insertResult.totalErrors} errors`);
    
    if (insertResult.errors.length > 0) {
      console.error('Insert errors:', insertResult.errors.slice(0, 10)); // Show first 10 errors
    }
    
    // Also update any existing rows that might have ws_account_code but no ClientId
    console.log('Updating ClientId for any existing bonus rows...');
    const updateResult = await updateClientIds(clientIdsMapping);
    console.log(`Updated ${updateResult.totalUpdated} existing rows, ${updateResult.totalErrors} errors, ${updateResult.totalSkipped || 0} skipped`);
    
    console.log('\n=== Seed Summary ===');
    console.log(`Total rows parsed: ${bonusRows.length}`);
    console.log(`ClientId mappings loaded: ${clientIdsMapping.size}`);
    console.log(`Rows with matched ClientId: ${matchedCount}`);
    console.log(`Rows without matching ClientId: ${unmatchedCount}`);
    console.log(`Rows inserted: ${insertResult.totalInserted}`);
    console.log(`Insert errors: ${insertResult.totalErrors}`);
    console.log(`Existing rows updated: ${updateResult.totalUpdated}`);
    console.log(`Update errors: ${updateResult.totalErrors}`);
    console.log(`Rows skipped (no matching ClientId): ${updateResult.totalSkipped || 0}`);
    console.log('Seed completed successfully!');
    
  } catch (err) {
    console.error('Seed script error:', err);
    console.error('Stack:', err.stack);
    process.exit(1);
  }
}

// Run the seed script
if (require.main === module) {
  seedBonus()
    .then(() => {
      console.log('Script finished');
      process.exit(0);
    })
    .catch((err) => {
      console.error('Fatal error:', err);
      process.exit(1);
    });
}

module.exports = { seedBonus, parseBonusFile, loadClientIdsMapping, updateClientIds };

