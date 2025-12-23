'use strict';

const catalyst = require('zcatalyst-sdk-node');

/**
 * Catalyst Function to check for bonuses for a specific client and company
 * 
 * Request Parameters (query string or body):
 * - clientId: The client ID (e.g., 8800046)
 * - companyName: The company name (e.g., "Astral Ltd.")
 * 
 * Example: /check-bonus?clientId=8800046&companyName=Astral%20Ltd.
 */
module.exports = async (request, response) => {
  try {
    // Initialize Catalyst app
    const app = catalyst.initialize(request);
    if (!app) {
      return response.status(500).json({ 
        success: false,
        message: "Catalyst app context missing" 
      });
    }

    // Get parameters from query string or request body
    const clientIdRaw = request.query?.clientId || request.body?.clientId || request.params?.clientId;
    const companyName = request.query?.companyName || request.body?.companyName || request.params?.companyName;

    if (!clientIdRaw || !companyName) {
      return response.status(400).json({ 
        success: false,
        message: "Both clientId and companyName are required",
        example: "/check-bonus?clientId=8800046&companyName=Astral Ltd."
      });
    }

    const clientId = parseInt(String(clientIdRaw).trim(), 10);
    if (isNaN(clientId)) {
      return response.status(400).json({ 
        success: false,
        message: "Invalid clientId format. Must be a number."
      });
    }

    const zcql = app.zcql();
    const bonusTableName = 'Bonus';
    const bonusQuery = `SELECT * FROM ${bonusTableName}`;

    console.log(`[checkBonus] Checking for clientId: ${clientId}, companyName: "${companyName}"`);

    // Fetch all bonus records
    const bonusRows = await zcql.executeZCQLQuery(bonusQuery, []);
    console.log(`[checkBonus] Total bonus records in database: ${bonusRows.length}`);

    // Normalize function to handle company name variations
    const normalizeName = (name) => {
      if (!name) return '';
      return String(name)
        .trim()
        .toUpperCase()
        .replace(/\s+/g, ' ')
        .replace(/[^\w\s]/g, '')
        .replace(/\bLIMITED\b/g, 'LTD')
        .replace(/\bINCORPORATED\b/g, 'INC')
        .replace(/\bCORPORATION\b/g, 'CORP')
        .replace(/\bPRIVATE\b/g, 'PVT')
        .replace(/\s+/g, ' ');
    };

    const normalizedCompanyName = normalizeName(companyName);
    console.log(`[checkBonus] Normalized company name: "${normalizedCompanyName}"`);

    const matchingBonuses = [];
    const allBonusesForClient = [];
    const allBonusesForCompany = [];

    // Process all bonus rows
    for (const row of bonusRows) {
      const b = row.Bonus || row[bonusTableName] || row;
      
      // Extract ClientId with multiple fallback options
      const rawClientId = b.ClientId !== undefined ? b.ClientId :
                         (b.clientId !== undefined ? b.clientId :
                          (b[`${bonusTableName}.ClientId`] !== undefined ? b[`${bonusTableName}.ClientId`] :
                           (b['Bonus.ClientId'] !== undefined ? b['Bonus.ClientId'] : null)));
      
      const bonusClientId = rawClientId !== undefined && rawClientId !== null
        ? (typeof rawClientId === 'number' ? rawClientId : Number(rawClientId))
        : null;

      // Extract CompanyName with multiple fallback options
      const bonusCompanyName = b.CompanyName || b['CompanyName'] || b[`${bonusTableName}.CompanyName`] || b['Bonus.CompanyName'] || '';
      const normalizedBonusName = normalizeName(bonusCompanyName);

      // Extract other fields
      const bonusShare = b.BonusShare || b['BonusShare'] || b[`${bonusTableName}.BonusShare`] || b['Bonus.BonusShare'] || 0;
      const exDate = b.ExDate || b['ExDate'] || b[`${bonusTableName}.ExDate`] || b['Bonus.ExDate'] || '';
      const securityCode = b.SecurityCode || b['SecurityCode'] || b[`${bonusTableName}.SecurityCode`] || b['Bonus.SecurityCode'] || '';
      const wsAccountCode = b.wsAccountCode || b['wsAccountCode'] || null;

      // Track all bonuses for this client
      if (bonusClientId === clientId || bonusClientId === null) {
        allBonusesForClient.push({
          ClientId: bonusClientId,
          CompanyName: bonusCompanyName,
          SecurityCode: securityCode,
          ExDate: exDate,
          BonusShare: bonusShare,
          wsAccountCode: wsAccountCode,
          normalizedName: normalizedBonusName
        });
      }

      // Track all bonuses for this company
      if (normalizedBonusName === normalizedCompanyName) {
        allBonusesForCompany.push({
          ClientId: bonusClientId,
          CompanyName: bonusCompanyName,
          SecurityCode: securityCode,
          ExDate: exDate,
          BonusShare: bonusShare,
          wsAccountCode: wsAccountCode
        });
      }

      // Check if matches both client and company
      const matchesClient = bonusClientId === null || bonusClientId === clientId;
      const matchesCompany = normalizedBonusName === normalizedCompanyName;

      if (matchesClient && matchesCompany) {
        matchingBonuses.push({
          ROWID: b.ROWID || b.rowid,
          ClientId: bonusClientId,
          CompanyName: bonusCompanyName,
          SecurityCode: securityCode,
          ExDate: exDate,
          BonusShare: bonusShare,
          wsAccountCode: wsAccountCode,
          rawData: {
            rawClientId,
            rawClientIdType: typeof rawClientId,
            normalizedBonusName,
            normalizedCompanyName,
            matchesClient,
            matchesCompany
          }
        });
      }
    }

    console.log(`[checkBonus] Found ${matchingBonuses.length} matching bonus(es)`);
    console.log(`[checkBonus] All bonuses for client ${clientId}: ${allBonusesForClient.length}`);
    console.log(`[checkBonus] All bonuses for company "${companyName}": ${allBonusesForCompany.length}`);

    // Return response
    return response.status(200).json({
      success: true,
      query: {
        clientId,
        companyName,
        normalizedCompanyName
      },
      summary: {
        totalBonusesInDB: bonusRows.length,
        matchingBonuses: matchingBonuses.length,
        allBonusesForClient: allBonusesForClient.length,
        allBonusesForCompany: allBonusesForCompany.length
      },
      matchingBonuses: matchingBonuses,
      debug: {
        allBonusesForClient: allBonusesForClient.slice(0, 10), // First 10 only
        allBonusesForCompany: allBonusesForCompany.slice(0, 10) // First 10 only
      }
    });

  } catch (err) {
    console.error(`[checkBonus] Error:`, err);
    return response.status(500).json({ 
      success: false,
      message: err.message || 'Internal server error',
      error: err.toString(),
      stack: err.stack
    });
  }
};

