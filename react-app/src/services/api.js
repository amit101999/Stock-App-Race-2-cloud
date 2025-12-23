import axios from 'axios';

// Backend root provided by user (no trailing slash to avoid double slashes)
// const API_ROOT = 'https://backend-10110149335.development.catalystappsail.com';
const API_ROOT = 'http://localhost:3001';

const api = axios.create({
  baseURL: API_ROOT,
  headers: {
    'Content-Type': 'application/json',
  },
});

export const tradesAPI = {
  // Get all trades with filters -> map to backend /api/stocks
  getTrades: async (params = {}) => {
    // Map UI filters to backend query params
    const mapped = {
      page: params.page,
      limit: params.limit,
      // equality filters
      exchg: params.exchange,
      tran_type: params.tradeType,
      ws_client_id: params.customerId,
      security_code: params.stockSymbol, // Changed from security_name to security_code
      // date range
      trandate_from: params.startDate,
      trandate_to: params.endDate,
    };
    const res = await api.get('/api/stocks', { params: mapped });
    const { data, page, limit, total } = res.data || {};
    const rawRows = Array.isArray(data) ? data : [];
    // Rows may be wrapped like { Transaction: { ...cols } } from ZCQL
    const rows = rawRows.map((row) => {
      const r = row.Transaction ? row.Transaction : row;
      return {
        // Client Information
        wsClientId: r.WS_client_id ?? r.ws_client_id,
        wsAccountCode: r.WS_Account_code ?? r.ws_account_code,
        // Transaction Dates
        trandate: r.TRANDATE ?? r.trandate,
        setdate: r.SETDATE ?? r.setdate,
        // Transaction Details
        tranType: r.Tran_Type ?? r.tran_type,
        tranDesc: r.Tran_Desc ?? r.tran_desc,
        // Security Information
        securityType: r.Security_Type ?? r.security_type,
        securityTypeDescription: r.Security_Type_Description ?? r.security_type_description,
        detailTypeName: r.DETAILTYPENAME ?? r.detailtypename,
        isin: r.ISIN ?? r.isin,
        securityCode: r.Security_code ?? r.security_code,
        securityName: r.Security_Name ?? r.security_name,
        // Exchange & Broker
        exchg: r.EXCHG ?? r.exchg,
        brokerCode: r.BROKERCODE ?? r.brokercode,
        // Depository/Registrar
        depositoryRegistrar: r.Depositoy_Registrar ?? r.Depository_Registrar ?? r.depository_registrar ?? r['Depositoy/Registrar'],
        dpidAmc: r.DPID_AMC ?? r.dpid_amc ?? r['DPID/AMC'],
        dpClientIdFolio: r.Dp_Client_id_Folio ?? r.dp_client_id_folio ?? r['Dp Client id/Folio'],
        // Bank Information
        bankCode: r.BANKCODE ?? r.bankcode,
        bankAcid: r.BANKACID ?? r.bankacid,
        // Trade Details
        qty: r.QTY ?? r.qty,
        rate: r.RATE ?? r.rate,
        brokerage: r.BROKERAGE ?? r.brokerage,
        serviceTax: r.SERVICETAX ?? r.servicetax ?? r.serviceTax,
        netRate: r.NETRATE ?? r.netrate,
        netAmount: r['Net_Amount'] ?? r.net_amount ?? r.netAmount,
        stt: r.STT ?? r.stt,
        // Transfer Details
        trfdate: r.TRFDATE ?? r.trfdate,
        trfrate: r.TRFRATE ?? r.trfrate,
        trfamt: r.TRFAMT ?? r.trfamt,
        // Transaction Fees
        totalTrxnFee: r.TOTAL_TRXNFEE ?? r.total_trxnfee,
        totalTrxnFeeStax: r.TOTAL_TRXNFEE_STAX ?? r.total_trxnfee_stax,
        // Transaction Reference
        txnRefNo: r['Txn Ref No'] ?? r.txn_ref_no,
        descMemo: r.DESCMEMO ?? r.descmemo,
        // Payment Details
        chequeNo: r.CHEQUENO ?? r.chequeno,
        chequeDtl: r.CHEQUEDTL ?? r.chequedtl,
        portfolioId: r.PORTFOLIOID ?? r.portfolioid ?? r.portfolioId,
        deliveryDate: r.DELIVERYDATE ?? r.deliverydate,
        paymentDate: r.PAYMENTDATE ?? r.paymentdate,
        accruedInterest: r.ACCRUEDINTEREST ?? r.accruedinterest,
        // Issuer Information
        issuer: r.ISSUER ?? r.issuer ?? r.Issuer,
        issuerName: r.ISSUERNAME ?? r.issuername ?? r.issuerName,
        tdsAmount: r.TDSAMOUNT ?? r.tdsamount,
        stampDuty: r.STAMPDUTY ?? r.stampduty,
        tpmsgain: r.TPMSGAIN ?? r.tpmsgain,
        // Relationship Manager
        rmid: r.RMID ?? r.rmid ?? r.rmId,
        rmname: r.RMNAME ?? r.rmname ?? r.rmName,
        // Advisor Information
        advisorId: r.ADVISORID ?? r.advisorid,
        advisorName: r.ADVISORNAME ?? r.advisorname,
        // Branch Information
        branchId: r.BRANCHID ?? r.branchid,
        branchName: r.BRANCHNAME ?? r.branchname,
        // Group Information
        groupId: r.GROUPID ?? r.groupid,
        groupName: r.GROUPNAME ?? r.groupname,
        // Owner Information
        ownerId: r.OWNERID ?? r.ownerid,
        ownerName: r.OWNERNAME ?? r.ownername,
        wealthAdvisorName: r['WEALTHADVISOR NAME'] ?? r.wealthadvisor_name,
        // Scheme Information
        schemeId: r.SCHEMEID ?? r.schemeid ?? r.schemeId,
        schemeName: r.SCHEMENAME ?? r.schemename ?? r.schemeName,
      };
    });
    const pages = total && limit ? Math.ceil(total / limit) : 1;
    return {
      data: {
        data: rows,
        pagination: { page: page || 1, limit: limit || 50, total: total || 0, pages },
      },
    };
  },

  // Get trade statistics
  getStats: async (params = {}) => {
    const mapped = {
      exchg: params.exchange,
      tran_type: params.tradeType,
      ws_client_id: params.customerId,
      security_code: params.stockSymbol, // Changed from security_name to security_code
      trandate_from: params.startDate,
      trandate_to: params.endDate,
    };
    const res = await api.get('/api/stocks/stats/summary', { params: mapped });
    // Return axios-like response so consumers can access res.data directly
    return res;
  },

  // Get trades by stock symbol
  getTradesByStock: (symbol) => {
    return api.get(`/api/stocks`, { params: { q: symbol } });
  },

  // Get unique stock symbols
  getStocks: async () => {
    const res = await api.get('/api/stocks/meta/symbols');
    return { data: { data: res.data || [] } };
  },

  // Get unique exchanges
  getExchanges: async () => {
    const res = await api.get('/api/stocks/meta/exchanges');
    return { data: { data: res.data || [] } };
  },

  // Get unique transaction types
  getTransactionTypes: async () => {
    const res = await api.get('/api/stocks/meta/transaction-types');
    return { data: { data: res.data || [] } };
  },

  // Get unique client IDs
  getClientIds: async () => {
    // Add cache-busting timestamp to ensure fresh data
    const timestamp = new Date().getTime();
    const res = await api.get('/api/stocks/meta/client-ids', {
      params: { _t: timestamp },
      headers: {
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        'Pragma': 'no-cache',
        'Expires': '0'
      }
    });
    // Backend returns array directly, not wrapped
    const clientIds = Array.isArray(res.data) ? res.data : [];
    console.log(`[API] getClientIds returned ${clientIds.length} client IDs`);
    return { data: { data: clientIds } };
  },

  // Get unique account codes
  getAccountCodes: async () => {
    const timestamp = new Date().getTime();
    const res = await api.get('/api/stocks/meta/account-codes', {
      params: { _t: timestamp },
      headers: {
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        'Pragma': 'no-cache',
        'Expires': '0'
      }
    });
    const accountCodes = Array.isArray(res.data) ? res.data : [];
    console.log(`[API] getAccountCodes returned ${accountCodes.length} account codes`);
    return { data: { data: accountCodes } };
  },

  // Get Client ID from Account Code
  getClientIdByAccountCode: async (accountCode) => {
    if (!accountCode) {
      console.warn('[API] getClientIdByAccountCode called without accountCode');
      return { data: { clientId: null } };
    }
    try {
      const res = await api.get('/api/stocks/meta/client-id-by-account-code', {
        params: { accountCode },
        headers: {
          'Cache-Control': 'no-cache, no-store, must-revalidate',
          'Pragma': 'no-cache',
          'Expires': '0'
        }
      });
      console.log(`[API] getClientIdByAccountCode returned clientId: ${res.data?.clientId} for accountCode: ${accountCode}`);
      return res;
    } catch (error) {
      console.error(`[API] Error getting clientId for accountCode ${accountCode}:`, error);
      throw error;
    }
  },

  // Get unique stocks for a specific client ID
  getStocksByClientId: async (clientId) => {
    if (!clientId) {
      console.warn('[API] getStocksByClientId called without clientId');
      return { data: { data: [] } };
    }
    // Add cache-busting timestamp to ensure fresh data
    const timestamp = new Date().getTime();
    const res = await api.get('/api/stocks/meta/stocks-by-client', {
      params: { 
        clientId: clientId,
        _t: timestamp 
      },
      headers: {
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        'Pragma': 'no-cache',
        'Expires': '0'
      }
    });
    // Backend returns array directly, not wrapped
    const stocks = Array.isArray(res.data) ? res.data : [];
    console.log(`[API] getStocksByClientId returned ${stocks.length} stocks for client ${clientId}`);
    return { data: { data: stocks } };
  },

  // Get clients that hold a given security (with current qty)
  getClientsBySecurity: async ({ securityName, securityCode }) => {
    const res = await api.get('/api/stocks/meta/clients-by-security', {
      params: {
        securityName,
        securityCode,
      },
    });
    const data = Array.isArray(res.data) ? res.data : [];
    return { data: { data } };
  },

  // Import trades from Excel file
  importExcel: (formData) => {
    return axios.post(`${API_ROOT}/api/import/excel`, formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
      timeout: 1800000, // 30 minutes timeout for very large files
    });
  },

  // Import trades from CSV file (streaming, chunked processing)
  importCSV: (formData) => {
    return axios.post(`${API_ROOT}/api/import/csv`, formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
      timeout: 1800000, // 30 minutes timeout for very large files
    });
  },

  // Import bonus from Excel file
  importBonus: (formData) => {
    return axios.post(`${API_ROOT}/api/import/bonus`, formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
      timeout: 1800000, // 30 minutes timeout for very large files
    });
  },

  // Seed bonus data from Stocks-bouns.txt file
  seedBonus: async () => {
    return axios.post(`${API_ROOT}/api/import/seed/bonus`, {}, {
      headers: {
        'Content-Type': 'application/json',
      },
      timeout: 300000, // 5 minutes timeout
    });
  },

  // Get import progress
  getImportProgress: (importId) => {
    // Must call the /api route with absolute base, not relative, to avoid 404s
    return axios.get(`${API_ROOT}/api/import/progress/${importId}`);
  },

  // Delete all trades
  deleteAll: () => {
    return api.delete('/trades/all');
  },

  // Get all clients with cumulative holdings
  getClientsWithCumulativeHoldings: async (endDate) => {
    const params = {};
    if (endDate) params.endDate = endDate;
    const res = await api.get('/api/stocks/holdings/clients-cumulative', { params });
    return { data: { data: res.data || [] } };
  },

  // Get holdings summary for a client (stock-wise holdings)
  getHoldingsSummary: async (clientId, endDate) => {
    const params = { clientId };
    if (endDate) params.endDate = endDate;
    const res = await api.get('/api/stocks/holdings/summary', { params });
    return { data: { data: res.data || [] } };
  },

  // Get transaction history for a specific stock
  getStockTransactionHistory: async (clientId, stockName, endDate, stockCode) => {
    const params = { clientId };
    if (endDate) params.endDate = endDate;
    if (stockCode) params.stockCode = stockCode; // Pass stockCode to filter by Security_code
    const encodedStockName = encodeURIComponent(stockName);
    const res = await api.get(`/api/stocks/holdings/${encodedStockName}/transactions`, { params });
    // Backend returns array directly
    const transactions = Array.isArray(res.data) ? res.data : [];
    return { data: { data: transactions } };
  },
};

export default api;

