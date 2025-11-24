import axios from 'axios';

// Backend root provided by user (no trailing slash to avoid double slashes)
const API_ROOT = 'https://backend-10110149335.development.catalystappsail.com';
// const API_ROOT = 'http://localhost:3001';

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
        depositoryRegistrar: r['Depositoy/Registrar'] ?? r.Depository_Registrar ?? r.depository_registrar,
        dpidAmc: r['DPID/AMC'] ?? r.dpid_amc,
        dpClientIdFolio: r['Dp Client id/Folio'] ?? r.dp_client_id_folio,
        // Bank Information
        bankCode: r.BANKCODE ?? r.bankcode,
        bankAcid: r.BANKACID ?? r.bankacid,
        // Trade Details
        qty: r.QTY ?? r.qty,
        rate: r.RATE ?? r.rate,
        brokerage: r.BROKERAGE ?? r.brokerage,
        serviceTax: r.SERVICETAX ?? r.servicetax,
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
        portfolioId: r.PORTFOLIOID ?? r.portfolioid,
        deliveryDate: r.DELIVERYDATE ?? r.deliverydate,
        paymentDate: r.PAYMENTDATE ?? r.paymentdate,
        accruedInterest: r.ACCRUEDINTEREST ?? r.accruedinterest,
        // Issuer Information
        issuer: r.ISSUER ?? r.issuer,
        issuerName: r.ISSUERNAME ?? r.issuername,
        tdsAmount: r.TDSAMOUNT ?? r.tdsamount,
        stampDuty: r.STAMPDUTY ?? r.stampduty,
        tpmsgain: r.TPMSGAIN ?? r.tpmsgain,
        // Relationship Manager
        rmid: r.RMID ?? r.rmid,
        rmname: r.RMNAME ?? r.rmname,
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
        schemeId: r.SCHEMEID ?? r.schemeid,
        schemeName: r.SCHEMENAME ?? r.schemename,
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
    // Backend already returns the structure needed by Charts and cards
    return { data: { data: res.data } };
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

  // Get import progress
  getImportProgress: (importId) => {
    // Must call the /api route with absolute base, not relative, to avoid 404s
    return axios.get(`${API_ROOT}/api/import/progress/${importId}`);
  },

  // Delete all trades
  deleteAll: () => {
    return api.delete('/trades/all');
  },

  // Get holdings summary for a client (stock-wise holdings)
  getHoldingsSummary: async (clientId, endDate) => {
    const params = { clientId };
    if (endDate) params.endDate = endDate;
    const res = await api.get('/api/stocks/holdings/summary', { params });
    return { data: { data: res.data || [] } };
  },

  // Get transaction history for a specific stock
  getStockTransactionHistory: async (clientId, stockName, endDate) => {
    const params = { clientId };
    if (endDate) params.endDate = endDate;
    const encodedStockName = encodeURIComponent(stockName);
    const res = await api.get(`/api/stocks/holdings/${encodedStockName}/transactions`, { params });
    // Backend returns array directly
    const transactions = Array.isArray(res.data) ? res.data : [];
    return { data: { data: transactions } };
  },
};

export default api;

