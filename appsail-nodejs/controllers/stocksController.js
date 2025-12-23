"use strict";

const DEFAULT_TABLE = "Transaction";

function sanitizeIdentifier(identifier) {
  return String(identifier).replace(/[^a-zA-Z0-9_]/g, "");
}

// Helper functions for ZCQL response handling (shared across all functions)
const toNumber = (value) => {
  if (value === null || value === undefined || value === "") {
    return 0;
  }
  if (typeof value === "string") {
    const cleaned = value.replace(/,/g, "").trim();
    if (cleaned === "") return 0;
    const parsed = Number(cleaned);
    return Number.isNaN(parsed) ? 0 : parsed;
  }
  const parsed = Number(value);
  return Number.isNaN(parsed) ? 0 : parsed;
};

const flattenRow = (row) => {
  if (!row) return {};
  const flat = {};
  const stack = [row];
  while (stack.length) {
    const current = stack.pop();
    if (!current || typeof current !== "object") continue;
    Object.entries(current).forEach(([key, value]) => {
      if (value && typeof value === "object" && !Array.isArray(value)) {
        stack.push(value);
      } else {
        flat[key] = value;
      }
    });
  }
  return flat;
};

const pickValue = (row, keys = []) => {
  if (!row) return undefined;
  for (const key of keys) {
    if (row[key] !== undefined) {
      return row[key];
    }
    const lowerKey = key.toLowerCase();
    const match = Object.keys(row).find((k) => k.toLowerCase() === lowerKey);
    if (match && row[match] !== undefined) {
      return row[match];
    }
  }
  return undefined;
};

const pickNumber = (row, keys = [], fuzzyKeys = []) => {
  if (!row) return 0;
  const direct = pickValue(row, keys);
  if (direct !== undefined) {
    return toNumber(direct);
  }
  for (const fuzzy of fuzzyKeys) {
    const match = Object.keys(row).find((k) =>
      k.toLowerCase().includes(fuzzy.toLowerCase())
    );
    if (match && row[match] !== undefined) {
      return toNumber(row[match]);
    }
  }
  return 0;
};

/* =====================================================
   ✅ SINGLE FIFO ENGINE — SOURCE OF TRUTH
   ===================================================== */

function fifoProcess(transactions) {
  if (!Array.isArray(transactions) || transactions.length === 0) {
    return {
      holdingQty: 0,
      remainingCost: 0,
      avgCost: 0,
      profit: 0,
      buyQty: 0,
      sellQty: 0,
      buyValue: 0,
      sellValue: 0,
    };
  }

  // Make sure transactions are in chronological order
  const txns = [...transactions].sort((a, b) => {
    const d1 = a.trandate ? new Date(a.trandate).getTime() : 0;
    const d2 = b.trandate ? new Date(b.trandate).getTime() : 0;
    if (d1 !== d2) return d1 - d2;
    return (a.rowid || 0) - (b.rowid || 0);
  });

  const buyQueue = [];

  let buyQty = 0;
  let sellQty = 0;
  let buyValue = 0;
  let sellValue = 0;
  let sellCost = 0;
  let availableQty = 0; // total quantity currently in buyQueue

  for (const t of txns) {
    const type = String(t.tranType || "")
      .toUpperCase()
      .trim();
    let qty = Number(t.qty) || 0;
    let value = Number(t.netAmount) || 0;
    
    // Skip transactions with zero quantity
    if (qty === 0) continue;
    
    // Determine if this is a buy or sell transaction
    // Priority: Transaction type > Quantity sign
    let isBuy = false;
    let isSell = false;
    
    if (type.startsWith("B")) {
      isBuy = true;
    } else if (type.startsWith("S")) {
      isSell = true;
    } else if (qty < 0) {
      // Negative quantity typically means sell
      isSell = true;
    } else if (qty > 0) {
      // Positive quantity with unknown type, assume buy
      isBuy = true;
    }
    
    // Skip if we can't determine transaction type
    if (!isBuy && !isSell) {
      console.warn(`[fifoProcess] Unknown transaction type: ${type}, qty: ${qty}, skipping`);
      continue;
    }
    
    // Use absolute values for calculations
    const absQty = Math.abs(qty);
    const absValue = Math.abs(value);

    /* ===== BUY ===== */
    if (isBuy && !isSell) {
      const unitCost = absQty !== 0 ? absValue / absQty : 0;
      buyQueue.push({ qty: absQty, unitCost });
      buyQty += absQty;
      buyValue += absValue;
      availableQty += absQty;
    } 
    /* ===== SELL ===== */
    else if (isSell) {
      sellQty += absQty;
      sellValue += absValue;

      let remainingToMatch = Math.min(absQty, availableQty); // we can only sell what we have
      let unmatchedExtra = absQty - remainingToMatch; // this part is assumed from opening balance (outside dataset)

      // Consume from FIFO queue
      while (remainingToMatch > 0 && buyQueue.length) {
        const lot = buyQueue[0];
        const consume = Math.min(lot.qty, remainingToMatch);

        sellCost += consume * lot.unitCost;
        lot.qty -= consume;
        remainingToMatch -= consume;
        availableQty -= consume;

        if (lot.qty === 0) buyQueue.shift();
      }

      // unmatchedExtra: sold from outside holdings → ignore for cost & holdings
      if (unmatchedExtra > 0) {
        console.warn(
          `[fifoProcess] Sell ${absQty} but only ${
            absQty - unmatchedExtra
          } available. Treating extra ${unmatchedExtra} as opening balance sell.`
        );
      }
    }
  }

  const holdingQty = buyQueue.reduce((sum, l) => sum + l.qty, 0);
  const remainingCost = buyQueue.reduce(
    (sum, l) => sum + l.qty * l.unitCost,
    0
  );

  return {
    holdingQty,
    remainingCost,
    avgCost: holdingQty > 0 ? remainingCost / holdingQty : 0,
    profit: sellValue - sellCost,
    buyQty,
    sellQty,
    buyValue,
    sellValue,
  };
}

function buildWhereClause(filters, params, tableName = DEFAULT_TABLE) {
  const conditions = [];

  // Map allowed filters: query param -> column name
  const filterMap = {
    Ws_client_id: "WS_client_id",
    ws_account_code: "WS_Account_code",
    trandate_from: "TRANDATE",
    trandate_to: "TRANDATE",
    setdate_from: "SETDATE",
    setdate_to: "SETDATE",
    tran_type: "Tran_Type",
    tran_desc: "Tran_Desc",
    security_type: "Security_Type",
    security_type_description: "Security_Type_Description",
    detailtypename: "DETAILTYPENAME",
    isin: "ISIN",
    security_code: "Security_code",
    security_name: "Security_Name",
    exchg: "EXCHG",
    brokercode: "BROKERCODE",
    portfolioid: "PORTFOLIOID",
    branchid: "BRANCHID",
    ownerid: "OWNERID",
    advisorid: "ADVISORID",
    groupid: "GROUPID",
  };

  // Equality filters
  Object.entries(filterMap).forEach(([key, column]) => {
    if (filters[key] && !key.endsWith("_from") && !key.endsWith("_to")) {
      // For client ID, use table prefix and direct value (not parameterized) to match console format
      if (key === "ws_client_id") {
        const clientIdValue = String(filters[key]).trim();
        // Use table prefix format: Transaction.WS_client_id = 8800001 (direct value, not parameterized)
        // This matches the working console query format
        if (/^\d+$/.test(clientIdValue)) {
          const numValue = parseInt(clientIdValue, 10);
          // Use direct value insertion for numeric client IDs (matches console format)
          conditions.push(`${tableName}.${column} = ${numValue}`);
          // Don't add to params since we're using direct value
        } else {
          // For non-numeric, use parameterized
          conditions.push(`${tableName}.${column} = ?`);
          params.push(clientIdValue);
        }
      } else if (key === "security_name") {
        // For Security_Name, embed directly in query with single quotes (like dates)
        // ZCQL requires: Security_Name='Stock Name' (not parameterized)
        // Based on working query: SELECT * from Transaction where Transaction.WS_client_id=8800001 AND Security_Name='Shree Cement Limited'
        const stockName = String(filters[key]).trim();
        // Escape single quotes in stock name by doubling them
        const escapedStockName = stockName.replace(/'/g, "''");
        conditions.push(`${column} = '${escapedStockName}'`);
        // Don't add to params since we're embedding directly
        console.log(
          `[buildWhereClause] Stock filter (security_name): "${stockName}"`
        );
      } else if (key === "security_code") {
        // For Security_code, embed directly in query with single quotes (like dates)
        // ZCQL requires: Security_code='STOCKCODE' (not parameterized)
        const stockCode = String(filters[key]).trim();
        // Escape single quotes in stock code by doubling them
        const escapedStockCode = stockCode.replace(/'/g, "''");
        conditions.push(`${column} = '${escapedStockCode}'`);
        // Don't add to params since we're embedding directly
        console.log(
          `[buildWhereClause] Stock filter (security_code): "${stockCode}"`
        );
      } else if (key === "exchg") {
        // For EXCHG, embed directly in query with single quotes (like dates)
        // ZCQL requires: EXCHG='NSE' (not parameterized)
        const exchange = String(filters[key]).trim();
        // Escape single quotes in exchange by doubling them
        const escapedExchange = exchange.replace(/'/g, "''");
        conditions.push(`${column} = '${escapedExchange}'`);
        // Don't add to params since we're embedding directly
        console.log(
          `[buildWhereClause] Exchange filter (exchg): "${exchange}"`
        );
      } else {
        conditions.push(`${column} = ?`);
        params.push(filters[key]);
      }
    }
  });

  // Date range filters (ZCQL v2 requires dates in single quotes, not as parameters)
  // Format: 'YYYY-MM-DD' - embed directly in query string for ZCQL v2 compatibility
  if (filters.trandate_from) {
    // Validate and sanitize date format (YYYY-MM-DD)
    const dateFrom = String(filters.trandate_from).trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(dateFrom)) {
      // Embed date directly in query with single quotes (ZCQL v2 requirement)
      conditions.push(`TRANDATE >= '${dateFrom}'`);
      console.log(
        `[buildWhereClause] Date filter (trandate_from): ${dateFrom}`
      );
    } else {
      console.warn(
        `[buildWhereClause] Invalid date format for trandate_from: ${dateFrom}`
      );
    }
  }
  if (filters.trandate_to) {
    // Date filter: show all trades up to and including the selected date
    // ZCQL v2 requires date values in single quotes: 'YYYY-MM-DD'
    const dateTo = String(filters.trandate_to).trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(dateTo)) {
      // Embed date directly in query with single quotes (ZCQL v2 requirement)
      conditions.push(`TRANDATE <= '${dateTo}'`);
      console.log(`[buildWhereClause] Date filter (trandate_to): ${dateTo}`);
    } else {
      console.warn(
        `[buildWhereClause] Invalid date format for trandate_to: ${dateTo}`
      );
    }
  }
  if (filters.setdate_from) {
    const dateFrom = String(filters.setdate_from).trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(dateFrom)) {
      conditions.push(`SETDATE >= '${dateFrom}'`);
    } else {
      console.warn(
        `[buildWhereClause] Invalid date format for setdate_from: ${dateFrom}`
      );
    }
  }
  if (filters.setdate_to) {
    const dateTo = String(filters.setdate_to).trim();
    if (/^\d{4}-\d{2}-\d{2}$/.test(dateTo)) {
      conditions.push(`SETDATE <= '${dateTo}'`);
    } else {
      console.warn(
        `[buildWhereClause] Invalid date format for setdate_to: ${dateTo}`
      );
    }
  }

  // Free-text search on Security_Name
  if (filters.q) {
    conditions.push(`(Security_Name LIKE ? OR Security_code LIKE ?)`);
    const like = `%${filters.q}%`;
    params.push(like, like);
  }

  if (conditions.length === 0) {
    return "";
  }
  return " WHERE " + conditions.join(" AND ");
}

// 1.
exports.listStocks = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({ message: "Catalyst app context missing" });
    }

    const tableName = sanitizeIdentifier(req.query.table || DEFAULT_TABLE);

    const page = Math.max(parseInt(req.query.page || "1", 10), 1);
    const limit = Math.min(
      Math.max(parseInt(req.query.limit || "50", 10), 1),
      200
    );
    const offset = (page - 1) * limit;

    const params = [];
    const where = buildWhereClause(req.query, params, tableName);

    // Order by TRANDATE desc by default (use table prefix)
    const orderBy = ` ORDER BY ${tableName}.TRANDATE DESC`;

    const query = `select * from ${tableName}${where}${orderBy} limit ${limit} offset ${offset}`;

    // Log query details for debugging (client ID, date, and stock filters)
    if (
      req.query.ws_client_id ||
      req.query.trandate_to ||
      req.query.security_name
    ) {
      console.log(`[listStocks] ===== QUERY DEBUG =====`);
      if (req.query.ws_client_id) {
        console.log(
          `[listStocks] Client ID from request: "${req.query.ws_client_id}"`
        );
      }
      if (req.query.trandate_to) {
        console.log(
          `[listStocks] Date filter (trandate_to): "${req.query.trandate_to}"`
        );
      }
      if (req.query.security_name) {
        console.log(
          `[listStocks] Stock filter (security_name): "${req.query.security_name}"`
        );
      }
      console.log(`[listStocks] WHERE clause: ${where}`);
      console.log(`[listStocks] Params:`, params);
      console.log(`[listStocks] Full query: ${query}`);

      // Test the exact query format that works in console
      try {
        const testZcql = app.zcql();
        const clientIdValue = String(req.query.ws_client_id).trim();

        // Test with exact format from console: Transaction.WS_client_id = number
        if (/^\d+$/.test(clientIdValue)) {
          const numValue = parseInt(clientIdValue, 10);
          const testQuery = `select * from ${tableName} where ${tableName}.WS_client_id = ${numValue} limit 5`;
          console.log(
            `[listStocks] Testing exact console format: ${testQuery}`
          );
          const testRows = await testZcql.executeZCQLQuery(testQuery, []);
          console.log(
            `[listStocks] Test query returned ${
              testRows ? testRows.length : 0
            } rows`
          );
          if (testRows && testRows.length > 0) {
            console.log(`[listStocks] First test row:`, testRows[0]);
          }
        }
      } catch (testErr) {
        console.error(`[listStocks] Test query error:`, testErr.message);
      }
      console.log(`[listStocks] =================================`);
    }

    const zcql = app.zcql();
    let rows;
    try {
      rows = await zcql.executeZCQLQuery(query, params);
    } catch (queryErr) {
      console.error(`[listStocks] Query execution error:`, queryErr);
      console.error(`[listStocks] Query that failed: ${query}`);
      console.error(`[listStocks] Params that failed:`, params);
      throw queryErr;
    }

    if (req.query.ws_client_id || req.query.trandate_to) {
      const filterDesc = req.query.ws_client_id
        ? `client ${req.query.ws_client_id}${
            req.query.trandate_to ? ` up to ${req.query.trandate_to}` : ""
          }`
        : `up to date ${req.query.trandate_to}`;
      console.log(
        `[listStocks] Main query returned ${
          rows ? rows.length : 0
        } rows for ${filterDesc}`
      );
      if (rows && rows.length > 0) {
        console.log(`[listStocks] First row sample:`, rows[0]);
      }
    }

    // Count query for total (optional; can be heavy on large tables)
    let total = null;
    try {
      const countQuery = `select count(${tableName}.ROWID) as total_count from ${tableName}${where}`;
      const countRows = await zcql.executeZCQLQuery(countQuery, params);
      if (countRows && countRows.length > 0) {
        // SDK returns objects wrapped under table alias sometimes; normalize
        const first = countRows[0];
        // Try multiple shapes
        const nested = first[tableName] || first["Transaction"];
        total =
          Number(first.total_count) ||
          Number(first["total_count"]) ||
          Number(first.count) ||
          Number(first["COUNT"]) ||
          Number(first["COUNT(ROWID)"]) ||
          (nested
            ? Number(nested.total_count) ||
              Number(nested["total_count"]) ||
              Number(nested.count) ||
              Number(nested["COUNT"]) ||
              Number(nested["COUNT(ROWID)"])
            : 0) ||
          0;
      }
    } catch (e) {
      // Ignore count failure, still return data
    }

    return res.status(200).json({
      page,
      limit,
      total,
      data: rows,
    });
  } catch (err) {
    return res.status(500).json({
      message: "Failed to fetch stocks",
      error: String(err && err.message ? err.message : err),
    });
  }
};

exports.getStockById = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({ message: "Catalyst app context missing" });
    }
    const tableName = sanitizeIdentifier(req.query.table || DEFAULT_TABLE);
    const rowId = req.params.id;

    const zcql = app.zcql();
    const query = `select * from ${tableName} where ROWID = ?`;
    const rows = await zcql.executeZCQLQuery(query, [rowId]);

    if (!rows || rows.length === 0) {
      return res.status(404).json({ message: "Not found" });
    }
    return res.status(200).json(rows[0]);
  } catch (err) {
    return res.status(500).json({
      message: "Failed to fetch stock",
      error: String(err && err.message ? err.message : err),
    });
  }
};

// 2.
exports.getStats = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({ message: "Catalyst app context missing" });
    }
    const tableName = sanitizeIdentifier(req.query.table || DEFAULT_TABLE);
    const zcql = app.zcql();

    // Reuse filter builder
    const params = [];
    const where = buildWhereClause(req.query, params, tableName);

    // Overall totals
    const totalsQ = `select count(${tableName}.ROWID) as total_trades, sum(${tableName}.Net_Amount) as total_net_amount from ${tableName}${where}`;
    let totalsRows;
    try {
      totalsRows = await zcql.executeZCQLQuery(totalsQ, params);
    } catch (err) {
      console.error("Totals query error:", err);
      throw new Error(`Totals query failed: ${err.message}`);
    }
    const totals = totalsRows && totalsRows[0] ? flattenRow(totalsRows[0]) : {};

    // Extract from wrapped result if needed
    const totalTrades = pickNumber(
      totals,
      [
        "total_trades",
        `${tableName}.total_trades`,
        "count",
        `${tableName}.count`,
        "count(ROWID)",
      ],
      ["count(rowid)"]
    );
    const totalNetAmount = pickNumber(
      totals,
      ["total_net_amount", `${tableName}.total_net_amount`],
      ["sum(net_amount)"]
    );

    // Buy / Sell counts - use GROUP BY to get counts by transaction type
    // Using the query: SELECT count(Transaction.Tran_Type), Transaction.Tran_Type from Transaction GROUP BY Transaction.Tran_Type
    let buyTrades = 0;
    let sellTrades = 0;

    try {
      const buySellQ = `select Tran_Type as tran_type, count(${tableName}.ROWID) as total from ${tableName}${where} group by Tran_Type`;
      const tranTypeRows = await zcql.executeZCQLQuery(buySellQ, params);
      (tranTypeRows || []).forEach((row) => {
        const flat = flattenRow(row);
        const typeRaw = pickValue(flat, ["tran_type", "Tran_Type"]) || "";
        const tranTypeUpper = String(typeRaw).toUpperCase().trim();
        const count = pickNumber(
          flat,
          ["total", "count", `${tableName}.count`, "count(ROWID)"],
          ["count(rowid)"]
        );
        if (!count || !tranTypeUpper) {
          return;
        }
        if (tranTypeUpper.startsWith("B")) {
          buyTrades += count;
        } else if (tranTypeUpper.startsWith("S")) {
          sellTrades += count;
        }
      });
    } catch (err) {
      console.error("[getStats] Buy/Sell query error:", err);
    }

    // Completed trades (payment date present)
    let completedTrades = 0;

    try {
      const completedWhere =
        where && where.trim()
          ? `${where} AND ${tableName}.PAYMENTDATE is not null`
          : ` WHERE ${tableName}.PAYMENTDATE is not null`;
      const completedQ = `select count(${tableName}.ROWID) as completed_count from ${tableName}${completedWhere}`;
      const completedRows = await zcql.executeZCQLQuery(completedQ, params);
      const flatCompleted =
        completedRows && completedRows[0] ? flattenRow(completedRows[0]) : {};
      completedTrades = pickNumber(
        flatCompleted,
        ["completed_count", "count", `${tableName}.count`, "count(ROWID)"],
        ["count(rowid)"]
      );
    } catch (err) {
      console.error("[getStats] Completed query error:", err);
    }

    // Top 10 stocks by value
    let topStocks = [];

    // TODO: Remove dummy data once real queries are working
    // Using dummy data for now to test frontend
    // topStocks = [
    // 	{ _id: 'Reliance Industries Ltd', tradeCount: 245, totalValue: 12500000, totalQuantity: 50000 },
    // 	{ _id: 'TCS Limited', tradeCount: 198, totalValue: 9800000, totalQuantity: 35000 },
    // 	{ _id: 'HDFC Bank', tradeCount: 187, totalValue: 8750000, totalQuantity: 42000 },
    // 	{ _id: 'Infosys Limited', tradeCount: 165, totalValue: 7200000, totalQuantity: 28000 },
    // 	{ _id: 'ICICI Bank', tradeCount: 152, totalValue: 6500000, totalQuantity: 38000 },
    // 	{ _id: 'Hindustan Unilever', tradeCount: 138, totalValue: 5800000, totalQuantity: 15000 },
    // 	{ _id: 'Bharti Airtel', tradeCount: 124, totalValue: 5200000, totalQuantity: 25000 },
    // 	{ _id: 'ITC Limited', tradeCount: 112, totalValue: 4800000, totalQuantity: 18000 },
    // 	{ _id: 'State Bank of India', tradeCount: 98, totalValue: 4200000, totalQuantity: 32000 },
    // 	{ _id: 'Bajaj Finance', tradeCount: 87, totalValue: 3800000, totalQuantity: 12000 }
    // ];
    // console.log(`[getStats] Using DUMMY DATA - Top Stocks: ${topStocks.length} stocks`);

    try {
      const topStocksQ = `select Security_Name as _id, count(${tableName}.ROWID) as tradeCount, sum(${tableName}.Net_Amount) as totalValue, sum(${tableName}.QTY) as totalQuantity from ${tableName}${where} group by Security_Name order by sum(${tableName}.Net_Amount) desc limit 10`;
      const topStocksRows = await zcql.executeZCQLQuery(topStocksQ, params);
      topStocks = (topStocksRows || [])
        .map((row) => {
          const flat = flattenRow(row);
          const stockName =
            pickValue(flat, [
              "_id",
              "Security_Name",
              `${tableName}.Security_Name`,
            ]) ||
            pickValue(flat, ["security_name"]) ||
            "Unknown";
          return {
            _id: stockName,
            tradeCount: pickNumber(
              flat,
              [
                "tradeCount",
                `${tableName}.tradeCount`,
                "count",
                `${tableName}.count`,
                "count(ROWID)",
              ],
              ["count(rowid)"]
            ),
            totalValue: pickNumber(
              flat,
              ["totalValue", `${tableName}.totalValue`],
              ["sum(net_amount)"]
            ),
            totalQuantity: pickNumber(
              flat,
              ["totalQuantity", `${tableName}.totalQuantity`],
              ["sum(qty)"]
            ),
          };
        })
        .filter((stock) => stock._id);
      console.log(`[getStats] Top Stocks fetched: ${topStocks.length} stocks`);
    } catch (err) {
      console.error("Top stocks query error:", err);
      // Return empty array
    }

    // Exchange distribution
    let exchangeStats = [];

    // TODO: Remove dummy data once real queries are working
    // Using dummy data for now to test frontend
    // exchangeStats = [
    // 	{ _id: 'NSE', count: 1250, totalValue: 45000000 },
    // 	{ _id: 'BSE', count: 890, totalValue: 32000000 },
    // 	{ _id: 'MCX', count: 145, totalValue: 8500000 }
    // ];
    // console.log(`[getStats] Using DUMMY DATA - Exchange Stats: ${exchangeStats.length} exchanges`);

    try {
      const exchgQ = `select EXCHG as _id, count(${tableName}.ROWID) as count, sum(${tableName}.Net_Amount) as totalValue from ${tableName}${where} group by EXCHG`;
      const exchgRows = await zcql.executeZCQLQuery(exchgQ, params);
      exchangeStats = (exchgRows || [])
        .map((row) => {
          const flat = flattenRow(row);
          const name =
            pickValue(flat, ["_id", "EXCHG", `${tableName}.EXCHG`]) ||
            pickValue(flat, ["exchg"]) ||
            "Unknown";
          return {
            _id: name,
            count: pickNumber(
              flat,
              ["count", `${tableName}.count`, "count(ROWID)"],
              ["count(rowid)"]
            ),
            totalValue: pickNumber(
              flat,
              ["totalValue", `${tableName}.totalValue`],
              ["sum(net_amount)"]
            ),
          };
        })
        .filter((item) => item._id);
      console.log(
        `[getStats] Exchange Stats fetched: ${exchangeStats.length} exchanges`
      );
    } catch (err) {
      console.error("Exchange stats query error:", err);
      // Return empty array
    }

    // Yearly volume - with buy/sell breakdown (grouped by year)
    let yearlyVolume = [];

    // TODO: Remove dummy data once real queries are working
    // Using dummy data for now to test frontend - generate last 30 days
    // const today = new Date();
    // dailyVolume = [];
    // for (let i = 29; i >= 0; i--) {
    // 	const date = new Date(today);
    // 	date.setDate(date.getDate() - i);
    // 	const dateStr = date.toISOString().split('T')[0]; // Format: YYYY-MM-DD

    // 	// Generate random but realistic data
    // 	const baseCount = 40 + Math.floor(Math.random() * 60); // 40-100 trades per day
    // 	const buyCount = Math.floor(baseCount * 0.6); // 60% buy trades
    // 	const sellCount = baseCount - buyCount; // 40% sell trades
    // 	const totalValue = baseCount * (50000 + Math.floor(Math.random() * 100000)); // Random value

    // 	dailyVolume.push({
    // 		_id: dateStr,
    // 		count: baseCount,
    // 		totalValue: totalValue,
    // 		buyTrades: buyCount,
    // 		sellTrades: sellCount
    // 	});
    // }
    // console.log(`[getStats] Using DUMMY DATA - Daily Volume: ${dailyVolume.length} days`);

    try {
      const currentYear = new Date().getFullYear();
      const years = [];
      for (let i = 0; i < 5; i++) {
        years.push(currentYear - i);
      }

      const yearlyVolumeWithBuySell = await Promise.all(
        years.map(async (year) => {
          const startDate = `${year}-01-01`;
          const endDate = `${year}-12-31`;
          const yearWhere =
            where && where.trim()
              ? `${where} AND ${tableName}.TRANDATE >= '${startDate}' AND ${tableName}.TRANDATE <= '${endDate}'`
              : ` WHERE ${tableName}.TRANDATE >= '${startDate}' AND ${tableName}.TRANDATE <= '${endDate}'`;

          const yearTotalQ = `select count(ROWID) as count, sum(Net_Amount) as totalValue from ${tableName}${yearWhere}`;
          const yearTotalRows = await zcql.executeZCQLQuery(yearTotalQ, params);
          const flattenedYearTotal =
            yearTotalRows && yearTotalRows[0]
              ? flattenRow(yearTotalRows[0])
              : {};
          const totalCount = pickNumber(
            flattenedYearTotal,
            ["count", `${tableName}.count`, "count(ROWID)"],
            ["count(rowid)"]
          );
          const totalValue = pickNumber(
            flattenedYearTotal,
            ["totalValue", `${tableName}.totalValue`],
            ["sum(net_amount)"]
          );

          let buyCount = 0;
          try {
            const buyWhere =
              yearWhere +
              ` AND (${tableName}.Tran_Type LIKE 'B%' OR ${tableName}.Tran_Type LIKE 'b%')`;
            const buyRows = await zcql.executeZCQLQuery(
              `select count(ROWID) as c from ${tableName}${buyWhere}`,
              params
            );
            const flatBuy = buyRows && buyRows[0] ? flattenRow(buyRows[0]) : {};
            buyCount = pickNumber(
              flatBuy,
              [
                "c",
                "count",
                `${tableName}.c`,
                `${tableName}.count`,
                "count(ROWID)",
              ],
              ["count(rowid)"]
            );
          } catch (err) {
            console.error(`Error getting buy count for year ${year}:`, err);
          }

          let sellCount = 0;
          try {
            const sellWhere =
              yearWhere +
              ` AND (${tableName}.Tran_Type LIKE 'S%' OR ${tableName}.Tran_Type LIKE 's%')`;
            const sellRows = await zcql.executeZCQLQuery(
              `select count(ROWID) as c from ${tableName}${sellWhere}`,
              params
            );
            const flatSell =
              sellRows && sellRows[0] ? flattenRow(sellRows[0]) : {};
            sellCount = pickNumber(
              flatSell,
              [
                "c",
                "count",
                `${tableName}.c`,
                `${tableName}.count`,
                "count(ROWID)",
              ],
              ["count(rowid)"]
            );
          } catch (err) {
            console.error(`Error getting sell count for year ${year}:`, err);
          }

          return {
            _id: String(year),
            count: totalCount,
            totalValue: totalValue,
            buyTrades: buyCount,
            sellTrades: sellCount,
          };
        })
      );

      yearlyVolume = yearlyVolumeWithBuySell.sort(
        (a, b) => Number(a._id) - Number(b._id)
      );
      console.log(
        `[getStats] Yearly Volume fetched: ${yearlyVolume.length} years`
      );
    } catch (err) {
      console.error("Yearly volume query error:", err);
      // Return empty array
    }
    const responseData = {
      overall: {
        totalTrades,
        totalNetAmount,
        // Return actual calculated average without rounding/truncation.
        avgTradeValue: totalTrades > 0 ? totalNetAmount / totalTrades : 0,
        buyTrades,
        sellTrades,
        completedTrades,
      },
      topStocks,
      exchangeStats,
      dailyVolume: yearlyVolume, // Using yearlyVolume for charts (frontend expects dailyVolume key)
    };

    console.log(`[getStats] ========== FINAL RESPONSE DATA ==========`);
    console.log(`[getStats] Total Trades: ${totalTrades}`);
    console.log(`[getStats] Total Net Amount: ${totalNetAmount}`);
    console.log(`[getStats] Buy Trades: ${buyTrades}`);
    console.log(`[getStats] Sell Trades: ${sellTrades}`);
    console.log(`[getStats] Completed Trades: ${completedTrades}`);
    console.log(
      `[getStats] Response JSON:`,
      JSON.stringify(responseData, null, 2)
    );
    console.log(`[getStats] =========================================`);

    return res.status(200).json(responseData);
  } catch (err) {
    console.error("Stats endpoint error:", err);
    return res.status(500).json({
      message: "Failed to fetch stats",
      error: String(err && err.message ? err.message : err),
      stack: err.stack,
    });
  }
};

// 4
exports.getExchanges = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({ message: "Catalyst app context missing" });
    }
    const tableName = sanitizeIdentifier(req.query.table || DEFAULT_TABLE);
    const zcql = app.zcql();
    const query = `select distinct EXCHG from ${tableName} where EXCHG is not null`;
    const rows = await zcql.executeZCQLQuery(query, []);
    // Handle different ZCQL result formats
    const data = rows
      .map((r) => {
        return (
          r.EXCHG ||
          r[`${tableName}.EXCHG`] ||
          (r[tableName] && r[tableName].EXCHG) ||
          (r.Transaction && r.Transaction.EXCHG)
        );
      })
      .filter(Boolean);
    return res.status(200).json(data);
  } catch (err) {
    return res.status(500).json({
      message: "Failed to fetch exchanges",
      error: String(err && err.message ? err.message : err),
    });
  }
};

exports.getTransactionTypes = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({ message: "Catalyst app context missing" });
    }
    const tableName = sanitizeIdentifier(req.query.table || DEFAULT_TABLE);
    const zcql = app.zcql();
    const query = `select distinct Tran_Type from ${tableName} where Tran_Type is not null`;
    const rows = await zcql.executeZCQLQuery(query, []);
    const data = rows.map((r) => r.Tran_Type).filter(Boolean);
    return res.status(200).json(data);
  } catch (err) {
    return res.status(500).json({
      message: "Failed to fetch transaction types",
      error: String(err && err.message ? err.message : err),
    });
  }
};
// 3.
exports.getClientIds = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({ message: "Catalyst app context missing" });
    }
    
    // Use clientIds table instead of Transaction table
    const tableName = "clientIds";
    const zcql = app.zcql();

    // Use pagination to fetch all client IDs (ZCQL may have default limit of 300)
    const batchSize = 250; // ZCQL max is 300, use 250 to be safe
    let offset = 0;
    let hasMore = true;
    const allClientIds = new Set(); // Use Set to automatically handle duplicates
    let batchNumber = 0;

    console.log(`[getClientIds] Starting pagination to fetch all client IDs from ${tableName} table...`);

    while (hasMore) {
      batchNumber++;
      // Query clientIds table - clientId column is int type
      const query = `SELECT * FROM ${tableName} WHERE ${tableName}.clientId IS NOT NULL ORDER BY ${tableName}.clientId LIMIT ${batchSize} OFFSET ${offset}`;
      
      try {
        const rows = await zcql.executeZCQLQuery(query, []);

        if (!rows || rows.length === 0) {
          console.log(`[getClientIds] No more rows at offset ${offset}`);
          hasMore = false;
          break;
        }

        // Extract client IDs from results and add to Set (automatically deduplicates)
        rows.forEach((row, index) => {
          // Handle different ZCQL result formats
          const r = row.clientIds || row[tableName] || row;
          const clientId = r.clientId || r.ClientId;
          
          // Debug first row of first batch to see structure
          if (batchNumber === 1 && index === 0) {
            console.log(`[getClientIds] Sample row structure:`, {
              hasClientIds: !!row.clientIds,
              hasTableName: !!row[tableName],
              directClientId: row.clientId,
              extractedClientId: clientId,
              rowKeys: Object.keys(row)
            });
          }
          
          if (clientId !== null && clientId !== undefined && clientId !== "") {
            // Convert to string and add to set (handles both int and string formats)
            allClientIds.add(String(clientId).trim());
          }
        });

        console.log(`[getClientIds] Batch ${batchNumber} at offset ${offset}: Fetched ${rows.length} rows, unique client IDs so far: ${allClientIds.size}`);

        if (rows.length < batchSize) {
          hasMore = false;
        } else {
          offset += batchSize;
          // Safety limit to prevent infinite loops
          if (offset > 100000) {
            console.warn(`[getClientIds] Reached safety limit of 100K rows, stopping`);
            hasMore = false;
          }
        }
      } catch (batchErr) {
        console.error(`[getClientIds] Batch ${batchNumber} error at offset ${offset}:`, batchErr);
        hasMore = false;
      }
    }

    // Convert Set to sorted array (numeric sort for client IDs)
    const uniqueData = Array.from(allClientIds)
      .map(id => parseInt(id, 10))
      .filter(id => !isNaN(id))
      .sort((a, b) => a - b)
      .map(id => String(id));

    console.log(`[getClientIds] Total unique client IDs found: ${uniqueData.length}`);
    
    return res.status(200).json(uniqueData);
  } catch (err) {
    console.error("[getClientIds] Error:", err);
    return res.status(500).json({
      message: "Failed to fetch client ids",
      error: String(err && err.message ? err.message : err),
    });
  }
};

// Get all Account Codes from clientIds table
exports.getAccountCodes = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({ message: "Catalyst app context missing" });
    }
    
    const tableName = "clientIds";
    const zcql = app.zcql();

    // Use pagination to fetch all account codes
    const batchSize = 250;
    let offset = 0;
    let hasMore = true;
    const allAccountCodes = new Set();
    let batchNumber = 0;
    let columnName = null; // Will be determined on first successful query

    console.log(`[getAccountCodes] Starting pagination to fetch all account codes from ${tableName} table...`);

    while (hasMore) {
      batchNumber++;
      
      // Determine column name on first batch if not already determined
      if (!columnName && offset === 0) {
        // Try different column name variations
        const columnVariations = ['ws_account_code', 'WS_Account_code', 'wsAccountCode'];
        let foundColumn = null;
        
        for (const colName of columnVariations) {
          try {
            const testQuery = `SELECT * FROM ${tableName} WHERE ${tableName}.${colName} IS NOT NULL LIMIT 1`;
            const testRows = await zcql.executeZCQLQuery(testQuery, []);
            if (testRows && testRows.length > 0) {
              // Check if column exists in result
              const testRow = testRows[0].clientIds || testRows[0];
              if (testRow[colName] !== undefined || testRow[colName.toLowerCase()] !== undefined || testRow[colName.toUpperCase()] !== undefined) {
                columnName = colName;
                console.log(`[getAccountCodes] Found column name: ${columnName}`);
                break;
              }
            }
          } catch (testErr) {
            console.log(`[getAccountCodes] Column ${colName} not found, trying next...`);
            continue;
          }
        }
        
        if (!columnName) {
          // If no column found, try selecting all and checking row structure
          try {
            const allQuery = `SELECT * FROM ${tableName} LIMIT 1`;
            const allRows = await zcql.executeZCQLQuery(allQuery, []);
            if (allRows && allRows.length > 0) {
              const testRow = allRows[0].clientIds || allRows[0];
              console.log(`[getAccountCodes] Sample row keys:`, Object.keys(testRow));
              // Try to find account code column
              for (const key of Object.keys(testRow)) {
                if (key.toLowerCase().includes('account') || key.toLowerCase().includes('code')) {
                  columnName = key;
                  console.log(`[getAccountCodes] Found account code column: ${columnName}`);
                  break;
                }
              }
            }
          } catch (allErr) {
            console.error(`[getAccountCodes] Error querying table structure:`, allErr);
          }
        }
        
        if (!columnName) {
          return res.status(500).json({ 
            message: "Could not find account code column in clientIds table",
            error: "Please check table schema. Expected columns: ws_account_code, WS_Account_code, or wsAccountCode"
          });
        }
      }
      
      // Build query with determined column name
      const query = `SELECT * FROM ${tableName} WHERE ${tableName}.${columnName} IS NOT NULL ORDER BY ${tableName}.${columnName} LIMIT ${batchSize} OFFSET ${offset}`;
      
      try {
        const rows = await zcql.executeZCQLQuery(query, []);

        if (!rows || rows.length === 0) {
          console.log(`[getAccountCodes] No more rows at offset ${offset}`);
          hasMore = false;
          break;
        }

        // Extract account codes from results
        rows.forEach((row, index) => {
          // Handle different ZCQL result formats
          const r = row.clientIds || row[tableName] || row;
          
          // Try multiple column name variations (case-sensitive)
          const accountCode = r.ws_account_code || 
                             r.WS_Account_code || 
                             r.wsAccountCode || 
                             r['WS_Account_code'] ||
                             r['ws_account_code'] ||
                             r['wsAccountCode'];
          
          // Debug first row of first batch
          if (batchNumber === 1 && index === 0) {
            console.log(`[getAccountCodes] Sample row structure:`, {
              hasClientIds: !!row.clientIds,
              hasTableName: !!row[tableName],
              rowKeys: Object.keys(row),
              rKeys: Object.keys(r),
              accountCode: accountCode
            });
          }
          
          if (accountCode !== null && accountCode !== undefined && accountCode !== "") {
            allAccountCodes.add(String(accountCode).trim());
          }
        });

        console.log(`[getAccountCodes] Batch ${batchNumber} at offset ${offset}: Fetched ${rows.length} rows, unique account codes so far: ${allAccountCodes.size}`);

        if (rows.length < batchSize) {
          hasMore = false;
        } else {
          offset += batchSize;
          if (offset > 100000) {
            console.warn(`[getAccountCodes] Reached safety limit of 100K rows, stopping`);
            hasMore = false;
          }
        }
      } catch (batchErr) {
        console.error(`[getAccountCodes] Batch ${batchNumber} error at offset ${offset}:`, batchErr);
        // If query fails, try alternative column name
        if (batchNumber === 1 && offset === 0) {
          try {
            console.log(`[getAccountCodes] Trying alternative column name WS_Account_code...`);
            const altQuery = `SELECT * FROM ${tableName} WHERE ${tableName}.WS_Account_code IS NOT NULL ORDER BY ${tableName}.WS_Account_code LIMIT ${batchSize} OFFSET ${offset}`;
            const altRows = await zcql.executeZCQLQuery(altQuery, []);
            if (altRows && altRows.length > 0) {
              // Use alternative query for remaining batches
              query = altQuery;
              // Process this batch
              altRows.forEach((row, index) => {
                const r = row.clientIds || row[tableName] || row;
                const accountCode = r.WS_Account_code || r.ws_account_code || r.wsAccountCode || r['WS_Account_code'] || r['ws_account_code'];
                if (accountCode !== null && accountCode !== undefined && accountCode !== "") {
                  allAccountCodes.add(String(accountCode).trim());
                }
              });
              console.log(`[getAccountCodes] Alternative query worked! Using WS_Account_code column.`);
              if (altRows.length < batchSize) {
                hasMore = false;
              } else {
                offset += batchSize;
              }
              continue;
            }
          } catch (altErr) {
            console.error(`[getAccountCodes] Alternative query also failed:`, altErr);
          }
        }
        hasMore = false;
      }
    }

    // Convert Set to sorted array
    const uniqueData = Array.from(allAccountCodes)
      .filter(code => code && code.trim() !== '')
      .sort();

    console.log(`[getAccountCodes] Total unique account codes found: ${uniqueData.length}`);
    
    return res.status(200).json(uniqueData);
  } catch (err) {
    console.error("[getAccountCodes] Error:", err);
    return res.status(500).json({
      message: "Failed to fetch account codes",
      error: String(err && err.message ? err.message : err),
    });
  }
};

// Get Client ID from Account Code
exports.getClientIdByAccountCode = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({ message: "Catalyst app context missing" });
    }
    
    const accountCode = req.query.accountCode || req.query.wsAccountCode;
    
    if (!accountCode) {
      return res.status(400).json({ message: "accountCode is required" });
    }

    const tableName = "clientIds";
    const zcql = app.zcql();

    // Query clientIds table to get Client ID for the given Account Code
    const query = `SELECT * FROM ${tableName} WHERE ${tableName}.ws_account_code = '${String(accountCode).trim().replace(/'/g, "''")}' LIMIT 1`;
    const rows = await zcql.executeZCQLQuery(query, []);
    
    if (!rows || rows.length === 0) {
      return res.status(404).json({ message: `No client found for accountCode: ${accountCode}` });
    }

    const clientRow = rows[0].clientIds || rows[0];
    const clientId = Number(clientRow.clientId || clientRow.ClientId || clientRow.client_id);
    
    if (!clientId || isNaN(clientId)) {
      return res.status(400).json({ message: `Invalid clientId for accountCode: ${accountCode}` });
    }

    console.log(`[getClientIdByAccountCode] Found clientId: ${clientId} for accountCode: ${accountCode}`);
    
    return res.status(200).json({ clientId: String(clientId), accountCode: String(accountCode) });
  } catch (err) {
    console.error("[getClientIdByAccountCode] Error:", err);
    return res.status(500).json({
      message: "Failed to fetch client id for account code",
      error: String(err && err.message ? err.message : err),
    });
  }
};

exports.getSymbols = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({ message: "Catalyst app context missing" });
    }
    const tableName = sanitizeIdentifier(req.query.table || DEFAULT_TABLE);
    const zcql = app.zcql();
    // Return both Security_Name and Security_code for full display
    const query = `select distinct Security_Name, Security_code from ${tableName} where Security_Name is not null order by Security_Name`;
    const rows = await zcql.executeZCQLQuery(query, []);
    // Handle different ZCQL result formats and return objects with both name and code
    const data = rows
      .map((r) => {
        const securityName =
          r.Security_Name ||
          r[`${tableName}.Security_Name`] ||
          (r[tableName] && r[tableName].Security_Name) ||
          (r.Transaction && r.Transaction.Security_Name) ||
          '';
        const securityCode =
          r.Security_code ||
          r[`${tableName}.Security_code`] ||
          (r[tableName] && r[tableName].Security_code) ||
          (r.Transaction && r.Transaction.Security_code) ||
          '';
        if (!securityName) return null;
        return {
          securityName: String(securityName).trim(),
          securityCode: String(securityCode).trim(),
        };
      })
      .filter(Boolean);
    return res.status(200).json(data);
  } catch (err) {
    return res.status(500).json({
      message: "Failed to fetch symbols",
      error: String(err && err.message ? err.message : err),
    });
  }
};

// Get unique stocks (Security_Name) for a specific client ID
exports.getStocksByClientId = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      console.error("[getStocksByClientId] Catalyst app context missing");
      return res.status(500).json({ message: "Catalyst app context missing" });
    }

    const clientId = req.query.clientId || req.query.ws_client_id;
    if (!clientId) {
      return res.status(400).json({ message: "Client ID is required" });
    }

    const tableName = sanitizeIdentifier(req.query.table || DEFAULT_TABLE);
    console.log(
      `[getStocksByClientId] Fetching stocks for client ID: ${clientId}`
    );

    const zcql = app.zcql();
    const allStocks = new Set();

    // Strategy: Fetch ALL rows for this client in batches, then deduplicate stock names
    const batchSize = 250; // ZCQL max is 300, use 250 to be safe
    let offset = 0;
    let hasMore = true;
    let totalRowsFetched = 0;
    let batchNumber = 0;

    // Validate client ID is numeric (for direct value insertion)
    const clientIdValue = String(clientId).trim();
    if (!/^\d+$/.test(clientIdValue)) {
      return res.status(400).json({ message: "Invalid client ID format" });
    }
    const numClientId = parseInt(clientIdValue, 10);

    console.log(
      `[getStocksByClientId] Starting pagination approach for client ${numClientId}...`
    );

    // Fetch all rows for this client in batches
    while (hasMore) {
      batchNumber++;

      // Query to get Security_Name for this client
      // Use direct value insertion for client ID (matches working format)
      const query = `select ${tableName}.Security_Name from ${tableName} where ${tableName}.WS_client_id = ${numClientId} and ${tableName}.Security_Name is not null order by Security_Name limit ${batchSize} offset ${offset}`;

      try {
        console.log(
          `[getStocksByClientId] Batch ${batchNumber}: Fetching rows with offset ${offset}...`
        );
        const rows = await zcql.executeZCQLQuery(query, []);

        if (!rows || rows.length === 0) {
          console.log(
            `[getStocksByClientId] Batch ${batchNumber}: No more rows at offset ${offset}`
          );
          hasMore = false;
          break;
        }

        totalRowsFetched += rows.length;

        // Extract stock names and add to Set (automatically deduplicates)
        let batchUniqueCount = 0;
        rows.forEach((row) => {
          // Handle different ZCQL result formats
          const stockName =
            row.Security_Name ||
            row[`${tableName}.Security_Name`] ||
            (row[tableName] && row[tableName].Security_Name);
          if (stockName && String(stockName).trim() !== "") {
            const trimmedStock = String(stockName).trim();
            const beforeSize = allStocks.size;
            allStocks.add(trimmedStock);
            if (allStocks.size > beforeSize) {
              batchUniqueCount++;
            }
          }
        });

        console.log(
          `[getStocksByClientId] Batch ${batchNumber}: Fetched ${rows.length} rows, ${batchUniqueCount} new unique stocks, total unique so far: ${allStocks.size}, total rows processed: ${totalRowsFetched}`
        );

        // If we got fewer rows than batchSize, we've reached the end
        if (rows.length < batchSize) {
          hasMore = false;
          console.log(
            `[getStocksByClientId] Batch ${batchNumber}: Reached end of data (got ${rows.length} < ${batchSize} rows)`
          );
        } else {
          offset += batchSize;
          // Safety limit
          if (offset > 1000000) {
            console.warn(
              `[getStocksByClientId] Reached safety limit of 1M rows, stopping`
            );
            hasMore = false;
          }
        }
      } catch (batchErr) {
        console.error(
          `[getStocksByClientId] Batch ${batchNumber} error at offset ${offset}:`,
          batchErr.message
        );

        // If OFFSET fails on first batch, try without OFFSET
        if (offset === 0) {
          console.log(
            `[getStocksByClientId] First batch with OFFSET failed, trying without OFFSET...`
          );
          try {
            const simpleQuery = `select ${tableName}.Security_Name from ${tableName} where ${tableName}.WS_client_id = ${numClientId} and Security_Name is not null limit ${batchSize}`;
            const simpleRows = await zcql.executeZCQLQuery(simpleQuery, []);

            if (simpleRows && simpleRows.length > 0) {
              totalRowsFetched += simpleRows.length;
              let batchUniqueCount = 0;
              simpleRows.forEach((row) => {
                const stockName =
                  row.Security_Name ||
                  row[`${tableName}.Security_Name`] ||
                  (row[tableName] && row[tableName].Security_Name);
                if (stockName && String(stockName).trim() !== "") {
                  const trimmedStock = String(stockName).trim();
                  const beforeSize = allStocks.size;
                  allStocks.add(trimmedStock);
                  if (allStocks.size > beforeSize) {
                    batchUniqueCount++;
                  }
                }
              });
              console.log(
                `[getStocksByClientId] Simple query (no OFFSET) returned ${simpleRows.length} rows, ${batchUniqueCount} new unique stocks, total unique: ${allStocks.size}`
              );
            }
          } catch (simpleErr) {
            console.error(
              `[getStocksByClientId] Simple query also failed:`,
              simpleErr.message
            );
          }
        }
        hasMore = false;
      }
    }

    // Convert Set to sorted array
    const uniqueStocks = Array.from(allStocks).sort();

    console.log(`[getStocksByClientId] ===== FINAL RESULT =====`);
    console.log(
      `[getStocksByClientId] Total batches processed: ${batchNumber}`
    );
    console.log(
      `[getStocksByClientId] Total rows fetched: ${totalRowsFetched}`
    );
    console.log(
      `[getStocksByClientId] Total unique stocks for client ${numClientId}: ${uniqueStocks.length}`
    );
    console.log(
      `[getStocksByClientId] First 5 stocks:`,
      uniqueStocks.slice(0, 5)
    );
    console.log(`[getStocksByClientId] ========================`);

    return res.status(200).json(uniqueStocks);
  } catch (err) {
    console.error("[getStocksByClientId] Fatal error:", err);
    console.error("[getStocksByClientId] Error details:", err.message);
    console.error("[getStocksByClientId] Error stack:", err.stack);
    return res.status(500).json({
      message: "Failed to fetch stocks for client",
      error: String(err && err.message ? err.message : err),
      details: err.toString(),
    });
  }
};

// Get clients (with current holdings) for a specific security
exports.getClientsBySecurityHoldings = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({ message: "Catalyst app context missing" });
    }

    const securityNameRaw = req.query.securityName;
    const securityCodeRaw = req.query.securityCode;

    if (!securityNameRaw && !securityCodeRaw) {
      return res.status(400).json({ message: "securityName or securityCode is required" });
    }

    const tableName = sanitizeIdentifier(req.query.table || DEFAULT_TABLE);
    const zcql = app.zcql();

    const securityName = securityNameRaw ? String(securityNameRaw).trim() : null;
    const securityCode = securityCodeRaw ? String(securityCodeRaw).trim() : null;

    // Helpers for transaction classification
    const isBuyTransaction = (tranType) => {
      if (!tranType) return false;
      const type = String(tranType).toUpperCase().trim();
      const isBuy = type.startsWith('B') || type === 'BUY' || type === 'PURCHASE' || type.includes('BUY');
      const isSQB = type === 'SQB';
      const isOPI = type === 'OPI';
      const isDividend =
        type === 'DIO' ||
        type === 'DIVIDEND' ||
        type === 'DIVIDEND REINVEST' ||
        type === 'DIVIDEND REINVESTMENT' ||
        type === 'DIVIDEND RECEIVED' ||
        type.startsWith('DIVIDEND') ||
        type.includes('DIVIDEND');
      return (isBuy || isSQB || isOPI) && !isDividend;
    };

    const isSellTransaction = (tranType) => {
      if (!tranType) return false;
      const type = String(tranType).toUpperCase().trim();
      const isSell = type.startsWith('S') || type === 'SELL' || type === 'SALE' || type.includes('SELL');
      const isSQS = type === 'SQS';
      const isOPO = type === 'OPO';
      const isNF = type === 'NF-' || type.startsWith('NF-');
      return isSell || isSQS || isOPO || isNF;
    };

    // Build WHERE clause
    let where = `WHERE ${tableName}.Security_Name IS NOT NULL`;
    if (securityName) {
      const esc = securityName.replace(/'/g, "''");
      where += ` AND ${tableName}.Security_Name = '${esc}'`;
    }
    if (securityCode) {
      const esc = securityCode.replace(/'/g, "''");
      where += ` AND ${tableName}.Security_code = '${esc}'`;
    }

    const batchSize = 250;
    let offset = 0;
    let hasMore = true;
    const clientMap = new Map(); // clientId -> {clientId, securityName, securityCode, buyQty, sellQty}

    while (hasMore) {
      const query = `SELECT WS_client_id, Security_Name, Security_code, Tran_Type, QTY FROM ${tableName} ${where} ORDER BY ${tableName}.TRANDATE ASC, ${tableName}.ROWID ASC LIMIT ${batchSize} OFFSET ${offset}`;
      const rows = await zcql.executeZCQLQuery(query, []);

      if (!rows || rows.length === 0) {
        hasMore = false;
        break;
      }

      rows.forEach((row) => {
        const r = row.Transaction || row[tableName] || row;
        const clientId = r.WS_client_id ?? r.ws_client_id;
        if (!clientId) return;
        const qty = Math.abs(toNumber(r.QTY));
        const tranType = r.Tran_Type || r.tran_type;

        const existing = clientMap.get(clientId) || {
          clientId,
          securityName: r.Security_Name || r.security_name || securityName || '',
          securityCode: r.Security_code || r.security_code || securityCode || '',
          buyQty: 0,
          sellQty: 0,
        };

        if (isBuyTransaction(tranType)) {
          existing.buyQty += qty;
        } else if (isSellTransaction(tranType)) {
          existing.sellQty += qty;
        }

        clientMap.set(clientId, existing);
      });

      if (rows.length < batchSize) {
        hasMore = false;
      } else {
        offset += batchSize;
      }
    }

    const result = Array.from(clientMap.values())
      .map((entry) => ({
        clientId: entry.clientId,
        securityName: entry.securityName,
        securityCode: entry.securityCode,
        currentQty: entry.buyQty - entry.sellQty,
      }))
      .filter((entry) => entry.currentQty > 0);

    return res.status(200).json(result);
  } catch (err) {
    console.error("[getClientsBySecurityHoldings] Error:", err);
    return res.status(500).json({
      message: "Failed to fetch clients by security",
      error: String(err && err.message ? err.message : err),
    });
  }
};

// Get holdings summary for a client (stock-wise holdings with calculations)
// OPTIMIZED: Uses aggregation queries instead of fetching all transactions
// Get holdings summary for a client (stock-wise holdings with FIFO cost)
exports.getHoldingsSummary = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({ message: "Catalyst app context missing" });
    }

    // Accept both ?clientId= and ?ws_client_id=
    const clientIdRaw = req.query.clientId || req.query.ws_client_id;
    if (!clientIdRaw) {
      return res.status(400).json({ message: "Client ID required" });
    }

    const clientIdStr = String(clientIdRaw).trim();
    if (!/^\d+$/.test(clientIdStr)) {
      return res.status(400).json({ message: "Invalid client ID format" });
    }
    const clientId = parseInt(clientIdStr, 10);

    const tableName = sanitizeIdentifier(req.query.table || DEFAULT_TABLE);
    const zcql = app.zcql();

    // Build base WHERE
    let where = ` WHERE ${tableName}.WS_client_id = ${clientId}`;
    console.log(`[getHoldingsSummary] Filtering by client ID: ${clientId}`);
    
    // Optional as-of date filter
    let endDate = null;
    if (req.query.endDate || req.query.trandate_to) {
      const endDateStr = String(req.query.endDate || req.query.trandate_to).trim();
      if (/^\d{4}-\d{2}-\d{2}$/.test(endDateStr)) {
        endDate = endDateStr;
        where += ` AND ${tableName}.TRANDATE <= '${endDate}'`;
        console.log(`[getHoldingsSummary] Date filter applied: <= ${endDate}`);
      }
    }
    
    console.log(`[getHoldingsSummary] WHERE clause: ${where}`);

    // Exclude pseudo/non-equity rows
    where += ` AND ${tableName}.Security_Name IS NOT NULL`;
    // Note: We filter out non-equity rows in code instead of SQL to see what's being excluded
    // where += ` AND ${tableName}.Security_Name NOT IN (
    //   'CASH',
    //   'Tax Deducted at Source',
    //   'TAX',
    //   'TDS',
    //   'TAX DEDUCTED AT SOURCE'
    // )`;
    // Security_code is optional - we group by Security_Name only

    // Fetch all transactions in batches to avoid ZCQL row limit (300 rows)
    const batchSize = 250; // ZCQL max is 300, use 250 to be safe
    let offset = 0;
    let hasMore = true;
    let allRows = [];
    let batchNumber = 0;

    console.log(`[getHoldingsSummary] Starting pagination to fetch all transactions for client ${clientId}...`);

    while (hasMore) {
      batchNumber++;
      const query = `
        SELECT *
        FROM ${tableName}
        ${where}
        ORDER BY ${tableName}.Security_code ASC,
                 ${tableName}.TRANDATE ASC,
                 ${tableName}.ROWID ASC
        LIMIT ${batchSize} OFFSET ${offset}
      `;

      try {
        const rows = await zcql.executeZCQLQuery(query, []);
        
        if (!rows || rows.length === 0) {
          hasMore = false;
          break;
        }

        allRows.push(...rows);
        console.log(`[getHoldingsSummary] Batch ${batchNumber}: Fetched ${rows.length} rows, total so far: ${allRows.length}`);

        if (rows.length < batchSize) {
          hasMore = false;
        } else {
          offset += batchSize;
          // Safety limit
          if (offset > 100000) {
            console.warn(`[getHoldingsSummary] Reached safety limit of 100K rows, stopping`);
            hasMore = false;
          }
        }
      } catch (batchErr) {
        console.error(`[getHoldingsSummary] Batch ${batchNumber} error:`, batchErr.message);
        hasMore = false;
      }
    }

    console.log(
      `[getHoldingsSummary] Total transactions fetched for client ${clientId}: ${allRows.length}`
    );

    // Helper function for name normalization (same as in getStockTransactionHistory)
    const normalizeName = (name) => {
      if (!name) return '';
      return String(name)
        .trim()
        .toUpperCase()
        .replace(/\s+/g, ' ') // Replace multiple spaces with single space
        .replace(/[^\w\s]/g, '') // Remove special characters
        .replace(/\bLIMITED\b/g, 'LTD') // Convert LIMITED to LTD
        .replace(/\bINCORPORATED\b/g, 'INC') // Convert INCORPORATED to INC
        .replace(/\bCORPORATION\b/g, 'CORP') // Convert CORPORATION to CORP
        .replace(/\bPRIVATE\b/g, 'PVT') // Convert PRIVATE to PVT
        .replace(/\s+/g, ' '); // Clean up any extra spaces after replacements
    };

    // Fetch bonuses for this client (paginated to avoid ZCQL 300-row limit)
    console.log(`[getHoldingsSummary] Fetching bonuses for client ${clientId}...`);
    const bonusTableName = 'Bonus';
    const bonusBatchSize = 250;
    let bonusOffset = 0;
    let bonusHasMore = true;
    const allBonusRows = [];

    while (bonusHasMore) {
      // Inline ClientId to avoid ZCQL parameter binding issues
      const bonusQuery = `SELECT * FROM ${bonusTableName} WHERE ${bonusTableName}.ClientId = ${clientId} LIMIT ${bonusBatchSize} OFFSET ${bonusOffset}`;
      try {
        const bonusRows = await zcql.executeZCQLQuery(bonusQuery, []);
        if (!bonusRows || bonusRows.length === 0) {
          bonusHasMore = false;
          break;
        }
        allBonusRows.push(...bonusRows);
        if (bonusRows.length < bonusBatchSize) {
          bonusHasMore = false;
        } else {
          bonusOffset += bonusBatchSize;
          if (bonusOffset > 100000) bonusHasMore = false; // Safety limit
        }
      } catch (bonusErr) {
        console.error(`[getHoldingsSummary] Error fetching bonus batch at offset ${bonusOffset}:`, bonusErr.message);
        bonusHasMore = false;
      }
    }

    console.log(`[getHoldingsSummary] Total bonus records fetched for client ${clientId}: ${allBonusRows.length}`);

    // Pre-group bonuses by normalized CompanyName to avoid repeated scanning per stock
    const bonusesByName = new Map(); // key: normalized company name, value: [{ date, qty }]

    if (allBonusRows.length > 0) {
      console.log('[getHoldingsSummary] Building bonus index by normalized company name...');
      allBonusRows.forEach((bonusRow) => {
        const b = bonusRow.Bonus || bonusRow[bonusTableName] || bonusRow;

        // Extract and normalize CompanyName
        const bonusCompanyName =
          b.CompanyName ||
          b['CompanyName'] ||
          b[`${bonusTableName}.CompanyName`] ||
          b['Bonus.CompanyName'] ||
          '';

        const normalizedCompany = normalizeName(bonusCompanyName);
        if (!normalizedCompany) {
          return;
        }

        // Extract and validate ClientId
        const rawClientId =
          b.ClientId !== undefined
            ? b.ClientId
            : b.clientId !== undefined
            ? b.clientId
            : b[`${bonusTableName}.ClientId`] !== undefined
            ? b[`${bonusTableName}.ClientId`]
            : b['Bonus.ClientId'] !== undefined
            ? b['Bonus.ClientId']
            : null;

        let bonusClientId = null;
        if (rawClientId !== undefined && rawClientId !== null) {
          bonusClientId =
            typeof rawClientId === 'number' ? rawClientId : Number(rawClientId);
          if (isNaN(bonusClientId)) {
            bonusClientId = null;
          }
        }

        // Client must match (or be null)
        if (bonusClientId !== null && bonusClientId !== clientId) {
          return;
        }

        // Extract ExDate and respect endDate filter if provided
        const exDate =
          b.ExDate ||
          b['ExDate'] ||
          b[`${bonusTableName}.ExDate`] ||
          b['Bonus.ExDate'] ||
          '';

        if (endDate && exDate) {
          const exDateObj = new Date(exDate);
          const endDateObj = new Date(endDate);
          if (exDateObj > endDateObj) {
            return; // outside of as-of date window
          }
        }

        // Extract BonusShare
        const bonusShare =
          b.BonusShare ||
          b['BonusShare'] ||
          b[`${bonusTableName}.BonusShare`] ||
          b['Bonus.BonusShare'] ||
          0;
        const bonusQty = Number(bonusShare) || 0;
        if (!bonusQty) {
          return;
        }

        const bonusDate =
          exDate && String(exDate).trim() !== '' ? exDate : '1900-01-01';

        if (!bonusesByName.has(normalizedCompany)) {
          bonusesByName.set(normalizedCompany, []);
        }
        bonusesByName.get(normalizedCompany).push({
          date: bonusDate,
          qty: bonusQty,
        });
      });
      console.log(
        `[getHoldingsSummary] Bonus index built with ${bonusesByName.size} unique company names`
      );
    }

    // Track all unique Security_Name values from raw data (before filtering)
    const allUniqueNames = new Set();
    (allRows || []).forEach((r) => {
      const t = r.Transaction || r[tableName] || r;
      const stockName = t.Security_Name || t.security_name || t.SecurityName || t.securityName;
      if (stockName) {
        allUniqueNames.add(String(stockName).trim());
      }
    });
    console.log(`[getHoldingsSummary] Total unique Security_Name values in raw data: ${allUniqueNames.size}`);

    // Group by Security_Name only (Security_code is not reliable in some places)
    const byStock = {};
    const stockInfo = {}; // Track stock info for each unique stock
    const excludedStocks = new Set(); // Track excluded stocks

    (allRows || []).forEach((r) => {
      const t = r.Transaction || r[tableName] || r;
      
      // Verify client ID matches (safety check)
      const rowClientId = t.WS_client_id || t.ws_client_id || t.WSClientId || t.wsClientId;
      if (rowClientId && Number(rowClientId) !== clientId) {
        console.warn(`[getHoldingsSummary] Row client ID mismatch: expected ${clientId}, got ${rowClientId}, skipping`);
        return; // Skip this row if client ID doesn't match
      }
      
      // Handle different field name formats (case-insensitive)
      const stockName = t.Security_Name || t.security_name || t.SecurityName || t.securityName;
      const stockCode = t.Security_code || t.security_code || t.SecurityCode || t.securityCode || "";
      const tranType = t.Tran_Type || t.tran_type || t.TranType || t.tranType || "";
      const qty = t.QTY || t.qty || t.Qty || 0;
      const netAmount = t.Net_Amount || t.net_amount || t.NetAmount || t.netAmount || 0;
      const rate = t.RATE || t.rate || t.Rate || 0;
      const trandate = t.TRANDATE || t.trandate || t.Trandate || "";
      const rowid = t.ROWID || t.rowid || t.Rowid || 0;

      // Only require Security_Name (Security_code is optional)
      if (!stockName) return;

      // Filter out non-equity rows (do this in code to see what's being excluded)
      const normalizedName = String(stockName).trim().toUpperCase();
      const excludedNames = ['CASH', 'TAX', 'TDS', 'TAX DEDUCTED AT SOURCE'];
      if (excludedNames.includes(normalizedName)) {
        excludedStocks.add(String(stockName).trim());
        return; // Skip non-equity rows
      }

      // Use Security_Name as the unique key (normalize to handle case differences)
      const key = normalizedName;
      
      // Store stock info for this unique stock (use first Security_code encountered)
      if (!stockInfo[key]) {
        stockInfo[key] = {
          stockName: String(stockName).trim(),
          stockCode: String(stockCode).trim() || "" // Store code if available, empty string if not
        };
      }

      if (!byStock[key]) byStock[key] = [];

      byStock[key].push({
        tranType: String(tranType).trim(),
        qty: Number(qty) || 0,
        netAmount: Number(netAmount) || 0,
        rate: Number(rate) || 0,
        netrate:
          Number(t.NETRATE) ||
          Number(t.netrate) ||
          Number(t.netRate) ||
          0,
        trandate: String(trandate).trim(),
        rowid: Number(rowid) || 0,
      });
    });

    const result = [];

    for (const key of Object.keys(byStock)) {
      const info = stockInfo[key] || { stockName: "Unknown", stockCode: "" };
      const stockName = info.stockName;
      const stockCode = info.stockCode;
      const transactions = byStock[key];

      // Log for debugging specific stock
      if (stockName.toLowerCase().includes('gujarat narmada')) {
        console.log(`[getHoldingsSummary] Processing Gujarat Narmada stock: ${stockName} (${stockCode})`);
        console.log(`[getHoldingsSummary] Transaction count: ${transactions.length}`);
        console.log(`[getHoldingsSummary] Transactions:`, transactions.map(t => ({
          type: t.tranType,
          qty: t.qty,
          netAmount: t.netAmount,
          date: t.trandate
        })));
      }

      // Process transactions chronologically (same logic as getStockTransactionHistory)
      // Sort transactions by date
      const sortedTransactions = [...transactions].sort((a, b) => {
        const dateA = a.trandate ? new Date(a.trandate).getTime() : 0;
        const dateB = b.trandate ? new Date(b.trandate).getTime() : 0;
        if (dateA !== dateB) return dateA - dateB;
        return (a.rowid || 0) - (b.rowid || 0);
      });

      // Get matching bonuses for this stock via precomputed index (already date/client filtered)
      const normalizedStockName = normalizeName(stockName);
      const matchingBonuses = bonusesByName.get(normalizedStockName) || [];

      // Build a combined list of transactions and bonus events and compute holdings
      // using the SAME logic as the StockDetailModal on the frontend so values match.
      const combinedEvents = [];

      // Real trade transactions (exclude all dividend-related types)
      sortedTransactions.forEach((t) => {
        const rawType = String(t.tranType || "").toUpperCase().trim();
        const isDividend =
          rawType === "DIO" ||
          rawType === "DIVIDEND" ||
          rawType === "DIVIDEND REINVEST" ||
          rawType === "DIVIDEND REINVESTMENT" ||
          rawType === "DIVIDEND RECEIVED" ||
          rawType.startsWith("DIVIDEND") ||
          rawType.includes("DIVIDEND");
        if (isDividend) return;

        combinedEvents.push({
          tranType: rawType,
          qty: t.qty,
          netAmount: t.netAmount,
          rate: t.rate,
          netrate: t.netrate || t.netRate || t.NETRATE || 0,
          trandate: t.trandate,
          rowid: t.rowid,
          isBonus: false,
        });
      });

      // Treat matched bonuses as explicit BONUS events with zero cost
      matchingBonuses.forEach((b, idx) => {
        combinedEvents.push({
          tranType: "BONUS",
          qty: b.qty,
          netAmount: 0,
          rate: 0,
          trandate: b.date || "1900-01-01",
          rowid: 1000000 + idx,
          isBonus: true,
        });
      });

      // Sort chronologically (same ordering as frontend)
      combinedEvents.sort((a, b) => {
        const d1 = a.trandate ? new Date(a.trandate).getTime() : 0;
        const d2 = b.trandate ? new Date(b.trandate).getTime() : 0;
        if (d1 !== d2) return d1 - d2;
        return (a.rowid || 0) - (b.rowid || 0);
      });

      // FIFO lots: { qty, price }. Mirrors the lotQueue logic in StockDetailModal.
      const lotQueue = [];

      combinedEvents.forEach((transaction) => {
        const tranType = transaction.tranType || "";
        const isBuy =
          tranType.startsWith("B") ||
          tranType === "BUY" ||
          tranType === "PURCHASE" ||
          tranType.includes("BUY");
        const isSell =
          tranType.startsWith("S") ||
          tranType === "SELL" ||
          tranType === "SALE" ||
          tranType.includes("SELL");
        const isBonus = tranType === "BONUS" || transaction.isBonus === true;
        const isSQB = tranType === "SQB";
        const isSQS = tranType === "SQS";
        const isOPI = tranType === "OPI";
        const isOPO = tranType === "OPO";
        const isNF = tranType === "NF-" || tranType.startsWith("NF-");

        const isBuyType = isBuy || isSQB || isOPI;
        const isSellType = isSell || isSQS || isOPO || isNF;

        const qty = Math.abs(Number(transaction.qty) || 0);
        if (qty === 0) return;

        // Match frontend pricing: prefer netrate, then rate, then derive from netAmount/qty
        const netrate =
          Number(transaction.netrate) ||
          Number(transaction.netRate) ||
          Number(transaction.NETRATE) ||
          0;
        let price = netrate > 0 ? netrate : Number(transaction.rate) || 0;
        if (price === 0 && transaction.netAmount && Math.abs(transaction.netAmount) > 0) {
          price = Math.abs(transaction.netAmount) / qty;
        }

        if (isBuyType && !isBonus) {
          lotQueue.push({ qty, price });
        } else if (isBonus) {
          const bonusQty = qty;
          if (bonusQty > 0) {
            lotQueue.push({ qty: bonusQty, price: 0 });
          }
        } else if (isSellType) {
          let remaining = qty;
          while (remaining > 0 && lotQueue.length > 0) {
            const lot = lotQueue[0];
            if (lot.qty <= remaining) {
              remaining -= lot.qty;
              lotQueue.shift();
            } else {
              lot.qty -= remaining;
              remaining = 0;
            }
          }
        }
      });

      const holdingQty = lotQueue.reduce((sum, lot) => sum + lot.qty, 0);
      const totalCostBasis = lotQueue.reduce(
        (sum, lot) => sum + lot.qty * lot.price,
        0
      );
      const avgCost = holdingQty > 0 ? totalCostBasis / holdingQty : 0;
      const holdingValue = holdingQty > 0 ? totalCostBasis : 0;

      // Include all stocks, even with zero holdings
      // Return stock name, code, current holding quantity and cost-based holding value/avg cost
      result.push({
        stockName,
        stockCode,
        currentHolding: holdingQty,
        avgCost,
        holdingValue,
      });
    }

    // Sort alphabetically by Security_Name
    result.sort((a, b) => a.stockName.localeCompare(b.stockName));

    console.log(
      `[getHoldingsSummary] Final holdings for client ${clientId}: ${result.length} stocks`
    );
    console.log(`[getHoldingsSummary] Excluded stocks (non-equity): ${excludedStocks.size}`, Array.from(excludedStocks));
    console.log(`[getHoldingsSummary] Summary: ${allUniqueNames.size} unique names in DB, ${excludedStocks.size} excluded, ${result.length} returned`);
    
    // Log if there's a mismatch
    const excludedNamesList = ['CASH', 'TAX', 'TDS', 'TAX DEDUCTED AT SOURCE'];
    if (allUniqueNames.size - excludedStocks.size !== result.length) {
      console.warn(`[getHoldingsSummary] Mismatch detected! Expected ${allUniqueNames.size - excludedStocks.size} stocks, got ${result.length}`);
      const processedNames = new Set(result.map(r => r.stockName));
      const missingNames = Array.from(allUniqueNames).filter(name => 
        !excludedNamesList.includes(String(name).trim().toUpperCase()) && 
        !processedNames.has(String(name).trim())
      );
      if (missingNames.length > 0) {
        console.warn(`[getHoldingsSummary] Missing stocks:`, missingNames);
      }
    }

    return res.status(200).json(result);
  } catch (err) {
    console.error("[getHoldingsSummary] Error:", err);
    return res.status(500).json({
      message: "Failed to fetch holdings summary",
      error: String(err && err.message ? err.message : err),
    });
  }
};

// Get all unique clients with their cumulative holdings (sum of all stock holdings)
exports.getClientsWithCumulativeHoldings = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({ message: "Catalyst app context missing" });
    }

    const tableName = sanitizeIdentifier(req.query.table || DEFAULT_TABLE);
    const zcql = app.zcql();

    // Optional as-of date filter
    let dateFilter = '';
    if (req.query.endDate || req.query.trandate_to) {
      const endDate = String(req.query.endDate || req.query.trandate_to).trim();
      if (/^\d{4}-\d{2}-\d{2}$/.test(endDate)) {
        dateFilter = ` AND ${tableName}.TRANDATE <= '${endDate}'`;
      }
    }

    // Get all unique client IDs
    const clientQuery = `SELECT DISTINCT ${tableName}.WS_client_id FROM ${tableName} WHERE ${tableName}.WS_client_id IS NOT NULL`;
    const clientRows = await zcql.executeZCQLQuery(clientQuery, []);
    
    const clientIds = [];
    if (clientRows && clientRows.length > 0) {
      clientRows.forEach((row) => {
        const clientId = row.WS_client_id || row[`${tableName}.WS_client_id`] || (row[tableName] && row[tableName].WS_client_id);
        if (clientId && String(clientId).trim() !== "") {
          const numClientId = parseInt(String(clientId).trim(), 10);
          if (!isNaN(numClientId)) {
            clientIds.push(numClientId);
          }
        }
      });
    }

    console.log(`[getClientsWithCumulativeHoldings] Found ${clientIds.length} unique clients`);

    // Helper function for name normalization
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

    // For each client, calculate cumulative holdings by fetching their holdings summary
    const results = [];
    for (const clientId of clientIds) {
      try {
        // Build WHERE clause for this client
        let where = ` WHERE ${tableName}.WS_client_id = ${clientId}`;
        if (dateFilter) {
          where += dateFilter;
        }
        where += ` AND ${tableName}.Security_Name IS NOT NULL`;

        // Fetch all transactions for this client in batches
        const batchSize = 250;
        let offset = 0;
        let hasMore = true;
        let allRows = [];

        while (hasMore) {
          const query = `SELECT * FROM ${tableName} ${where} ORDER BY ${tableName}.Security_Name ASC, ${tableName}.TRANDATE ASC LIMIT ${batchSize} OFFSET ${offset}`;
          const rows = await zcql.executeZCQLQuery(query, []);
          
          if (!rows || rows.length === 0) {
            hasMore = false;
            break;
          }
          
          allRows.push(...rows);
          if (rows.length < batchSize) {
            hasMore = false;
          } else {
            offset += batchSize;
            if (offset > 100000) {
              hasMore = false;
            }
          }
        }

        // Fetch bonuses for this client
        const bonusTableName = 'Bonus';
        const bonusBatchSize = 250;
        let bonusOffset = 0;
        let bonusHasMore = true;
        const allBonusRows = [];

        while (bonusHasMore) {
          const bonusQuery = `SELECT * FROM ${bonusTableName} WHERE ${bonusTableName}.ClientId = ${clientId} LIMIT ${bonusBatchSize} OFFSET ${bonusOffset}`;
          try {
            const bonusRows = await zcql.executeZCQLQuery(bonusQuery, []);
            if (!bonusRows || bonusRows.length === 0) {
              bonusHasMore = false;
              break;
            }
            allBonusRows.push(...bonusRows);
            if (bonusRows.length < bonusBatchSize) {
              bonusHasMore = false;
            } else {
              bonusOffset += bonusBatchSize;
              if (bonusOffset > 100000) bonusHasMore = false;
            }
          } catch (bonusErr) {
            console.error(`[getClientsWithCumulativeHoldings] Error fetching bonus batch for client ${clientId}:`, bonusErr.message);
            bonusHasMore = false;
          }
        }

        // Calculate holdings per stock (same logic as getHoldingsSummary)
        const stockMap = new Map();
        allRows.forEach((row) => {
          const r = row.Transaction || row[tableName] || row;
          const stockName = (r.Security_Name || '').trim();
          if (!stockName) return;

          const type = String(r.Tran_Type || '').toUpperCase().trim();
          const isBuy = type.startsWith('B') || type === 'SQB' || type === 'OPI';
          const isSell = type.startsWith('S') || type === 'SQS' || type === 'OPO' || type === 'NF-' || type.startsWith('NF-');
          const isDividend = type === 'DIO' || type === 'DIVIDEND' || type.startsWith('DIVIDEND') || type.includes('DIVIDEND');
          
          if (isDividend) return; // Exclude dividends

          if (!stockMap.has(stockName)) {
            stockMap.set(stockName, { buyQty: 0, sellQty: 0, bonusQty: 0 });
          }
          
          const qty = Math.abs(toNumber(r.QTY) || 0);
          if (isBuy) {
            stockMap.get(stockName).buyQty += qty;
          } else if (isSell) {
            stockMap.get(stockName).sellQty += qty;
          }
        });

        // Add bonus shares to stockMap
        allBonusRows.forEach((bonusRow) => {
          const b = bonusRow.Bonus || bonusRow[bonusTableName] || bonusRow;
          const bonusCompanyName = b.CompanyName || 
                                  b['CompanyName'] || 
                                  b[`${bonusTableName}.CompanyName`] || 
                                  b['Bonus.CompanyName'] || '';
          
          if (!bonusCompanyName) return;
          
          // Find matching stock by normalized name
          const normalizedBonusName = normalizeName(bonusCompanyName);
          for (const [stockName, stockData] of stockMap.entries()) {
            const normalizedStockName = normalizeName(stockName);
            if (normalizedBonusName === normalizedStockName) {
              const bonusShare = b.BonusShare || 
                                b['BonusShare'] || 
                                b[`${bonusTableName}.BonusShare`] || 
                                b['Bonus.BonusShare'] || 0;
              stockData.bonusQty += (Number(bonusShare) || 0);
              break; // Found match, move to next bonus
            }
          }
        });

        // Calculate cumulative holding (including bonuses)
        let cumulativeHolding = 0;
        stockMap.forEach((stock) => {
          cumulativeHolding += Math.max(0, stock.buyQty - stock.sellQty + stock.bonusQty);
        });

        results.push({
          clientId: String(clientId),
          cumulativeHolding: cumulativeHolding
        });
      } catch (err) {
        console.error(`[getClientsWithCumulativeHoldings] Error calculating holdings for client ${clientId}:`, err);
        results.push({
          clientId: String(clientId),
          cumulativeHolding: 0
        });
      }
    }

    // Sort by client ID
    results.sort((a, b) => parseInt(a.clientId) - parseInt(b.clientId));

    console.log(`[getClientsWithCumulativeHoldings] Returning ${results.length} clients with cumulative holdings`);
    return res.status(200).json(results);
  } catch (err) {
    console.error("[getClientsWithCumulativeHoldings] Error:", err);
    return res.status(500).json({
      message: "Failed to fetch clients with cumulative holdings",
      error: String(err && err.message ? err.message : err),
    });
  }
};

// Get transaction history for a specific stock of a client
exports.getStockTransactionHistory = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({ message: "Catalyst app context missing" });
    }

    const clientId = req.query.clientId || req.query.ws_client_id;
    const stockName = req.params.stockName ? decodeURIComponent(req.params.stockName) : null;
    const stockCode = req.query.stockCode || req.query.security_code; // Support filtering by Security_code

    if (!clientId || (!stockName && !stockCode)) {
      return res
        .status(400)
        .json({ message: "Client ID and Stock Name or Stock Code are required" });
    }

    const tableName = sanitizeIdentifier(req.query.table || DEFAULT_TABLE);
    const zcql = app.zcql();

    // Validate client ID
    const clientIdValue = String(clientId).trim();
    if (!/^\d+$/.test(clientIdValue)) {
      return res.status(400).json({ message: "Invalid client ID format" });
    }
    const numClientId = parseInt(clientIdValue, 10);

    // Build WHERE clause - Fetch ALL transactions for this stock (all transaction types: Buy, Sell, Dividend, etc.)
    // No filtering by Tran_Type - we want to show everything including Buy, Sell, Dividend, Dividend Reinvest, Dividend Received
    let whereClause = `WHERE ${tableName}.WS_client_id = ${numClientId}`;
    
    // Filter by Security_Name first (since holdings are grouped by Security_Name), then Security_code as additional filter if both provided
    if (stockName) {
      const escapedStockName = String(stockName).trim().replace(/'/g, "''");
      whereClause += ` AND ${tableName}.Security_Name = '${escapedStockName}'`;
    } else if (stockCode) {
      // Fallback to Security_code only if Security_Name is not provided
      const escapedStockCode = String(stockCode).trim().replace(/'/g, "''");
      whereClause += ` AND ${tableName}.Security_code = '${escapedStockCode}'`;
    }

    // Add date filter if provided
    if (req.query.endDate || req.query.trandate_to) {
      const endDate = String(req.query.endDate || req.query.trandate_to).trim();
      if (/^\d{4}-\d{2}-\d{2}$/.test(endDate)) {
        whereClause += ` AND ${tableName}.TRANDATE <= '${endDate}'`;
      }
    }

    const stockIdentifier = stockName || stockCode;
    console.log(
      `[getStockTransactionHistory] Fetching ALL transactions (all types: Buy, Sell, Dividend, etc.) for client ${numClientId}, ${stockName ? 'stockName' : 'stockCode'}: ${stockIdentifier}`
    );
    console.log(`[getStockTransactionHistory] WHERE clause: ${whereClause}`);

    // Fetch all transactions for this stock (with pagination if needed)
    const allTransactions = [];
    const batchSize = 250;
    let offset = 0;
    let hasMore = true;
    let totalFetched = 0;

    console.log(
      `[getStockTransactionHistory] Starting to fetch transactions for stock: ${stockName}`
    );

    while (hasMore) {
      const query = `SELECT * FROM ${tableName} ${whereClause} ORDER BY ${tableName}.TRANDATE ASC, ${tableName}.ROWID ASC LIMIT ${batchSize} OFFSET ${offset}`;

      try {
        console.log(
          `[getStockTransactionHistory] Fetching batch: offset=${offset}, limit=${batchSize}`
        );
        const rows = await zcql.executeZCQLQuery(query, []);

        if (!rows || rows.length === 0) {
          console.log(
            `[getStockTransactionHistory] No more rows at offset ${offset}`
          );
          hasMore = false;
          break;
        }

        console.log(
          `[getStockTransactionHistory] Batch ${
            Math.floor(offset / batchSize) + 1
          }: Fetched ${rows.length} rows`
        );
        allTransactions.push(...rows);
        totalFetched += rows.length;

        // Log sample of transaction types to debug
        if (offset === 0 && rows.length > 0) {
          const sampleTypes = rows.slice(0, 5).map((r) => {
            const r2 = r.Transaction || r[tableName] || r;
            return r2.Tran_Type || r2.tran_type || "UNKNOWN";
          });
          console.log(
            `[getStockTransactionHistory] Sample transaction types:`,
            sampleTypes
          );
        }

        if (rows.length < batchSize) {
          console.log(
            `[getStockTransactionHistory] Reached end of data (got ${rows.length} < ${batchSize} rows)`
          );
          hasMore = false;
        } else {
          offset += batchSize;
          // Increased limit for transaction history
          if (offset > 50000) {
            console.warn(
              `[getStockTransactionHistory] Reached safety limit of 50K rows, stopping`
            );
            hasMore = false;
          }
        }
      } catch (queryErr) {
        console.error("[getStockTransactionHistory] Query error:", queryErr);
        console.error("[getStockTransactionHistory] Query that failed:", query);
        throw queryErr;
      }
    }

    console.log(
      `[getStockTransactionHistory] Total fetched: ${totalFetched} transactions`
    );

    // Verify we have all transaction types (Buy, Sell, Dividend, Dividend Reinvest, Dividend Received, etc.)
    const buyCountBeforeTransform = allTransactions.filter((row) => {
      const r = row.Transaction || row[tableName] || row;
      const tranType = r.Tran_Type || r.tran_type || "";
      return String(tranType).toUpperCase().trim().startsWith("B");
    }).length;

    const sellCountBeforeTransform = allTransactions.filter((row) => {
      const r = row.Transaction || row[tableName] || row;
      const tranType = r.Tran_Type || r.tran_type || "";
      return String(tranType).toUpperCase().trim().startsWith("S");
    }).length;

    console.log(
      `[getStockTransactionHistory] Transaction breakdown: BUY=${buyCountBeforeTransform}, SELL=${sellCountBeforeTransform}, Total=${totalFetched}`
    );

    // Transform rows to match frontend format (same as in api.js)
    // IMPORTANT: Include ALL transactions - don't filter any out
    // But filter to ensure we only return transactions for the requested stock
    const transactions = allTransactions
      .map((row, index) => {
      // Handle Transaction wrapper from ZCQL
      const r = row.Transaction || row[tableName] || row;

      // Extract transaction type with multiple fallbacks
      const tranType =
        r.Tran_Type ||
        r.tran_type ||
        row.Tran_Type ||
        row.tran_type ||
        r[`${tableName}.Tran_Type`] ||
        row[`${tableName}.Tran_Type`];

      const transaction = {
        wsClientId:
          r.WS_client_id ??
          r.ws_client_id ??
          (row.WS_client_id || row.ws_client_id),
        wsAccountCode:
          r.WS_Account_code ??
          r.ws_account_code ??
          (row.WS_Account_code || row.ws_account_code),
        trandate: r.TRANDATE ?? r.trandate ?? (row.TRANDATE || row.trandate),
        tranType: tranType,
        securityName:
          r.Security_Name ??
          r.security_name ??
          (row.Security_Name || row.security_name),
        securityCode:
          r.Security_code ??
          r.security_code ??
          (row.Security_code || row.security_code),
        exchg: r.EXCHG ?? r.exchg ?? (row.EXCHG || row.exchg),
        qty: r.QTY ?? r.qty ?? (row.QTY || row.qty),
        rate: r.RATE ?? r.rate ?? (row.RATE || row.rate),
        netrate: r.NETRATE ?? r.netrate ?? r.netRate ?? (row.NETRATE || row.netrate || row.netRate),
        netAmount:
          r.Net_Amount ??
          r.net_amount ??
          r.netAmount ??
          (row.Net_Amount || row.net_amount || row.netAmount),
        rowid: r.ROWID ?? r.rowid ?? (row.ROWID || row.rowid) ?? 0,
      };

      // Debug first few transactions
      if (index < 3) {
        console.log(`[getStockTransactionHistory] Transaction ${index + 1}:`, {
          tranType: transaction.tranType,
          securityName: transaction.securityName,
          qty: transaction.qty,
          rawRow: Object.keys(row),
          hasTransaction: !!row.Transaction,
          hasTableName: !!row[tableName],
        });
      }

      return transaction;
    })
    .filter((transaction) => {
      // Safety check: Only return transactions that match the requested stock name
      // This ensures we don't accidentally return transactions from other stocks
      if (stockName) {
        const transactionStockName = String(transaction.securityName || "").trim();
        const requestedStockName = String(stockName).trim();
        const matches = transactionStockName.toUpperCase() === requestedStockName.toUpperCase();
        if (!matches) {
          console.warn(
            `[getStockTransactionHistory] Filtered out transaction: stockName mismatch. Expected: "${requestedStockName}", Got: "${transactionStockName}"`
          );
        }
        return matches;
      }
      // If filtering by stockCode only, verify it matches
      if (stockCode) {
        const transactionStockCode = String(transaction.securityCode || "").trim();
        const requestedStockCode = String(stockCode).trim();
        return transactionStockCode === requestedStockCode;
      }
      return true; // If neither stockName nor stockCode provided, return all (shouldn't happen)
    });

    // Normalize function for matching Security-Name with Company-Name
    // Handles common variations like "LTD" vs "LIMITED", "INC" vs "INCORPORATED", etc.
    const normalizeName = (name) => {
      if (!name) return '';
      return String(name)
        .trim()
        .toUpperCase()
        .replace(/\s+/g, ' ') // Replace multiple spaces with single space
        .replace(/[^\w\s]/g, '') // Remove special characters
        .replace(/\bLIMITED\b/g, 'LTD') // Convert LIMITED to LTD
        .replace(/\bINCORPORATED\b/g, 'INC') // Convert INCORPORATED to INC
        .replace(/\bCORPORATION\b/g, 'CORP') // Convert CORPORATION to CORP
        .replace(/\s+/g, ' '); // Clean up any extra spaces after replacements
    };

    // Fetch bonuses matching by Company-Name = Security-Name
    let bonuses = [];
    if (stockName) {
      try {
        const normalizedStockName = normalizeName(stockName);
        console.log(`[getStockTransactionHistory] Fetching bonuses for stock: ${stockName} (normalized: ${normalizedStockName})`);
        
        // IMPORTANT:
        // ZCQL caps results (~300 rows). Never do `SELECT * FROM Bonus`.
        // Also, exact CompanyName string comparisons are brittle (trailing dots/spaces/case).
        // So we fetch bonuses for this client (paginated) and match by normalized company name in JS.
        const bonusTableName = 'Bonus';

        // Fetch all bonuses for this client in batches (usually small; still safe if larger)
        const bonusBatchSize = 250;
        let bonusOffset = 0;
        let bonusHasMore = true;
        const bonusRows = [];

        while (bonusHasMore) {
          // NOTE: For some Catalyst projects, binding numeric params against INT columns
          // can throw range errors even for valid values. Since numClientId is already
          // validated as digits and parsed, it is safe to inline here.
          const bonusQuery = `SELECT * FROM ${bonusTableName} WHERE ${bonusTableName}.ClientId = ${numClientId} LIMIT ${bonusBatchSize} OFFSET ${bonusOffset}`;
          const rows = await zcql.executeZCQLQuery(bonusQuery, []);
          if (!rows || rows.length === 0) {
            bonusHasMore = false;
            break;
          }
          bonusRows.push(...rows);
          if (rows.length < bonusBatchSize) {
            bonusHasMore = false;
          } else {
            bonusOffset += bonusBatchSize;
            // hard safety guard
            if (bonusOffset > 100000) bonusHasMore = false;
          }
        }
        
        try {
          console.log(`[getStockTransactionHistory] ===== BONUS TABLE QUERY RESULTS =====`);
          console.log(`[getStockTransactionHistory] Total bonus records fetched from database (client filtered): ${bonusRows.length}`);
          console.log(`[getStockTransactionHistory] Requested clientId: ${numClientId}`);
          console.log(`[getStockTransactionHistory] Requested security: "${stockName}" (normalized: "${normalizedStockName}")`);
          console.log(`[getStockTransactionHistory] Bonus fetch strategy: WHERE ClientId = ${numClientId} (paginated), match CompanyName in JS`);
          
          // Log ALL bonuses for the requested client
          const clientBonusesRaw = bonusRows.filter(row => {
            const b = row.Bonus || row['Bonus'] || row;
            const rawClientId = b.ClientId !== undefined ? b.ClientId : 
                               (b.clientId !== undefined ? b.clientId : 
                                (b['Bonus.ClientId'] !== undefined ? b['Bonus.ClientId'] : null));
            const clientId = rawClientId !== undefined && rawClientId !== null 
              ? (typeof rawClientId === 'number' ? rawClientId : Number(rawClientId))
              : null;
            return clientId === numClientId || clientId === null;
          });
          
          console.log(`[getStockTransactionHistory] Bonuses for client ${numClientId} (or null): ${clientBonusesRaw.length}`);
          
          // Log first 10 bonuses for this client
          clientBonusesRaw.slice(0, 10).forEach((row, idx) => {
            const b = row.Bonus || row['Bonus'] || row;
            const companyName = b.CompanyName || b['CompanyName'] || b['Bonus.CompanyName'] || 'MISSING';
            const normalized = normalizeName(companyName);
            const matchesSecurity = normalized === normalizedStockName;
            console.log(`[getStockTransactionHistory] Client Bonus ${idx + 1} (RAW from DB):`, {
              CompanyName: companyName,
              NormalizedCompanyName: normalized,
              MatchesSecurity: matchesSecurity,
              ClientId: b.ClientId || b['ClientId'] || b['Bonus.ClientId'] || 'MISSING',
              ClientIdType: typeof (b.ClientId || b['ClientId'] || b['Bonus.ClientId']),
              ExDate: b.ExDate || b['ExDate'] || b['Bonus.ExDate'] || 'MISSING',
              BonusShare: b.BonusShare || b['BonusShare'] || b['Bonus.BonusShare'] || 'MISSING',
              SecurityCode: b.SecurityCode || b['SecurityCode'] || b['Bonus.SecurityCode'] || 'MISSING',
              allKeys: Object.keys(b)
            });
          });
          
          // Check for bonuses matching the security name
          const securityBonusesRaw = bonusRows.filter(row => {
            const b = row.Bonus || row['Bonus'] || row;
            const companyName = b.CompanyName || b['CompanyName'] || b['Bonus.CompanyName'] || '';
            const normalized = normalizeName(companyName);
            return normalized === normalizedStockName;
          });
          
          console.log(`[getStockTransactionHistory] Bonuses matching security "${stockName}": ${securityBonusesRaw.length}`);
          securityBonusesRaw.slice(0, 5).forEach((row, idx) => {
            const b = row.Bonus || row['Bonus'] || row;
            const rawClientId = b.ClientId || b['ClientId'] || b['Bonus.ClientId'];
            const clientId = rawClientId !== undefined && rawClientId !== null 
              ? (typeof rawClientId === 'number' ? rawClientId : Number(rawClientId))
              : null;
            const matchesClient = clientId === null || clientId === numClientId;
            console.log(`[getStockTransactionHistory] Security Bonus ${idx + 1}:`, {
              CompanyName: b.CompanyName || b['CompanyName'] || 'MISSING',
              ClientId: clientId,
              MatchesClient: matchesClient,
              ExDate: b.ExDate || b['ExDate'] || 'MISSING',
              BonusShare: b.BonusShare || b['BonusShare'] || 'MISSING',
              WillMatch: matchesClient
            });
          });
          
          // Debug: Log first bonus row structure to understand data format
          if (bonusRows && bonusRows.length > 0) {
            console.log(`[getStockTransactionHistory] Sample bonus row structure:`, {
              rowKeys: Object.keys(bonusRows[0]),
              sampleBonus: bonusRows[0].Bonus || bonusRows[0]['Bonus'] || bonusRows[0],
              allKeys: Object.keys(bonusRows[0].Bonus || bonusRows[0]['Bonus'] || bonusRows[0])
            });
          }
          console.log(`[getStockTransactionHistory] ==========================================`);
          
          // Filter bonuses that match the stock's Security-Name AND ClientId
          let debugCounter = 0;
          bonuses = bonusRows
            .map((row) => {
              const b = row.Bonus || row[bonusTableName] || row;
              
              // Try multiple ways to access BonusShare field (ZCQL may return it in different formats)
              const bonusShareValue = b.BonusShare || 
                                     b['BonusShare'] || 
                                     b[`${bonusTableName}.BonusShare`] ||
                                     b[`Bonus.BonusShare`] ||
                                     b.bonus_share || 
                                     b.bonusShare || 
                                     b['bonus_share'] ||
                                     (typeof b.BonusShare === 'number' ? b.BonusShare : 
                                      (typeof b['BonusShare'] === 'number' ? b['BonusShare'] : 0));
              
              // Extract ClientId - handle various formats (number, string, null)
              // ZCQL might return it as Bonus.ClientId or just ClientId
              let extractedClientId = null;
              const rawClientId = b.ClientId !== undefined ? b.ClientId : 
                                 (b.clientId !== undefined ? b.clientId : 
                                  (b[`${bonusTableName}.ClientId`] !== undefined ? b[`${bonusTableName}.ClientId`] :
                                   (b['Bonus.ClientId'] !== undefined ? b['Bonus.ClientId'] : null)));
              
              if (rawClientId !== undefined && rawClientId !== null) {
                extractedClientId = typeof rawClientId === 'number' ? rawClientId : Number(rawClientId);
                if (isNaN(extractedClientId)) {
                  extractedClientId = null;
                }
              }
              
              // Extract CompanyName with more fallback options
              const companyName = b.CompanyName || 
                                  b['CompanyName'] || 
                                  b[`${bonusTableName}.CompanyName`] || 
                                  b['Bonus.CompanyName'] ||
                                  b['Company-Name'] ||
                                  b['company_name'] ||
                                  b.companyName ||
                                  '';
              
              // Extract ExDate with more fallback options
              const exDate = b.ExDate || 
                            b['ExDate'] || 
                            b[`${bonusTableName}.ExDate`] || 
                            b['Bonus.ExDate'] ||
                            b['Ex-Date'] ||
                            b['ex_date'] ||
                            b.exDate ||
                            '';
              
              const mappedBonus = {
                companyName: companyName,
                securityCode: b.SecurityCode || b['SecurityCode'] || b[`${bonusTableName}.SecurityCode`] || b['Bonus.SecurityCode'] || '',
                series: b.Series || b.series,
                bonusShare: bonusShareValue,
                exDate: exDate,
                clientId: extractedClientId,
              };
              
              // Debug first few bonuses to see field extraction
              if (bonusRows.indexOf(row) < 3) {
                console.log(`[getStockTransactionHistory] Bonus mapping ${bonusRows.indexOf(row) + 1}:`, {
                  rawRowKeys: Object.keys(row),
                  bonusObjectKeys: Object.keys(b),
                  extractedCompanyName: mappedBonus.companyName,
                  extractedClientId: mappedBonus.clientId,
                  extractedExDate: mappedBonus.exDate,
                  rawClientId: rawClientId,
                  rawClientIdType: typeof rawClientId
                });
              }
              
              // Debug: Log raw values for first bonus to verify data mapping
              if (bonusRows.indexOf(row) === 0) {
                console.log(`[getStockTransactionHistory] Raw bonus data mapping:`, {
                  rowKeys: Object.keys(row),
                  bonusObjectKeys: Object.keys(b),
                  rawBonusShare_b_BonusShare: b.BonusShare,
                  rawBonusShare_b_BracketBonusShare: b['BonusShare'],
                  rawBonusShare_tablePrefix: b[`${bonusTableName}.BonusShare`],
                  rawBonusShare_bonusPrefix: b[`Bonus.BonusShare`],
                  rawBonusShare_bonus_share: b.bonus_share,
                  rawBonusShare_bonusShare: b.bonusShare,
                  bonusShareValue: bonusShareValue,
                  mappedBonusShare: mappedBonus.bonusShare,
                  rawExDate: b.ExDate || b['ExDate'],
                  mappedExDate: mappedBonus.exDate,
                  allBonusKeys: Object.keys(b).filter(k => k.toLowerCase().includes('bonus')),
                  fullRow: JSON.stringify(row, null, 2)
                });
              }
              
              return mappedBonus;
            })
            .filter((bonus) => {
              // Check if companyName exists and is not empty
              if (!bonus.companyName || bonus.companyName.trim() === '') {
                console.log(`[getStockTransactionHistory] Skipping bonus: companyName is empty`, {
                  bonusKeys: Object.keys(bonus),
                  bonusData: bonus
                });
                return false;
              }
              
              const normalizedCompanyName = normalizeName(bonus.companyName);
              let matchesStock = normalizedCompanyName === normalizedStockName;
              
              // Fallback: If exact match fails, try partial matching for common cases
              // This handles cases like "HDFC BANK" vs "HDFC BANK LTD" or "HDFC BANK LIMITED"
              if (!matchesStock && normalizedStockName && normalizedCompanyName) {
                // Extract core company name (remove common suffixes)
                const coreCompanyName = normalizedCompanyName
                  .replace(/\b(LTD|LIMITED|INC|INCORPORATED|CORP|CORPORATION|PVT|PRIVATE)\b/g, '')
                  .trim();
                const coreStockName = normalizedStockName
                  .replace(/\b(LTD|LIMITED|INC|INCORPORATED|CORP|CORPORATION|PVT|PRIVATE)\b/g, '')
                  .trim();
                
                // Match if core names are the same (handles "HDFC BANK" vs "HDFC BANK LTD")
                if (coreCompanyName === coreStockName && coreCompanyName.length > 0) {
                  matchesStock = true;
                  console.log(`[getStockTransactionHistory] Partial match: "${normalizedCompanyName}" matches "${normalizedStockName}" (core: "${coreCompanyName}")`);
                }
                
                // Additional fallback: Check if one contains the other (for variations like "Astral Ltd." vs "Astral Ltd")
                if (!matchesStock && (normalizedCompanyName.includes(normalizedStockName) || normalizedStockName.includes(normalizedCompanyName))) {
                  // Only match if the shorter name is at least 5 characters (to avoid false matches)
                  const shorter = normalizedCompanyName.length < normalizedStockName.length ? normalizedCompanyName : normalizedStockName;
                  if (shorter.length >= 5) {
                    matchesStock = true;
                    console.log(`[getStockTransactionHistory] Contains match: "${normalizedCompanyName}" matches "${normalizedStockName}"`);
                  }
                }
              }
              
              // Match by ClientId
              // bonus.clientId is already a number or null from mapping, but handle undefined
              const bonusClientId = bonus.clientId !== undefined ? bonus.clientId : null;
              const matchesClient = bonusClientId === null || bonusClientId === numClientId;
              
              // Enhanced debugging for ALL bonuses for this client
              if (bonusClientId === numClientId || bonusClientId === null) {
                console.log(`[getStockTransactionHistory] Checking bonus for client ${numClientId}:`, {
                  companyName: bonus.companyName,
                  normalizedCompanyName,
                  normalizedStockName,
                  matchesStock,
                  bonusClientId,
                  selectedClientId: numClientId,
                  matchesClient,
                  willMatch: matchesStock && matchesClient,
                  exDate: bonus.exDate,
                  bonusShare: bonus.bonusShare
                });
              }
              
              // Both stock name and client ID must match
              const matches = matchesStock && matchesClient;
              
              // Apply date filter if provided
              // IMPORTANT: Always include bonuses that match stock and client
              // Bonuses represent corporate actions and should be shown regardless of date filter
              // The date filter is primarily for transactions, not bonuses
              if (matches && (req.query.endDate || req.query.trandate_to)) {
                const endDate = String(req.query.endDate || req.query.trandate_to).trim();
                const bonusDate = bonus.exDate; // Use only Ex-Date
                // Log but don't exclude - bonuses should always be shown if they match stock and client
                if (bonusDate && /^\d{4}-\d{2}-\d{2}$/.test(endDate)) {
                  if (bonusDate > endDate) {
                    console.log(`[getStockTransactionHistory] Bonus date ${bonusDate} is after endDate ${endDate}, but including it anyway (bonuses should always be shown)`);
                  }
                }
                // Always return true if matches stock and client, regardless of date
              }
              
              if (matches) {
                console.log(`[getStockTransactionHistory] ✅ MATCHED BONUS: ${bonus.companyName} (ClientId: ${bonusClientId}, Ex-Date: ${bonus.exDate}, BonusShare: ${bonus.bonusShare})`);
              } else {
                // Log why it didn't match for debugging
                if (bonusClientId === numClientId || bonusClientId === null) {
                  console.log(`[getStockTransactionHistory] ❌ BONUS NOT MATCHED: ${bonus.companyName}`, {
                    reason: !matchesStock ? 'Company name mismatch' : 'ClientId mismatch',
                    companyName: bonus.companyName,
                    normalizedCompanyName,
                    normalizedStockName,
                    matchesStock,
                    bonusClientId,
                    numClientId,
                    matchesClient
                  });
                }
              }
              return matches;
            })
            .sort((a, b) => {
              // Sort by Ex-Date ascending (oldest first) for chronological merging
              const dateA = a.exDate || '';
              const dateB = b.exDate || '';
              if (dateA && dateB) {
                return dateA.localeCompare(dateB);
              }
              return 0;
            });
          
          console.log(`[getStockTransactionHistory] Found ${bonuses.length} matching bonuses`);
          
          // Enhanced debugging: Always log bonus matching details
          console.log(`[getStockTransactionHistory] ===== BONUS MATCHING DEBUG =====`);
          console.log(`[getStockTransactionHistory] Requested stock: "${stockName}" (normalized: "${normalizedStockName}")`);
          console.log(`[getStockTransactionHistory] Requested clientId: ${numClientId}`);
          console.log(`[getStockTransactionHistory] Total bonus rows fetched: ${bonusRows.length}`);
          console.log(`[getStockTransactionHistory] Matched bonuses: ${bonuses.length}`);
          
          // Log ALL bonuses for this client to see why they're not matching
          const clientBonuses = bonusRows
            .map((row) => {
              const b = row.Bonus || row[bonusTableName] || row;
              const rawClientId = b.ClientId !== undefined ? b.ClientId : 
                                 (b.clientId !== undefined ? b.clientId : 
                                  (b[`${bonusTableName}.ClientId`] !== undefined ? b[`${bonusTableName}.ClientId`] :
                                   (b['Bonus.ClientId'] !== undefined ? b['Bonus.ClientId'] : null)));
              const clientId = rawClientId !== undefined && rawClientId !== null 
                ? (typeof rawClientId === 'number' ? rawClientId : Number(rawClientId))
                : null;
              return {
                companyName: b.CompanyName || b['CompanyName'] || b[`${bonusTableName}.CompanyName`] || b['Bonus.CompanyName'] || '',
                clientId: clientId,
                exDate: b.ExDate || b['ExDate'] || '',
                bonusShare: b.BonusShare || b['BonusShare'] || 0
              };
            })
            .filter(b => b.clientId === numClientId || b.clientId === null);
          
          console.log(`[getStockTransactionHistory] Bonuses for client ${numClientId}: ${clientBonuses.length}`);
          clientBonuses.slice(0, 20).forEach((b, idx) => {
            const normalized = normalizeName(b.companyName);
            const matches = normalized === normalizedStockName;
            console.log(`[getStockTransactionHistory] Client bonus ${idx + 1}:`, {
              companyName: `"${b.companyName}"`,
              normalized: `"${normalized}"`,
              requestedNormalized: `"${normalizedStockName}"`,
              matches,
              clientId: b.clientId,
              exDate: b.exDate,
              bonusShare: b.bonusShare
            });
          });
          
          if (bonuses.length === 0 && bonusRows.length > 0) {
            console.log(`[getStockTransactionHistory] ⚠️ WARNING: No bonuses matched!`);
            console.log(`[getStockTransactionHistory] This could be due to:`);
            console.log(`[getStockTransactionHistory] 1. Company name mismatch (normalized names don't match)`);
            console.log(`[getStockTransactionHistory] 2. ClientId mismatch`);
            console.log(`[getStockTransactionHistory] 3. Missing Ex-Date`);
          }
          console.log(`[getStockTransactionHistory] ===== END BONUS MATCHING DEBUG =====`);
        } catch (bonusErr) {
          console.error(`[getStockTransactionHistory] Error fetching bonuses:`, bonusErr);
          console.error(`[getStockTransactionHistory] Bonus query that failed: ${bonusQuery}`);
          console.error(`[getStockTransactionHistory] Error details:`, {
            message: bonusErr.message,
            code: bonusErr.code,
            statusCode: bonusErr.statusCode
          });
          // Continue without bonuses if there's an error
        }
      } catch (err) {
        console.error(`[getStockTransactionHistory] Error processing bonuses:`, err);
        // Continue without bonuses if there's an error
      }
    }

    // Helper function to determine if transaction is buy
    const isBuyTransaction = (tranType) => {
      if (!tranType) return false;
      const type = String(tranType).toUpperCase().trim();
      const isBuy = type.startsWith('B') || type === 'BUY' || type === 'PURCHASE' || type.includes('BUY');
      const isSQB = type === 'SQB';
      const isOPI = type === 'OPI';
      // Exclude dividends
      const isDividend = type === 'DIO' || 
                        type === 'DIVIDEND' || 
                        type === 'DIVIDEND REINVEST' || 
                        type === 'DIVIDEND REINVESTMENT' ||
                        type === 'DIVIDEND RECEIVED' ||
                        type.startsWith('DIVIDEND') ||
                        type.includes('DIVIDEND');
      return (isBuy || isSQB || isOPI) && !isDividend;
    };

    // Helper function to determine if transaction is sell
    const isSellTransaction = (tranType) => {
      if (!tranType) return false;
      const type = String(tranType).toUpperCase().trim();
      const isSell = type.startsWith('S') || type === 'SELL' || type === 'SALE' || type.includes('SELL');
      const isSQS = type === 'SQS';
      const isOPO = type === 'OPO';
      const isNF = type === 'NF-' || type.startsWith('NF-');
      return isSell || isSQS || isOPO || isNF;
    };

    // Sort transactions by date in ascending order (oldest first) for chronological journey
    transactions.sort((a, b) => {
      const dateA = a.trandate ? new Date(a.trandate).getTime() : 0;
      const dateB = b.trandate ? new Date(b.trandate).getTime() : 0;
      if (dateA !== dateB) {
        return dateA - dateB; // Ascending order (oldest first)
      }
      // If same date, sort by ROWID for consistent ordering
      return (a.rowid || 0) - (b.rowid || 0);
    });

    // Merge bonuses with transactions chronologically
    // Calculate holdings as we go and insert bonus rows with calculated bonus shares
    // Implement FIFO (First In, First Out) for profit/loss calculation
    const mergedTransactions = [];
    let currentHoldings = 0; // Track holdings chronologically
    
    // FIFO queue: Array of buy lots, each with { quantity, buyPrice }
    // Oldest buys are at the front of the array
    const buyQueue = [];
    
    // Combine transactions and bonuses, then sort by date
      const allEvents = [
        ...transactions.map(t => ({ type: 'transaction', data: t, date: t.trandate })),
        ...bonuses.map(b => {
          // Ensure exDate is valid, use fallback if missing
          const bonusDate = b.exDate && b.exDate.trim() !== '' ? b.exDate : '1900-01-01';
          if (!b.exDate || b.exDate.trim() === '') {
            console.log(`[getStockTransactionHistory] WARNING: Bonus has no ExDate, using fallback date:`, {
              companyName: b.companyName,
              clientId: b.clientId,
              bonusShare: b.bonusShare
            });
          }
          return {
            type: 'bonus', 
            data: b, 
            date: bonusDate
          };
        })
      ];
      
      console.log(`[getStockTransactionHistory] Total events to merge: ${allEvents.length} (${transactions.length} transactions, ${bonuses.length} bonuses)`);
    
    // Sort all events by date
    allEvents.sort((a, b) => {
      const dateA = a.date ? new Date(a.date).getTime() : 0;
      const dateB = b.date ? new Date(b.date).getTime() : 0;
      if (dateA !== dateB) {
        return dateA - dateB;
      }
      // Transactions come before bonuses on the same date
      if (a.type !== b.type) {
        return a.type === 'transaction' ? -1 : 1;
      }
      return 0;
    });

    // Log all events for debugging
    console.log(`[getStockTransactionHistory] Total events to process: ${allEvents.length} (${allEvents.filter(e => e.type === 'transaction').length} transactions, ${allEvents.filter(e => e.type === 'bonus').length} bonuses)`);
    console.log(`[getStockTransactionHistory] Event dates:`, allEvents.map(e => ({
      type: e.type,
      date: e.date,
      isTransaction: e.type === 'transaction' ? `${e.data.tranType} (${e.data.qty})` : 'N/A',
      isBonus: e.type === 'bonus' ? `${e.data.companyName} (${e.data.exDate})` : 'N/A'
    })).slice(0, 20)); // Show first 20 events
    
    // Process events chronologically
    for (const event of allEvents) {
      if (event.type === 'transaction') {
        const transaction = event.data;
        const qty = Math.abs(Number(transaction.qty) || 0);
        // Use netrate for profit calculation, fallback to rate if netrate is not available
        let price = Number(transaction.netrate) || Number(transaction.netRate) || Number(transaction.NETRATE) || 0;
        if (price === 0) {
          price = Number(transaction.rate) || 0;
        }
        // If still 0, calculate from netAmount as last resort
        if (price === 0 && qty > 0 && transaction.netAmount && Math.abs(transaction.netAmount) > 0) {
          price = Math.abs(transaction.netAmount) / qty;
        }
        const holdingsBefore = currentHoldings;
        
        // Update holdings based on transaction type
        const isBuy = isBuyTransaction(transaction.tranType);
        const isSell = isSellTransaction(transaction.tranType);
        
        let profitLoss = null; // Only set for SELL transactions
        
        if (isBuy) {
          // BUY: Add to FIFO queue
          if (qty > 0 && price > 0) {
            buyQueue.push({
              quantity: qty,
              buyPrice: price
            });
            currentHoldings += qty;
            console.log(`[getStockTransactionHistory] BUY: Added ${qty} @ ${price} to queue. Queue length: ${buyQueue.length}`);
          }
        } else if (isSell) {
          // SELL: Consume from FIFO queue and calculate profit/loss
          if (qty > 0 && price > 0) {
            let remainingSellQty = qty;
            let sellCostBasis = 0; // Cost basis for this specific sell
            
            // Consume from buy queue FIFO (oldest first)
            while (remainingSellQty > 0 && buyQueue.length > 0) {
              const oldestLot = buyQueue[0];
              
              if (oldestLot.quantity <= remainingSellQty) {
                // Consume entire lot
                const consumedQty = oldestLot.quantity;
                const lotCost = consumedQty * oldestLot.buyPrice;
                sellCostBasis += lotCost;
                remainingSellQty -= consumedQty;
                buyQueue.shift(); // Remove from queue
                console.log(`[getStockTransactionHistory] SELL: Consumed entire lot ${consumedQty} @ ${oldestLot.buyPrice}, remaining sell qty: ${remainingSellQty}`);
              } else {
                // Consume partial lot
                const consumedFromThisLot = remainingSellQty;
                const lotCost = consumedFromThisLot * oldestLot.buyPrice;
                sellCostBasis += lotCost;
                oldestLot.quantity -= consumedFromThisLot;
                remainingSellQty = 0;
                console.log(`[getStockTransactionHistory] SELL: Consumed partial lot ${consumedFromThisLot} @ ${oldestLot.buyPrice} from lot of ${oldestLot.quantity + consumedFromThisLot}`);
              }
            }
            
            // If we still have remaining sell quantity after consuming all FIFO lots,
            // those remaining shares come from bonus shares (0 cost basis)
            // This means the remaining quantity has 0 cost, so profit = sell price * remaining qty
            if (remainingSellQty > 0) {
              console.log(`[getStockTransactionHistory] SELL: ${remainingSellQty} shares from bonus (0 cost basis)`);
              // No additional cost basis for bonus shares (already 0)
            }
            
            // Calculate profit/loss for this SELL transaction
            const sellValue = qty * price;
            profitLoss = sellValue - sellCostBasis;
            
            currentHoldings -= qty;
            
            console.log(`[getStockTransactionHistory] SELL: Qty=${qty}, Price=${price}, CostBasis=${sellCostBasis}, SellValue=${sellValue}, ProfitLoss=${profitLoss}`);
          } else {
            // If price is 0 or invalid, still reduce holdings but no profit calculation
            currentHoldings -= qty;
          }
        }
        // Dividends don't affect holdings or FIFO queue
        
        // Add profitLoss to transaction object (only for SELL)
        if (isSell && profitLoss !== null) {
          transaction.profitLoss = profitLoss;
        }
        
        // Calculate Average Cost of All Holdings after this transaction
        // Simple formula: WAP × HOLDING
        // Match frontend calculation exactly: WAP = totalCostBasis / holdingAfter
        let averageCostOfHoldings = 0;
        if (currentHoldings > 0) {
          // Calculate WAP exactly like frontend: totalCostBasis / holdingAfter
          // holdingAfter = currentHoldings (sum of all lots in buyQueue)
          const totalCostBasis = buyQueue.reduce((sum, lot) => sum + (lot.quantity * lot.buyPrice), 0);
          const holdingAfter = buyQueue.reduce((sum, lot) => sum + lot.quantity, 0);
          // WAP = totalCostBasis / holdingAfter (same as frontend)
          const wap = holdingAfter > 0 ? (totalCostBasis / holdingAfter) : 0;
          // Average Cost of Holdings = WAP × HOLDING
          averageCostOfHoldings = currentHoldings * wap;
        }
        transaction.averageCostOfHoldings = averageCostOfHoldings;
        
        // Debug logging for all transactions to track holdings (no rounding)
        console.log(
          `[getStockTransactionHistory] Transaction: ${transaction.trandate}, Type: ${transaction.tranType}, Qty: ${qty}, IsBuy: ${isBuy}, IsSell: ${isSell}, Holdings: ${holdingsBefore} -> ${currentHoldings}, ProfitLoss: ${
            profitLoss !== null ? profitLoss : 'N/A'
          }, AvgCostOfHoldings: ${averageCostOfHoldings}`
        );
        
        mergedTransactions.push(transaction);
      } else if (event.type === 'bonus') {
        const bonus = event.data;
        const bonusDate = bonus.exDate; // Use only Ex-Date
        
        console.log(`[getStockTransactionHistory] ===== PROCESSING BONUS =====`);
        console.log(`[getStockTransactionHistory] Bonus Date: ${bonusDate}`);
        console.log(`[getStockTransactionHistory] Current Holdings at this point: ${currentHoldings}`);
        console.log(`[getStockTransactionHistory] Transactions processed so far: ${mergedTransactions.length}`);
        
        // Get bonus share quantity directly from BonusShare field
        // BonusShare now contains the total bonus quantity (no calculation needed)
        // Try multiple ways to access the field in case ZCQL returns it differently
        const rawBonusShare = bonus.bonusShare !== undefined && bonus.bonusShare !== null 
          ? bonus.bonusShare 
          : (bonus.BonusShare !== undefined && bonus.BonusShare !== null ? bonus.BonusShare : 0);
        const bonusShare = Number(rawBonusShare);
        const validBonusShare = !isNaN(bonusShare) && bonusShare >= 0 ? bonusShare : 0;
        
        // BonusShare is the total bonus quantity received
        // No calculation needed - use it directly
        const bonusSharesReceived = validBonusShare;
        
        // Debug logging for bonus calculation
        console.log(`[getStockTransactionHistory] Bonus calculation details:`, {
          exDate: bonusDate,
          companyName: bonus.companyName,
          currentHoldings,
          rawBonusShare: rawBonusShare,
          bonusShareValue: bonus.bonusShare,
          BonusShareValue: bonus.BonusShare,
          allBonusFields: Object.keys(bonus).filter(k => k.toLowerCase().includes('bonus')),
          validBonusShare,
          bonusSharesReceived,
          calculation: `BonusShare = ${validBonusShare} (total bonus quantity)`
        });
        console.log(`[getStockTransactionHistory] ================================`);
        
        // Update holdings after bonus
        const holdingsBeforeBonus = currentHoldings;
        currentHoldings += bonusSharesReceived;
        
        // Add bonus shares to buyQueue with price 0 (to match frontend calculation)
        if (bonusSharesReceived > 0) {
          buyQueue.push({
            quantity: bonusSharesReceived,
            buyPrice: 0
          });
        }
        
        // Create bonus transaction row
        // Ensure qty is always a number (0 if no bonus shares received)
        const bonusQty = Number(bonusSharesReceived) || 0;
        
        const bonusTransaction = {
          wsClientId: numClientId,
          wsAccountCode: null,
          trandate: bonus.exDate, // Use only Ex-Date
          tranType: 'BONUS',
          securityName: stockName,
          securityCode: bonus.securityCode || null,
          exchg: '-',
          qty: bonusQty, // Always a number (total bonus quantity from BonusShare field)
          rate: 0,
          netAmount: 0,
          rowid: 0,
          isBonus: true,
          bonusShare: validBonusShare,
          holdingsBeforeBonus: holdingsBeforeBonus,
          holdingsAfterBonus: currentHoldings
        };
        
        console.log(`[getStockTransactionHistory] Bonus transaction created:`, {
          date: bonusTransaction.trandate,
          qty: bonusTransaction.qty,
          type: bonusTransaction.tranType,
          bonusShare: bonusTransaction.bonusShare,
          holdingsBefore: bonusTransaction.holdingsBeforeBonus,
          holdingsAfter: bonusTransaction.holdingsAfterBonus
        });
        
        // Calculate Average Cost of All Holdings after bonus
        // Simple formula: WAP × HOLDING
        // Match frontend calculation exactly: WAP = totalCostBasis / holdingAfter
        let averageCostOfHoldings = 0;
        if (currentHoldings > 0) {
          // Calculate WAP exactly like frontend: totalCostBasis / holdingAfter
          // holdingAfter = currentHoldings (sum of all lots in buyQueue)
          const totalCostBasis = buyQueue.reduce((sum, lot) => sum + (lot.quantity * lot.buyPrice), 0);
          const holdingAfter = buyQueue.reduce((sum, lot) => sum + lot.quantity, 0);
          // WAP = totalCostBasis / holdingAfter (same as frontend)
          const wap = holdingAfter > 0 ? (totalCostBasis / holdingAfter) : 0;
          // Average Cost of Holdings = WAP × HOLDING
          averageCostOfHoldings = currentHoldings * wap;
        }
        bonusTransaction.averageCostOfHoldings = averageCostOfHoldings;
        
        // Always add bonus transaction, even if qty is 0
        // This ensures the bonus event is visible in the transaction history
        mergedTransactions.push(bonusTransaction);
        
        console.log(`[getStockTransactionHistory] Bonus added to transactions: ${bonusSharesReceived} shares (Holdings: ${holdingsBeforeBonus} -> ${currentHoldings}), AvgCostOfHoldings: ${averageCostOfHoldings.toFixed(2)}`);
        console.log(`[getStockTransactionHistory] Total transactions after adding bonus: ${mergedTransactions.length}`);
      }
    }

    // Use merged transactions instead of original transactions
    const finalTransactions = mergedTransactions;

    // Log summary of transaction types - verify ALL transactions are included
    const buyCount = finalTransactions.filter(
      (t) =>
        t.tranType && String(t.tranType).toUpperCase().trim().startsWith("B")
    ).length;
    const sellCount = finalTransactions.filter(
      (t) =>
        t.tranType && String(t.tranType).toUpperCase().trim().startsWith("S")
    ).length;
    const bonusCount = finalTransactions.filter(
      (t) => t.isBonus === true || (t.tranType && String(t.tranType).toUpperCase().trim() === 'BONUS')
    ).length;
    
    // Log bonus details for verification
    const bonusTransactions = finalTransactions.filter(
      (t) => t.isBonus === true || (t.tranType && String(t.tranType).toUpperCase().trim() === 'BONUS')
    );
    if (bonusCount > 0) {
      console.log(`[getStockTransactionHistory] Bonus transactions details:`, bonusTransactions.map(b => ({
        date: b.trandate,
        qty: b.qty,
        stockName: b.securityName,
        bonusShare: b.bonusShare
      })));
    } else {
      console.log(`[getStockTransactionHistory] WARNING: No bonus transactions found in final response!`);
      console.log(`[getStockTransactionHistory] Total bonuses matched: ${bonuses.length}`);
    }
    const unknownCount = finalTransactions.length - buyCount - sellCount - bonusCount;

    console.log(
      `[getStockTransactionHistory] ===== FINAL TRANSACTION SUMMARY =====`
    );
    console.log(
      `[getStockTransactionHistory] Total transactions returned: ${finalTransactions.length}`
    );
    console.log(`[getStockTransactionHistory] BUY transactions: ${buyCount}`);
    console.log(`[getStockTransactionHistory] SELL transactions: ${sellCount}`);
    console.log(`[getStockTransactionHistory] BONUS transactions: ${bonusCount}`);
    console.log(
      `[getStockTransactionHistory] Other transactions (Dividend, Dividend Reinvest, Dividend Received, etc.): ${unknownCount}`
    );
    console.log(
      `[getStockTransactionHistory] Transactions sorted chronologically (oldest to newest)`
    );
    console.log(
      `[getStockTransactionHistory] ======================================`
    );

    return res.status(200).json(finalTransactions);
  } catch (err) {
    console.error("[getStockTransactionHistory] Error:", err);
    return res.status(500).json({
      message: "Failed to fetch stock transaction history",
      error: String(err && err.message ? err.message : err),
    });
  }
};

// Calculate Weighted Average Cost for holdings (Cost Management)
// OPTIMIZED: Uses aggregation queries for large datasets
// Calculate FIFO-based Weighted Average Cost for holdings (Cost Management)
// Uses aggregation + single-pass FIFO for large datasets
exports.getWeightedAverageCost = async (req, res) => {
  try {
    const app = req.catalystApp;
    const clientId = Number(req.query.ws_client_id);
    if (!clientId) {
      return res.status(400).json({ message: "Client ID required" });
    }

    const zcql = app.zcql();

    const query = `
      SELECT *
      FROM Transaction
      WHERE Transaction.WS_client_id = ${clientId}
      AND Transaction.Security_Name IS NOT NULL
      ORDER BY Transaction.Security_Name ASC,
               Transaction.TRANDATE ASC,
               Transaction.ROWID ASC
    `;

    const rows = await zcql.executeZCQLQuery(query);
    const byStock = {};

    rows.forEach((r) => {
      const t = r.Transaction || r;
      const key = `${t.Security_Name}|${t.Security_code || ""}`;
      if (!byStock[key]) byStock[key] = [];
      byStock[key].push({
        tranType: t.Tran_Type,
        qty: t.QTY,
        netAmount: t.Net_Amount,
      });
    });

    const data = [];

    for (const key of Object.keys(byStock)) {
      const [name, code] = key.split("|");
      const fifo = fifoProcess(byStock[key]);

      if (fifo.holdingQty <= 0) continue;

      data.push({
        stockName: name,
        stockCode: code,
        totalQuantity: fifo.holdingQty,
        remainingCost: fifo.remainingCost, // Use full precision for calculations
        weightedAverageCost: fifo.avgCost, // Use full precision for calculations
      });
    }

    return res.json({
      clientId,
      totalStocks: data.length,
      data,
    });
  } catch (err) {
    return res.status(500).json({
      message: err.message,
    });
  }
};

// Test function to check for specific bonus
exports.checkBonus = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({ message: "Catalyst app context missing" });
    }

    const clientId = parseInt(req.query.clientId || req.params.clientId, 10);
    const companyName = req.query.companyName || req.params.companyName;

    if (!clientId || !companyName) {
      return res.status(400).json({ 
        message: "Both clientId and companyName are required",
        example: "/api/stocks/check-bonus?clientId=8800046&companyName=Astral Ltd."
      });
    }

    const zcql = app.zcql();
    const bonusTableName = 'Bonus';
    const bonusQuery = `SELECT * FROM ${bonusTableName}`;

    console.log(`[checkBonus] Checking for clientId: ${clientId}, companyName: "${companyName}"`);

    const bonusRows = await zcql.executeZCQLQuery(bonusQuery, []);
    console.log(`[checkBonus] Total bonus records in database: ${bonusRows.length}`);

    // Normalize function
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

    for (const row of bonusRows) {
      const b = row.Bonus || row[bonusTableName] || row;
      
      // Extract ClientId
      const rawClientId = b.ClientId !== undefined ? b.ClientId :
                         (b.clientId !== undefined ? b.clientId :
                          (b[`${bonusTableName}.ClientId`] !== undefined ? b[`${bonusTableName}.ClientId`] :
                           (b['Bonus.ClientId'] !== undefined ? b['Bonus.ClientId'] : null)));
      
      const bonusClientId = rawClientId !== undefined && rawClientId !== null
        ? (typeof rawClientId === 'number' ? rawClientId : Number(rawClientId))
        : null;

      // Extract CompanyName
      const bonusCompanyName = b.CompanyName || b['CompanyName'] || b[`${bonusTableName}.CompanyName`] || b['Bonus.CompanyName'] || '';
      const normalizedBonusName = normalizeName(bonusCompanyName);

      // Check if matches
      const matchesClient = bonusClientId === null || bonusClientId === clientId;
      const matchesCompany = normalizedBonusName === normalizedCompanyName;

      if (matchesClient && matchesCompany) {
        const bonusShare = b.BonusShare || b['BonusShare'] || b[`${bonusTableName}.BonusShare`] || b['Bonus.BonusShare'] || 0;
        const exDate = b.ExDate || b['ExDate'] || b[`${bonusTableName}.ExDate`] || b['Bonus.ExDate'] || '';
        const securityCode = b.SecurityCode || b['SecurityCode'] || b[`${bonusTableName}.SecurityCode`] || b['Bonus.SecurityCode'] || '';

        matchingBonuses.push({
          ROWID: b.ROWID || b.rowid,
          ClientId: bonusClientId,
          CompanyName: bonusCompanyName,
          SecurityCode: securityCode,
          ExDate: exDate,
          BonusShare: bonusShare,
          wsAccountCode: b.wsAccountCode || b['wsAccountCode'] || null,
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

    return res.status(200).json({
      clientId,
      companyName,
      normalizedCompanyName,
      totalBonusesInDB: bonusRows.length,
      matchingBonuses: matchingBonuses.length,
      bonuses: matchingBonuses
    });

  } catch (err) {
    console.error(`[checkBonus] Error:`, err);
    return res.status(500).json({ message: err.message, error: err.toString() });
  }
};

// Export transactions and bonuses to Excel for a client
exports.exportClientTransactionsToExcel = async (req, res) => {
  try {
    const app = req.catalystApp;
    if (!app) {
      return res.status(500).json({ message: "Catalyst app context missing" });
    }

    const wsAccountCode = req.query.wsAccountCode || req.query.accountCode;
    const securityName = req.query.securityName || req.query.stockName;
    
    if (!wsAccountCode) {
      return res.status(400).json({ message: "wsAccountCode is required" });
    }
    
    if (!securityName) {
      return res.status(400).json({ message: "securityName is required" });
    }

    const zcql = app.zcql();
    const XLSX = require('xlsx');

    console.log(`[exportClientTransactionsToExcel] Starting export for wsAccountCode: ${wsAccountCode}, securityName: ${securityName}`);

    // Step 1: Get clientId from wsAccountCode
    const clientIdsQuery = `SELECT * FROM clientIds WHERE clientIds.ws_account_code = '${String(wsAccountCode).trim().replace(/'/g, "''")}' LIMIT 1`;
    const clientIdsRows = await zcql.executeZCQLQuery(clientIdsQuery, []);
    
    if (!clientIdsRows || clientIdsRows.length === 0) {
      return res.status(404).json({ message: `No client found for wsAccountCode: ${wsAccountCode}` });
    }

    const clientRow = clientIdsRows[0].clientIds || clientIdsRows[0];
    const clientId = Number(clientRow.clientId || clientRow.ClientId || clientRow.client_id);
    
    if (!clientId || isNaN(clientId)) {
      return res.status(400).json({ message: `Invalid clientId for wsAccountCode: ${wsAccountCode}` });
    }

    console.log(`[exportClientTransactionsToExcel] Found clientId: ${clientId} for wsAccountCode: ${wsAccountCode}`);

    // Helper function for name normalization
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

    // Step 2: Fetch bonuses for this client and security
    const bonusTableName = 'Bonus';
    const bonusBatchSize = 250;
    let bonusOffset = 0;
    let bonusHasMore = true;
    const allBonusRows = [];
    const normalizedSecurityName = normalizeName(securityName);

    while (bonusHasMore) {
      const bonusQuery = `SELECT * FROM ${bonusTableName} WHERE ${bonusTableName}.ClientId = ${clientId} LIMIT ${bonusBatchSize} OFFSET ${bonusOffset}`;
      const rows = await zcql.executeZCQLQuery(bonusQuery, []);
      
      if (!rows || rows.length === 0) {
        bonusHasMore = false;
        break;
      }
      
      // Filter bonuses that match the security name
      rows.forEach(row => {
        const b = row.Bonus || row[bonusTableName] || row;
        const bonusCompanyName = b.CompanyName || b['CompanyName'] || b['Bonus.CompanyName'] || '';
        const normalizedBonusName = normalizeName(bonusCompanyName);
        
        // Match by normalized name or exact name
        if (normalizedBonusName === normalizedSecurityName || 
            bonusCompanyName.trim() === securityName.trim()) {
          allBonusRows.push(row);
        }
      });
      
      if (rows.length < bonusBatchSize) {
        bonusHasMore = false;
      } else {
        bonusOffset += bonusBatchSize;
        if (bonusOffset > 100000) bonusHasMore = false;
      }
    }

    if (allBonusRows.length === 0) {
      return res.status(404).json({ 
        message: `No bonus records found for client ${clientId} (wsAccountCode: ${wsAccountCode}) and security: ${securityName}` 
      });
    }

    console.log(`[exportClientTransactionsToExcel] Found ${allBonusRows.length} bonus records for security: ${securityName}`);

    const isBuyTransaction = (tranType) => {
      if (!tranType) return false;
      const type = String(tranType).toUpperCase().trim();
      const isBuy = type.startsWith('B') || type === 'BUY' || type === 'PURCHASE' || type.includes('BUY');
      const isSQB = type === 'SQB';
      const isOPI = type === 'OPI';
      const isDividend = type === 'DIO' || 
                        type === 'DIVIDEND' || 
                        type === 'DIVIDEND REINVEST' || 
                        type === 'DIVIDEND REINVESTMENT' ||
                        type === 'DIVIDEND RECEIVED' ||
                        type.startsWith('DIVIDEND') ||
                        type.includes('DIVIDEND');
      return (isBuy || isSQB || isOPI) && !isDividend;
    };

    const isSellTransaction = (tranType) => {
      if (!tranType) return false;
      const type = String(tranType).toUpperCase().trim();
      const isSell = type.startsWith('S') || type === 'SELL' || type === 'SALE' || type.includes('SELL');
      const isSQS = type === 'SQS';
      const isOPO = type === 'OPO';
      const isNF = type === 'NF-' || type.startsWith('NF-');
      return isSell || isSQS || isOPO || isNF;
    };

    // Step 3: Process transactions and bonuses for the specified security
    const tableName = 'Transaction';
    const allExcelRows = [];

    console.log(`[exportClientTransactionsToExcel] Processing security: ${securityName}`);
    
    // Fetch all transactions for this security
    const escapedStockName = String(securityName).trim().replace(/'/g, "''");
    const whereClause = `WHERE ${tableName}.WS_client_id = ${clientId} AND ${tableName}.Security_Name = '${escapedStockName}'`;
    
    const allTransactions = [];
    let txOffset = 0;
    let txHasMore = true;
    
    while (txHasMore) {
      const txQuery = `SELECT * FROM ${tableName} ${whereClause} ORDER BY ${tableName}.TRANDATE ASC, ${tableName}.ROWID ASC LIMIT 250 OFFSET ${txOffset}`;
      const txRows = await zcql.executeZCQLQuery(txQuery, []);
      
      if (!txRows || txRows.length === 0) {
        txHasMore = false;
        break;
      }
      
      // Flatten and format transactions
      txRows.forEach(row => {
        const t = row.Transaction || row[tableName] || row;
        const flat = flattenRow(t);
        const qty = toNumber(flat.QTY || flat.qty || flat.Qty);
        const netAmount = toNumber(flat.Net_Amount || flat.net_amount || flat.netAmount || flat.NetAmount);
        
        // Extract netrate first (for profit calculation), then fallback to rate
        let netrate = toNumber(flat.NETRATE || flat.netrate || flat.netRate || flat.NetRate);
        let rate = toNumber(flat.RATE || flat.rate || flat.Rate || flat.Price || flat.price || flat.PRICE);
        
        // Use netrate if available, otherwise use rate
        let price = netrate > 0 ? netrate : rate;
        
        // If price is still 0 or missing, calculate from netAmount / qty
        if ((price === 0 || isNaN(price)) && qty > 0 && netAmount > 0) {
          price = netAmount / qty;
        }
        
        allTransactions.push({
          trandate: flat.TRANDATE || flat.trandate || '',
          tranType: flat.Tran_Type || flat.tran_type || flat.TRAN_TYPE || '',
          securityName: flat.Security_Name || flat.security_name || securityName,
          securityCode: flat.Security_code || flat.security_code || '',
          qty: qty,
          rate: rate,
          netrate: netrate,
          netAmount: netAmount,
          rowid: flat.ROWID || flat.rowid || 0
        });
      });
      
      if (txRows.length < 250) {
        txHasMore = false;
      } else {
        txOffset += 250;
      }
    }

    // Format matching bonuses
    const matchingBonuses = [];
    
    allBonusRows.forEach(row => {
      const b = row.Bonus || row[bonusTableName] || row;
      const bonusCompanyName = b.CompanyName || b['CompanyName'] || b['Bonus.CompanyName'] || '';
      matchingBonuses.push({
        exDate: b.ExDate || b['ExDate'] || b['Bonus.ExDate'] || '',
        companyName: bonusCompanyName,
        bonusShare: toNumber(b.BonusShare || b['BonusShare'] || b['Bonus.BonusShare'] || 0),
        securityCode: b.SecurityCode || b['SecurityCode'] || b['Bonus.SecurityCode'] || ''
      });
    });

      // Sort transactions by date
      allTransactions.sort((a, b) => {
        const dateA = a.trandate ? new Date(a.trandate).getTime() : 0;
        const dateB = b.trandate ? new Date(b.trandate).getTime() : 0;
        if (dateA !== dateB) return dateA - dateB;
        return (a.rowid || 0) - (b.rowid || 0);
      });

      // Merge transactions and bonuses chronologically
      const allEvents = [
        ...allTransactions.map(t => ({ type: 'transaction', data: t, date: t.trandate })),
        ...matchingBonuses.map(b => ({ 
          type: 'bonus', 
          data: b, 
          date: b.exDate && b.exDate.trim() ? b.exDate : '1900-01-01'
        }))
      ];

      allEvents.sort((a, b) => {
        const dateA = a.date ? new Date(a.date).getTime() : 0;
        const dateB = b.date ? new Date(b.date).getTime() : 0;
        if (dateA !== dateB) return dateA - dateB;
        return a.type === 'transaction' ? -1 : 1; // Transactions before bonuses on same date
      });

      // Process events chronologically and calculate holdings, WAP, etc.
      let currentHoldings = 0;
      const buyQueue = []; // FIFO queue: [{ quantity, buyPrice }]

      for (const event of allEvents) {
        if (event.type === 'transaction') {
          const tx = event.data;
          const qty = Math.abs(tx.qty || 0);
          // Use netrate for profit calculation, fallback to rate if netrate is not available
          let price = Math.abs(tx.netrate || tx.netRate || tx.NETRATE || 0);
          if (price === 0) {
            price = Math.abs(tx.rate || 0);
          }
          // If still 0, try to calculate from netAmount as last resort
          if (price === 0 && qty > 0 && tx.netAmount && Math.abs(tx.netAmount) > 0) {
            price = Math.abs(tx.netAmount) / qty;
          }
          
          const holdingsBefore = currentHoldings;
          const isBuy = isBuyTransaction(tx.tranType);
          const isSell = isSellTransaction(tx.tranType);
          let profitLoss = null;
          let wap = 0;
          let avgCostOfHoldings = 0;

          if (isBuy && qty > 0) {
            // For buy transactions, use price if available, otherwise calculate from netAmount
            const buyPrice = price > 0 ? price : (qty > 0 && tx.netAmount && Math.abs(tx.netAmount) > 0 ? Math.abs(tx.netAmount) / qty : 0);
            if (buyPrice > 0) {
              buyQueue.push({ quantity: qty, buyPrice: buyPrice });
              currentHoldings += qty;
            } else if (qty > 0) {
              // If no price but we have quantity, still add to holdings (might be OPI with 0 cost)
              // Don't add to buyQueue if price is 0 (no cost basis)
              currentHoldings += qty;
            }
          } else if (isSell && qty > 0) {
            // For sell transactions, calculate profit/loss
            let remainingSellQty = qty;
            let sellCostBasis = 0;

            // Consume from FIFO queue
            while (remainingSellQty > 0 && buyQueue.length > 0) {
              const oldestLot = buyQueue[0];
              
              if (oldestLot.quantity <= remainingSellQty) {
                const consumedQty = oldestLot.quantity;
                sellCostBasis += consumedQty * oldestLot.buyPrice;
                remainingSellQty -= consumedQty;
                buyQueue.shift();
              } else {
                const consumedFromThisLot = remainingSellQty;
                sellCostBasis += consumedFromThisLot * oldestLot.buyPrice;
                oldestLot.quantity -= consumedFromThisLot;
                remainingSellQty = 0;
              }
            }

            // Calculate sell value
            const sellValue = price > 0 ? (qty * price) : Math.abs(tx.netAmount || 0);
            profitLoss = sellValue - sellCostBasis;
            currentHoldings -= qty;
            if (currentHoldings < 0) currentHoldings = 0; // Safety check
          } else {
            // Other transaction types (dividends, etc.) - don't affect holdings
            // But still show in the export
          }

          // Calculate WAP and Avg Cost of Holdings (always calculate if we have holdings)
          if (currentHoldings > 0) {
            const totalCostBasis = buyQueue.reduce((sum, lot) => sum + (lot.quantity * lot.buyPrice), 0);
            const holdingAfter = buyQueue.reduce((sum, lot) => sum + lot.quantity, 0);
            // WAP = total cost basis / total holdings (including bonus shares)
            wap = currentHoldings > 0 ? (totalCostBasis / currentHoldings) : 0;
            avgCostOfHoldings = currentHoldings * wap;
          }

          // Calculate display price (use calculated price if original was 0)
          const displayPrice = price > 0 ? price : (qty > 0 && tx.netAmount ? Math.abs(tx.netAmount) / qty : 0);

          // Add to Excel rows
          allExcelRows.push({
            DATE: tx.trandate || '',
            TYPE: tx.tranType || '',
            'STOCK NAME': tx.securityName || securityName,
            QUANTITY: qty,
            // Use full precision for numeric values; Excel can format as needed.
            PRICE: displayPrice > 0 ? displayPrice : 0,
            'TOTAL AMOUNT': Math.abs(tx.netAmount || qty * displayPrice),
            HOLDING: currentHoldings,
            WAP: wap > 0 ? wap : '-',
            'AVG COST OF HOLDINGS': avgCostOfHoldings > 0 ? avgCostOfHoldings : '-',
            'P/L': profitLoss !== null ? profitLoss : '-',
          });

        } else if (event.type === 'bonus') {
          const bonus = event.data;
          const bonusShare = Math.abs(bonus.bonusShare || 0);
          
          if (bonusShare > 0) {
            // Bonus shares have 0 cost, so WAP decreases
            currentHoldings += bonusShare;
            
            // Recalculate WAP (cost basis stays same, but spread over more shares)
            let wap = 0;
            let avgCostOfHoldings = 0;
            if (currentHoldings > 0) {
              const totalCostBasis = buyQueue.reduce((sum, lot) => sum + (lot.quantity * lot.buyPrice), 0);
              wap = totalCostBasis / currentHoldings;
              avgCostOfHoldings = currentHoldings * wap;
            }

            allExcelRows.push({
              DATE: bonus.exDate || '',
              TYPE: 'BONUS',
              'STOCK NAME': bonus.companyName || securityName,
              QUANTITY: bonusShare,
              PRICE: 0,
              'TOTAL AMOUNT': 0,
              HOLDING: currentHoldings,
              WAP: wap > 0 ? wap : '-',
              'AVG COST OF HOLDINGS': avgCostOfHoldings > 0 ? avgCostOfHoldings : '-',
              'P/L': '-',
            });
          }
        }
      }

    if (allExcelRows.length === 0) {
      return res.status(404).json({ message: `No transaction data found for client ${clientId}` });
    }

    // Step 4: Generate Excel file
    const worksheet = XLSX.utils.json_to_sheet(allExcelRows);
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, 'Transactions');

    // Generate buffer
    const excelBuffer = XLSX.write(workbook, { type: 'buffer', bookType: 'xlsx' });

    // Set response headers for file download
    const safeSecurityName = String(securityName).replace(/[^a-zA-Z0-9]/g, '_').substring(0, 30);
    const fileName = `client_${wsAccountCode}_${safeSecurityName}_${new Date().toISOString().split('T')[0]}.xlsx`;
    res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
    res.setHeader('Content-Disposition', `attachment; filename="${fileName}"`);
    res.setHeader('Content-Length', excelBuffer.length);

    console.log(`[exportClientTransactionsToExcel] Generated Excel with ${allExcelRows.length} rows for client ${clientId}`);

    // Send the file
    res.send(excelBuffer);

  } catch (err) {
    console.error(`[exportClientTransactionsToExcel] Error:`, err);
    return res.status(500).json({ 
      message: err.message, 
      error: err.toString(),
      stack: err.stack 
    });
  }
};
