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
    const qty = Number(t.qty) || 0;
    const value = Number(t.netAmount) || 0;
    if (!qty || qty <= 0) continue;

    /* ===== BUY ===== */
    if (type.startsWith("B")) {
      const unitCost = qty !== 0 ? value / qty : 0;
      buyQueue.push({ qty, unitCost });
      buyQty += qty;
      buyValue += value;
      availableQty += qty;
    } else if (type.startsWith("S")) {

    /* ===== SELL ===== */
      sellQty += qty;
      sellValue += value;

      let remainingToMatch = Math.min(qty, availableQty); // we can only sell what we have
      let unmatchedExtra = qty - remainingToMatch; // this part is assumed from opening balance (outside dataset)

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
          `[fifoProcess] Sell ${qty} but only ${
            qty - unmatchedExtra
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
        avgTradeValue:
          totalTrades > 0 ? Math.round(totalNetAmount / totalTrades) : 0,
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
    const tableName = sanitizeIdentifier(req.query.table || DEFAULT_TABLE);

    const zcql = app.zcql();

    // Use DISTINCT query to get unique client IDs
    const query = `SELECT DISTINCT ${tableName}.WS_client_id FROM ${tableName}`;
    const rows = await zcql.executeZCQLQuery(query, []);

    // Extract client IDs from results
    const clientIds = [];
    if (rows && rows.length > 0) {
      rows.forEach((row) => {
        // Handle different ZCQL result formats
        const clientId =
          row.WS_client_id ||
          row[`${tableName}.WS_client_id`] ||
          (row[tableName] && row[tableName].WS_client_id) ||
          (row.Transaction && row.Transaction.WS_client_id);
        if (clientId && String(clientId).trim() !== "") {
          clientIds.push(String(clientId).trim());
        }
      });
    }

    // Sort and return unique client IDs
    const uniqueData = [...new Set(clientIds)].sort();

    return res.status(200).json(uniqueData);
  } catch (err) {
    console.error("[getClientIds] Error:", err);
    return res.status(500).json({
      message: "Failed to fetch client ids",
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
    const query = `select distinct Security_code from ${tableName} where Security_code is not null`;
    const rows = await zcql.executeZCQLQuery(query, []);
    // Handle different ZCQL result formats
    const data = rows
      .map((r) => {
        return (
          r.Security_code ||
          r[`${tableName}.Security_code`] ||
          (r[tableName] && r[tableName].Security_code) ||
          (r.Transaction && r.Transaction.Security_code)
        );
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
    // Optional as-of date filter
    if (req.query.endDate || req.query.trandate_to) {
      const endDate = String(req.query.endDate || req.query.trandate_to).trim();
      if (/^\d{4}-\d{2}-\d{2}$/.test(endDate)) {
        where += ` AND ${tableName}.TRANDATE <= '${endDate}'`;
      }
    }

    // Exclude pseudo/non-equity rows
    where += ` AND ${tableName}.Security_Name IS NOT NULL`;
    where += ` AND ${tableName}.Security_Name NOT IN (
      'CASH',
      'Tax Deducted at Source',
      'TAX',
      'TDS',
      'TAX DEDUCTED AT SOURCE'
    )`;

    const query = `
      SELECT *
      FROM ${tableName}
      ${where}
      ORDER BY ${tableName}.Security_Name ASC,
               ${tableName}.Security_code ASC,
               ${tableName}.TRANDATE ASC,
               ${tableName}.ROWID ASC
    `;

    console.log("[getHoldingsSummary] Query:", query);

    const rows = await zcql.executeZCQLQuery(query, []);
    console.log(
      `[getHoldingsSummary] Total transactions fetched for client ${clientId}: ${
        rows ? rows.length : 0
      }`
    );

    const byStock = {};

    (rows || []).forEach((r) => {
      const t = r.Transaction || r[tableName] || r;
      const stockName = t.Security_Name;
      const stockCode = t.Security_code || "";

      if (!stockName) return;

      const key = `${stockName}|${stockCode}`;
      if (!byStock[key]) byStock[key] = [];

      byStock[key].push({
        tranType: t.Tran_Type,
        qty: t.QTY,
        netAmount: t.Net_Amount,
        trandate: t.TRANDATE,
        rowid: t.ROWID,
      });
    });

    const result = [];

    for (const key of Object.keys(byStock)) {
      const [stockName, stockCode] = key.split("|");

      const fifo = fifoProcess(byStock[key]);

      // Only keep stocks with positive holdings
      if (fifo.holdingQty <= 0) continue;

      result.push({
        stockName,
        stockCode,
        currentHolding: fifo.holdingQty,
        totalBuyQty: fifo.buyQty,
        totalSellQty: fifo.sellQty,
        totalBuyAmount: fifo.buyValue,
        totalSellAmount: fifo.sellValue,
        weightedAverageBuyPrice: fifo.avgCost,
        profit: fifo.profit,
      });
    }

    // Sort alphabetically
    result.sort((a, b) => a.stockName.localeCompare(b.stockName));

    console.log(
      `[getHoldingsSummary] Final holdings for client ${clientId}: ${result.length} stocks`
    );

    return res.status(200).json(result);
  } catch (err) {
    console.error("[getHoldingsSummary] Error:", err);
    return res.status(500).json({
      message: "Failed to fetch holdings summary",
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
    const stockName = decodeURIComponent(req.params.stockName); // URL parameter

    if (!clientId || !stockName) {
      return res
        .status(400)
        .json({ message: "Client ID and Stock Name are required" });
    }

    const tableName = sanitizeIdentifier(req.query.table || DEFAULT_TABLE);
    const zcql = app.zcql();

    // Validate client ID
    const clientIdValue = String(clientId).trim();
    if (!/^\d+$/.test(clientIdValue)) {
      return res.status(400).json({ message: "Invalid client ID format" });
    }
    const numClientId = parseInt(clientIdValue, 10);

    // Escape single quotes in stock name (ZCQL v2 requirement)
    const escapedStockName = String(stockName).trim().replace(/'/g, "''");

    // Build WHERE clause - Fetch ALL transactions (both BUY and SELL) for this stock
    // No filtering by Tran_Type - we want to show everything
    let whereClause = `WHERE ${tableName}.WS_client_id = ${numClientId} AND ${tableName}.Security_Name = '${escapedStockName}'`;

    // Add date filter if provided
    if (req.query.endDate || req.query.trandate_to) {
      const endDate = String(req.query.endDate || req.query.trandate_to).trim();
      if (/^\d{4}-\d{2}-\d{2}$/.test(endDate)) {
        whereClause += ` AND ${tableName}.TRANDATE <= '${endDate}'`;
      }
    }

    console.log(
      `[getStockTransactionHistory] Fetching ALL transactions (BUY + SELL) for client ${numClientId}, stock: ${stockName}`
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

    // Verify we have both BUY and SELL transactions
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
    const transactions = allTransactions.map((row, index) => {
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
    });

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

    // Log summary of transaction types - verify ALL transactions are included
    const buyCount = transactions.filter(
      (t) =>
        t.tranType && String(t.tranType).toUpperCase().trim().startsWith("B")
    ).length;
    const sellCount = transactions.filter(
      (t) =>
        t.tranType && String(t.tranType).toUpperCase().trim().startsWith("S")
    ).length;
    const unknownCount = transactions.length - buyCount - sellCount;

    console.log(
      `[getStockTransactionHistory] ===== FINAL TRANSACTION SUMMARY =====`
    );
    console.log(
      `[getStockTransactionHistory] Total transactions returned: ${transactions.length}`
    );
    console.log(`[getStockTransactionHistory] BUY transactions: ${buyCount}`);
    console.log(`[getStockTransactionHistory] SELL transactions: ${sellCount}`);
    console.log(
      `[getStockTransactionHistory] Unknown/Other transactions: ${unknownCount}`
    );
    console.log(
      `[getStockTransactionHistory] Transactions sorted chronologically (oldest to newest)`
    );
    console.log(
      `[getStockTransactionHistory] ======================================`
    );

    // Verify we're returning all transactions (no filtering)
    if (transactions.length !== totalFetched) {
      console.warn(
        `[getStockTransactionHistory] WARNING: Transaction count mismatch! Fetched=${totalFetched}, Returned=${transactions.length}`
      );
    }

    return res.status(200).json(transactions);
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
        remainingCost: Number(fifo.remainingCost.toFixed(2)),
        weightedAverageCost: Number(fifo.avgCost.toFixed(2)),
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
