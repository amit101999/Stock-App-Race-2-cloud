const Express = require("express");
const port = process.env.X_ZOHO_CATALYST_LISTEN_PORT || 9000;
const catalyst = require('zcatalyst-sdk-node');
const cors = require('cors');
const expressApp = Express();

// Disable etag so progress polling doesn't get 304 Not Modified
expressApp.set('etag', false);

expressApp.use(cors({
	origin: 'http://localhost:3000',
	credentials: true,
	methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
	allowedHeaders: ['Content-Type', 'Authorization', 'Cache-Control', 'Pragma', 'Expires'],
}));
expressApp.use(Express.json());

// Inject Catalyst app per request (following Catalyst SDK pattern)
expressApp.use((req, res, next) => {
	try {
		const app = catalyst.initialize(req);
		// This app variable is used to access the catalyst components
		req.catalystApp = app;
		next();
	} catch (err) {
		console.error('Catalyst initialization error:', err);
		req.catalystApp = null;
		next();
	}
});

// Global error handler
expressApp.use((err, req, res, next) => {
	console.error('Unhandled error:', err);
	res.status(500).json({
		error: 'Internal server error',
		message: err.message || 'An unexpected error occurred'
	});
});

// Health check route
expressApp.get('/', (req, res) => {
	// Catalyst app is already available via middleware (req.catalystApp)
	res.status(200).json({ 
		status: 'ok', 
		service: 'server', 
		message: 'Catalyst Express backend is running' 
	});
});

// API Routes
expressApp.use('/api/stocks', require('./routes/stocks'));
expressApp.use('/api/import', require('./routes/import'));

expressApp.listen(port, () => {
  console.log(`Example app listening on port ${port}`);
  console.log(`http://localhost:${port}/`);
});



