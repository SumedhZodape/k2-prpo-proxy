// server.js - Production-Ready Configuration
const cds = require('@sap/cds');
const cors = require('cors');
const express = require('express');

// =======================================================================
// 🛡️ GLOBAL ERROR HANDLERS
// =======================================================================
process.on('uncaughtException', (error) => {
    console.error('🚨 FATAL: Uncaught Exception', error.stack || error.message);
    process.exit(1);
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('⚠️ CRITICAL: Unhandled Promise Rejection', reason);
    process.exit(1);
});

// =======================================================================
// 🌍 CORS Configuration - Centralized
// =======================================================================
const corsOptions = {
    // origin: function (origin, callback) {
    //     // Allow requests with no origin (mobile apps, Postman, etc.)
    //     if (!origin) return callback(null, true);
        
    //     const allowedOrigins = [
    //         '*',
    //         'http://localhost:3000',          // React/Vite local frontend
    //         'http://127.0.0.1:5500',          // VSCode Live Server
    //         'http://127.0.0.1:8080',          // CAP local UI or HTML preview
    //         'https://your-deployed-ui-domain.com', // Add your real UI domain
    //         'http://127.0.0.1:80',  
    //         'http://localhost:80' ,
    //         'http://localhost' ,
    //         'https://devksl-3bk1u0k3.launchpad.cfapps.ap11.hana.ondemand.com/',
    //         'http://devksl-3bk1u0k3.launchpad.cfapps.ap11.hana.ondemand.com',
    //         'http://kuok_app.cfapps.ap11.hana.ondemand.com',
    //         'https://kuok_app.cfapps.ap11.hana.ondemand.com/'
    //     ];
        
    //     // Allow all origins in development
    //     if (process.env.NODE_ENV !== 'production') {
    //         return callback(null, true);
    //     }
        
    //     if (allowedOrigins.indexOf(origin) !== -1 || origin.includes('localhost')) {
    //         callback(null, true);
    //     } else {
    //         callback(new Error('Not allowed by CORS'));
    //     }
    // },
    origin: '*',
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS', 'PATCH'],
    allowedHeaders: [
        'Origin',
        'X-Requested-With',
        'Content-Type',
        'Accept',
        'Authorization',
        'x-csrf-token',
        'X-CSRF-Token',
        'Cookie'
    ],
    exposedHeaders: ['x-csrf-token', 'set-cookie'],
    credentials: true,
    maxAge: 86400, // 24 hours - cache preflight requests
    optionsSuccessStatus: 200
};

// =======================================================================
// 🧩 Bootstrap Phase - BEFORE CAP Routes
// =======================================================================
cds.on('bootstrap', (app) => {
    console.log('🚀 Initializing CAP Server with custom middleware...');
    
    // 1. Apply CORS FIRST (before any other middleware)
    app.use(cors(corsOptions));
    
    // 2. Handle preflight requests explicitly
    app.options('*', cors(corsOptions));
    
    // 3. Increase payload limits for file uploads
    app.use(express.json({ limit: '50mb' }));
    app.use(express.urlencoded({ extended: true, limit: '50mb' }));
    
    // 4. Request logging middleware
    app.use((req, res, next) => {
        const start = Date.now();
        res.on('finish', () => {
            const duration = Date.now() - start;
            console.log(`[${req.method}] ${req.path} - ${res.statusCode} (${duration}ms)`);
        });
        next();
    });
    
    // 5. Security headers
    app.use((req, res, next) => {
        res.setHeader('X-Content-Type-Options', 'nosniff');
        res.setHeader('X-Frame-Options', 'SAMEORIGIN');
        res.setHeader('X-XSS-Protection', '1; mode=block');
        next();
    });
    
    console.log('✅ Global middleware configured');
});

// =======================================================================
// 🚀 Served Phase - AFTER CAP Routes
// =======================================================================
cds.on('served', () => {
    const app = cds.app;
    
    // Configure server timeouts
    app.on('listening', ({ server }) => {
        server.keepAliveTimeout = 150000; // 2.5 minutes
        server.headersTimeout = 155000;   // Slightly higher than keepAlive
        server.timeout = 150000;
        console.log('✅ Server timeouts configured: 150s');
    });
    
    // Health check endpoint
    app.get('/health', (req, res) => {
        res.status(200).json({ 
            status: 'UP', 
            timestamp: new Date().toISOString(),
            uptime: process.uptime()
        });
    });
    
    // Readiness probe
    app.get('/ready', (req, res) => {
        res.status(200).json({ ready: true });
    });
    
    console.log('✅ Server routes initialized');
});

module.exports = cds.server;