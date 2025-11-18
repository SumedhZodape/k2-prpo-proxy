const cds = require('@sap/cds');
const express = require('express');
const multer = require('multer');
const path = require('path');


// =======================================================================
// 🔧 REQUEST QUEUE for Concurrent API Calls
// =======================================================================
class RequestQueue {
    constructor(maxConcurrent = 5) {
        this.maxConcurrent = maxConcurrent;
        this.running = 0;
        this.queue = [];
    }

    async add(fn) {
        while (this.running >= this.maxConcurrent) {
            await new Promise(resolve => this.queue.push(resolve));
        }
        this.running++;

        try {
            return await fn();
        } finally {
            this.running--;
            const resolve = this.queue.shift();
            if (resolve) resolve();
        }
    }
}

// Create queue with max 5 concurrent S/4 requests
const s4RequestQueue = new RequestQueue(5);

// =======================================================================
// 🔧 CSRF Token Cache (Prevents token conflicts)
// =======================================================================
class CSRFTokenCache {
    constructor() {
        this.tokens = new Map();
        this.locks = new Map();
    }

    async get(service) {
        const cacheKey = service;
        const cached = this.tokens.get(cacheKey);

        // Return cached token if valid (within 5 minutes)
        if (cached && (Date.now() - cached.timestamp) < 300000) {
            return cached;
        }

        // Acquire lock to prevent parallel token fetches
        if (this.locks.has(cacheKey)) {
            await this.locks.get(cacheKey);
            return this.tokens.get(cacheKey);
        }

        // Set lock
        let resolveLock;
        const lockPromise = new Promise(resolve => { resolveLock = resolve; });
        this.locks.set(cacheKey, lockPromise);

        try {
            // Fetch new token
            const { executeHttpRequest } = require('@sap-cloud-sdk/http-client');
            const csrfResponse = await executeHttpRequest(
                { destinationName: 'S4-API' },
                {
                    method: 'GET',
                    url: service,
                    headers: {
                        'X-CSRF-Token': 'Fetch',
                        'Accept': 'application/json'
                    },
                    timeout: 30000
                }
            );

            const token = {
                csrfToken: csrfResponse.headers['x-csrf-token'],
                cookies: csrfResponse.headers['set-cookie'],
                timestamp: Date.now()
            };

            this.tokens.set(cacheKey, token);
            return token;
        } finally {
            this.locks.delete(cacheKey);
            resolveLock();
        }
    }

    clear(service) {
        this.tokens.delete(service);
    }
}

const csrfCache = new CSRFTokenCache();

// =======================================================================
// 🔧 Multer Configuration
// =======================================================================
const upload = multer({
    storage: multer.memoryStorage(),
    limits: {
        fileSize: 50 * 1024 * 1024,
        files: 10
    }
});

// =======================================================================
// 🔒 BASIC AUTH CREDENTIALS
// =======================================================================
const REQUIRED_USERNAME = 'MARK9BATCH';
const REQUIRED_PASSWORD = 'dsfhi@gf43$#@';

function basicAuthMiddleware(req, res, next) {
    const authHeader = req.headers.authorization;

    if (!authHeader || !authHeader.startsWith('Basic ')) {
        res.setHeader('WWW-Authenticate', 'Basic realm="CAP Proxy API"');
        return res.status(401).json({ success: false, message: 'Authentication required.' });
    }

    try {
        const encodedCreds = authHeader.substring(6);
        const decodedCreds = Buffer.from(encodedCreds, 'base64').toString('utf8');
        const [username, password] = decodedCreds.split(':');

        if (username === REQUIRED_USERNAME && password === REQUIRED_PASSWORD) {
            next();
        } else {
            res.setHeader('WWW-Authenticate', 'Basic realm="CAP Proxy API"');
            return res.status(401).json({ success: false, message: 'Invalid credentials.' });
        }
    } catch (e) {
        res.setHeader('WWW-Authenticate', 'Basic realm="CAP Proxy API"');
        return res.status(401).json({ success: false, message: 'Malformed Authorization header.' });
    }
}

// =======================================================================
// 🔸 S/4 REQUEST HELPER with Retry Logic
// =======================================================================
async function executeS4Request(config, retries = 2) {
    const { executeHttpRequest } = require('@sap-cloud-sdk/http-client');

    for (let attempt = 1; attempt <= retries; attempt++) {
        try {
            return await s4RequestQueue.add(() =>
                executeHttpRequest({ destinationName: 'S4-API' }, config)
            );
        } catch (error) {
            console.error(`[S4 Request] Attempt ${attempt}/${retries} failed:`, error.message);

            // Retry on network errors or 5xx status codes
            if (attempt < retries &&
                (error.code === 'ECONNRESET' ||
                    error.code === 'ETIMEDOUT' ||
                    (error.response?.status >= 500 && error.response?.status < 600))) {

                // Exponential backoff
                await new Promise(resolve => setTimeout(resolve, 1000 * attempt));
                continue;
            }

            throw error;
        }
    }
}


module.exports = cds.service.impl(async function () {

    const app = cds.app;

    // REMOVE THIS - Don't add middleware here, it's already in server.js
    // app.use(express.json({ limit: '10mb' }));
    // app.use(express.urlencoded({ extended: true, limit: '10mb' }));

    // =======================================================================
    // 🔹 FILE ATTACHMENT ENDPOINTS
    // =======================================================================

    app.get('/', (req, res) => {
        res.sendFile(path.join(__dirname, '../app/upload.html'));
    });

    app.get('/upload.html', (req, res) => {
        res.sendFile(path.join(__dirname, '../app/upload.html'));
    });


    app.post('/odata/v4/attachment/upload', upload.array('media', 10), async (req, res) => {
        try {
            const businessPartner = req.body.BPnumber;
            const files = req.files;

            if (!businessPartner) {
                return res.status(400).json({
                    success: false,
                    message: 'BPnumber is required in form data'
                });
            }

            if (!files || files.length === 0) {
                return res.status(400).json({
                    success: false,
                    message: 'At least one file is required in form data'
                });
            }

            console.log(`Processing ${files.length} file(s) for Business Partner: ${businessPartner}`);

            // Get CSRF token using cache
            const tokenData = await csrfCache.get('/sap/opu/odata/sap/ZAPI_PO_ATTACH_SRV/AttachmentSet');

            if (!tokenData.csrfToken) {
                throw new Error(`Failed to fetch CSRF token`);
            }

            const successfulUploads = [];
            const failedUploads = [];

            // Process files sequentially to avoid overwhelming S/4
            for (let i = 0; i < files.length; i++) {
                const file = files[i];
                const fileName = file.originalname;
                const fileBuffer = file.buffer;
                const mimeType = file.mimetype;
                const fileExtension = fileName.substring(fileName.lastIndexOf('.'));

                try {
                    const slug = `${businessPartner}/${businessPartner}_${i + 1}${fileExtension}`;

                    const postResponse = await executeS4Request({
                        method: 'POST',
                        url: '/sap/opu/odata/sap/ZAPI_PO_ATTACH_SRV/AttachmentSet',
                        headers: {
                            'X-CSRF-Token': tokenData.csrfToken,
                            'Slug': slug,
                            'Content-Type': mimeType,
                            'Cookie': Array.isArray(tokenData.cookies) ? tokenData.cookies.join('; ') : tokenData.cookies
                        },
                        data: fileBuffer,
                        timeout: 90000
                    });

                    successfulUploads.push({
                        fileName: fileName,
                        fileSize: fileBuffer.length,
                        mimeType: mimeType,
                        status: 'success',
                        details: postResponse.data
                    });

                } catch (fileError) {
                    console.error(`File upload failed: ${fileName}`, fileError.message);
                    failedUploads.push({
                        fileName: fileName,
                        fileSize: fileBuffer.length,
                        mimeType: mimeType,
                        status: 'failed',
                        error: fileError.message
                    });
                }
            }

            const totalFiles = files.length;
            const successCount = successfulUploads.length;
            const failCount = failedUploads.length;

            if (failCount === 0) {
                return res.status(200).json({
                    success: true,
                    message: `All ${successCount} file(s) attached successfully`,
                    summary: { total: totalFiles, successful: successCount, failed: failCount },
                    uploads: successfulUploads
                });
            } else if (successCount === 0) {
                return res.status(500).json({
                    success: false,
                    message: `Failed to upload all files`,
                    summary: { total: totalFiles, successful: successCount, failed: failCount },
                    uploads: failedUploads
                });
            } else {
                return res.status(207).json({
                    success: true,
                    message: `Uploaded ${successCount} of ${totalFiles} files`,
                    summary: { total: totalFiles, successful: successCount, failed: failCount },
                    successfulUploads: successfulUploads,
                    failedUploads: failedUploads
                });
            }

        } catch (error) {
            console.error('File upload error:', error);
            return res.status(500).json({
                success: false,
                message: `Failed to process file upload: ${error.message}`
            });
        }
    });

    // =======================================================================
    // 🔹 TRANSACTIONAL ENDPOINTS (PO/PR)
    // =======================================================================


    // --- 1. PO Type Determination Logic (Preserved) ---
    const determinePurchaseOrderType = (companyId, budgeted) => {
        if (!companyId || !String(companyId).trim() || !budgeted) {
            return "";
        }
        const companyIdUpper = String(companyId).trim().toUpperCase();
        const budgetedValue = String(budgeted).trim();
        const isBudgeted = budgetedValue.toUpperCase() === "YES" ||
            budgetedValue.toUpperCase() === "Y" ||
            budgetedValue.toUpperCase() === "TRUE";

        if (["ZB01", "ZP01", "ZP03", "ZC01"].includes(companyIdUpper)) {
            return isBudgeted ? "ZINA" : "ZINB";
        }
        if (["ZA01", "ZA02"].includes(companyIdUpper)) {
            return isBudgeted ? "ZINF" : "ZING";
        }
        return "";
    };
    // -----------------------------------------------------------------


    // --- 2. Purchasing Organization Logic (Preserved) ---
    const determinePurchasingOrganisation = (companyId) => {
        if (!companyId || !String(companyId).trim()) {
            return "";
        }
        const companyIdUpper = String(companyId).trim().toUpperCase();

        if (companyIdUpper === "ZB01") {
            return "ZB01";
        }
        if (companyIdUpper === "ZC01") {
            return "ZC01";
        }
        if (["ZP01", "ZP03"].includes(companyIdUpper)) {
            return "ZP01";
        }
        if (["ZA01", "ZA02"].includes(companyIdUpper)) {
            return "ZA01";
        }
        return "";
    };
    // -----------------------------------------------------------------


    // --- 3. Condition Rate Value Logic (Preserved) ---
    const determineConditionRateValue = (conditionType, discount, discountAmt, lumpsumDiscount, lumpsumDiscountAmt) => {
        if (!conditionType || !String(conditionType).trim()) {
            return "0";
        }
        const conditionTypeUpper = String(conditionType).trim().toUpperCase();

        if (conditionTypeUpper === "RA00") {
            return String(discount || 0);
        }
        if (conditionTypeUpper === "RB00") {
            return String(discountAmt || 0);
        }
        if (conditionTypeUpper === "HA00") {
            return String(lumpsumDiscount || 0);
        }
        if (conditionTypeUpper === "HB00") {
            return String(lumpsumDiscountAmt || 0);
        }
        return "0";
    };
    // -----------------------------------------------------------------


    const convertDateToODataFormat = (dateString) => {
        if (!dateString) return null;
        const date = new Date(dateString);
        if (isNaN(date)) return null;
        return `/Date(${date.getTime()})/`;
    };


    // CREATE PO
    app.post('/http/post/data', basicAuthMiddleware, async (req, res) => {
        try {
            console.log('=== PO Creation Started ===');
            const poPayload = req.body;

            // Validation check
            if (!poPayload || !poPayload.context || !poPayload.context.prRequisitionInputs) {
                return res.status(400).json({
                    success: false,
                    message: 'Invalid payload. Expecting poPayload.context.prRequisitionInputs.',
                    receivedPayload: poPayload
                });
            }

            // Extract the source data object
            const sourceData = poPayload.context.prRequisitionInputs;

            // --- Logic Determinations ---
            const poType = determinePurchaseOrderType(sourceData.CompanyId, sourceData.Budgeted);
            const purchasingOrg = determinePurchasingOrganisation(sourceData.CompanyId);
            const poDateOData = convertDateToODataFormat(sourceData.Estimated_DeliveryDate); // Assuming header date comes from a source field

            // --- Dynamic Item Mapping ---
            const itemResults = sourceData.Item && sourceData.Item.length > 0
                ? sourceData.Item.map((item, index) => {
                    const itemNumber = String((index + 1) * 10).padStart(5, '0');
                    const conditionType = item.ConditionType || ""; // Assuming ConditionType is on the item level

                    // Determine Pricing Value
                    const conditionRateValue = determineConditionRateValue(
                        conditionType,
                        item.Discount,
                        item.DiscountAmt,
                        sourceData.LumpsumDiscount,
                        sourceData.LumpsumDiscountAmt
                    );

                    // Build Pricing Element (only if rate is not zero)
                    const pricingElement = (conditionRateValue !== "0" && conditionType)
                        ? {
                            to_PurchaseOrderPricingElement: {
                                results: [{
                                    ConditionType: conditionType,
                                    ConditionRateValue: conditionRateValue,
                                }]
                            }
                        }
                        : {}; // Empty object if no pricing element is needed

                    // Build Schedule Line
                    const scheduleLineOData = convertDateToODataFormat(item.LineEstDelivDate);
                    const scheduleLine = {
                        to_ScheduleLine: {
                            results: [{
                                ScheduleLineDeliveryDate: scheduleLineOData,
                            }]
                        }
                    };

                    // Build Account Assignment
                    const accountAssignment = {
                        to_AccountAssignment: {
                            results: [
                                {
                                    PurchaseOrder: "",
                                    PurchaseOrderItem: itemNumber,
                                    AccountAssignmentNumber: "1",
                                    Quantity: String(item.Quantity || 0),
                                    GLAccount: item.GLaccount || "",
                                    CostCenter: item.CostCenter || "",
                                    MasterFixedAsset: item.AssetCode || "",
                                    OrderID: item.NominalCode || ""
                                }
                            ]
                        }
                    };

                    // Return the complete item payload
                    return {
                        "PurchaseOrder": "",
                        "PurchaseOrderItem": itemNumber, // Dynamic Counter
                        "Plant": sourceData.CompanyId || "",
                        "ProductType": "1",
                        "MaterialGroup": item.MaterialGroup || "",
                        "OrderQuantity": String(item.Quantity || 0),
                        "NetPriceAmount": String(item.UnitPrice || 0),
                        "OrderPriceUnit": "EA",
                        "DocumentCurrency": sourceData.Currency_Code || "",
                        "NetPriceQuantity": "1",
                        "RequisitionerName": sourceData.RequestorName || "",
                        "PurchaseOrderItemText": item.ItemDescription || "",
                        "AccountAssignmentCategory": (item.AssetAccountAssignmentCategory && item.AssetAccountAssignmentCategory.toUpperCase() === "A")
                            ? "A"
                            : "K",
                        "GoodsReceiptIsNonValuated": true,
                        "PurchaseOrderItemCategory": "0",
                        "PurchaseOrderQuantityUnit": "EA",
                        "OrderPriceUnitToOrderUnitNmrtr": "1",
                        "OrdPriceUnitToOrderUnitDnmntr": "1",
                        "GoodsReceiptIsExpected": true,
                        "ReferenceDeliveryAddressID": sourceData.Delivery_Address || "",
                        "InvoiceIsGoodsReceiptBased": true,
                        ...accountAssignment, // Nested structure
                        ...pricingElement,  // Nested structure
                        ...scheduleLine     // Nested structure
                    };
                })
                : []; // Empty array if no items exist


            // --- Final Payload Construction (A_PurchaseOrder) ---
            const mainPayload = {
                "A_PurchaseOrder": {
                    "PurchaseOrder": "",
                    "PurchaseOrderType": poType,
                    "CompanyCode": sourceData.CompanyId || "",
                    "Supplier": sourceData.Vendor_Recommendation || "",
                    "Language": "EN",
                    "PaymentTerms": "0030",
                    "PurchasingGroup": sourceData.PurchasingGroup || "",
                    "DocumentCurrency": sourceData.Currency_Code || "",
                    "PurchaseOrderDate": convertDateToODataFormat(new Date()),
                    "PurchasingOrganization": purchasingOrg, // Determined dynamically
                    "PurchasingDocumentOrigin": "9",
                    "SupplierRespSalesPersonName": `${sourceData.PRNumber || ''} - Created`,
                    "to_PurchaseOrderItem": {
                        "results": itemResults // Populated by the dynamic mapping
                    },
                    "ReleaseIsNotCompleted": true,
                    "PurchasingCompletenessStatus": false
                }
            };




            const poResponse = await executeS4Request({
                method: 'POST',
                url: '/sap/opu/odata/sap/API_PURCHASEORDER_PROCESS_SRV/A_PurchaseOrder',
                headers: {
                    'X-Requested-With': 'X',
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                },
                data: mainPayload.A_PurchaseOrder,
                timeout: 90000
            });

            const createdPOData = poResponse.data?.d || poResponse.data;
            const poNumber = createdPOData?.PurchaseOrder || 'Unknown';

            console.log('✓ PO Created:', poNumber);

            // The desired final structure
            const transformedResponse = {
                A_PurchaseOrder: {
                    A_PurchaseOrderType: {}
                }
            };

            // Get the inner object for easier mapping
            const targetObject = transformedResponse.A_PurchaseOrder.A_PurchaseOrderType;

            // Mapping properties from createdPOData to targetObject
            // Note: Data types (string, boolean) might need conversion/coercion 
            // to match the exact string format of your desired output.

            // Example Mapping (Based on your desired output keys):
            targetObject.CreationDate = createdPOData.CreationDate ? convertODataDateToISO(createdPOData.CreationDate) : ''; // Function needed for date conversion
            targetObject.CreatedByUser = createdPOData.CreatedByUser || '';
            targetObject.IsEndOfPurposeBlocked = createdPOData.IsEndOfPurposeBlocked || '';
            // Convert boolean to string "false" or "true" if necessary, otherwise use the value or an empty string
            targetObject.PurchasingCompletenessStatus = String(createdPOData.PurchasingCompletenessStatus || false);
            targetObject.CashDiscount1Days = createdPOData.CashDiscount1Days || '0';
            targetObject.PurchaseOrderType = createdPOData.PurchaseOrderType || '';
            targetObject.PurchasingOrganization = createdPOData.PurchasingOrganization || '';
            targetObject.PurchasingDocumentDeletionCode = createdPOData.PurchasingDocumentDeletionCode || '';
            targetObject.NetPaymentDays = createdPOData.NetPaymentDays || '0';
            targetObject.ManualSupplierAddressID = createdPOData.ManualSupplierAddressID || '';
            targetObject.IncotermsVersion = createdPOData.IncotermsVersion || '';
            targetObject.AddressRegion = createdPOData.AddressRegion || '';
            targetObject.PurchasingGroup = createdPOData.PurchasingGroup || '';
            targetObject.IncotermsClassification = createdPOData.IncotermsClassification || '';
            targetObject.AddressName = createdPOData.AddressName || '';
            targetObject.InvoicingParty = createdPOData.InvoicingParty || '';
            targetObject.SupplyingPlant = createdPOData.SupplyingPlant || '';
            targetObject.PurchasingDocumentOrigin = createdPOData.PurchasingDocumentOrigin || '';
            targetObject.AddressCityName = createdPOData.AddressCityName || '';
            targetObject.AddressStreetName = createdPOData.AddressStreetName || '';
            targetObject.CashDiscount2Percent = createdPOData.CashDiscount2Percent || '0.000';
            targetObject.ValidityStartDate = createdPOData.ValidityStartDate ? convertODataDateToISO(createdPOData.ValidityStartDate) : '';
            targetObject.ExchangeRate = createdPOData.ExchangeRate || '0.00000';
            targetObject.SupplyingSupplier = createdPOData.SupplyingSupplier || '';
            targetObject.PaymentTerms = createdPOData.PaymentTerms || '';
            targetObject.AddressCountry = createdPOData.AddressCountry || '';
            targetObject.AddressPostalCode = createdPOData.AddressPostalCode || '';
            targetObject.PurchaseOrderSubtype = createdPOData.PurchaseOrderSubtype || '';
            targetObject.Language = createdPOData.Language || '';
            targetObject.SupplierRespSalesPersonName = createdPOData.SupplierRespSalesPersonName || '';
            targetObject.SupplierQuotationExternalID = createdPOData.SupplierQuotationExternalID || '';
            targetObject.Supplier = createdPOData.Supplier || '';
            targetObject.ValidityEndDate = createdPOData.ValidityEndDate ? convertODataDateToISO(createdPOData.ValidityEndDate) : '';
            targetObject.IncotermsLocation2 = createdPOData.IncotermsLocation2 || '';
            targetObject.IncotermsLocation1 = createdPOData.IncotermsLocation1 || '';
            targetObject.AddressFaxNumber = createdPOData.AddressFaxNumber || '';
            targetObject.AddressPhoneNumber = createdPOData.AddressPhoneNumber || '';
            targetObject.AddressCorrespondenceLanguage = createdPOData.AddressCorrespondenceLanguage || '';
            targetObject.DocumentCurrency = createdPOData.DocumentCurrency || '';
            targetObject.ReleaseIsNotCompleted = String(createdPOData.ReleaseIsNotCompleted || false);
            // Note: Your desired format shows 'PurchaseOrderDate' field holding the PO number:
            // "PurchaseOrderDate": "6900000858", 
            // If this is a mistake and it should be the date, use 'convertODataDateToISO(createdPOData.PurchaseOrderDate)'.
            // Based *strictly* on your desired output, we map the PO number here:
            targetObject.PurchaseOrderDate = createdPOData.PurchaseOrderDate; // Mapped to PO number based on the example.

            targetObject.PurchasingProcessingStatus = createdPOData.PurchasingProcessingStatus || '';
            targetObject.PurchaseOrder = poNumber;
            targetObject.LastChangeDateTime = createdPOData.LastChangeDateTime ? convertODataDateToISO(createdPOData.LastChangeDateTime) : '';
            targetObject.SupplierPhoneNumber = createdPOData.SupplierPhoneNumber || '';
            targetObject.CashDiscount2Days = createdPOData.CashDiscount2Days || '0';
            targetObject.CompanyCode = createdPOData.CompanyCode || '';
            targetObject.CashDiscount1Percent = createdPOData.CashDiscount1Percent || '0.000';
            targetObject.AddressHouseNumber = createdPOData.AddressHouseNumber || '';

            // This helper function is crucial for transforming SAP/OData date format to standard ISO 8601 string.
            // You MUST define this function somewhere in your file.
            function convertODataDateToISO(odataDateString) {
                if (!odataDateString || odataDateString.includes('null')) return '';
                // Extracts the millisecond timestamp from /Date(1762732800000)/
                const match = odataDateString.match(/\/Date\((\d+)\)\//);
                if (match) {
                    const timestamp = parseInt(match[1], 10);
                    // Create Date object and format to ISO-like string
                    return new Date(timestamp).toISOString().split('.')[0] + '.000';
                }
                return ''; // Return empty string for invalid dates
            }

            // Return the transformed response structure
            return res.status(201).json(transformedResponse);

        } catch (error) {
            console.error('PO creation error:', error);
            return res.status(500).json({
                success: false,
                message: `Failed to create PO: ${error.message}`,
                error: error.response?.data || error.message
            });
        }
    });

    app.post("/odata/v4/pr/generate", basicAuthMiddleware, async (req, res) => {
        try {
            console.log('=== PR Number Generation Started ===');
            const payload = req.body;

            if (!payload) {
                return res.status(400).json({
                    success: false,
                    message: 'Invalid payload. Request body is required.'
                });
            }

            const prNumberResponse = await executeS4Request({
                method: 'POST',
                url: '/sap/opu/odata/sap/ZAPI_001_K2_PR_NUMBER_SRV_01/ZAPIS_K2PR_NUMSet',
                headers: {
                    'X-Requested-With': 'X',
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                },
                data: payload,
                timeout: 90000
            });

            const createdPR = prNumberResponse.data?.d || prNumberResponse.data;
            const prNumber = createdPR?.PRNumber || createdPR?.PurchaseRequisition || createdPR?.Number || 'Unknown Generated Number';

            console.log('✓ PR Number Generated:', prNumber);

            return res.status(201).json({
                success: true,
                message: `PR Number ${prNumber} generated successfully.`,
                prNumber: prNumber,
                data: createdPR
            });

        } catch (error) {
            console.error('PR generation error:', error);
            return res.status(500).json({
                success: false,
                message: `Failed to generate PR number: ${error.message}`,
                error: error.response?.data || error.message
            });
        }
    });

    // 2️⃣1️⃣ Delegate Approver POST
    app.post("/odata/v4/proxy/postdeligate", basicAuthMiddleware, async (req, res) => {
        const fullServiceUrl = '/sap/opu/odata/sap/ZAPI_DEL_APPROVER_SRV';
        const postEntityUrl = `${fullServiceUrl}/UpdateReqSet`;

        try {
            console.log('=== Delegate Approver POST Started ===');
            const payload = req.body;

            if (!payload || Object.keys(payload).length === 0) {
                return res.status(400).json({
                    success: false,
                    message: 'Invalid payload. Request body is required for delegation.'
                });
            }

            // Get CSRF token using cache
            const tokenData = await csrfCache.get(`${fullServiceUrl}/UpdateReqSet`);

            if (!tokenData.csrfToken) {
                throw new Error(`Failed to fetch CSRF token for delegation service.`);
            }

            const postResponse = await executeS4Request({
                method: 'POST',
                url: postEntityUrl,
                headers: {
                    'X-CSRF-Token': tokenData.csrfToken,
                    'Content-Type': 'application/json',
                    'Accept': 'application/json',
                    'Cookie': Array.isArray(tokenData.cookies) ? tokenData.cookies.join('; ') : tokenData.cookies
                },
                data: payload,
                timeout: 90000
            });

            const postedData = postResponse.data?.d || postResponse.data;

            console.log('✓ Delegate Approver POST Success');

            return res.status(201).json({
                success: true,
                message: `Delegate Approver record posted successfully.`,
                data: postedData
            });

        } catch (error) {
            console.error('Delegate approver error:', error);
            const status = error.response?.status || 500;
            return res.status(status).json({
                success: false,
                message: `Failed to post Delegate Approver data: ${error.message}`,
                error: error.response?.data || error.message
            });
        }
    });



    // =======================================================================
    // 🔸 S/4 OData Proxy Helper (Optimized for Filtering/Pagination)
    // =======================================================================

    async function fetchFromS4(url, entity, req, res) {
        try {
            const queryParams = [];

            // 1. Filter Handling
            const filterValue = req.query.filter || req.query['$filter'];
            if (filterValue) {
                queryParams.push(`$filter=${encodeURIComponent(filterValue)}`);
            }

            // 2. Pagination Handling
            const top = req.query.top || req.query.limit || '3000';
            queryParams.push(`$top=${top}`);

            if (req.query.skip) {
                queryParams.push(`$skip=${req.query.skip}`);
            }

            // 3. Select Handling
            if (req.query.select) {
                queryParams.push(`$select=${encodeURIComponent(req.query.select)}`);
            }

            // 4. Construct Final URL
            let fullUrl = url;
            if (queryParams.length > 0) {
                fullUrl = `${url}?${queryParams.join('&')}`;
            }

            console.log(`[S4 Proxy] Fetching ${entity}. S/4 URL: ${fullUrl}`);

            const s4Response = await executeS4Request({
                method: 'GET',
                url: fullUrl,
                headers: { 'Accept': 'application/json' },
                timeout: 120000
            });

            let responseData;
            if (s4Response.data?.d?.results) {
                responseData = s4Response.data.d.results;
            } else if (Array.isArray(s4Response.data?.value)) {
                responseData = s4Response.data.value;
            } else {
                responseData = s4Response.data;
            }

            console.log(`[S4 Proxy] ${entity} fetch success. Records: ${responseData.length || 'Unknown'}`);

            return res.status(200).json({
                success: true,
                message: `Fetched ${entity} data successfully`,
                entity: entity,
                data: responseData
            });

        } catch (error) {
            console.error(`[S4 Proxy] Fetch error for ${entity}:`, error.message);
            const status = error.response?.status || 500;
            return res.status(status).json({
                success: false,
                message: `Failed to fetch ${entity} data: ${error.message}`,
                errorDetails: error.response?.data || error.message
            });
        }
    }

    async function fetchFromS4_POST_FAL(url, entity, req, res) {
        try {
            const payload = req.body;
            const filterParts = [];

            if (!payload || Object.keys(payload).length === 0) {
                return res.status(400).json({ success: false, message: 'Request body with filter parameters is required.' });
            }

            if (payload.Currency) {
                filterParts.push(`Waers eq '${payload.Currency}'`);
            }
            if (payload.CompanyCode) {
                filterParts.push(`Bukrs eq '${payload.CompanyCode}'`);
            }
            if (payload.PurchasingGroup) {
                filterParts.push(`Ekgrp eq '${payload.PurchasingGroup}'`);
            }
            if (payload.Amount) {
                filterParts.push(`Netwr eq ${Number(payload.Amount)}`);
            }

            const filterString = filterParts.join(' and ');
            const queryParams = [`$filter=${encodeURIComponent(filterString)}`, `$top=3000`];

            let fullUrl = `${url}?${queryParams.join('&')}`;

            console.log(`[S4 Proxy FAL POST] Fetching ${entity}. S/4 URL: ${fullUrl}`);

            const s4Response = await executeS4Request({
                method: 'GET',
                url: fullUrl,
                headers: { 'Accept': 'application/json' },
                timeout: 120000
            });

            let responseData = s4Response.data?.d?.results || s4Response.data?.value || s4Response.data;

            console.log(`[S4 Proxy FAL POST] ${entity} fetch success. Records: ${responseData.length || 'Unknown'}`);

            const approvers = {};
            for (let i = 1; i <= 8; i++) {
                const stepData = responseData.find(item => item.Stepn === i.toString());
                const existKey = `L${i}Exist`;
                const emailKey = `L${i}email`;

                if (stepData) {
                    approvers[existKey] = "true";
                    approvers[emailKey] = stepData.SmtpAddr ? stepData.SmtpAddr.toLowerCase() : "";
                } else {
                    approvers[existKey] = "false";
                    approvers[emailKey] = "";
                }
            }

            return res.status(200).json({
                Approvers: approvers
            });

        } catch (error) {
            console.error(`[S4 Proxy FAL POST] Fetch error for ${entity}:`, error.message);
            const status = error.response?.status || 500;
            return res.status(status).json({
                success: false,
                message: `Failed to fetch ${entity} data: ${error.message}`,
                errorDetails: error.response?.data || error.message
            });
        }
    }

    // =======================================================================
    // 🔹 S/4 OData Proxy Endpoints (GET)
    // =======================================================================

    // Note: All endpoints now utilize the robust filter/top/skip logic in fetchFromS4

    // 1️⃣ Company Codes
    app.get("/odata/v4/proxy/getcompanycode", basicAuthMiddleware, (req, res) => {
        return fetchFromS4('/sap/opu/odata/sap/ZAPI_COMPCODE_CDS/ZAPI_COMPCODE', 'ZAPI_COMPCODE', req, res);
    });

    // 2️⃣ Suppliers
    app.get("/odata/v4/proxy/getsupplier", basicAuthMiddleware, (req, res) => {
        return fetchFromS4('/sap/opu/odata/sap/ZAPI_SUPPLIER_CDS/ZAPI_SUPPLIER', 'ZAPI_SUPPLIER', req, res);
    });

    // 3️⃣ GL Accounts (Critical for filtering)
    app.get("/odata/v4/proxy/getglaccount", basicAuthMiddleware, (req, res) => {
        return fetchFromS4('/sap/opu/odata/sap/ZAPI_GLACCOUNT_CDS/ZAPI_GLACCOUNT', 'ZAPI_GLACCOUNT', req, res);
    });

    // 4️⃣ Cost Centers
    app.get("/odata/v4/proxy/getcostcenter", basicAuthMiddleware, (req, res) => {
        return fetchFromS4('/sap/opu/odata/sap/ZAPI_COSTCENTER_CDS/ZAPI_COSTCENTER', 'ZAPI_COSTCENTER', req, res);
    });

    // 5️⃣ Asset Numbers
    app.get("/odata/v4/proxy/getassetnumber", basicAuthMiddleware, (req, res) => {
        return fetchFromS4('/sap/opu/odata/sap/ZAPI_ASSET_NUM_CDS/ZAPI_ASSET_NUM', 'ZAPI_ASSET_NUM', req, res);
    });

    // 6️⃣ Internal Orders
    app.get("/odata/v4/proxy/getinternalorder", basicAuthMiddleware, (req, res) => {
        return fetchFromS4('/sap/opu/odata/sap/ZAPI_INT_ORDER_CDS/ZAPI_INT_ORDER', 'ZAPI_INT_ORDER', req, res);
    });

    // 7️⃣ PR/PO Data - UI + BPA
    app.get("/odata/v4/proxy/getprpodata", basicAuthMiddleware, async (req, res) => {
        try {
            await fetchFromS4('/sap/opu/odata/sap/ZAPI_PRPO_CDS/ZAPI_PRPO', 'ZAPI_PRPO', req, {
                status: (code) => ({
                    json: async (data) => {
                        if (data.data && Array.isArray(data.data)) {
                            data.data.sort((a, b) => {
                                const dateA = a.badat && a.badat !== "00000000" ? new Date(a.badat) : new Date(0);
                                const dateB = b.badat && b.badat !== "00000000" ? new Date(b.badat) : new Date(0);

                                return dateA - dateB;
                            });
                        }
                        res.status(code).json(data);
                    }
                })
            });
        } catch (error) {
            console.error("Error fetching or sorting PR/PO data:", error);
            res.status(500).json({ success: false, message: "Internal server error." });
        }
    });

    // 8️⃣ Currencies
    app.get("/odata/v4/proxy/getcurrency", basicAuthMiddleware, (req, res) => {
        return fetchFromS4('/sap/opu/odata/sap/ZAPI_CURRENCY_CDS/ZAPI_CURRENCY', 'ZAPI_CURRENCY', req, res);
    });

    // 9️⃣ BTP 1251 Data
    app.get("/odata/v4/proxy/getbtp1251data", basicAuthMiddleware, (req, res) => {
        return fetchFromS4('/sap/opu/odata/sap/ZAPIT_BTP_1251_SRV/ZAPIT_BTP_1251Set', 'ZAPIT_BTP_1251Set', req, res);
    });

    // 🔟 PO FAL Data - UI + BPA
    app.get("/odata/v4/proxy/getpofaldata", basicAuthMiddleware, (req, res) => {
        return fetchFromS4('/sap/opu/odata/sap/ZAPI_001_PO_FAL_SRV/FALSet', 'FALSet', req, res);
    });

    // 1️⃣1️⃣ Threshold Value
    app.get("/odata/v4/proxy/getthrshld", basicAuthMiddleware, (req, res) => {
        return fetchFromS4('/sap/opu/odata/sap/ZAPI_PO_THRSHLD_CDS/ZAPI_PO_THRSHLD', 'ZAPI_PO_THRSHLD', req, res);
    });

    // 1️⃣2 Reason of cancellation
    app.get("/odata/v4/proxy/getcclreas", basicAuthMiddleware, (req, res) => {
        return fetchFromS4('/sap/opu/odata/sap/ZAPI_PO_CCLREAS_CDS/ZAPI_PO_CCLREAS', 'ZAPI_PO_CCLREAS', req, res);
    });

    // 1️⃣3 Mandatory GL Accounts
    app.get("/odata/v4/proxy/getglNc", basicAuthMiddleware, (req, res) => {
        return fetchFromS4('/sap/opu/odata/sap/ZAPI_PO_GL_NC_CDS/ZAPI_PO_GL_NC', 'ZAPI_PO_GL_NC', req, res);
    });


    // 1️⃣4 Delegate Approver
    app.get("/odata/v4/proxy/getdelapprover", basicAuthMiddleware, (req, res) => {
        return fetchFromS4('/sap/opu/odata/sap/Z_SAP_SUBSTITUT_CDS/Z_SAP_SUBSTITUT', 'Z_SAP_SUBSTITUT', req, res);
    });

    // 1️⃣5 Purchase Order Process - BPA
    app.get("/odata/v4/proxy/getpurchaseorder", basicAuthMiddleware, (req, res) => {
        return fetchFromS4('/sap/opu/odata/sap/API_PURCHASEORDER_PROCESS_SRV/A_POSubcontractingComponent', 'A_POSubcontractingComponent', req, res);
    });

    // 1️⃣6 PO Fal - BPA
    app.get("/odata/v4/proxy/getpofal", basicAuthMiddleware, (req, res) => {
        return fetchFromS4('/sap/opu/odata/sap/ZAPI_002_PO_FAL_SRV/FALSet', 'FALSet', req, res);
    });

    // 1️⃣7 Material Document - BPA
    app.get("/odata/v4/proxy/getmtrldocument", basicAuthMiddleware, (req, res) => {
        return fetchFromS4('/sap/opu/odata/sap/API_MATERIAL_DOCUMENT_SRV/A_MaterialDocumentHeader', 'A_MaterialDocumentHeader', req, res);
    });

    // 1️⃣8 Delivery Address
    app.get("/odata/v4/proxy/getdeliveryaddr", basicAuthMiddleware, (req, res) => {
        return fetchFromS4('/sap/opu/odata/sap/ZAPI_PO_DELV_AD_CDS/ZAPI_PO_DELV_AD', 'ZAPI_PO_DELV_AD', req, res);
    });

    // MATKL GL account
    app.get("/odata/v4/proxy/getMaterialGroup", basicAuthMiddleware, (req, res) => {
        return fetchFromS4('/sap/opu/odata/sap/ZAPI_MATKL_GL_CDS/ZAPI_MATKL_GL', 'ZAPI_MATKL_GL', req, res);
    });



    // GET FAL - BPA
    app.post("/http/Get/FALSet", basicAuthMiddleware, (req, res) => {
        // Use a new helper function designed for POST requests
        return fetchFromS4_POST_FAL('/sap/opu/odata/sap/ZAPI_001_PO_FAL_SRV/FALSet', 'FALSet', req, res);
    });


    // Dynamic Email Link - BPA
    app.post("/http/get/Approverlink", basicAuthMiddleware, (req, res) => {
        try {
            const { Role, email, Workflow_id, DelegateApprover } = req.body;
            const Base_URL = "https://kuok--singapore--limited-devksl-3bk1u0k3-dev-kuokk2prauth.cfapps.ap11.hana.ondemand.com/odata/v4/Catalog/prreview"

            if (!Role || !email || !Workflow_id) {
                return res.status(400).json({
                    success: false,
                    message: "Missing required fields. Required: Role, email, Workflow_id"
                });
            }

            let combinedString = `workflowid=${Workflow_id}&role=${Role}&email=${email}`;

            if (DelegateApprover && DelegateApprover.trim() !== "") {
                combinedString += `&delegateapprover=${DelegateApprover}`;
            }

            const encodedValue = Buffer.from(combinedString).toString("base64");
            const finalUrl = `${Base_URL}(value='${encodedValue}')`;

            return res.status(200).json({
                response: {
                    url: finalUrl
                }
            });

        } catch (error) {
            console.error(`[Dynamic Email Link] Error:`, error.message);
            return res.status(500).json({
                success: false,
                message: "Failed to generate dynamic link",
                error: error.message
            });
        }
    });


    // GET Delegate Approvers - BPA
    app.post("/http/Get/Delegates", basicAuthMiddleware, async (req, res) => {
        // Use a new helper function designed for POST requests

        const url = "/sap/opu/odata/sap/Z_SAP_SUBSTITUT_CDS/Z_SAP_SUBSTITUT";
        const entity = "Z_SAP_SUBSTITUT";
        try {
            const payload = req.body;
            const filterParts = [];

            if (!payload || Object.keys(payload).length === 0) {
                return res.status(400).json({ success: false, message: 'Request body with filter parameters is required.' });
            }

            if (payload.smtp_addr_p) {
                filterParts.push(`smtp_addr_p eq '${payload.smtp_addr_p.toUpperCase()}'`);
            }

            const filterString = filterParts.join(' and ');
            const queryParams = [`$filter=${encodeURIComponent(filterString)}`, `$top=3000`];

            let fullUrl = `${url}?${queryParams.join('&')}`;

            console.log(`[S4 Proxy FAL POST] Fetching ${entity}. S/4 URL: ${fullUrl}`);

            const s4Response = await executeS4Request({
                method: 'GET',
                url: fullUrl,
                headers: { 'Accept': 'application/json' },
                timeout: 120000
            });

            let responseData = s4Response.data?.d?.results || s4Response.data?.value || s4Response.data;

            console.log(`[S4 Proxy FAL POST] ${entity} fetch success. Records: ${responseData.length || 'Unknown'}`);

            // --- Start Transformation Logic ---
            let delegateApproverEmail = "";

            // Ensure responseData is a non-empty array before processing
            if (Array.isArray(responseData) && responseData.length > 0) {
                // 1. Extract the delegate email (smtp_addr_r) from each object
                const emails = responseData.map(item => item.smtp_addr_r);

                // 2. Join the emails into a comma-separated string and convert to lowercase
                delegateApproverEmail = emails.join(',').toLowerCase();
            }


            return res.status(200).json({
                "delegateapproveremail": delegateApproverEmail
            });

        } catch (error) {
            console.error(`[S4 Proxy Delegate POST] Fetch error for ${entity}:`, error.message);
            const status = error.response?.status || 500;
            return res.status(status).json({
                success: false,
                message: `Failed to fetch ${entity} data: ${error.message}`,
                errorDetails: error.response?.data || error.message
            });
        }
    });



    function base64Encode(str) {
        return btoa(str);
    }

    // --- 1. Get OAuth Token ---
    async function getOAuthToken(tokenUrl, clientId, clientSecret) {
        const authString = base64Encode(`${clientId}:${clientSecret}`);
        const tokenResponse = await fetch(tokenUrl, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Authorization': `Basic ${authString}`
            },
            body: 'grant_type=client_credentials'
        });

        if (!tokenResponse.ok) {
            throw new Error(`OAuth token request failed: ${tokenResponse.status} ${tokenResponse.statusText}`);
        }
        const data = await tokenResponse.json();
        return data.access_token;
    }

    async function getWorkflowContext(workflowId, accessToken) {
        const workflowUrl = `https://ksl.test01.apimanagement.ap11.hana.ondemand.com/dev/public/workflow/rest/v1/workflow-instances/${workflowId}/context`;

        const workflowResponse = await fetch(workflowUrl, {
            method: 'GET',
            headers: {
                'Accept': 'application/json',
                'Authorization': `Bearer ${accessToken}`
            }
        });

        if (!workflowResponse.ok) {
            throw new Error(`Workflow context request failed: ${workflowResponse.status} ${workflowResponse.statusText}`);
        }

        return workflowResponse.json();
    }

    // GET Single  - BPA
    app.post("/http/Get/pr", basicAuthMiddleware, async (req, res) => {
        // Use a new helper function designed for POST requests

        const url = "/sap/opu/odata/sap/ZAPI_PRPO_CDS/ZAPI_PRPO";
        const entity = "ZAPI_PRPO";

        // OAuth Details for Workflow API
        const OAUTH_TOKEN_URL = "https://devtyo-tlgm9pd3.authentication.jp10.hana.ondemand.com/oauth/token";
        const CLIENT_ID = "sb-e9cc1a95-d05b-47d1-a278-b86f3cea4291!b3180|xsuaa!b1358";
        const CLIENT_SECRET = "a024e418-1399-4411-b41c-2e1ac1b26236$25rAx1dZhKRhfYeT3LAY039U3kc0INC5yPZaYY7pviw=";

        try {
            const payload = req.body;
            const filterParts = [];

            if (!payload || Object.keys(payload).length === 0) {
                return res.status(400).json({ success: false, message: 'Request body with filter parameters is required.' });
            }

            if (payload.prnum) {
                filterParts.push(`prnum eq '${payload.prnum}'`);
            }

            const filterString = filterParts.join(' and ');
            const queryParams = [`$filter=${encodeURIComponent(filterString)}`, `$top=3000`];

            let fullUrl = `${url}?${queryParams.join('&')}`;

            console.log(`[S4 Proxy FAL POST] Fetching ${entity}. S/4 URL: ${fullUrl}`);

            const s4Response = await executeS4Request({
                method: 'GET',
                url: fullUrl,
                headers: { 'Accept': 'application/json' },
                timeout: 120000
            });

            let prData = s4Response.data?.d?.results?.[0] || s4Response.data?.value?.[0] || s4Response.data?.[0];


            if (!prData) {
                console.log(`[S4 Proxy FAL POST] ${entity} fetch success. Records: 0`);
                return res.status(200).json({ "prdata": null });
            }


            console.log(`[S4 Proxy FAL POST] ${entity} fetch success. Records: ${s4Response.length || 'Unknown'}`);

            const workflowId = prData.workflowid;
            const role = prData.prstat?.split(" ")[1]


            let finalPRData = { ...prData, Role: role };

            // 2. Fetch Workflow Context data if workflowid exists
            if (workflowId) {
                try {
                    // Get OAuth Token
                    const accessToken = await getOAuthToken(OAUTH_TOKEN_URL, CLIENT_ID, CLIENT_SECRET);

                    // Fetch Workflow Context
                    const workflowContext = await getWorkflowContext(workflowId, accessToken);

                    // Extract and merge desired fields
                    const projectDescription = workflowContext.startEvent?.prRequisitionInputs?.ProjectDescription;
                    const totalAmount = workflowContext.startEvent?.prRequisitionInputs?.TotalAmount;

                    if (projectDescription) {
                        finalPRData.ProjectDescription = projectDescription;
                    }
                    if (totalAmount !== undefined && totalAmount !== null) {
                        finalPRData.TotalAmount = totalAmount;
                    }

                    console.log(`[S4 Proxy FAL POST] Workflow Context fetch success for ID: ${workflowId}`);

                } catch (workflowError) {
                    // Log workflow error but proceed with PR data
                    console.error(`[S4 Proxy Delegate POST] Workflow fetch error for ID ${workflowId}:`, workflowError.message);
                    // Optionally add a status/error flag to the response here if needed
                }
            }

            return res.status(200).json({
                "prdata": finalPRData,
            });

        } catch (error) {
            console.error(`[S4 Proxy Delegate POST] Fetch error for ${entity}:`, error.message);
            const status = error.response?.status || 500;
            return res.status(status).json({
                success: false,
                message: `Failed to fetch ${entity} data: ${error.message}`,
                errorDetails: error.response?.data || error.message
            });
        }
    });


    // Check if Approvers are Required
    app.post("/http/PRPO/ApproverRequired", basicAuthMiddleware, async (req, res) => {

        const url = "/sap/opu/odata/sap/API_PURCHASEORDER_PROCESS_SRV/A_PurchaseOrder";
        const entity = "A_PurchaseOrder";

        try {
            const data = req.body;
            // NOTE: I'm keeping the original PO_number extraction path from your prior request for consistency.
            const PurchaseOrderNumber = data.context?.prRequisitionInputs?.PO_number;

            if (!PurchaseOrderNumber) {
                return res.status(400).json({
                    success: false,
                    message: 'Missing required field: context.PO_number'
                });
            }

            const filterString = `PurchaseOrder eq '${PurchaseOrderNumber}'`;

            // --- MODIFICATION START ---
            // 1. Start with the direct expansions: to_PurchaseOrderItem and to_PurchaseOrderNote
            // 2. Use a forward slash (/) to define the nested expansions inside to_PurchaseOrderItem
            const expandString = `to_PurchaseOrderItem/to_AccountAssignment,`
                // `to_PurchaseOrderItem/to_PurchaseOrderItemNote,` +
                // `to_PurchaseOrderItem/to_PurchaseOrderPricingElement,` +
                // `to_PurchaseOrderItem/to_ScheduleLine,` 


            const queryParams = [
                `$filter=${encodeURIComponent(filterString)}`,
                `$top=1`,
                `$expand=${expandString}` // Use the new comprehensive expand string
            ];
            // --- MODIFICATION END ---

            let fullUrl = `${url}?${queryParams.join('&')}`;

            console.log(`[S4 Proxy PO Approver] Fetching ${entity}. S/4 URL: ${fullUrl}`);

            const s4Response = await executeS4Request({
                method: 'GET',
                url: fullUrl,
                headers: { 'Accept': 'application/json' },
                timeout: 120000
            });

            // ... (rest of the code remains the same)
            let poData = s4Response.data?.d?.results?.[0] || s4Response.data?.value?.[0] || null;

            if (!poData) {
                console.log(`[S4 Proxy PO Approver] ${entity} fetch successful. Records: 0`);
            } else {
                console.log(`[S4 Proxy PO Approver] ${entity} fetch successful. Records: 1`);
            }

            return res.status(200).json({
                "poData": poData
            });

        } catch (error) {
            console.error(`[S4 Proxy PO Approver] Fetch error for ${entity}:`, error.message);
            const status = error.response?.status || 500;
            return res.status(status).json({
                success: false,
                message: `Failed to fetch ${entity} data: ${error.message}`,
                errorDetails: error.response?.data || error.message
            });
        }
    });


//  PO Update & Cancel
app.post("/http/PRPO/Update", basicAuthMiddleware, async (req, res) => {

    try {
        console.log('=== PO Update/Cancel Started ===');
        const poPayload = req.body;

        // Validation check for mandatory input structure
        if (!poPayload || !poPayload.context || !poPayload.context.prRequisitionInputs) {
            return res.status(400).json({
                success: false,
                message: 'Invalid payload. Expecting poPayload.context.prRequisitionInputs.',
                receivedPayload: poPayload
            });
        }

        // Extract the source data object
        const sourceData = poPayload.context.prRequisitionInputs;
        const poNumberToUpdate = sourceData.PO_number;

        // *** CRITICAL VALIDATION FOR UPDATE OPERATION ***
        if (!poNumberToUpdate) {
            return res.status(400).json({
                success: false,
                message: 'Missing Purchase Order number (sourceData.PO_number) required for update operation.',
            });
        }

        // --- Logic Determinations ---
        const poType = determinePurchaseOrderType(sourceData.CompanyId, sourceData.Budgeted);
        const purchasingOrg = determinePurchasingOrganisation(sourceData.CompanyId);

        // *** Validate and adjust dates to be in the future ***
        const getValidFutureDate = (dateString) => {
            if (!dateString) {
                const futureDate = new Date();
                futureDate.setDate(futureDate.getDate() + 30);
                return futureDate.toISOString().split('T')[0];
            }
            
            const inputDate = new Date(dateString);
            const today = new Date();
            today.setHours(0, 0, 0, 0);
            
            if (inputDate < today) {
                console.warn(`Date ${dateString} is in the past. Setting to tomorrow.`);
                const tomorrow = new Date();
                tomorrow.setDate(tomorrow.getDate() + 1);
                return tomorrow.toISOString().split('T')[0];
            }
            
            return dateString;
        };

        // ***************************************************************
        // STEP 1: Update PO Header (Only header fields, NO nested items)
        // ***************************************************************
        const headerPayload = {
            // "Supplier": sourceData.Vendor_Recommendation || "",
            "PurchasingGroup": sourceData.PurchasingGroup || "",
            "DocumentCurrency": sourceData.Currency_Code || "",
            "SupplierRespSalesPersonName": `${sourceData.PO_Request === "2" 
                ? `${sourceData.PRNumber} - Cancel` 
                : `${sourceData.PRNumber} - PO Updated`
            }`
        };

        console.log('Step 1: Updating PO Header:', poNumberToUpdate);
        
        await executeS4Request({
            method: 'PATCH',
            url: `/sap/opu/odata/sap/API_PURCHASEORDER_PROCESS_SRV/A_PurchaseOrder('${poNumberToUpdate}')`, 
            headers: {
                'X-Requested-With': 'X',
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            },
            data: headerPayload,
            timeout: 90000
        });

        console.log('✓ PO Header updated');

        // ***************************************************************
        // STEP 2: Update Each Item Individually
        // ***************************************************************
        const itemUpdateResults = [];
        
        if (sourceData.Item && sourceData.Item.length > 0) {
            for (let index = 0; index < sourceData.Item.length; index++) {
                const item = sourceData.Item[index];
                const itemNumber = String((index + 1) * 10).padStart(5, '0');

                console.log(`Step 2.${index + 1}: Updating Item ${itemNumber}`);

                // Update Item Basic Data
                const itemPayload = {
                    "OrderQuantity": String(item.Quantity || 0),
                    "NetPriceAmount": String(item.UnitPrice || 0),
                    "PurchaseOrderItemText": item.ItemDescription || "",
                    "MaterialGroup": item.MaterialGroup || ""
                };

                try {
                    await executeS4Request({
                        method: 'PATCH',
                        url: `/sap/opu/odata/sap/API_PURCHASEORDER_PROCESS_SRV/A_PurchaseOrderItem(PurchaseOrder='${poNumberToUpdate}',PurchaseOrderItem='${itemNumber}')`,
                        headers: {
                            'X-Requested-With': 'X',
                            'Content-Type': 'application/json',
                            'Accept': 'application/json'
                        },
                        data: itemPayload,
                        timeout: 90000
                    });

                    console.log(`✓ Item ${itemNumber} basic data updated`);

                    // ***************************************************************
                    // STEP 3: Update Schedule Line for this Item
                    // ***************************************************************
                    const validDeliveryDate = getValidFutureDate(item.LineEstDelivDate);
                    const scheduleLineOData = convertDateToODataFormat(validDeliveryDate);

                    const scheduleLinePayload = {
                        "ScheduleLineDeliveryDate": scheduleLineOData
                    };

                    await executeS4Request({
                        method: 'PATCH',
                        url: `/sap/opu/odata/sap/API_PURCHASEORDER_PROCESS_SRV/A_PurOrdScheduleLine(PurchaseOrder='${poNumberToUpdate}',PurchaseOrderItem='${itemNumber}',ScheduleLine='0001')`,
                        headers: {
                            'X-Requested-With': 'X',
                            'Content-Type': 'application/json',
                            'Accept': 'application/json'
                        },
                        data: scheduleLinePayload,
                        timeout: 90000
                    });

                    console.log(`✓ Item ${itemNumber} schedule line updated`);

                    // ***************************************************************
                    // STEP 4: Update Account Assignment for this Item
                    // ***************************************************************
                    const accountPayload = {
                        "Quantity": String(item.Quantity || 0),
                        "GLAccount": item.GLaccount || "",
                        "CostCenter": item.CostCenter || "",
                        "MasterFixedAsset": item.AssetCode || "",
                        "OrderID": item.NominalCode || ""
                    };

                    await executeS4Request({
                        method: 'PATCH',
                        url: `/sap/opu/odata/sap/API_PURCHASEORDER_PROCESS_SRV/A_PurOrdAccountAssignment(PurchaseOrder='${poNumberToUpdate}',PurchaseOrderItem='${itemNumber}',AccountAssignmentNumber='01')`,
                        headers: {
                            'X-Requested-With': 'X',
                            'Content-Type': 'application/json',
                            'Accept': 'application/json'
                        },
                        data: accountPayload,
                        timeout: 90000
                    });

                    console.log(`✓ Item ${itemNumber} account assignment updated`);

                    // ***************************************************************
                    // STEP 5: Update Pricing Element if applicable
                    // ***************************************************************
                    const conditionType = item.ConditionType || "";
                    const conditionRateValue = determineConditionRateValue(
                        conditionType, item.Discount, item.DiscountAmt,
                        sourceData.LumpsumDiscount, sourceData.LumpsumDiscountAmt
                    );

                    if (conditionRateValue !== "0" && conditionType) {
                        const pricingPayload = {
                            "ConditionRateValue": conditionRateValue
                        };

                        // Note: You may need to find the existing condition record first
                        // This is a simplified example - adjust based on your needs
                        await executeS4Request({
                            method: 'PATCH',
                            url: `/sap/opu/odata/sap/API_PURCHASEORDER_PROCESS_SRV/A_PurOrdPricingElement(PurchaseOrder='${poNumberToUpdate}',PurchaseOrderItem='${itemNumber}',PricingDocument='',PricingDocumentItem='',PricingProcedureStep='',PricingProcedureCounter='')`,
                            headers: {
                                'X-Requested-With': 'X',
                                'Content-Type': 'application/json',
                                'Accept': 'application/json'
                            },
                            data: pricingPayload,
                            timeout: 90000
                        }).catch(err => {
                            console.warn(`Pricing element update skipped for item ${itemNumber}:`, err.message);
                        });
                    }

                    itemUpdateResults.push({
                        itemNumber,
                        success: true,
                        message: 'Item updated successfully'
                    });

                } catch (itemError) {
                    console.error(`Error updating item ${itemNumber}:`, itemError.message);
                    itemUpdateResults.push({
                        itemNumber,
                        success: false,
                        error: itemError.message
                    });
                }
            }
        }

        console.log('✓ All updates completed for PO:', poNumberToUpdate);

        // ***************************************************************
        // STEP 6: Fetch the updated PO to return current state
        // ***************************************************************
        const poResponse = await executeS4Request({
            method: 'GET',
            url: `/sap/opu/odata/sap/API_PURCHASEORDER_PROCESS_SRV/A_PurchaseOrder('${poNumberToUpdate}')`,
            headers: {
                'Accept': 'application/json'
            },
            timeout: 90000
        });

        // --- Response Transformation Logic ---
        function convertODataDateToISO(odataDateString) {
            if (!odataDateString || odataDateString.includes('null')) return '';
            const match = odataDateString.match(/\/Date\((\d+)\)\//);
            if (match) {
                const timestamp = parseInt(match[1], 10);
                return new Date(timestamp).toISOString().split('.')[0] + '.000';
            }
            return ''; 
        }
        
        const transformedResponse = {
            A_PurchaseOrder: {
                A_PurchaseOrderType: {}
            }
        };

        const sourceForMapping = poResponse.data?.d || poResponse.data;
        const targetObject = transformedResponse.A_PurchaseOrder.A_PurchaseOrderType;

        // Mapping properties
        targetObject.CreationDate = sourceForMapping.CreationDate ? convertODataDateToISO(sourceForMapping.CreationDate) : ''; 
        targetObject.CreatedByUser = sourceForMapping.CreatedByUser || '';
        targetObject.IsEndOfPurposeBlocked = sourceForMapping.IsEndOfPurposeBlocked || '';
        targetObject.PurchasingCompletenessStatus = String(sourceForMapping.PurchasingCompletenessStatus || false);
        targetObject.CashDiscount1Days = sourceForMapping.CashDiscount1Days || '0';
        targetObject.PurchaseOrderType = sourceForMapping.PurchaseOrderType || '';
        targetObject.PurchasingOrganization = sourceForMapping.PurchasingOrganization || '';
        targetObject.PurchasingDocumentDeletionCode = sourceForMapping.PurchasingDocumentDeletionCode || '';
        targetObject.NetPaymentDays = sourceForMapping.NetPaymentDays || '0';
        targetObject.ManualSupplierAddressID = sourceForMapping.ManualSupplierAddressID || '';
        targetObject.IncotermsVersion = sourceForMapping.IncotermsVersion || '';
        targetObject.AddressRegion = sourceForMapping.AddressRegion || '';
        targetObject.PurchasingGroup = sourceForMapping.PurchasingGroup || '';
        targetObject.IncotermsClassification = sourceForMapping.IncotermsClassification || '';
        targetObject.AddressName = sourceForMapping.AddressName || '';
        targetObject.InvoicingParty = sourceForMapping.InvoicingParty || '';
        targetObject.SupplyingPlant = sourceForMapping.SupplyingPlant || '';
        targetObject.PurchasingDocumentOrigin = sourceForMapping.PurchasingDocumentOrigin || '';
        targetObject.AddressCityName = sourceForMapping.AddressCityName || '';
        targetObject.AddressStreetName = sourceForMapping.AddressStreetName || '';
        targetObject.CashDiscount2Percent = sourceForMapping.CashDiscount2Percent || '0.000';
        targetObject.ValidityStartDate = sourceForMapping.ValidityStartDate ? convertODataDateToISO(sourceForMapping.ValidityStartDate) : '';
        targetObject.ExchangeRate = sourceForMapping.ExchangeRate || '0.00000';
        targetObject.SupplyingSupplier = sourceForMapping.SupplyingSupplier || '';
        targetObject.PaymentTerms = sourceForMapping.PaymentTerms || '';
        targetObject.AddressCountry = sourceForMapping.AddressCountry || '';
        targetObject.AddressPostalCode = sourceForMapping.AddressPostalCode || '';
        targetObject.PurchaseOrderSubtype = sourceForMapping.PurchaseOrderSubtype || '';
        targetObject.Language = sourceForMapping.Language || '';
        targetObject.SupplierRespSalesPersonName = sourceForMapping.SupplierRespSalesPersonName || '';
        targetObject.SupplierQuotationExternalID = sourceForMapping.SupplierQuotationExternalID || '';
        targetObject.Supplier = sourceForMapping.Supplier || '';
        targetObject.ValidityEndDate = sourceForMapping.ValidityEndDate ? convertODataDateToISO(sourceForMapping.ValidityEndDate) : '';
        targetObject.IncotermsLocation2 = sourceForMapping.IncotermsLocation2 || '';
        targetObject.IncotermsLocation1 = sourceForMapping.IncotermsLocation1 || '';
        targetObject.AddressFaxNumber = sourceForMapping.AddressFaxNumber || '';
        targetObject.AddressPhoneNumber = sourceForMapping.AddressPhoneNumber || '';
        targetObject.AddressCorrespondenceLanguage = sourceForMapping.AddressCorrespondenceLanguage || '';
        targetObject.DocumentCurrency = sourceForMapping.DocumentCurrency || '';
        targetObject.ReleaseIsNotCompleted = String(sourceForMapping.ReleaseIsNotCompleted || false);
        targetObject.PurchasingProcessingStatus = sourceForMapping.PurchasingProcessingStatus || '';
        targetObject.PurchaseOrder = poNumberToUpdate;
        targetObject.LastChangeDateTime = sourceForMapping.LastChangeDateTime ? convertODataDateToISO(sourceForMapping.LastChangeDateTime) : '';
        targetObject.SupplierPhoneNumber = sourceForMapping.SupplierPhoneNumber || '';
        targetObject.CashDiscount2Days = sourceForMapping.CashDiscount2Days || '0';
        targetObject.CompanyCode = sourceForMapping.CompanyCode || '';
        targetObject.CashDiscount1Percent = sourceForMapping.CashDiscount1Percent || '0.000';
        targetObject.AddressHouseNumber = sourceForMapping.AddressHouseNumber || '';

        // Add item update summary
        transformedResponse.itemUpdateResults = itemUpdateResults;

        // Return the transformed response structure
        return res.status(200).json(transformedResponse);

    } catch (error) {
        console.error('PO update error:', error);
        return res.status(500).json({
            success: false,
            message: `Failed to update PO: ${error.message}`,
            error: error.response?.data || error.message
        });
    }
});


   

});
