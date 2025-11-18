const cds = require('@sap/cds');
const { executeHttpRequest } = require('@sap-cloud-sdk/http-client');

// --- Configuration ---
const S4_DESTINATION_NAME = 'S4-API-TEST'; // Ensure this matches your BTP destination name

/**
 * Executes a GET request to an S/4HANA OData service via the specified destination.
  * Handles response parsing (OData V2/V4) and comprehensive error logging.
   * @param {string} serviceRoot - The OData service root (e.g., '/sap/opu/odata/sap/ZAPI_CDS').
    * @param {string} entitySetName - The primary entity set to query (e.g., 'ZAPI_ENTITY').
     * @returns {object} - Standardized response object {success, message, data (JSON string), error?, httpStatus?}.
      */
async function fetchFromS4(serviceRoot, entitySetName) {
    const fullServiceUrl = `${serviceRoot}/${entitySetName}`;

    console.log(`[S4 Proxy] Fetching ${entitySetName} from S/4HANA via ${fullServiceUrl}`);

    try {
        const s4Response = await executeHttpRequest(
            { destinationName: 'S4_DESTINATION_NAME' },
            {
                method: 'GET',
                url: fullServiceUrl,
                headers: {
                    'Accept': 'application/json'
                },
                timeout: 30000 // 30 second timeout
            }
        );

        // --- Response Processing ---
        let formattedData = {};

        // Check for standard OData V2/V4 wrapper: .d.results (V2) or .value (V4)
        if (s4Response.data && s4Response.data.d && Array.isArray(s4Response.data.d.results)) {
            // OData V2 structure
            formattedData = s4Response.data.d.results;
            console.log(`[S4 Proxy] Extracted OData V2 results array for ${entitySetName}.`);
        } else if (s4Response.data && Array.isArray(s4Response.data.value)) {
            // OData V4 structure (assuming standard JSON format)
            formattedData = s4Response.data.value;
            console.log(`[S4 Proxy] Extracted OData V4 value array for ${entitySetName}.`);
        } else {
            // Fallback: return the whole body
            formattedData = s4Response.data;
            console.log(`[S4 Proxy] No standard wrapper found for ${entitySetName}. Returning raw data.`);
        }

        console.log(`[S4 Proxy] Fetch Success for ${entitySetName}. Status: ${s4Response.status}`);

        return {
            success: true,
            message: `Successfully fetched data from S/4HANA Entity Set: ${entitySetName}. HTTP Status: ${s4Response.status}`,
            data: JSON.stringify(formattedData)
        };

    } catch (error) {

        // --- Centralized Error Handling ---
        const status = error.response?.status || error.status;
        let errorMessage = 'Failed to connect or fetch data from S/4HANA system.';
        let errorCode = 'S4_FETCH_ERROR';

        if (status === 401 || status === 407) { // 407 Proxy Auth Required can occur with Destinations
            errorMessage = 'Destination authentication failed. Check destination configuration for S/4HANA access.';
            errorCode = 'DESTINATION_AUTH_FAILED';
        } else if (status === 404) {
            errorMessage = `OData Entity Set/Service not found at path ${fullServiceUrl}. Check service activation.`;
            errorCode = 'SERVICE_NOT_FOUND';
        } else if (status === 403) {
            errorMessage = 'Access denied by S/4HANA. Check destination user authorizations.';
            errorCode = 'ACCESS_DENIED';
        } else if (error.code === 'ECONNREFUSED' || error.code === 'ETIMEDOUT') {
            errorMessage = 'Cannot connect to S/4HANA system. Check Cloud Connector/system availability or firewall rules.';
            errorCode = 'CONNECTION_ERROR';
        } else if (status >= 500) {
            errorMessage = `S/4HANA system error (${status}). Review S/4HANA logs.`;
            errorCode = 'SYSTEM_ERROR';
        }

        console.error(`[S4 Proxy] Fatal Error for ${entitySetName}:`, {
            status: status,
            message: error.message,
            rawError: error.response?.data || error
        });

        return {
            success: false,
            message: errorMessage,
            data: JSON.stringify({ details: error.message, code: errorCode, s4Error: error.response?.data }),
            error: errorCode,
            httpStatus: status
        };
    }
}


/**
 * Fetches CSRF token from S/4HANA system for POST/PUT/PATCH requests.
  * CRITICAL FIX: Fetches from $metadata endpoint which always returns CSRF tokens
   * @param {string} serviceRoot - The service root to fetch CSRF token from
    */
async function fetchCsrfToken(serviceRoot) {
    // CRITICAL FIX: Use $metadata endpoint to get CSRF token
    const metadataUrl = `${serviceRoot}/$metadata`;

    console.log(`[S4 Proxy] Fetching CSRF Token from ${metadataUrl}`);

    try {
        const response = await executeHttpRequest(
            { destinationName: S4_DESTINATION_NAME },
            {
                method: 'GET',
                url: metadataUrl,
                headers: {
                    'X-CSRF-Token': 'Fetch',
                    'Accept': 'application/xml' // $metadata returns XML
                },
                timeout: 30000
            }
        );

        console.log('[S4 Proxy] Response headers:', JSON.stringify(response.headers, null, 2));

        // CSRF token can be in different case variations
        const token = response.headers?.['x-csrf-token'] ||
            response.headers?.['X-CSRF-Token'] ||
            response.headers?.['X-Csrf-Token'];

        let cookies = '';

        // Handle cookies properly - CRITICAL for maintaining session
        const setCookieHeaders = response.headers?.['set-cookie'] ||
            response.headers?.['Set-Cookie'];

        if (setCookieHeaders) {
            if (Array.isArray(setCookieHeaders)) {
                // Extract cookie values without attributes (path, domain, etc.)
                cookies = setCookieHeaders.map(cookie => {
                    return cookie.split(';')[0];
                }).join('; ');
            } else {
                cookies = setCookieHeaders.split(';')[0];
            }
        }

        if (!token) {
            console.error('[S4 Proxy] No CSRF token received from S/4HANA.');
            console.error('[S4 Proxy] Available headers:', Object.keys(response.headers));
            return null;
        }

        console.log('[S4 Proxy] CSRF token successfully fetched:', token.substring(0, 30) + '...');
        console.log('[S4 Proxy] Cookies extracted:', cookies ? 'Yes' : 'No');

        return { token, cookies };
    } catch (error) {
        console.error('[S4 Proxy] Failed to fetch CSRF token:', {
            message: error.message,
            status: error.response?.status,
            statusText: error.response?.statusText,
            headers: error.response?.headers,
            rawError: error.response?.data || error
        });

        return null;
    }
}


/**
 * Executes a POST request to an S/4HANA OData service via the specified destination.
  * Automatically handles CSRF token fetching.
   * @param {string} serviceRoot - The OData service root (e.g., '/sap/opu/odata/sap/API_PURCHASEORDER_PROCESS_SRV;v=2').
    * @param {string} entitySetName - The primary entity set to post to (e.g., 'A_PurchaseOrder').
     * @param {object} payload - The JavaScript object representing the data to be posted.
      * @returns {object} - Standardized response object {success, message, data (JSON string), error?, httpStatus?}.
       */
async function postToS4(serviceRoot, entitySetName, payload) {
    const fullServiceUrl = `${serviceRoot}/${entitySetName}`;

    console.log(`[S4 Proxy] Posting to ${entitySetName} at S/4HANA via ${fullServiceUrl}`);
    console.log(`[S4 Proxy POST] Payload to send:`, JSON.stringify(payload, null, 2));

    try {
        // Step 1: Fetch CSRF Token from $metadata endpoint
        const csrfData = await fetchCsrfToken(serviceRoot);

        if (!csrfData || !csrfData.token) {
            return {
                success: false,
                message: 'Failed to obtain CSRF token from S/4HANA system. Check destination authentication and service availability.',
                data: JSON.stringify({
                    error: 'CSRF token fetch failed',
                    hint: 'Verify destination configuration and user permissions in BTP cockpit'
                }),
                error: 'CSRF_TOKEN_ERROR',
                httpStatus: 500
            };
        }

        // Step 2: Execute POST with CSRF Token
        console.log(`[S4 Proxy POST] Using CSRF Token: ${csrfData.token.substring(0, 20)}...`);
        console.log(`[S4 Proxy POST] Session cookies: ${csrfData.cookies ? 'Available' : 'None'}`);

        const headers = {
            'X-CSRF-Token': csrfData.token,
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        };

        // CRITICAL: Add cookies to maintain the same session
        if (csrfData.cookies) {
            headers['Cookie'] = csrfData.cookies;
        }

        console.log(`[S4 Proxy POST] Request Headers:`, JSON.stringify(headers, null, 2));

        const s4Response = await executeHttpRequest(
            { destinationName: S4_DESTINATION_NAME },
            {
                method: 'POST',
                url: fullServiceUrl,
                data: payload,
                headers: headers,
                timeout: 60000 // Increased timeout for transactional POST
            }
        );

        console.log(`[S4 Proxy] POST Success for ${entitySetName}. Status: ${s4Response.status}`);
        console.log(`[S4 Proxy] POST Response:`, JSON.stringify(s4Response.data, null, 2));

        return {
            success: true,
            message: `Successfully posted data to S/4HANA Entity Set: ${entitySetName}. HTTP Status: ${s4Response.status}`,
            data: JSON.stringify(s4Response.data)
        };

    } catch (error) {
        // --- Centralized Error Handling for POST ---
        const status = error.response?.status || error.status;
        let errorMessage = 'Failed to execute POST to S/4HANA system.';
        let errorCode = 'S4_POST_ERROR';

        if (status === 400) {
            errorMessage = 'Bad Request. Data validation failed in S/4HANA (e.g., missing mandatory field, incorrect format).';
            errorCode = 'S4_BAD_REQUEST';
        } else if (status === 403) {
            errorMessage = 'CSRF validation or authorization failed. Check destination configuration and user permissions.';
            errorCode = 'S4_CSRF_OR_AUTH_ERROR';
        } else if (status === 401 || status === 407) {
            errorMessage = 'Destination authentication failed. Check destination configuration for S/4HANA access.';
            errorCode = 'DESTINATION_AUTH_FAILED';
        } else if (status === 404) {
            errorMessage = `OData Entity Set/Service not found at path ${fullServiceUrl}. Check service activation.`;
            errorCode = 'SERVICE_NOT_FOUND';
        } else if (error.code === 'ECONNREFUSED' || error.code === 'ETIMEDOUT') {
            errorMessage = 'Cannot connect to S/4HANA system. Check Cloud Connector/system availability or firewall rules.';
            errorCode = 'CONNECTION_ERROR';
        } else if (status >= 500) {
            errorMessage = `S/4HANA system error (${status}). Review S/4HANA logs.`;
            errorCode = 'SYSTEM_ERROR';
        }

        console.error(`[S4 Proxy] Fatal POST Error for ${entitySetName}:`, {
            status: status,
            message: error.message,
            responseData: error.response?.data,
            rawError: error
        });

        return {
            success: false,
            message: errorMessage,
            data: JSON.stringify({
                details: error.message,
                code: errorCode,
                s4Error: error.response?.data
            }),
            error: errorCode,
            httpStatus: status
        };
    }
}


module.exports = cds.service.impl(async function () {

    // --- OData Service Mappings (Assumes Entity Set Name = Service Root Name) ---

    // 1. Existing Function (Renamed) - ZAPI_SMR_MR_HDR_CDS
    this.on('fetchMRData', async (req) => {
        return fetchFromS4('/sap/opu/odata/sap/ZAPI_SMR_MR_HDR_CDS', 'ZAPI_SMR_MR_HDR');
    });

    // 2. ZAPI_COMPCODE_CDS
    this.on('fetchCompanyCode', async (req) => {
        return fetchFromS4('/sap/opu/odata/sap/ZAPI_COMPCODE_CDS', 'ZAPI_COMPCODE');
    });

    // 3. ZAPI_SUPPLIER_CDS
    this.on('fetchSupplier', async (req) => {
        return fetchFromS4('/sap/opu/odata/sap/ZAPI_SUPPLIER_CDS', 'ZAPI_SUPPLIER');
    });

    // 4. ZAPI_GLACCOUNT_CDS
    this.on('fetchGLAccount', async (req) => {
        return fetchFromS4('/sap/opu/odata/sap/ZAPI_GLACCOUNT_CDS', 'ZAPI_GLACCOUNT');
    });

    // 5. ZAPI_COSTCENTER_CDS
    this.on('fetchCostCenter', async (req) => {
        return fetchFromS4('/sap/opu/odata/sap/ZAPI_COSTCENTER_CDS', 'ZAPI_COSTCENTER');
    });

    // 6. ZAPI_ASSET_NUM_CDS
    this.on('fetchAssetNumber', async (req) => {
        return fetchFromS4('/sap/opu/odata/sap/ZAPI_ASSET_NUM_CDS', 'ZAPI_ASSET_NUM');
    });

    // 7. ZAPI_INT_ORDER_CDS
    this.on('fetchInternalOrder', async (req) => {
        return fetchFromS4('/sap/opu/odata/sap/ZAPI_INT_ORDER_CDS', 'ZAPI_INT_ORDER');
    });

    // 8. ZAPI_PRPO_CDS
    this.on('fetchPRPOData', async (req) => {
        return fetchFromS4('/sap/opu/odata/sap/ZAPI_PRPO_CDS', 'ZAPI_PRPO');
    });

    // 9. ZAPI_CURRENCY_CDS
    this.on('fetchCurrency', async (req) => {
        return fetchFromS4('/sap/opu/odata/sap/ZAPI_CURRENCY_CDS', 'ZAPI_CURRENCY');
    });

    // 10. ZAPIT_BTP_1251_SRV
    this.on('fetchBTP1251Data', async (req) => {
        return fetchFromS4('/sap/opu/odata/sap/ZAPIT_BTP_1251_SRV', 'ZAPIT_BTP_1251');
    });

    // 11. ZAPI_001_PO_FAL_SRV
    this.on('fetchPOFALData', async (req) => {
        return fetchFromS4('/sap/opu/odata/sap/ZAPI_001_PO_FAL_SRV', 'ZAPI_001_PO_FAL');
    });

    // 12. POST ACTION - API_PURCHASEORDER_PROCESS_SRV
    this.on('postPRPOCreation', async (req) => {
        try {
            // ✅ CORRECT CAP SYNTAX: req.data.payload accesses the data from the CDS action parameter
            // We assume the client sends: { "payload": { "A_PurchaseOrderType": { ... } } }
            const poPayloadWrapper = req.data.payload;

            if (!poPayloadWrapper || !poPayloadWrapper.A_PurchaseOrderType) {
                // ✅ CORRECT CAP ERROR RESPONSE STRUCTURE
                return {
                    success: false,
                    message: 'Invalid payload structure: Missing A_PurchaseOrderType wrapper.',
                    data: JSON.stringify({ error: 'Missing structure' }),
                    error: 'MISSING_PAYLOAD_STRUCTURE'
                };
            }

            // Extract the actual S/4HANA payload object
            // This is the object that will be sent to the S/4HANA OData service
            const s4Payload = poPayloadWrapper.A_PurchaseOrderType;

            // Target S/4HANA Service Root and Entity Set
            const serviceRoot = '/sap/opu/odata/sap/API_PURCHASEORDER_PROCESS_SRV';
            const entitySet = 'A_PurchaseOrder'; // POST to the main entity set

            console.log('[S4 Proxy] Initiating POST to S/4HANA...');

            // ✅ BEST PRACTICE: Use the existing postToS4 function which handles CSRF and errors
            return await postToS4(serviceRoot, entitySet, s4Payload);

        } catch (e) {
            console.error('[S4 Proxy] Error processing payload for POST:', e);

            // ✅ CORRECT CAP ERROR RESPONSE STRUCTURE
            return {
                success: false,
                message: 'Internal CAP service processing error.',
                data: JSON.stringify({ error: e.message, stack: e.stack }),
                error: 'PROCESSING_ERROR'
            };
        }
    });
    
});