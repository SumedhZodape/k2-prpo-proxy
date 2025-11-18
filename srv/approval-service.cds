using { Currency, managed, cuid, } from '@sap/cds/common';

namespace cap.po.approval;

/**
 * Standardized structure for the response from S/4HANA OData calls.
 * This ensures consistency across all custom functions.
 */
type S4_Response : {
    success: Boolean @title: 'Success Status';
    message: String(255) @title: 'Response Message';
    // Increased size for potentially large S/4HANA data payload
    data: String(1000000) @title: 'S/4HANA Response Data (JSON string)'; 
};

service ApprovalService {
    
    // Original Function (renamed for clarity)
    function fetchMRData() returns S4_Response;
    
    // New Functions for Reference Data
    function fetchCompanyCode() returns S4_Response;
    function fetchSupplier() returns S4_Response;
    function fetchGLAccount() returns S4_Response;
    function fetchCostCenter() returns S4_Response;
    function fetchAssetNumber() returns S4_Response;
    function fetchInternalOrder() returns S4_Response;
    function fetchCurrency() returns S4_Response;
    
    // New Functions for Transactional Data (GET)
    function fetchPRPOData() returns S4_Response; // Maps to ZAPI_PRPO_CDS
    function fetchBTP1251Data() returns S4_Response; // Maps to ZAPIT_BTP_1251_SRV
    function fetchPOFALData() returns S4_Response; // Maps to ZAPI_001_PO_FAL_SRV

    // CHANGED FROM 'function' TO 'action' for POST operations
    // action postPRPOCreation(payload: String(10000000)) returns S4_Response; 
   action postPRPOCreation(payload: String) returns S4_Response;

}