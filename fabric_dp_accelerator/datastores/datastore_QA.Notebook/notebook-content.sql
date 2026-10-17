-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "warehouse": {
-- META       "default_warehouse": "5f2870bb-dd99-a88b-46f5-3d839571eb93",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "5f2870bb-dd99-a88b-46f5-3d839571eb93",
-- META           "type": "Datawarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

-- =====================================================================
-- Datastore Configuration: QA Environment
-- Environment: QA
-- Generated: 2026-10-17
-- =====================================================================
-- Purpose:
--   Register all Fabric datastores (Lakehouses, Warehouses) for the QA environment.
--   This replaces Variable Library entries for datastore details.
--
-- CI/CD Notes:
--   - DEV is generated with live deployment workspace IDs and endpoints.
--   - QA and PROD are generated with placeholder values for later environment-specific replacement.
--   - Deploy the matching datastore_<ENV> notebook as part of CI/CD.
--
-- To Add a New Datastore:
--   1. Create the Lakehouse/Warehouse in Fabric
--   2. Copy the artifact GUID and workspace GUID from Fabric
--   3. Add a row to the STEP 2 VALUES list before its terminating semicolon
--   4. Run this cell or deploy via CI/CD
-- =====================================================================

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

-- =====================================================================
-- STEP 1: Replace datastore configuration atomically (full refresh)
-- =====================================================================
BEGIN TRY
    BEGIN TRANSACTION;

TRUNCATE TABLE dbo.Datastore_Configuration;

-- =====================================================================
-- STEP 2: Insert QA environment datastore configuration
-- =====================================================================

INSERT INTO [dbo].[Datastore_Configuration] 
    (Datastore_Name, Datastore_Type, Datastore_ID, Workspace_ID, Workspace_Name, Medallion_Layer, Endpoint, Connection_ID)
VALUES
-- Core Medallion Architecture Datastores
('bronze', 'Lakehouse', 'REPLACE_WITH_QA_BRONZE_LAKEHOUSE_ID', 'REPLACE_WITH_QA_WORKSPACE_ID', 'REPLACE_WITH_QA_WORKSPACE_NAME', 'Bronze', 'REPLACE_WITH_QA_BRONZE_SQL_ENDPOINT', NULL),
('silver', 'Lakehouse', 'REPLACE_WITH_QA_SILVER_LAKEHOUSE_ID', 'REPLACE_WITH_QA_WORKSPACE_ID', 'REPLACE_WITH_QA_WORKSPACE_NAME', 'Silver', 'REPLACE_WITH_QA_SILVER_SQL_ENDPOINT', NULL),
('gold', 'Lakehouse', 'REPLACE_WITH_QA_GOLD_LAKEHOUSE_ID', 'REPLACE_WITH_QA_WORKSPACE_ID', 'REPLACE_WITH_QA_WORKSPACE_NAME', 'Gold', 'REPLACE_WITH_QA_GOLD_SQL_ENDPOINT', NULL),
('metadata_lakehouse', 'Lakehouse', 'REPLACE_WITH_QA_METADATA_LAKEHOUSE_LAKEHOUSE_ID', 'REPLACE_WITH_QA_WORKSPACE_ID', 'REPLACE_WITH_QA_WORKSPACE_NAME', 'Bronze', 'REPLACE_WITH_QA_METADATA_LAKEHOUSE_SQL_ENDPOINT', NULL),
('metadata_warehouse', 'Warehouse', 'REPLACE_WITH_QA_METADATA_WAREHOUSE_WAREHOUSE_ID', 'REPLACE_WITH_QA_WORKSPACE_ID', 'REPLACE_WITH_QA_WORKSPACE_NAME', 'Gold', 'REPLACE_WITH_QA_METADATA_WAREHOUSE_WAREHOUSE_ENDPOINT', NULL)
-- Add additional rows above the terminating semicolon. Prefix each row after the first with a comma.
-- Example:
-- ,('sales_bronze', 'Lakehouse', '<lakehouse-guid>', '<workspace-guid>', 'qa', 'Bronze', '<sql-analytics-endpoint>', NULL)
;

    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;
    THROW;
END CATCH;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
