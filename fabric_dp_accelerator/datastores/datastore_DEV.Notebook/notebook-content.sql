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
-- Datastore Configuration: DEV Environment
-- Environment: DEV
-- Generated: 2026-10-17
-- =====================================================================
-- Purpose:
--   Register all Fabric datastores (Lakehouses, Warehouses) for the DEV environment.
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
-- STEP 2: Insert DEV environment datastore configuration
-- =====================================================================

INSERT INTO [dbo].[Datastore_Configuration] 
    (Datastore_Name, Datastore_Type, Datastore_ID, Workspace_ID, Workspace_Name, Medallion_Layer, Endpoint, Connection_ID)
VALUES
-- Core Medallion Architecture Datastores
('bronze', 'Lakehouse', '466eb9f8-c484-4988-b3ab-a53f1a72f908', '76e43058-b99b-403a-8603-174631f16837', 'OneHub', 'Bronze', '6s3w3rwx57zelkdrd3lzl6ukpq-layoi5u3xe5ebbqdc5ddd4lig4.datawarehouse.fabric.microsoft.com', NULL),
('silver', 'Lakehouse', 'ef98847b-2d5e-4974-a373-e144bc7cc2a6', '76e43058-b99b-403a-8603-174631f16837', 'OneHub', 'Silver', '6s3w3rwx57zelkdrd3lzl6ukpq-layoi5u3xe5ebbqdc5ddd4lig4.datawarehouse.fabric.microsoft.com', NULL),
('gold', 'Lakehouse', '32eeae16-8986-40eb-a59c-f31271302e33', '76e43058-b99b-403a-8603-174631f16837', 'OneHub', 'Gold', '6s3w3rwx57zelkdrd3lzl6ukpq-layoi5u3xe5ebbqdc5ddd4lig4.datawarehouse.fabric.microsoft.com', NULL),
('metadata_lakehouse', 'Lakehouse', '69777b32-f6cf-4286-88c8-c96ec30a177c', '76e43058-b99b-403a-8603-174631f16837', 'OneHub', 'Bronze', '6s3w3rwx57zelkdrd3lzl6ukpq-layoi5u3xe5ebbqdc5ddd4lig4.datawarehouse.fabric.microsoft.com', NULL),
('metadata_warehouse', 'Warehouse', '9571eb93-3d83-46f5-a88b-dd995f2870bb', '76e43058-b99b-403a-8603-174631f16837', 'OneHub', 'Gold', '6s3w3rwx57zelkdrd3lzl6ukpq-layoi5u3xe5ebbqdc5ddd4lig4.datawarehouse.fabric.microsoft.com', NULL)
-- Add additional rows above the terminating semicolon. Prefix each row after the first with a comma.
-- Example:
-- ,('sales_bronze', 'Lakehouse', '<lakehouse-guid>', '<workspace-guid>', 'dev', 'Bronze', '<sql-analytics-endpoint>', NULL)
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
