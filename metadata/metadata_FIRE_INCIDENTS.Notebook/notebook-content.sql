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
-- Metadata Configuration: FIRE_INCIDENTS
-- Trigger Name: FIRE_INCIDENTS
-- =====================================================================
-- Purpose: Ingest daily fire incident CSVs and master/code reference CSVs,
--          curate them in Silver (dedupe, rename, typing, DQ), and publish
--          an enriched incident fact in Gold joined to all reference data.
-- Source:  bronze lakehouse Files/ready/<yyyyMMdd>/*.csv
-- Target:  bronze.dbo.* (append) -> silver.dbo.* (merge) -> gold.dbo.fact_fire_incidents (merge)
-- =====================================================================

BEGIN TRY
    BEGIN TRANSACTION;

-- =====================================================================
-- STEP 1: DELETE existing records for this trigger
-- =====================================================================
DELETE FROM dbo.Data_Pipeline_Metadata_Advanced_Configuration WHERE Table_ID IN (SELECT Table_ID FROM dbo.Data_Pipeline_Metadata_Orchestration WHERE Trigger_Name = 'FIRE_INCIDENTS');
DELETE FROM dbo.Data_Pipeline_Metadata_Primary_Configuration WHERE Table_ID IN (SELECT Table_ID FROM dbo.Data_Pipeline_Metadata_Orchestration WHERE Trigger_Name = 'FIRE_INCIDENTS');
DELETE FROM dbo.Data_Pipeline_Metadata_Orchestration WHERE Trigger_Name = 'FIRE_INCIDENTS';

-- =====================================================================
-- STEP 2: Orchestration
-- =====================================================================
INSERT INTO [dbo].[Data_Pipeline_Metadata_Orchestration] (Trigger_Name, Order_Of_Operations, Table_ID, Target_Datastore, Target_Entity, Primary_Keys, Processing_Method, Ingestion_Active)
VALUES
-- Bronze: raw file ingestion (append, file_modified_time watermark)
('FIRE_INCIDENTS', 1, 101, 'bronze', 'dbo.fire_incidents', 'Incident_no', 'batch', 1),
('FIRE_INCIDENTS', 1, 102, 'bronze', 'dbo.code_ref', 'Code_ID', 'batch', 1),
('FIRE_INCIDENTS', 1, 103, 'bronze', 'dbo.severity_ref', 'Severity', 'batch', 1),
('FIRE_INCIDENTS', 1, 104, 'bronze', 'dbo.station_master', 'Station_ID', 'batch', 1),
('FIRE_INCIDENTS', 1, 105, 'bronze', 'dbo.property_type_ref', 'Property_Type', 'batch', 1),
('FIRE_INCIDENTS', 1, 106, 'bronze', 'dbo.cause_ref', 'Cause_ID', 'batch', 1),
('FIRE_INCIDENTS', 1, 107, 'bronze', 'dbo.status_ref', 'Status', 'batch', 1),
('FIRE_INCIDENTS', 1, 108, 'bronze', 'dbo.location_master', 'Postal_Code', 'batch', 1),
-- Silver: curated, deduplicated, merged on business key
('FIRE_INCIDENTS', 2, 201, 'silver', 'dbo.fire_incidents', 'Incident_No', 'batch', 1),
('FIRE_INCIDENTS', 2, 202, 'silver', 'dbo.code_ref', 'Code_ID', 'batch', 1),
('FIRE_INCIDENTS', 2, 203, 'silver', 'dbo.severity_ref', 'Severity', 'batch', 1),
('FIRE_INCIDENTS', 2, 204, 'silver', 'dbo.station_master', 'Station_ID', 'batch', 1),
('FIRE_INCIDENTS', 2, 205, 'silver', 'dbo.property_type_ref', 'Property_Type', 'batch', 1),
('FIRE_INCIDENTS', 2, 206, 'silver', 'dbo.cause_ref', 'Cause_ID', 'batch', 1),
('FIRE_INCIDENTS', 2, 207, 'silver', 'dbo.status_ref', 'Status', 'batch', 1),
('FIRE_INCIDENTS', 2, 208, 'silver', 'dbo.location_master', 'Postal_Code', 'batch', 1),
-- Gold: enriched incident fact joined to all master/code reference data
('FIRE_INCIDENTS', 3, 301, 'gold', 'dbo.fact_fire_incidents', 'Incident_No', 'batch', 1)
;

-- =====================================================================
-- STEP 3: Primary Configuration
-- =====================================================================
INSERT INTO [dbo].[Data_Pipeline_Metadata_Primary_Configuration] (Table_ID, Configuration_Category, Configuration_Name, Configuration_Value)
VALUES
(101, 'source_details', 'wildcard_folder_path', 'ready/*/fire_incidents_*.csv'),
(101, 'source_details', 'datastore_name', 'bronze'),
(101, 'source_details', 'file_has_header_row', 'true'),
(101, 'target_details', 'merge_type', 'append'),
(101, 'target_details', 'enable_change_data_feed', 'true'),
(101, 'watermark_details', 'strategy', 'file_modified_time'),
(102, 'source_details', 'wildcard_folder_path', 'ready/*/code_ref_*.csv'),
(102, 'source_details', 'datastore_name', 'bronze'),
(102, 'source_details', 'file_has_header_row', 'true'),
(102, 'target_details', 'merge_type', 'append'),
(102, 'target_details', 'enable_change_data_feed', 'true'),
(102, 'watermark_details', 'strategy', 'file_modified_time'),
(103, 'source_details', 'wildcard_folder_path', 'ready/*/severity_ref_*.csv'),
(103, 'source_details', 'datastore_name', 'bronze'),
(103, 'source_details', 'file_has_header_row', 'true'),
(103, 'target_details', 'merge_type', 'append'),
(103, 'target_details', 'enable_change_data_feed', 'true'),
(103, 'watermark_details', 'strategy', 'file_modified_time'),
(104, 'source_details', 'wildcard_folder_path', 'ready/*/station_master_*.csv'),
(104, 'source_details', 'datastore_name', 'bronze'),
(104, 'source_details', 'file_has_header_row', 'true'),
(104, 'target_details', 'merge_type', 'append'),
(104, 'target_details', 'enable_change_data_feed', 'true'),
(104, 'watermark_details', 'strategy', 'file_modified_time'),
(105, 'source_details', 'wildcard_folder_path', 'ready/*/property_type_ref_*.csv'),
(105, 'source_details', 'datastore_name', 'bronze'),
(105, 'source_details', 'file_has_header_row', 'true'),
(105, 'target_details', 'merge_type', 'append'),
(105, 'target_details', 'enable_change_data_feed', 'true'),
(105, 'watermark_details', 'strategy', 'file_modified_time'),
(106, 'source_details', 'wildcard_folder_path', 'ready/*/cause_ref_*.csv'),
(106, 'source_details', 'datastore_name', 'bronze'),
(106, 'source_details', 'file_has_header_row', 'true'),
(106, 'target_details', 'merge_type', 'append'),
(106, 'target_details', 'enable_change_data_feed', 'true'),
(106, 'watermark_details', 'strategy', 'file_modified_time'),
(107, 'source_details', 'wildcard_folder_path', 'ready/*/status_ref_*.csv'),
(107, 'source_details', 'datastore_name', 'bronze'),
(107, 'source_details', 'file_has_header_row', 'true'),
(107, 'target_details', 'merge_type', 'append'),
(107, 'target_details', 'enable_change_data_feed', 'true'),
(107, 'watermark_details', 'strategy', 'file_modified_time'),
(108, 'source_details', 'wildcard_folder_path', 'ready/*/location_master_*.csv'),
(108, 'source_details', 'datastore_name', 'bronze'),
(108, 'source_details', 'file_has_header_row', 'true'),
(108, 'target_details', 'merge_type', 'append'),
(108, 'target_details', 'enable_change_data_feed', 'true'),
(108, 'watermark_details', 'strategy', 'file_modified_time'),
(201, 'source_details', 'table_name', 'bronze.dbo.fire_incidents'),
(201, 'target_details', 'merge_type', 'merge'),
(201, 'watermark_details', 'strategy', 'change_data_feed'),
(202, 'source_details', 'table_name', 'bronze.dbo.code_ref'),
(202, 'target_details', 'merge_type', 'merge'),
(202, 'watermark_details', 'strategy', 'change_data_feed'),
(203, 'source_details', 'table_name', 'bronze.dbo.severity_ref'),
(203, 'target_details', 'merge_type', 'merge'),
(203, 'watermark_details', 'strategy', 'change_data_feed'),
(204, 'source_details', 'table_name', 'bronze.dbo.station_master'),
(204, 'target_details', 'merge_type', 'merge'),
(204, 'watermark_details', 'strategy', 'change_data_feed'),
(205, 'source_details', 'table_name', 'bronze.dbo.property_type_ref'),
(205, 'target_details', 'merge_type', 'merge'),
(205, 'watermark_details', 'strategy', 'change_data_feed'),
(206, 'source_details', 'table_name', 'bronze.dbo.cause_ref'),
(206, 'target_details', 'merge_type', 'merge'),
(206, 'watermark_details', 'strategy', 'change_data_feed'),
(207, 'source_details', 'table_name', 'bronze.dbo.status_ref'),
(207, 'target_details', 'merge_type', 'merge'),
(207, 'watermark_details', 'strategy', 'change_data_feed'),
(208, 'source_details', 'table_name', 'bronze.dbo.location_master'),
(208, 'target_details', 'merge_type', 'merge'),
(208, 'watermark_details', 'strategy', 'change_data_feed'),
(301, 'source_details', 'table_name', 'silver.dbo.fire_incidents'),
(301, 'target_details', 'merge_type', 'merge'),
(301, 'watermark_details', 'strategy', 'none')
;

-- =====================================================================
-- STEP 4: Advanced Configuration (transformations first, then DQ)
-- =====================================================================
INSERT INTO dbo.Data_Pipeline_Metadata_Advanced_Configuration (Table_ID, Configuration_Category, Configuration_Name, Configuration_Name_Instance_Number, Configuration_Attribute_Name, Configuration_Attribute_Value)
VALUES
-- 201 silver.dbo.fire_incidents: dedupe (latest file wins) -> rename -> typing, then DQ
(201, 'data_transformation_steps', 'drop_duplicates', 1, 'column_name', 'Incident_no'),
(201, 'data_transformation_steps', 'drop_duplicates', 1, 'order_by', 'delta__raw_folderpath'),
(201, 'data_transformation_steps', 'drop_duplicates', 1, 'order_direction', 'desc'),
(201, 'data_transformation_steps', 'columns_to_rename', 2, 'existing_column_name', 'Incident_no,Response_Time_Min'),
(201, 'data_transformation_steps', 'columns_to_rename', 2, 'new_column_name', 'Incident_No,Response_Time_Minutes'),
(201, 'data_transformation_steps', 'change_data_types', 3, 'column_name', 'Incident_Date'),
(201, 'data_transformation_steps', 'change_data_types', 3, 'new_type', 'date'),
(201, 'data_transformation_steps', 'change_data_types', 4, 'column_name', 'Response_Time_Minutes,Units_Dispatched,Persons_Rescued,Injuries,Fatalities'),
(201, 'data_transformation_steps', 'change_data_types', 4, 'new_type', 'int'),
(201, 'data_transformation_steps', 'change_data_types', 5, 'column_name', 'Latitude,Longitude'),
(201, 'data_transformation_steps', 'change_data_types', 5, 'new_type', 'decimal(9,6)'),
(201, 'data_transformation_steps', 'change_data_types', 6, 'column_name', 'Postal_Code'),
(201, 'data_transformation_steps', 'change_data_types', 6, 'new_type', 'string'),
(201, 'data_quality', 'validate_not_null', 1, 'column_name', 'Incident_No'),
(201, 'data_quality', 'validate_not_null', 1, 'message', 'Incident_No must not be null'),
(201, 'data_quality', 'validate_not_null', 1, 'if_not_compliant', 'quarantine'),
(201, 'data_quality', 'validate_unique', 2, 'column_name', 'Incident_No'),
(201, 'data_quality', 'validate_unique', 2, 'message', 'Incident_No must be unique after deduplication'),
(201, 'data_quality', 'validate_unique', 2, 'if_not_compliant', 'fail'),
-- 202 silver.dbo.code_ref
(202, 'data_transformation_steps', 'drop_duplicates', 1, 'column_name', 'Code_ID'),
(202, 'data_transformation_steps', 'drop_duplicates', 1, 'order_by', 'delta__raw_folderpath'),
(202, 'data_transformation_steps', 'columns_to_rename', 2, 'existing_column_name', 'Active_Flag'),
(202, 'data_transformation_steps', 'columns_to_rename', 2, 'new_column_name', 'Code_Active_Flag'),
(202, 'data_quality', 'validate_not_null', 1, 'column_name', 'Code_ID'),
(202, 'data_quality', 'validate_not_null', 1, 'message', 'Code_ID must not be null'),
(202, 'data_quality', 'validate_not_null', 1, 'if_not_compliant', 'quarantine'),
-- 203 silver.dbo.severity_ref
(203, 'data_transformation_steps', 'drop_duplicates', 1, 'column_name', 'Severity'),
(203, 'data_transformation_steps', 'drop_duplicates', 1, 'order_by', 'delta__raw_folderpath'),
(203, 'data_transformation_steps', 'change_data_types', 2, 'column_name', 'Severity_Rank'),
(203, 'data_transformation_steps', 'change_data_types', 2, 'new_type', 'int'),
(203, 'data_quality', 'validate_not_null', 1, 'column_name', 'Severity'),
(203, 'data_quality', 'validate_not_null', 1, 'message', 'Severity must not be null'),
(203, 'data_quality', 'validate_not_null', 1, 'if_not_compliant', 'quarantine'),
-- 204 silver.dbo.station_master
(204, 'data_transformation_steps', 'drop_duplicates', 1, 'column_name', 'Station_ID'),
(204, 'data_transformation_steps', 'drop_duplicates', 1, 'order_by', 'delta__raw_folderpath'),
(204, 'data_transformation_steps', 'columns_to_rename', 2, 'existing_column_name', 'Active_Flag'),
(204, 'data_transformation_steps', 'columns_to_rename', 2, 'new_column_name', 'Station_Active_Flag'),
(204, 'data_quality', 'validate_not_null', 1, 'column_name', 'Station_ID'),
(204, 'data_quality', 'validate_not_null', 1, 'message', 'Station_ID must not be null'),
(204, 'data_quality', 'validate_not_null', 1, 'if_not_compliant', 'quarantine'),
-- 205 silver.dbo.property_type_ref
(205, 'data_transformation_steps', 'drop_duplicates', 1, 'column_name', 'Property_Type'),
(205, 'data_transformation_steps', 'drop_duplicates', 1, 'order_by', 'delta__raw_folderpath'),
(205, 'data_quality', 'validate_not_null', 1, 'column_name', 'Property_Type'),
(205, 'data_quality', 'validate_not_null', 1, 'message', 'Property_Type must not be null'),
(205, 'data_quality', 'validate_not_null', 1, 'if_not_compliant', 'quarantine'),
-- 206 silver.dbo.cause_ref
(206, 'data_transformation_steps', 'drop_duplicates', 1, 'column_name', 'Cause_ID'),
(206, 'data_transformation_steps', 'drop_duplicates', 1, 'order_by', 'delta__raw_folderpath'),
(206, 'data_quality', 'validate_not_null', 1, 'column_name', 'Cause_ID'),
(206, 'data_quality', 'validate_not_null', 1, 'message', 'Cause_ID must not be null'),
(206, 'data_quality', 'validate_not_null', 1, 'if_not_compliant', 'quarantine'),
-- 207 silver.dbo.status_ref
(207, 'data_transformation_steps', 'drop_duplicates', 1, 'column_name', 'Status'),
(207, 'data_transformation_steps', 'drop_duplicates', 1, 'order_by', 'delta__raw_folderpath'),
(207, 'data_quality', 'validate_not_null', 1, 'column_name', 'Status'),
(207, 'data_quality', 'validate_not_null', 1, 'message', 'Status must not be null'),
(207, 'data_quality', 'validate_not_null', 1, 'if_not_compliant', 'quarantine'),
-- 208 silver.dbo.location_master
(208, 'data_transformation_steps', 'drop_duplicates', 1, 'column_name', 'Postal_Code'),
(208, 'data_transformation_steps', 'drop_duplicates', 1, 'order_by', 'delta__raw_folderpath'),
(208, 'data_transformation_steps', 'change_data_types', 2, 'column_name', 'Postal_Code'),
(208, 'data_transformation_steps', 'change_data_types', 2, 'new_type', 'string'),
(208, 'data_quality', 'validate_not_null', 1, 'column_name', 'Postal_Code'),
(208, 'data_quality', 'validate_not_null', 1, 'message', 'Postal_Code must not be null'),
(208, 'data_quality', 'validate_not_null', 1, 'if_not_compliant', 'quarantine'),
-- 301 gold.dbo.fact_fire_incidents: left-join every master/code reference (manifest join keys)
(301, 'data_transformation_steps', 'join_data', 1, 'join_type', 'left'),
(301, 'data_transformation_steps', 'join_data', 1, 'right_table_name', 'silver.dbo.code_ref'),
(301, 'data_transformation_steps', 'join_data', 1, 'join_condition', 'a.Code_ID = b.Code_ID'),
(301, 'data_transformation_steps', 'join_data', 1, 'left_columns', '*'),
(301, 'data_transformation_steps', 'join_data', 1, 'right_columns', 'Code_Desc,Incident_Category,Code_Active_Flag'),
(301, 'data_transformation_steps', 'join_data', 1, 'broadcast_hint', 'true'),
(301, 'data_transformation_steps', 'join_data', 2, 'join_type', 'left'),
(301, 'data_transformation_steps', 'join_data', 2, 'right_table_name', 'silver.dbo.severity_ref'),
(301, 'data_transformation_steps', 'join_data', 2, 'join_condition', 'a.Severity = b.Severity'),
(301, 'data_transformation_steps', 'join_data', 2, 'left_columns', '*'),
(301, 'data_transformation_steps', 'join_data', 2, 'right_columns', 'Severity_Desc,Severity_Rank'),
(301, 'data_transformation_steps', 'join_data', 2, 'broadcast_hint', 'true'),
(301, 'data_transformation_steps', 'join_data', 3, 'join_type', 'left'),
(301, 'data_transformation_steps', 'join_data', 3, 'right_table_name', 'silver.dbo.station_master'),
(301, 'data_transformation_steps', 'join_data', 3, 'join_condition', 'a.Station_ID = b.Station_ID'),
(301, 'data_transformation_steps', 'join_data', 3, 'left_columns', '*'),
(301, 'data_transformation_steps', 'join_data', 3, 'right_columns', 'Station_Name,Station_Zone,Station_Active_Flag'),
(301, 'data_transformation_steps', 'join_data', 3, 'broadcast_hint', 'true'),
(301, 'data_transformation_steps', 'join_data', 4, 'join_type', 'left'),
(301, 'data_transformation_steps', 'join_data', 4, 'right_table_name', 'silver.dbo.property_type_ref'),
(301, 'data_transformation_steps', 'join_data', 4, 'join_condition', 'a.Property_Type = b.Property_Type'),
(301, 'data_transformation_steps', 'join_data', 4, 'left_columns', '*'),
(301, 'data_transformation_steps', 'join_data', 4, 'right_columns', 'Property_Category,Property_Desc'),
(301, 'data_transformation_steps', 'join_data', 4, 'broadcast_hint', 'true'),
(301, 'data_transformation_steps', 'join_data', 5, 'join_type', 'left'),
(301, 'data_transformation_steps', 'join_data', 5, 'right_table_name', 'silver.dbo.cause_ref'),
(301, 'data_transformation_steps', 'join_data', 5, 'join_condition', 'a.Cause_ID = b.Cause_ID'),
(301, 'data_transformation_steps', 'join_data', 5, 'left_columns', '*'),
(301, 'data_transformation_steps', 'join_data', 5, 'right_columns', 'Cause_Category'),
(301, 'data_transformation_steps', 'join_data', 5, 'broadcast_hint', 'true'),
(301, 'data_transformation_steps', 'join_data', 6, 'join_type', 'left'),
(301, 'data_transformation_steps', 'join_data', 6, 'right_table_name', 'silver.dbo.status_ref'),
(301, 'data_transformation_steps', 'join_data', 6, 'join_condition', 'a.Status = b.Status'),
(301, 'data_transformation_steps', 'join_data', 6, 'left_columns', '*'),
(301, 'data_transformation_steps', 'join_data', 6, 'right_columns', 'Status_Desc,Is_Final'),
(301, 'data_transformation_steps', 'join_data', 6, 'broadcast_hint', 'true'),
(301, 'data_transformation_steps', 'join_data', 7, 'join_type', 'left'),
(301, 'data_transformation_steps', 'join_data', 7, 'right_table_name', 'silver.dbo.location_master'),
(301, 'data_transformation_steps', 'join_data', 7, 'join_condition', 'a.Postal_Code = b.Postal_Code'),
(301, 'data_transformation_steps', 'join_data', 7, 'left_columns', '*'),
(301, 'data_transformation_steps', 'join_data', 7, 'right_columns', 'Region'),
(301, 'data_transformation_steps', 'join_data', 7, 'broadcast_hint', 'true'),
(301, 'data_quality', 'validate_not_null', 1, 'column_name', 'Code_Desc'),
(301, 'data_quality', 'validate_not_null', 1, 'message', 'Incident Code_ID has no match in CODE_REF'),
(301, 'data_quality', 'validate_not_null', 1, 'if_not_compliant', 'warn'),
(301, 'data_quality', 'validate_not_null', 2, 'column_name', 'Station_Name'),
(301, 'data_quality', 'validate_not_null', 2, 'message', 'Incident Station_ID has no match in STATION_MASTER'),
(301, 'data_quality', 'validate_not_null', 2, 'if_not_compliant', 'warn')
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
