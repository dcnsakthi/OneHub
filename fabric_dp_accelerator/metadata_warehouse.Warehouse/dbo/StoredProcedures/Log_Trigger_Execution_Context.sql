CREATE   PROCEDURE [dbo].[Log_Trigger_Execution_Context]
(
    @trigger_execution_id varchar(500),
    @trigger_name varchar(500) = NULL,
    @data_product_key varchar(128) = 'default',
    @is_feature_workspace varchar(10) = 'false',
    @metadata_workspace_id varchar(36) = NULL,
    @metadata_workspace_name varchar(256) = NULL,
    @metadata_warehouse_id varchar(36) = NULL,
    @metadata_warehouse_endpoint varchar(512) = NULL,
    @metadata_lakehouse_id varchar(36) = NULL,
    @data_product_connection_id varchar(100) = NULL,
    @central_metadata_workspace_id varchar(36) = NULL,
    @central_metadata_warehouse_id varchar(36) = NULL,
    @central_metadata_warehouse_endpoint varchar(512) = NULL,
    @execution_start_time datetime2(6) = NULL,
    @execution_end_time datetime2(6) = NULL,
    @status varchar(50) = NULL,
    @fabric_monitor_url varchar(4000) = NULL
)
AS
BEGIN
    -- Insert-only trigger-execution event log (Started | Completed | Failed).
    -- One row per call. No UPDATE; current state is derived ad-hoc by queries,
    -- mirroring the Data_Pipeline_Logs pattern.
    --
    -- This procedure does NOT swallow errors. The best-effort guarantee is enforced
    -- by PL_01a: its logging activities are wired with "Completed" dependency
    -- conditions plus a terminal success handler, so a failed insert is still
    -- visible in Fabric monitoring but never fails the data run.
    SET NOCOUNT ON

    SET @data_product_key = COALESCE(NULLIF(TRIM(@data_product_key), ''), 'default')

    -- Transport is an all-string context object; cast the feature flag to bit here.
    DECLARE @is_feature_bit bit =
        CASE WHEN LOWER(LTRIM(RTRIM(COALESCE(@is_feature_workspace, 'false')))) IN ('true', '1')
             THEN 1 ELSE 0 END

    DECLARE @event_time datetime2(6) = COALESCE(@execution_end_time, @execution_start_time, SYSUTCDATETIME())

    INSERT INTO [dbo].[Trigger_Execution_Context] (
        [Trigger_Execution_ID]
        ,[Trigger_Name]
        ,[Data_Product_Key]
        ,[Is_Feature_Workspace]
        ,[Metadata_Workspace_ID]
        ,[Metadata_Workspace_Name]
        ,[Metadata_Warehouse_ID]
        ,[Metadata_Warehouse_Endpoint]
        ,[Metadata_Lakehouse_ID]
        ,[Data_Product_Connection_ID]
        ,[Central_Metadata_Workspace_ID]
        ,[Central_Metadata_Warehouse_ID]
        ,[Central_Metadata_Warehouse_Endpoint]
        ,[Execution_Start_Time]
        ,[Execution_End_Time]
        ,[Execution_Status]
        ,[Fabric_Monitor_URL]
        ,[Date_Key]
    )
    VALUES (
        @trigger_execution_id
        ,@trigger_name
        ,@data_product_key
        ,@is_feature_bit
        ,@metadata_workspace_id
        ,@metadata_workspace_name
        ,@metadata_warehouse_id
        ,@metadata_warehouse_endpoint
        ,@metadata_lakehouse_id
        ,@data_product_connection_id
        ,@central_metadata_workspace_id
        ,@central_metadata_warehouse_id
        ,@central_metadata_warehouse_endpoint
        ,@execution_start_time
        ,@execution_end_time
        ,@status
        ,@fabric_monitor_url
        ,CONVERT(VARCHAR, CAST(@event_time AS DATE), 112)
    )

    -- Fabric Warehouse has no cursor support, and a parameterized EXEC that returns
    -- neither rows nor a row count drives ODBC clients onto that unsupported path.
    SELECT 1
    RETURN
END

GO