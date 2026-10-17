CREATE   PROCEDURE [dbo].[Log_Data_Movement]
(
    -- Add the parameters for the stored procedure here
    @log_id varchar(4000) = NULL,
	@data_product_key varchar(128) = 'default',
    @table_id int = NULL,
    @source_details varchar(4000) = NULL,
	@target_datastore varchar(4000) = NULL,
    @target_entity varchar(4000) = NULL,
    @event_start_time DATETIME2(7) = NULL,
    @event_end_time DATETIME2(7) = NULL,
    @status varchar(4000) = NULL,
    @watermark_value varchar(4000) = NULL,
    @records_processed varchar(4000) = NULL,
    @quarantined_records varchar(4000) = NULL,
    @data_quality_warnings varchar(max) = NULL,
    @data_quality_failures varchar(max) = NULL,
    @fabric_monitor_url varchar(4000) = NULL,
	@trigger_name varchar(4000) = NULL,
	@trigger_step INT = NULL,
	@trigger_id varchar(4000) = NULL,
	@trigger_time DATETIME2(7) = NULL,
	-- New lineage-related parameters
	@source_medallion_layer varchar(50) = NULL,
	@source_type varchar(100) = NULL,
	@target_medallion_layer varchar(50) = NULL,
	@target_type varchar(100) = NULL,
	-- Processing phase: Staging (extract to files), Batch (load to tables)
	@processing_phase varchar(50) = 'Batch'
)
AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON
	SET @data_product_key = COALESCE(NULLIF(TRIM(@data_product_key), ''), 'default')

	-- Normalize datastore name to lowercase for case-insensitive matching
	-- (Fabric Warehouse uses BIN2 collation which is case-sensitive)
	SET @target_datastore = LOWER(@target_datastore)

	IF @status = 'Started'
	BEGIN
		INSERT INTO [dbo].[Data_Pipeline_Logs] (
			[Log_ID]
			,[Data_Product_Key]
			,[Table_ID] 
			,[Source_Medallion_Layer]
			,[Source_Type]
			,[Data_Source_Details] 
			,[Target_Medallion_Layer]
			,[Target_Type]
			,[Target_Datastore]
			,[Target_Entity]
			,[Ingestion_Start_Time] 
			,[Ingestion_End_Time] 
			,[Ingestion_Status]
			,[Processing_Phase]
			,[Watermark_Value] 
			,[Records_Processed] 
			,[Quarantined_Records] 
			,[Fabric_Monitor_URL] 
			,[Trigger_Name] 
			,[Trigger_Step] 
			,[Trigger_Execution_ID] 
			,[Trigger_Execution_Start_Time]
			,[Date_Key]
		)
		VALUES (
            @log_id
			,@data_product_key
            ,@table_id
			,@source_medallion_layer
			,@source_type
            ,@source_details
			,@target_medallion_layer
			,@target_type
			,@target_datastore
			,@target_entity
            ,@event_start_time 
            ,@event_end_time
            ,@status
			,@processing_phase
            ,@watermark_value
            ,@records_processed
			,@quarantined_records
            ,@fabric_monitor_url
			,@trigger_name
			,@trigger_step
			,@trigger_id
			,@trigger_time
			,CONVERT(VARCHAR, CAST(@event_start_time AS DATE), 112)
		)
	END

	IF @status = 'Processed'
	BEGIN
		INSERT INTO [dbo].[Data_Pipeline_Logs] (
			[Log_ID]
			,[Data_Product_Key]
			,[Table_ID] 
			,[Source_Medallion_Layer]
			,[Source_Type]
			,[Data_Source_Details] 
			,[Target_Medallion_Layer]
			,[Target_Type]
			,[Target_Datastore]
			,[Target_Entity] 
			,[Ingestion_Start_Time] 
			,[Ingestion_End_Time] 
			,[Ingestion_Status]
			,[Processing_Phase]
			,[Watermark_Value] 
			,[Records_Processed] 
			,[Quarantined_Records] 
			,[Fabric_Monitor_URL] 
			,[Trigger_Name] 
			,[Trigger_Step] 
			,[Trigger_Execution_ID] 
			,[Trigger_Execution_Start_Time]
			,[Date_Key]
		)
		VALUES (
            @log_id
			,@data_product_key
            ,@table_id
			,@source_medallion_layer
			,@source_type
            ,@source_details
			,@target_medallion_layer
			,@target_type
			,@target_datastore
			,@target_entity
            ,@event_start_time 
            ,@event_end_time
            ,@status
			,@processing_phase
            ,@watermark_value
            ,@records_processed
			,@quarantined_records
            ,@fabric_monitor_url
			,@trigger_name
			,@trigger_step
			,@trigger_id
			,@trigger_time
			,CONVERT(VARCHAR, CAST(@event_end_time AS DATE), 112)
		)
	END

	IF @status = 'Failed'
	BEGIN
		INSERT INTO [dbo].[Data_Pipeline_Logs] (
			[Log_ID]
			,[Data_Product_Key]
			,[Table_ID] 
			,[Source_Medallion_Layer]
			,[Source_Type]
			,[Data_Source_Details] 
			,[Target_Medallion_Layer]
			,[Target_Type]
			,[Target_Datastore]
			,[Target_Entity]
			,[Ingestion_Start_Time] 
			,[Ingestion_End_Time] 
			,[Ingestion_Status]
			,[Processing_Phase]
			,[Watermark_Value] 
			,[Records_Processed] 
			,[Quarantined_Records] 
			,[Fabric_Monitor_URL] 
			,[Trigger_Name] 
			,[Trigger_Step] 
			,[Trigger_Execution_ID] 
			,[Trigger_Execution_Start_Time]
			,[Date_Key]
		)
		VALUES (
            @log_id
			,@data_product_key
            ,@table_id
			,@source_medallion_layer
			,@source_type
            ,@source_details
			,@target_medallion_layer
			,@target_type
			,@target_datastore
			,@target_entity
            ,@event_start_time 
            ,@event_end_time
            ,@status
			,@processing_phase
            ,@watermark_value
            ,COALESCE(@records_processed,0)
			,@quarantined_records
            ,@fabric_monitor_url
			,@trigger_name
			,@trigger_step
			,@trigger_id
			,@trigger_time
            ,CONVERT(VARCHAR, CAST(@event_end_time AS DATE), 112)
		)
	END

	IF @data_quality_warnings IS NOT NULL AND @data_quality_warnings != '' AND @data_quality_warnings != '[]'
	BEGIN
		INSERT INTO [dbo].[Data_Quality_Notifications] (
			[Log_ID]
			,[Data_Product_Key]
			,[Datastore_Name]
			,[Table_ID]
			,[Table_Name]
			,[Data_Quality_Category]
			,[Data_Quality_Result]
			,[Data_Quality_Message]
			,[Rows_Impacted]
			,[Data_Quarantined]
			,[Rows_Quarantined]
			,[Ingestion_Start_Time]
			,[Ingestion_End_Time]
			,[Fabric_Monitor_URL]
			,[Date_Key]
		)
		SELECT @log_id [LogID]
				, @data_product_key
				, @target_datastore
				, @table_id [TableID]
				, @target_entity
			    , [Category] [DataQualityCategory]
			    , [Outcome] [DataQualityResult]
                , [Message]
				, COALESCE([RowsQuarantined], [Rows_Impacted], 0)
                , COALESCE([Quarantined],'No') [DataQuarantined]
				, COALESCE([RowsQuarantined], 0)
                , @event_start_time [EventStartTime]
                , @event_end_time [EventEndTime]
                , @fabric_monitor_url [FabricMonitorURL]
			    , CONVERT(VARCHAR, CAST(@event_end_time AS DATE) , 112)
		FROM OPENJSON(@data_quality_warnings) WITH (
                [Category] VARCHAR(255) '$.category',
                [Message] VARCHAR(max) '$.message',
                [Quarantined] VARCHAR(255) '$.quarantined',
				[RowsQuarantined] VARCHAR(255) '$.rows_quarantined',
				[Rows_Impacted] VARCHAR(max) '$.rows_impacted',
				[Outcome] VARCHAR(255) '$.outcome'
			)
	END

	IF @data_quality_failures IS NOT NULL AND @data_quality_failures != '' AND @data_quality_failures != '[]'
	BEGIN
		INSERT INTO [dbo].[Data_Quality_Notifications] (
			[Log_ID]
			,[Data_Product_Key]
			,[Datastore_Name]
			,[Table_ID]
			,[Table_Name]
			,[Data_Quality_Category]
			,[Data_Quality_Result]
			,[Data_Quality_Message]
			,[Rows_Impacted]
			,[Data_Quarantined]
			,[Rows_Quarantined]
			,[Ingestion_Start_Time]
			,[Ingestion_End_Time]
			,[Fabric_Monitor_URL]
			,[Date_Key]
		)
		SELECT    @log_id [LogID]
				, @data_product_key
				, @target_datastore
                , @table_id [TableID]
				, @target_entity
			    , [Category] [DataQualityCategory]
			    , 'Failure' [DataQualityResult]
                , [Message]
				, COALESCE([Rows_Impacted],0)
                , 'No' [DataQuarantined]
				, 0
                , @event_start_time [EventStartTime]
                , @event_end_time [EventEndTime]
                , @fabric_monitor_url [FabricMonitorURL]
			    , CONVERT(VARCHAR, CAST(@event_end_time AS DATE) , 112)
		FROM OPENJSON(@data_quality_failures) WITH (
			[Category] VARCHAR(255) '$.category',
			[Message] VARCHAR(max) '$.message',
			[Rows_Impacted] VARCHAR(max) '$.rows_impacted'
		)
	END


SELECT 1
RETURN

END

GO