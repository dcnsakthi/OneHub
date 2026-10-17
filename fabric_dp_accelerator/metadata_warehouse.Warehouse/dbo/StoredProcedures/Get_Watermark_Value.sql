CREATE   PROCEDURE [dbo].[Get_Watermark_Value]
(
    -- Add the parameters for the stored procedure here
	@data_product_key varchar(128) = 'default',
    @table_id int = null,
	@target_datastore varchar(255) = null,
	@processing_phase varchar(50) = 'Batch',
	@watermark_strategy varchar(255) = null,
	@watermark_field_data_type varchar(255) = null
)
AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON
	SET @data_product_key = COALESCE(NULLIF(TRIM(@data_product_key), ''), 'default')

    -- Insert statements for procedure here
    DECLARE @watermark_value varchar(255)

	-- Determine number of records in logging table for control table record id
	DECLARE @matchedrecords int

	-- Normalize datastore name to lowercase for case-insensitive matching
	-- (Fabric Warehouse uses BIN2 collation which is case-sensitive)
	SET @target_datastore = LOWER(@target_datastore)

	SET @matchedrecords = (
		SELECT	COUNT(*)
		FROM	[dbo].[Data_Pipeline_Logs]
		WHERE	COALESCE(NULLIF([Data_Product_Key], ''), 'default') = @data_product_key
		AND		[Table_ID] = @table_id
		AND 	LOWER([Target_Datastore]) = @target_datastore
		AND		[Processing_Phase] = @processing_phase
		AND		[Ingestion_Status] = 'Processed'
	)

	-- Determine watermark settings. Split metadata workspaces pass these in from
	-- product metadata; central-only mode can still fall back to local metadata.
	SET @watermark_field_data_type = NULLIF(LOWER(TRIM(@watermark_field_data_type)), '')
	SET @watermark_strategy = NULLIF(LOWER(TRIM(@watermark_strategy)), '')

	IF @watermark_field_data_type IS NULL
	BEGIN
		SET @watermark_field_data_type = NULLIF(LOWER(TRIM((
		SELECT	[Configuration_Value]
		FROM	[dbo].[Data_Pipeline_Metadata_Primary_Configuration]
		WHERE	[Table_ID] = @table_id
		AND		[Configuration_Category] = 'watermark_details'
        AND		[Configuration_Name] = 'data_type'
	))), '')
	END

	IF @watermark_strategy IS NULL
	BEGIN
		SET @watermark_strategy = NULLIF(LOWER(TRIM((
		SELECT	[Configuration_Value]
		FROM	[dbo].[Data_Pipeline_Metadata_Primary_Configuration]
		WHERE	[Table_ID] = @table_id
		AND		[Configuration_Category] = 'watermark_details'
        AND		[Configuration_Name] = 'strategy'
	))), '')
	END

	IF @watermark_strategy IS NULL OR @watermark_strategy = ''
	BEGIN
		DECLARE @missing_strategy_message varchar(500)
		SET @missing_strategy_message = CONCAT('Missing required watermark_details.strategy for Table_ID ', @table_id, '. Set strategy=''none'' explicitly when no watermarking is used.')
		RAISERROR (@missing_strategy_message, 16, 1)
		RETURN
	END

	IF @watermark_strategy NOT IN ('none', 'column', 'change_data_feed', 'file_modified_time', 'custom')
	BEGIN
		DECLARE @invalid_strategy_message varchar(500)
		SET @invalid_strategy_message = CONCAT('Invalid watermark_details.strategy for Table_ID ', @table_id, ': ', @watermark_strategy, '. Valid values are: none, column, change_data_feed, file_modified_time, custom.')
		RAISERROR (@invalid_strategy_message, 16, 1)
		RETURN
	END

	--default watermark value is datetime only for active column watermarking
	IF @watermark_field_data_type IS NULL AND @watermark_strategy = 'column'
	BEGIN
		SET @watermark_field_data_type = 'datetime'
	END


	--If no matches records, set watermark value to lowest value for dataset type
	IF @matchedrecords = 0
	BEGIN
		IF @watermark_strategy <> 'column'
		BEGIN
			SET @watermark_value = ''
		END
		ELSE IF @watermark_field_data_type = 'datetime'
		BEGIN
			SET @watermark_value = '1900-01-01 00:00:00'
		END
		ELSE
		BEGIN
			SET @watermark_value = '-1'
		END

		IF @watermark_field_data_type IS NULL
		BEGIN
			SET @watermark_value = ''
		END
	END

	--Otherwise, set it to latest watermark value
	IF @matchedrecords > 0
	BEGIN
		DECLARE @max_watermark_value varchar(255)
		IF @watermark_field_data_type = 'datetime'
		BEGIN
			SET @max_watermark_value = (
				SELECT		TOP 1 CASE
									WHEN [Watermark_Value] = 'FULL_RELOAD' 
										THEN '1900-01-01 14:44:44' 
									-- required when data is moved from bronze_files as watermark is integer value of folderpath
									WHEN ISNUMERIC([Watermark_Value]) = 1
                                        THEN [Watermark_Value]
									-- sql server throws an error on datetime 6 string values on putting date in single quotes
									WHEN @processing_phase = 'Staging'
                                        THEN CAST(CAST([Watermark_Value] AS DATETIME2(3)) AS varchar(255))
									ELSE CAST(CAST([Watermark_Value] AS DATETIME2(6)) AS varchar(255))
								  END
				FROM		[dbo].[Data_Pipeline_Logs]
				WHERE		COALESCE(NULLIF([Data_Product_Key], ''), 'default') = @data_product_key
				AND			[Table_ID] = @table_id
				AND 		LOWER([Target_Datastore]) = @target_datastore
				AND			[Processing_Phase] = @processing_phase
				AND			[Ingestion_Status] = 'Processed'
				ORDER BY	[Ingestion_End_Time] DESC
			)
		END
		ELSE
		BEGIN
			SET @max_watermark_value = (
				SELECT		TOP 1 CASE
									WHEN [Watermark_Value] = 'FULL_RELOAD' 
										THEN '-2'
									ELSE [Watermark_Value]
								  END
				FROM		[dbo].[Data_Pipeline_Logs]
				WHERE		COALESCE(NULLIF([Data_Product_Key], ''), 'default') = @data_product_key
				AND			[Table_ID] = @table_id
				AND 		LOWER([Target_Datastore]) = @target_datastore
				AND			[Processing_Phase] = @processing_phase
				AND			[Ingestion_Status] = 'Processed'
				ORDER BY	[Ingestion_End_Time] DESC
			)
		END

		SET @watermark_value = @max_watermark_value
	END

	DECLARE @full_reload varchar(255)
	IF @watermark_value = '1900-01-01 14:44:44' or @watermark_value = '-2'
	BEGIN
		SET @full_reload = 'Yes'
	END

	-- if watermark value empty or null, set @watermark_value to default
	IF @watermark_value = '' or @watermark_value IS NULL
	BEGIN
		IF @watermark_strategy <> 'column'
		BEGIN
			SET @watermark_value = ''
		END
		ELSE IF @watermark_field_data_type = 'datetime'
		BEGIN
			SET @watermark_value = '1900-01-01 00:00:00'
		END
		ELSE
		BEGIN
			SET @watermark_value = '-1'
		END

		IF @watermark_field_data_type IS NULL
		BEGIN
			SET @watermark_value = ''
		END
	END

	--return values for downstream processing
	SELECT @watermark_value [watermark_value], @matchedrecords [matched_records], @full_reload [full_reload]
    RETURN

END

GO