CREATE   PROCEDURE [dbo].[FIFO_Status]
(
    -- Add the parameters for the stored procedure here
	@data_product_key varchar(128) = 'default',
    @table_id int,
	@target_datastore varchar(255)
)
AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON
	SET @data_product_key = COALESCE(NULLIF(TRIM(@data_product_key), ''), 'default')

    -- Insert statements for procedure here
    DECLARE @proceed varchar(255)

	-- Determine number of records in logging table for control table record id
	DECLARE @matchedrecords int

	SET @matchedrecords = (
		SELECT	COUNT(*)
		FROM	[dbo].[Data_Pipeline_Logs]
		WHERE	COALESCE(NULLIF([Data_Product_Key], ''), 'default') = @data_product_key
		AND		[Table_ID] = @table_id
		AND		[Log_ID] IN (
							SELECT		[Log_ID]
							FROM		[dbo].[Data_Pipeline_Logs]
							WHERE		COALESCE(NULLIF([Data_Product_Key], ''), 'default') = @data_product_key
							AND			[Table_ID] = @table_id
							AND 		[Processing_Phase] = 'Staging'
							GROUP BY	[Log_ID]
							HAVING		MIN([Ingestion_Status]) = 'Started'
						)
		AND     [Ingestion_Start_Time] > DATEADD(hh, -24, GETUTCDATE())
	)

	IF @matchedrecords = 0
	BEGIN
		SET @proceed = 'Yes'
	END
	ELSE
	BEGIN
		DECLARE @failure_message varchar(5000) = 'The pipeline failed because another run associated with Data_Product_Key = ' + @data_product_key + ' and Table_ID = ' + CAST(@table_id AS varchar(10)) + ' is either currently in progress or was recently interrupted due to a pipeline failure. If the issue is due to an active run, please ensure that your triggers are spaced out appropriately to allow the current ingestion to complete before initiating a new one. If the failure was caused by a recently interrupted run, please delete all log IDs returned by the following query:

		SELECT  [Log_ID]
        FROM	[dbo].[Data_Pipeline_Logs]
		WHERE	COALESCE(NULLIF([Data_Product_Key], ''''), ''default'') = ''' + @data_product_key + '''
		AND		[Table_ID] = ' + CAST(@table_id AS VARCHAR(255)) + '
		AND		[Log_ID] IN (
							SELECT		[Log_ID]
							FROM		[dbo].[Data_Pipeline_Logs]
							WHERE		COALESCE(NULLIF([Data_Product_Key], ''''), ''default'') = ''' + @data_product_key + '''
							AND			[Table_ID] = ' + CAST(@table_id AS VARCHAR(255)) + '
							AND 		[Processing_Phase] = ''Staging''
							GROUP BY	[Log_ID]
							HAVING		MIN([Ingestion_Status]) = ''Started''
						)
		AND     [Ingestion_Start_Time] > DATEADD(hh, -24, GETUTCDATE())'

		RAISERROR (@failure_message, 16, 1);

	END

	--return values for downstream processing
	SELECT @proceed [Proceed]
    RETURN

END

GO