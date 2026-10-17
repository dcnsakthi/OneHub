CREATE   PROCEDURE [dbo].[Get_Pipeline_Tables]
(
    @data_product_key varchar(128) = 'default',
    @trigger_name varchar(255),
    @start_with_step int = 0,
    @table_ids varchar(4000) = '0'
)
AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON
    SET @data_product_key = COALESCE(NULLIF(TRIM(@data_product_key), ''), 'default')

    -- Returns the tables to process for a given trigger, grouped by Order_Of_Operations.
    -- Each row represents one processing step containing a comma-separated list of
    -- table details in the format: 'Table_ID:Processing_Method:Target_Datastore:Target_Entity:Source_Datastore:Staging_Lakehouse:Spark_Environment_ID:Notebook_Session_Tag:Log_ID'
    -- Source_Datastore is included from Primary Configuration if it exists, otherwise empty.
    -- Staging_Lakehouse is included from Primary Configuration if it exists, otherwise empty.
    -- Spark_Environment_ID is included from Primary Configuration (other_settings) if it exists, otherwise empty.
    -- Notebook_Session_Tag is included from Primary Configuration (other_settings) if it exists, otherwise empty.
    -- Log_ID is a pre-generated GUID so the calling pipeline can correlate Activity_Run_Logs entries
    --   even when the downstream notebook or activity fails to start.
    --   Only generated for non-batch methods; batch processing generates its own Log_ID internally.

    -- Fail fast if the filters would return zero rows. This prevents the Fabric pipeline from
    -- silently succeeding with an empty ForEach when, for example, a parameter name was passed
    -- in the wrong case (Fabric pipeline parameters are case-sensitive and the orchestration
    -- comparison treats Trigger_Name literally as supplied).
    DECLARE @matchedrecords int

    SET @matchedrecords = (
        SELECT  COUNT(*)
        FROM    [dbo].[Data_Pipeline_Metadata_Orchestration] o
        WHERE   o.[Trigger_Name] = @trigger_name
        AND     o.[Order_Of_Operations] >= @start_with_step
        AND     (o.[Table_ID] IN (SELECT CAST(TRIM(value) AS int) FROM STRING_SPLIT(@table_ids, ','))
        OR      @table_ids = '0')
        AND     o.[Ingestion_Active] = 1
    )

    IF @matchedrecords = 0
    BEGIN
        RAISERROR ('Get_Pipeline_Tables returned no rows. Check active metadata rows and use lower_case Fabric pipeline parameter names: trigger_name, start_with_step, table_ids.', 16, 1);
        RETURN;
    END

    SELECT      @data_product_key AS [Data_Product_Key],
                o.[Trigger_Name],
                o.[Order_Of_Operations],
                STRING_AGG(
                    CONVERT(VARCHAR(MAX), CONCAT(
                        '''',
                        o.[Table_ID], ':',
                        o.[Processing_Method], ':',
                        o.[Target_Datastore], ':',
                        o.[Target_Entity], ':',
                        ISNULL(pc.[Configuration_Value], ''), ':',
                        ISNULL(sl.[Configuration_Value], ''), ':',
                        ISNULL(se.[Configuration_Value], ''), ':',
                        ISNULL(nst.[Configuration_Value], ''), ':',
                        CASE WHEN o.[Processing_Method] != 'batch'
                             THEN LOWER(CONVERT(varchar(36), NEWID()))
                             ELSE ''
                        END,
                        ''''
                    )),
                    ','
                ) AS [Pipeline_Table_Details]
    FROM        [dbo].[Data_Pipeline_Metadata_Orchestration] o
    LEFT JOIN   [dbo].[Data_Pipeline_Metadata_Primary_Configuration] pc
        ON      pc.[Table_ID] = o.[Table_ID]
        AND     pc.[Configuration_Category] = 'source_details'
        AND     pc.[Configuration_Name] = 'datastore_name'
    LEFT JOIN   [dbo].[Data_Pipeline_Metadata_Primary_Configuration] sl
        ON      sl.[Table_ID] = o.[Table_ID]
        AND     sl.[Configuration_Category] = 'source_details'
        AND     sl.[Configuration_Name] = 'staging_lakehouse_name'
    LEFT JOIN   [dbo].[Data_Pipeline_Metadata_Primary_Configuration] se
        ON      se.[Table_ID] = o.[Table_ID]
        AND     se.[Configuration_Category] = 'other_settings'
        AND     se.[Configuration_Name] = 'spark_environment_id'
    LEFT JOIN   [dbo].[Data_Pipeline_Metadata_Primary_Configuration] nst
        ON      nst.[Table_ID] = o.[Table_ID]
        AND     nst.[Configuration_Category] = 'other_settings'
        AND     nst.[Configuration_Name] = 'notebook_session_tag'
    WHERE       o.[Trigger_Name] = @trigger_name
    AND         o.[Order_Of_Operations] >= @start_with_step
    AND         (o.[Table_ID] IN (SELECT CAST(TRIM(value) AS int) FROM STRING_SPLIT(@table_ids, ','))
    OR          @table_ids = '0')
    AND         o.[Ingestion_Active] = 1
    GROUP BY    o.[Trigger_Name], o.[Order_Of_Operations]
    ORDER BY    o.[Order_Of_Operations]
END

GO