CREATE   PROCEDURE [dbo].[Get_Exploratory_Analysis_Input]
(
    @data_product_key varchar(128) = 'default',
    @trigger_name varchar(255)
)
AS
BEGIN
    SET NOCOUNT ON;
    SET @data_product_key = COALESCE(NULLIF(TRIM(@data_product_key), ''), 'default');

    WITH profiling_config AS (
        SELECT      @data_product_key AS Data_Product_Key,
                    o.Table_ID,
                    o.Target_Datastore,
                    o.Target_Entity,
                    TRIM(LOWER(COALESCE(c.Configuration_Value, 'weekly'))) AS Data_Profiling_Frequency,
                    MAX(e.Data_Profile_Execution_Time) AS Last_Run_Datetime,
                    DATEDIFF(day, MAX(e.Data_Profile_Execution_Time), GETUTCDATE()) AS Days_Since_Last_Run
        FROM        [dbo].[Data_Pipeline_Metadata_Orchestration] o
        LEFT JOIN   [dbo].[Data_Pipeline_Metadata_Primary_Configuration] c
            ON      o.Table_ID = c.Table_ID
            AND     c.Configuration_Name = 'data_profiling_frequency'
        LEFT JOIN   [dbo].[Exploratory_Data_Analysis_Results] e
            ON      o.Table_ID = e.Table_ID
            AND     COALESCE(NULLIF(e.Data_Product_Key, ''), 'default') = @data_product_key
        WHERE       o.Target_Entity NOT LIKE '%/%'
            AND     o.Ingestion_Active = 1
            AND     o.Trigger_Name = @trigger_name
            AND     TRIM(LOWER(COALESCE(c.Configuration_Value, 'weekly'))) != 'never'
        GROUP BY    o.Table_ID, o.Target_Datastore, o.Target_Entity, c.Configuration_Value
    )
    SELECT      Data_Product_Key,
                Table_ID,
                Target_Datastore,
                Target_Entity,
                Data_Profiling_Frequency,
                Last_Run_Datetime,
                Days_Since_Last_Run
    FROM        profiling_config
    WHERE       (Data_Profiling_Frequency = 'monthly' AND Days_Since_Last_Run >= 30)
        OR      (Data_Profiling_Frequency = 'weekly' AND Days_Since_Last_Run >= 7)
        OR      (Data_Profiling_Frequency = 'daily' AND Days_Since_Last_Run >= 1)
        OR      Last_Run_Datetime IS NULL

END

GO