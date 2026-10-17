CREATE   PROCEDURE [dbo].[Get_Last_Run_Status]
(
    @data_product_key varchar(128) = 'default',
    @table_id int
)
AS
BEGIN
    SET NOCOUNT ON
    SET @data_product_key = COALESCE(NULLIF(TRIM(@data_product_key), ''), 'default')

    SELECT      COALESCE(MIN(Ingestion_Status), 'Failed') AS Last_Ingestion_Status
    FROM        [dbo].[Data_Pipeline_Logs]
    WHERE       Trigger_Execution_Start_Time = (
                    SELECT      MAX(Trigger_Execution_Start_Time)
                    FROM        [dbo].[Data_Pipeline_Logs]
                    WHERE       COALESCE(NULLIF(Data_Product_Key, ''), 'default') = @data_product_key
                    AND         Table_ID = @table_id
                )
    AND         COALESCE(NULLIF(Data_Product_Key, ''), 'default') = @data_product_key
    AND         Table_ID = @table_id
    AND         Processing_Phase = 'Batch'

END

GO