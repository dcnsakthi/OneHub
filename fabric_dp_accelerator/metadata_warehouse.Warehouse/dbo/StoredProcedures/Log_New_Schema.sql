CREATE   PROCEDURE [dbo].[Log_New_Schema]
(
    @data_product_key varchar(128) = 'default',
    @table_id int = NULL,
	@datastore_name varchar(4000) = NULL,
    @table_name varchar(4000) = NULL,
    @schema_id varchar(4000) = NULL,
    @schema_details varchar(max) = NULL,
    @schema_updates varchar(max) = NULL,
    @fabric_monitor_url varchar(4000) = NULL,
    @end_time datetime2(6)
)
AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON
    SET @data_product_key = COALESCE(NULLIF(TRIM(@data_product_key), ''), 'default')

	INSERT INTO [dbo].[Schema_Logs] (
        [Data_Product_Key]
    ,[Table_ID]
        ,[Datastore_Name]			
        ,[Table_Name]				
        ,[Schema_ID]				
        ,[Schema_Details]		
        ,[Schema_Arrival_Time]	
        ,[Fabric_Monitor_URL]		
        ,[Date_Key]
	)
	VALUES (
        @data_product_key
        ,@table_id
		,@datastore_name
        ,@table_name
        ,@schema_id
        ,@schema_details
        ,@end_time
        ,@fabric_monitor_url
        ,CONVERT(VARCHAR, CAST(@end_time AS DATE) , 112)
	)


    IF @schema_updates != ''
    BEGIN
        INSERT INTO [dbo].[Schema_Changes] (
            [Data_Product_Key]
            ,[Table_ID]
            ,[Datastore_Name]
            ,[Table_Name] 
            ,[Change_Type] 
            ,[Column_Name] 
            ,[Data_Type_Details] 
            ,[Schema_Arrival_Time]
            ,[Fabric_Monitor_URL] 	
            ,[Date_Key]
        )
		SELECT @data_product_key
        ,@table_id
                ,@datastore_name
                 ,@table_name		
                ,[change_type]
                ,[column] 
                ,[data_type]
                ,@end_time
                ,@fabric_monitor_url
                ,CONVERT(VARCHAR, CAST(@end_time AS DATE) , 112)
		FROM OPENJSON(@schema_updates) WITH (
                [column] VARCHAR(255) '$.column',
                [data_type] VARCHAR(max) '$.data_type',
                [change_type] VARCHAR(255) '$.change_type'
			)
    END

SELECT 1
RETURN

END

GO