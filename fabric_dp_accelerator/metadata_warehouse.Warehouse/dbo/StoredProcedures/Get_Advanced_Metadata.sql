CREATE   PROCEDURE [dbo].[Get_Advanced_Metadata]
(
    -- Add the parameters for the stored procedure here
	@table_id nvarchar(50)
)
AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON

	SELECT		[Configuration_Category], [Configuration_Name_Instance_Number], concat('{"Category": "', [Configuration_Name], '", '
					,STRING_AGG (CONVERT(VARCHAR(MAX), concat('"',[Configuration_Attribute_Name], '": "', [Configuration_Attribute_Value], '"')), ','), '}') [advanced_settings]
	FROM		[dbo].[Data_Pipeline_Metadata_Advanced_Configuration]
	WHERE		[Table_ID] = @table_id
	GROUP BY	[Configuration_Category], [Configuration_Name], [Configuration_Name_Instance_Number]
	UNION
	SELECT 		'NoData', 0, 'NoData' -- Placeholder values matching column types
	WHERE NOT EXISTS (	
	SELECT		*
	FROM		[dbo].[Data_Pipeline_Metadata_Advanced_Configuration]
	WHERE		[Table_ID] = @table_id)
	ORDER BY 	[Configuration_Name_Instance_Number]

	return

END

GO