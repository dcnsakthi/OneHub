CREATE   PROCEDURE [dbo].[Pivot_Primary_Config]
(
    -- Add the parameters for the stored procedure here
	@table_id nvarchar(50)
)
AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON

	DECLARE @cols AS NVARCHAR(MAX),
			@query  AS NVARCHAR(MAX)

	SELECT @cols = (SELECT		STRING_AGG(CONVERT(NVARCHAR(max), concat([Configuration_Category],'_',[Configuration_Name])), ',') 
					FROM	    [dbo].[Data_Pipeline_Metadata_Primary_Configuration]
					WHERE		[Table_ID] = @table_id)

	SET @query = N'SELECT ' + @cols + N' from 
				 (
					SELECT		[Configuration_Value], concat([Configuration_Category],''_'',[Configuration_Name]) [Column]
					FROM		[dbo].[Data_Pipeline_Metadata_Primary_Configuration]
					WHERE		[Table_ID] = @table_id
				) x
				pivot 
				(
					max([Configuration_Value])
					for [Column] in (' + @cols + N')
				) p '

	exec sp_executesql @query, N'@table_id nvarchar(50)', @table_id = @table_id;
	return

END

GO