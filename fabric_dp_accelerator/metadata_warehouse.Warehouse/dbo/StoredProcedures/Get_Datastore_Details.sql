CREATE   PROCEDURE [dbo].[Get_Datastore_Details]
(
    @datastore_name varchar(100)
)
AS
BEGIN
    -- SET NOCOUNT ON added to prevent extra result sets from
    -- interfering with SELECT statements.
    SET NOCOUNT ON

    -- Resolve a datastore_name to its full Datastore_Configuration row.
    -- Used by PL_02 pipelines to look up the environment-specific connection details
    -- at runtime, so that metadata SQL stays environment-agnostic (no raw GUIDs).

    -- Normalize datastore name to lowercase for case-insensitive matching
    -- (Fabric Warehouse uses BIN2 collation which is case-sensitive)
    SET @datastore_name = LOWER(@datastore_name)

    IF EXISTS (
        SELECT  1
        FROM    [dbo].[Datastore_Configuration]
        WHERE   LOWER([Datastore_Name]) = @datastore_name
    )
    BEGIN
        SELECT  [Datastore_Name],
                [Datastore_Type],
                [Datastore_ID],
                [Workspace_ID],
                [Workspace_Name],
                [Medallion_Layer],
                [Endpoint],
                [Connection_ID]
        FROM    [dbo].[Datastore_Configuration]
        WHERE   LOWER([Datastore_Name]) = @datastore_name
        RETURN
    END

    IF EXISTS (
        SELECT  1
        FROM    [metadata_lakehouse].[dbo].[central_datastore_configuration]
        WHERE   LOWER([Datastore_Name]) = @datastore_name
    )
    BEGIN
        SELECT  [Datastore_Name],
                [Datastore_Type],
                [Datastore_ID],
                [Workspace_ID],
                [Workspace_Name],
                [Medallion_Layer],
                [Endpoint],
                [Connection_ID]
        FROM    [metadata_lakehouse].[dbo].[central_datastore_configuration]
        WHERE   LOWER([Datastore_Name]) = @datastore_name
        RETURN
    END

    DECLARE @available_local_datastores varchar(max)
    DECLARE @available_central_datastores varchar(max)

    SELECT  @available_local_datastores = STRING_AGG(CONVERT(varchar(max), [Datastore_Name]), ', ')
    FROM    [dbo].[Datastore_Configuration]

    SELECT  @available_central_datastores = STRING_AGG(CONVERT(varchar(max), [Datastore_Name]), ', ')
    FROM    [metadata_lakehouse].[dbo].[central_datastore_configuration]

    DECLARE @error_message varchar(8000) = CONCAT(
        'Datastore ''', @datastore_name, ''' not found in Datastore_Configuration. Local datastores: ',
        COALESCE(@available_local_datastores, '(none)'),
        '. Central shared datastores: ',
        COALESCE(@available_central_datastores, '(none)'),
        '.'
    );
    THROW 50001, @error_message, 1;
END

GO