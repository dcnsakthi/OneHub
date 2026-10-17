CREATE   PROCEDURE [dbo].[Get_Datastore_Configuration_Catalog]
AS
BEGIN
    SET NOCOUNT ON

    SELECT  [Datastore_Name],
            [Datastore_Type],
            [Datastore_ID],
            [Workspace_ID],
            [Workspace_Name],
            [Medallion_Layer],
            [Endpoint],
            [Connection_ID]
    FROM    [dbo].[Datastore_Configuration]

    UNION ALL

    SELECT  central_config.[Datastore_Name],
            central_config.[Datastore_Type],
            central_config.[Datastore_ID],
            central_config.[Workspace_ID],
            central_config.[Workspace_Name],
            central_config.[Medallion_Layer],
            central_config.[Endpoint],
            central_config.[Connection_ID]
    FROM    [metadata_lakehouse].[dbo].[central_datastore_configuration] AS central_config
    WHERE   NOT EXISTS (
                SELECT  1
                FROM    [dbo].[Datastore_Configuration] AS local_config
                WHERE   LOWER(local_config.[Datastore_Name]) = LOWER(central_config.[Datastore_Name])
            )
END

GO