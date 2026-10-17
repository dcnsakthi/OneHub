CREATE TABLE [dbo].[Datastore_Configuration] (
    [Datastore_Name]  VARCHAR (100) NOT NULL,
    [Datastore_Type]  VARCHAR (50)  NOT NULL,
    [Datastore_ID]    VARCHAR (100) NULL,
    [Workspace_ID]    VARCHAR (100) NULL,
    [Workspace_Name]  VARCHAR (500) NULL,
    [Medallion_Layer] VARCHAR (50)  NULL,
    [Endpoint]        VARCHAR (500) NULL,
    [Connection_ID]   VARCHAR (100) NULL
);


GO