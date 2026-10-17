CREATE TABLE [dbo].[Data_Pipeline_Metadata_Orchestration] (
    [Trigger_Name]        VARCHAR (4000) NULL,
    [Order_Of_Operations] INT            NULL,
    [Table_ID]            INT            NULL,
    [Target_Datastore]    VARCHAR (4000) NULL,
    [Target_Entity]       VARCHAR (4000) NULL,
    [Primary_Keys]        VARCHAR (4000) NULL,
    [Processing_Method]   VARCHAR (200)  NULL,
    [Ingestion_Active]    INT            NULL
);


GO