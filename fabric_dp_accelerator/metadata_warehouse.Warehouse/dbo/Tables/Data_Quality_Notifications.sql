CREATE TABLE [dbo].[Data_Quality_Notifications] (
    [Log_ID]                VARCHAR (4000) NULL,
    [Data_Product_Key]      VARCHAR (128)  NULL,
    [Datastore_Name]        VARCHAR (255)  NULL,
    [Table_ID]              INT            NULL,
    [Table_Name]            VARCHAR (255)  NULL,
    [Data_Quality_Category] VARCHAR (255)  NULL,
    [Data_Quality_Result]   VARCHAR (255)  NULL,
    [Data_Quality_Message]  VARCHAR (8000) NULL,
    [Rows_Impacted]         INT            NULL,
    [Data_Quarantined]      VARCHAR (255)  NULL,
    [Rows_Quarantined]      INT            NULL,
    [Ingestion_Start_Time]  DATETIME2 (6)  NULL,
    [Ingestion_End_Time]    DATETIME2 (6)  NULL,
    [Fabric_Monitor_URL]    VARCHAR (500)  NULL,
    [Date_Key]              INT            NULL
);


GO