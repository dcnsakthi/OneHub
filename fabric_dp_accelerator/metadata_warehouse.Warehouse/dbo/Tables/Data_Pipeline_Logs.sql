CREATE TABLE [dbo].[Data_Pipeline_Logs] (
    [Log_ID]                       VARCHAR (4000) NULL,
    [Data_Product_Key]             VARCHAR (128)  NULL,
    [Table_ID]                     INT            NULL,
    [Source_Medallion_Layer]       VARCHAR (50)   NULL,
    [Source_Type]                  VARCHAR (100)  NULL,
    [Data_Source_Details]          VARCHAR (4000) NULL,
    [Target_Medallion_Layer]       VARCHAR (50)   NULL,
    [Target_Type]                  VARCHAR (100)  NULL,
    [Target_Datastore]             VARCHAR (255)  NULL,
    [Target_Entity]                VARCHAR (4000) NULL,
    [Ingestion_Start_Time]         DATETIME2 (6)  NULL,
    [Ingestion_End_Time]           DATETIME2 (6)  NULL,
    [Ingestion_Status]             VARCHAR (4000) NULL,
    [Processing_Phase]             VARCHAR (50)   NULL,
    [Watermark_Value]              VARCHAR (4000) NULL,
    [Records_Processed]            INT            NULL,
    [Quarantined_Records]          INT            NULL,
    [Fabric_Monitor_URL]           VARCHAR (4000) NULL,
    [Trigger_Name]                 VARCHAR (500)  NULL,
    [Trigger_Step]                 INT            NULL,
    [Trigger_Execution_ID]         VARCHAR (500)  NULL,
    [Trigger_Execution_Start_Time] DATETIME2 (6)  NULL,
    [Date_Key]                     INT            NULL
);


GO