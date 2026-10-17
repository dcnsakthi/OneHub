CREATE TABLE [dbo].[Trigger_Execution_Context] (
    [Trigger_Execution_ID]                VARCHAR (500)  NOT NULL,
    [Trigger_Name]                        VARCHAR (500)  NULL,
    [Data_Product_Key]                    VARCHAR (128)  NULL,
    [Is_Feature_Workspace]                BIT            NULL,
    [Metadata_Workspace_ID]               VARCHAR (36)   NULL,
    [Metadata_Workspace_Name]             VARCHAR (256)  NULL,
    [Metadata_Warehouse_ID]               VARCHAR (36)   NULL,
    [Metadata_Warehouse_Endpoint]         VARCHAR (512)  NULL,
    [Metadata_Lakehouse_ID]               VARCHAR (36)   NULL,
    [Data_Product_Connection_ID]          VARCHAR (100)  NULL,
    [Central_Metadata_Workspace_ID]       VARCHAR (36)   NULL,
    [Central_Metadata_Warehouse_ID]       VARCHAR (36)   NULL,
    [Central_Metadata_Warehouse_Endpoint] VARCHAR (512)  NULL,
    [Execution_Start_Time]                DATETIME2 (6)  NULL,
    [Execution_End_Time]                  DATETIME2 (6)  NULL,
    [Execution_Status]                    VARCHAR (50)   NULL,
    [Fabric_Monitor_URL]                  VARCHAR (4000) NULL,
    [Date_Key]                            INT            NULL
);


GO