CREATE TABLE [dbo].[Exploratory_Data_Analysis_Results] (
    [Data_Product_Key]            VARCHAR (128)   NULL,
    [Table_ID]                    INT             NULL,
    [Datastore_Name]              VARCHAR (255)   NULL,
    [Target_Type]                 VARCHAR (100)   NULL,
    [Target_Medallion_Layer]      VARCHAR (50)    NULL,
    [Table_Name]                  VARCHAR (4000)  NULL,
    [Table_Last_Modified_Time]    DATETIME2 (6)   NULL,
    [Column_Name]                 VARCHAR (255)   NULL,
    [Data_Type]                   VARCHAR (255)   NULL,
    [Total_Rows]                  INT             NULL,
    [Total_Columns]               INT             NULL,
    [Approx_Distinct_Values]      INT             NULL,
    [Null_Count]                  INT             NULL,
    [Null_Percent]                DECIMAL (20, 4) NULL,
    [Mean]                        DECIMAL (20, 4) NULL,
    [Std_Dev]                     DECIMAL (20, 4) NULL,
    [Min]                         VARCHAR (255)   NULL,
    [Max]                         VARCHAR (255)   NULL,
    [Data_Profile_Execution_Time] DATETIME2 (6)   NOT NULL,
    [Date_Key]                    INT             NULL
);


GO