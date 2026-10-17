CREATE TABLE [dbo].[Schema_Changes] (
    [Data_Product_Key]    VARCHAR (128)  NULL,
    [Table_ID]            INT            NULL,
    [Datastore_Name]      VARCHAR (255)  NULL,
    [Table_Name]          VARCHAR (4000) NULL,
    [Change_Type]         VARCHAR (255)  NULL,
    [Column_Name]         VARCHAR (255)  NULL,
    [Data_Type_Details]   VARCHAR (MAX)  NULL,
    [Schema_Arrival_Time] DATETIME2 (6)  NULL,
    [Fabric_Monitor_URL]  VARCHAR (4000) NULL,
    [Date_Key]            INT            NULL
);


GO