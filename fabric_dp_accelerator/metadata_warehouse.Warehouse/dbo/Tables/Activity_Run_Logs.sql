CREATE TABLE [dbo].[Activity_Run_Logs] (
    [Data_Product_Key] VARCHAR (128)  NULL,
    [Table_ID]         INT            NULL,
    [Log_ID]           VARCHAR (4000) NOT NULL,
    [Sequence_Number]  INT            NOT NULL,
    [Log_Timestamp]    DATETIME2 (6)  NULL,
    [Log_Level]        VARCHAR (10)   NOT NULL,
    [Step_Name]        VARCHAR (100)  NULL,
    [Step_Number]      INT            NULL,
    [Message]          VARCHAR (MAX)  NOT NULL,
    [Source_Type]      VARCHAR (20)   NULL
);


GO