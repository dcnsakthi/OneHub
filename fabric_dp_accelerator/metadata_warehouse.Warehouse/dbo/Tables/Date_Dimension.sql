CREATE TABLE [dbo].[Date_Dimension] (
    [Date_Key]          INT           NULL,
    [Date]              DATE          NULL,
    [Date_Text]         VARCHAR (255) NULL,
    [Year]              INT           NULL,
    [Quarter]           INT           NULL,
    [Month]             INT           NULL,
    [Month_Name]        VARCHAR (255) NULL,
    [Month_Name_Abbrev] VARCHAR (255) NULL,
    [Day]               INT           NULL,
    [Day_Name]          VARCHAR (255) NULL,
    [Day_Of_Week]       INT           NULL,
    [Week_Of_Year]      INT           NULL,
    [Is_Weekend]        BIT           NULL,
    [Month_Year]        VARCHAR (255) NULL,
    [Sort_Year]         INT           NULL,
    [Sort_Quarter]      INT           NULL,
    [Sort_Month]        INT           NULL,
    [Sort_Day]          INT           NULL,
    [Sort_Day_Of_Week]  INT           NULL,
    [Sort_Week_Of_Year] INT           NULL,
    [Sort_Year_Month]   INT           NULL
);


GO