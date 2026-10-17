CREATE   PROCEDURE [dbo].[Insert_Activity_Log_Entries]
(
    @log_id varchar(4000),
    @data_product_key varchar(128) = 'default',
    @table_id int,
    @entries varchar(max),  -- JSON array of structured log entries
    @source_type varchar(20)  -- Origin: 'Notebook', 'Pipeline', etc.
)
AS
BEGIN
    SET NOCOUNT ON
    SET @data_product_key = COALESCE(NULLIF(TRIM(@data_product_key), ''), 'default')

    -- Parse the JSON array and insert each entry as a row.
    -- Expected JSON structure:
    -- [
    --   {"seq": 1, "ts": "2026-03-17T14:22:01", "level": "INFO", "step_name": "Data Ingestion", "step_number": 6, "message": "Reading from source..."},
    --   {"seq": 2, "ts": "2026-03-17T14:22:05", "level": "WARNING", "step_name": "Data Ingestion", "step_number": 6, "message": "3 rows with null keys"}
    -- ]

    INSERT INTO [dbo].[Activity_Run_Logs] (
        [Data_Product_Key],
        [Table_ID],
        [Log_ID],
        [Sequence_Number],
        [Log_Timestamp],
        [Log_Level],
        [Step_Name],
        [Step_Number],
        [Message],
        [Source_Type]
    )
    SELECT
        @data_product_key,
        @table_id,
        @log_id,
        CAST(j.[seq] AS int),
        TRY_CAST(j.[ts] AS datetime2(6)),
        j.[level],
        j.[step_name],
        TRY_CAST(j.[step_number] AS int),
        j.[message],
        @source_type
    FROM OPENJSON(@entries)
    WITH (
        [seq]         int            '$.seq',
        [ts]          varchar(30)    '$.ts',
        [level]       varchar(10)    '$.level',
        [step_name]   varchar(100)   '$.step_name',
        [step_number] int            '$.step_number',
        [message]     varchar(max)   '$.message'
    ) AS j;

    SELECT @@ROWCOUNT AS Rows_Inserted;
    RETURN;
END

GO