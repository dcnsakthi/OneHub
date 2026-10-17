CREATE   PROCEDURE [dbo].[Insert_Activity_Log_Entry]
(
    @log_id varchar(4000),
    @data_product_key varchar(128) = 'default',
    @table_id int,
    @log_level varchar(10),
    @step_name varchar(100),
    @step_number int,
    @message varchar(max),
    @source_type varchar(20)
)
AS
BEGIN
    SET NOCOUNT ON
    SET @data_product_key = COALESCE(NULLIF(TRIM(@data_product_key), ''), 'default')

    -- Single-row insert for pipeline-originated log entries.
    -- Avoids JSON serialization in pipeline expressions.
    -- For bulk notebook log entries, use Insert_Activity_Log_Entries (plural) instead.

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
    VALUES (
        @data_product_key,
        @table_id,
        @log_id,
        1,
        GETUTCDATE(),
        @log_level,
        @step_name,
        @step_number,
        @message,
        @source_type
    );
END

GO