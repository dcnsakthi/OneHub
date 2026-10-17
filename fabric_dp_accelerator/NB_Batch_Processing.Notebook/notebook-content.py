# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "69777b32-f6cf-4286-88c8-c96ec30a177c",
# META       "default_lakehouse_name": "metadata_lakehouse",
# META       "default_lakehouse_workspace_id": "76e43058-b99b-403a-8603-174631f16837",
# META       "known_lakehouses": [
# META         {
# META           "id": "69777b32-f6cf-4286-88c8-c96ec30a177c"
# META         }
# META       ]
# META     },
# META     "warehouse": {}
# META   }
# META }

# MARKDOWN ********************

# # NB_Batch_Processing - Core Data Ingestion Engine
# 
# ## Overview
# This notebook serves as the primary data processing engine for the Fabric Data Platform Accelerator. It orchestrates batch data ingestion and transformation across all medallion architecture layers (Bronze, Silver, Gold) using a metadata-driven approach.
# 
# ### Key Capabilities
# - **Multi-Source Ingestion**: Processes data from files, Delta tables, and external databases
# - **Schema Evolution**: Automatically detects and handles schema changes with configurable actions
# - **Data Quality Management**: Executes comprehensive data quality checks with quarantine capabilities
# - **Transformation Pipeline**: Applies cleansing, custom transformations, and pre-built functions
# - **Advanced Features**: Supports SCD2 dimensions, surrogate key generation, and entity resolution
# 
# ### Integration Points
# - Executes helper functions from: `NB_Helper_Functions_1`, `NB_Helper_Functions_2`, `NB_Helper_Functions_3`
# - Integrates with metadata warehouse for configuration and logging
# - Supports dynamic Spark pool allocation for performance optimization


# CELL ********************

# MAGIC %%configure
# MAGIC {
# MAGIC     "defaultLakehouse": {
# MAGIC         "name": { 
# MAGIC             "parameterName": "default_lakehouse_name", 
# MAGIC             "defaultValue": "" 
# MAGIC         },
# MAGIC         "id": { 
# MAGIC             "parameterName": "default_lakehouse_id", 
# MAGIC             "defaultValue": "" 
# MAGIC         },
# MAGIC         "workspaceId": { 
# MAGIC             "parameterName": "default_lakehouse_workspace_id", 
# MAGIC             "defaultValue": "" 
# MAGIC         }
# MAGIC     },
# MAGIC     "environment": {
# MAGIC         "id": { 
# MAGIC             "parameterName": "spark_environment_id", 
# MAGIC             "defaultValue": "" 
# MAGIC         }
# MAGIC     },
# MAGIC     "retriableOptions": {
# MAGIC         "maxAttempt": { 
# MAGIC             "parameterName": "max_attempts", 
# MAGIC             "defaultValue": 1 
# MAGIC         },
# MAGIC         "enabled": true
# MAGIC     }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Notebook Parameters
# 
# The following parameters control the behavior of this notebook. They are populated by the orchestration pipeline based on metadata configurations.

# PARAMETERS CELL ********************

# Warehouse connection parameters (from Variable Library via pipeline)
metadata_warehouse_endpoint = ""   # String: SQL endpoint for the metadata warehouse
metadata_warehouse_name = ""       # String: Display name of the metadata warehouse
central_metadata_warehouse_endpoint = ""  # String: SQL endpoint for central metadata, logs, and shared datastore fallback
central_metadata_warehouse_name = ""      # String: Display/database name for central metadata, logs, and shared datastore fallback

# Table and trigger context (from pipeline orchestration)
data_product_key = "default"        # String: Data product key that scopes central logs for this run
table_id = ""                      # String: Table ID from orchestration metadata (cast to int at runtime)
trigger_name = ""                  # String: Trigger name for orchestration lookup
trigger_step = ""                  # String: Order of operations step number
trigger_execution_id = ""          # String: Trigger execution ID
trigger_execution_time = ""        # String: Trigger execution time
start_time = ""                    # String: Processing start time (UTC)
target_datastore = ""              # String: Target datastore name
target_entity = ""                 # String: Target table/entity name

# Remaining pipeline parameters
table_ddl = "[]"                   # JSON Array: Source database DDL for schema replication
event_payload = ""                 # String: Optional event data for event-driven scenarios
folder_path_from_trigger = ""      # String: Optional folder path for file-triggered ingestion
data_factory_run_id = ""           # String: Data Factory run ID so logs can be correlated between spark and data factory
runtime_environment = ""           # String: Logical environment label (dev, qa, prod) for environment-scoped DQ/reconciliation checks
custom_function_workspace_id = ""   # String: Workspace for notebook API fallback; defaults to selected lakehouse workspace
custom_function_load_mode = "notebook"  # String: file or notebook. CI/CD stamps file for QA/PROD.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Initialize Helper Functions
# 
# - Load reusable helper functions from companion notebooks. These functions encapsulate common operations and ensure consistency across the solution.

# CELL ********************

%run NB_Helper_Functions_1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run NB_Helper_Functions_2

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run NB_Helper_Functions_3

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Connect to Warehouses, Fetch Metadata, and Log "Started"
# 
# Establish pyodbc connections to the metadata and logging warehouses, fetch all
# configuration data (replacing the pipeline's Script activities), generate a log ID,
# and insert a "Started" record into Data_Pipeline_Logs.

# CELL ********************

clear_run_log_buffer()
configure_custom_function_runtime(
    workspace_id=custom_function_workspace_id or globals().get("default_lakehouse_workspace_id", ""),
    load_mode=custom_function_load_mode
)

metadata_conn = None
central_metadata_conn = None
log_entry = None

try:
    set_current_step(3, "Connect & Fetch Metadata")
    log_and_print(f"Data Factory Run ID: {data_factory_run_id}")

    # Cast pipeline string parameters to correct types
    data_product_key = (data_product_key or "default").strip() or "default"
    central_metadata_warehouse_endpoint = (central_metadata_warehouse_endpoint or metadata_warehouse_endpoint).strip() or metadata_warehouse_endpoint
    central_metadata_warehouse_name = (central_metadata_warehouse_name or metadata_warehouse_name).strip() or metadata_warehouse_name
    table_id = int(table_id)
    trigger_step = int(trigger_step) if trigger_step else None
    
    # ===========================================================================================
    # WAREHOUSE CONNECTIONS
    # ===========================================================================================
    metadata_conn, central_metadata_conn = open_startup_warehouse_connections(
        metadata_endpoint = metadata_warehouse_endpoint,
        metadata_warehouse_name = metadata_warehouse_name,
        central_metadata_endpoint = central_metadata_warehouse_endpoint,
        central_metadata_warehouse_name = central_metadata_warehouse_name
    )

    # ===========================================================================================
    # BUILD FABRIC SPARK MONITOR URL
    # ===========================================================================================
    runtime_ctx = notebookutils.runtime.context
    fabric_monitor_url = (
        f"https://app.powerbi.com/workloads/de-ds/sparkmonitor/"
        f"{runtime_ctx['currentNotebookId']}/{runtime_ctx['activityId']}"
        f"?experience=fabric-developer"
    )

    # ===========================================================================================
    # BUILD LOG ENTRY AND LOG "STARTED"
    # ===========================================================================================
    log_entry = DataMovementLogEntry(
        data_product_key=data_product_key,
        log_id=str(uuid.uuid4()),
        table_id=table_id,
        target_datastore=target_datastore,
        target_entity=target_entity,
        event_start_time=start_time,
        fabric_monitor_url=fabric_monitor_url,
        trigger_name=trigger_name,
        trigger_step=trigger_step,
        trigger_id=trigger_execution_id,
        trigger_time=trigger_execution_time,
        central_metadata_endpoint=central_metadata_warehouse_endpoint,
        central_metadata_warehouse_name=central_metadata_warehouse_name,
    )
    log_data_movement(central_metadata_conn, log_entry, status='Started')

    # ===========================================================================================
    # FETCH METADATA FROM WAREHOUSE (replaces pipeline Metadata Script activity)
    # ===========================================================================================
    raw_metadata = fetch_metadata_from_warehouse(
        conn=metadata_conn,
        table_id=table_id,
        target_datastore=target_datastore,
    )

    primary_config = raw_metadata["primary_config"]

    # ===========================================================================================
    # FETCH CENTRAL METADATA DETAILS FROM WAREHOUSE (replaces pipeline logging Script activity)
    # ===========================================================================================
    raw_central_metadata = fetch_logging_details(
        conn=central_metadata_conn,
        table_id=table_id,
        target_datastore=target_datastore,
        data_product_key=data_product_key,
        watermark_strategy=primary_config.get('watermark_details_strategy'),
        watermark_field_data_type=primary_config.get('watermark_details_data_type')
    )

    # Extract fetched data into working variables
    orchestration_metadata = raw_metadata["orchestration_metadata"]
    advanced_config = raw_metadata["advanced_config"]
    datastore_config = raw_metadata["all_datastore_config"]
    latest_schema_details = raw_central_metadata["schema_details"]
    watermark_value = raw_central_metadata["watermark_details"].get("watermark_value", "")
    full_reload = raw_central_metadata["watermark_details"].get("full_reload", "")

    # table_ddl is still a JSON string from pipeline parameter
    table_ddl = json.loads(table_ddl)

    log_and_print(f"Metadata fetched for Table_ID={table_id}, watermark_value={watermark_value}, full_reload={full_reload}")

except Exception as e:
    handle_startup_failure(central_metadata_conn, log_entry, e, step="Step 3: Connect & Fetch Metadata")
    raise

finally:
    close_startup_warehouse_connections(metadata_conn, central_metadata_conn)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Parse Configuration
# 
# Parse all configuration objects from the metadata fetched in Cell 3.

# CELL ********************

try:
    set_current_step(4, "Parse Configuration")
    # ===========================================================================================
    # PARSE CONFIGURATION (using dicts fetched from warehouse — no JSON parsing needed)
    # ===========================================================================================

    # Clean configuration data
    advanced_config = clean_advanced_config(advanced_config)
    latest_schema_details = normalize_schema_details(latest_schema_details)

    # Extract schema tracking information for change detection
    schema_tracking = extract_schema_tracking_info(latest_schema_details)

    # Parse source configuration
    source_config = parse_source_configuration(
        orchestration_metadata = orchestration_metadata,
        primary_config = primary_config,
        datastore_config = datastore_config,
        folder_path_from_trigger = folder_path_from_trigger
    )

    # Parse target configuration
    target_config = parse_target_configuration(
        orchestration_metadata = orchestration_metadata,
        primary_config = primary_config,
        datastore_config = datastore_config
    )

    # Parse file ingestion paths
    custom_paths = parse_file_ingestion_paths(
        datastore_config = datastore_config,
        source_datastore_name = source_config.source_datastore_name,
        target_datastore_workspace_id = target_config.target_datastore_workspace_id,
        target_datastore_id = target_config.target_datastore_id,
        table_id = source_config.table_id,
        wildcard_folder_path = source_config.wildcard_folder_path,
        primary_config = primary_config
    )

    # Parse watermark configuration
    watermark_config = parse_watermark_configuration(
        primary_config = primary_config,
        default_merge_type = target_config.default_merge_type,
        default_watermark_column_name = target_config.default_watermark_column_name,
        staging_folder_path = source_config.staging_folder_path,
        using_source_folder_path = source_config.using_source_folder_path,
        watermark_value = watermark_value
    )

    # Local alias — modified by determine_first_run_and_table_existence()
    watermark_value = watermark_config.watermark_value

    # Extract lineage information for logging
    lineage_info = extract_lineage_information(
        source_config = source_config,
        target_config = target_config,
        datastore_config = datastore_config,
        merge_type = watermark_config.merge_type
    )

    # Populate lineage fields on the log entry now that we have them
    log_entry.source_medallion_layer = lineage_info.source_medallion_layer
    log_entry.source_type = lineage_info.source_type
    log_entry.target_medallion_layer = lineage_info.target_medallion_layer
    log_entry.target_type = lineage_info.target_type

    # Construct all_metadata dictionary
    all_metadata = {
        "orchestration_metadata": orchestration_metadata,
        "primary_config": primary_config,
        "advanced_config": advanced_config,
        "datastore_config": datastore_config,
        "event_payload": event_payload
    }

    # Parse advanced processing configuration
    advanced_processing_config = parse_advanced_processing_configuration(
        primary_config = primary_config
    )

    # Local alias — modified by cleanse_column_names()
    liquid_clustering_columns = advanced_processing_config.liquid_clustering_columns

    # Parse primary key configuration
    primary_key_config = parse_primary_key_configuration(
        orchestration_metadata = orchestration_metadata,
        primary_config = primary_config
    )

    # Local alias — modified by cleanse_column_names()
    primary_keys = primary_key_config.primary_keys

    # Parse advanced configuration steps
    advanced_steps_config = parse_advanced_configuration_steps(
        advanced_config = advanced_config
    )

    # Parse dimension table configuration
    dimension_config = parse_dimension_table_configuration(
        primary_config = primary_config,
        advanced_config = advanced_config
    )

    # Parse warehouse configuration (only executes Spark config if merge_type is 'warehouse_spark_connector')
    warehouse_config = parse_warehouse_configuration(
        merge_type = watermark_config.merge_type,
        datastore_config = datastore_config,
        target_datastore_name = target_config.target_datastore_name,
        spark_module = spark
    )

    file_config = extract_file_configuration(
        primary_config = primary_config,
        fail_on_new_schema = advanced_processing_config.fail_on_new_schema
    )

    log_and_print("Configuration parsed successfully.")

except Exception as e:
    log_failure_and_cleanup(central_metadata_conn, log_entry, e, step="Step 4: Parse Configuration")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Configure Spark Session & Initialize Tables
# 
# Configure the Spark session based on the target lakehouse layer, and create
# the date dimension table if required by the configuration.

# CELL ********************

try:
    set_current_step(5, "Spark Configuration")
    performance_config = parse_performance_configuration(
        primary_config = primary_config,
        target_datastore_medallion_name = target_config.target_datastore_medallion_name
    )

    spark_config = parse_spark_configuration(
        primary_config = primary_config
    )

    layer_config = apply_spark_configurations(
        spark_timestamp_rebase_mode_write = spark_config.spark_timestamp_rebase_mode_write,
        spark_timestamp_rebase_mode_read = spark_config.spark_timestamp_rebase_mode_read,
        use_spark_config_for_lakehouse = performance_config.use_spark_config_for_lakehouse
    )

    create_date_dimension(
        fact_table_data_load = dimension_config.fact_table_data_load,
        target_datastore_workspace_id = target_config.target_datastore_workspace_id,
        target_datastore_id = target_config.target_datastore_id,
        date_table_schema_name = 'dbo',
        date_table_name = 'dim_date',
        date_dimension_table_key_column_name = "date_sk"
    )

except Exception as e:
    log_failure_and_cleanup(central_metadata_conn, log_entry, e, step="Step 5: Spark Configuration")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Full Reload Check & Data Ingestion
# 
# Determine if this is a first run or full reload, then ingest data from the
# configured source. If no new data is found, logs "Processed" with 0 records
# and exits the notebook gracefully.

# CELL ********************

try:
    set_current_step(6, "Data Ingestion")
    handle_custom_staging(
        central_metadata_conn = central_metadata_conn,
        batch_log_entry = log_entry,
        source_config = source_config,
        primary_config = primary_config,
        datastore_config = datastore_config,
        orchestration_metadata = orchestration_metadata,
        spark_session = spark,
        session_globals = globals()
    )

    first_run, target_table_exists, watermark_value = determine_first_run_and_table_existence(
        target_table_name = target_config.target_table_name,
        full_reload = full_reload,
        watermark_value = watermark_value,
        target_abfss_path = target_config.target_abfss_path,
        lakehouse_table_output = target_config.lakehouse_table_output
    )

    new_data, new_watermark_value, source_details = route_to_ingestion_method(
        source_config = source_config,
        watermark_config = watermark_config,
        custom_paths = custom_paths,
        file_config = file_config,
        all_metadata = all_metadata,
        folder_path_from_trigger = folder_path_from_trigger,
        first_run = first_run,
        lineage_info = lineage_info
    )

except NoDataFoundError as e:
    log_no_data_and_cleanup(central_metadata_conn, log_entry, e)
    notebookutils.notebook.exit("")
except Exception as e:
    log_failure_and_cleanup(central_metadata_conn, log_entry, e, step="Step 6: Data Ingestion")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Column Standardization & Schema Alignment
# 
# Cleanse column names, add metadata columns, create the target schema if needed,
# and align source data types to the target schema.

# CELL ********************

try:
    set_current_step(7, "Schema Alignment")
    new_data, primary_keys, liquid_clustering_columns = cleanse_column_names(
        df = new_data,
        primary_keys = primary_keys,
        liquid_clustering_columns = liquid_clustering_columns,
        advanced_processing_config = advanced_processing_config
    )

    new_data = add_timestamp_metadata_columns(
        df = new_data,
        target_table_name = target_config.target_table_name
    )

    create_schema_if_not_exists(
        target_abfss_path = target_config.target_abfss_path,
        lakehouse_table_output = target_config.lakehouse_table_output
    )

    column_definitions = build_source_column_definitions(
        table_ddl = table_ddl,
        enforce_not_null = target_config.enforce_not_null,
        source_category = source_config.source_category,
        advanced_processing_config = advanced_processing_config
    )

    new_data = apply_source_type_casts(
        df = new_data,
        column_definitions = column_definitions
    )

except Exception as e:
    log_failure_and_cleanup(central_metadata_conn, log_entry, e, step="Step 7: Schema Alignment")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Data Cleansing, Transformations & Data Quality
# 
# Apply data cleansing rules, execute transformation functions, run data quality
# checks including schema change detection, and finalize processing. Raises
# `DataQualityFailureError` (logged as "Failed") if any DQ force-failures exist.

# CELL ********************

try:
    set_current_step(8, "Data Quality & Transformations")
    new_data = data_cleansing(
        df = new_data,
        trim_data_in_string_columns = advanced_processing_config.trim_data_in_string_columns,
        replace_blank_with_null_in_string_columns = advanced_processing_config.replace_blank_with_null_in_string_columns
    )

    new_data = transformation_functions(
        df = new_data,
        data_transformation_steps = advanced_steps_config.data_transformation_steps,
        first_run = first_run,
        target_table_name = target_config.target_table_name,
        primary_keys = primary_keys,
        dimension_config = dimension_config,
        watermark_config = watermark_config,
        merge_type = watermark_config.merge_type,
        all_metadata = all_metadata,
        lineage_info = lineage_info
    )

    new_data = reorder_columns_for_output(new_data = new_data)

    dq_warnings, dq_force_failures, new_data, quarantined_data = execute_data_quality_checks(
        new_data = new_data,
        data_quality_steps = advanced_steps_config.data_quality_steps,
        if_duplicate_primary_keys = advanced_processing_config.if_duplicate_primary_keys,
        primary_keys = primary_keys,
        target_table_name = target_config.target_table_name,
        datastore_config = datastore_config,
        merge_in_batches_with_columns = watermark_config.merge_in_batches_with_columns,
        session_globals = globals(),
        runtime_environment = runtime_environment,
        table_id = source_config.table_id
    )

    new_schema, new_schema_details, new_data_schema_hash, schema_change_summary, schema_type_updates_summary, column_types_that_changed = analyze_schema_changes(
        new_data = new_data,
        last_schema_id = schema_tracking.last_schema_id,
        last_schema_details = schema_tracking.last_schema_details
    )

    dq_warnings, dq_force_failures = handle_schema_change_failures(
        new_schema = new_schema,
        fail_on_new_schema = advanced_processing_config.fail_on_new_schema,
        first_run = first_run,
        last_schema_id = schema_tracking.last_schema_id,
        schema_change_summary = schema_change_summary,
        fail_on_column_data_type_change = advanced_processing_config.fail_on_column_data_type_change,
        column_types_that_changed = column_types_that_changed,
        schema_type_updates_summary = schema_type_updates_summary,
        dq_warnings = dq_warnings,
        dq_force_failures = dq_force_failures
    )

    # Finalize processing — raises DataQualityFailureError if dq_force_failures exist
    new_data = finalize_processing(
        dq_force_failures = dq_force_failures,
        source_details = source_details,
        dq_warnings = dq_warnings,
        new_schema = new_schema,
        new_data = new_data,
        target_datastore_medallion_name = target_config.target_datastore_medallion_name,
        new_data_schema_hash = new_data_schema_hash,
        lineage_info = lineage_info
    )

except NoDataFoundError as e:
    log_no_data_and_cleanup(central_metadata_conn, log_entry, e)
    notebookutils.notebook.exit("")
except Exception as e:
    log_failure_and_cleanup(central_metadata_conn, log_entry, e, step="Step 8: Data Quality & Transformations")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9. Write Data, Reconciliation & Post-Processing
# 
# Write validated data to the target Delta table, quarantine rejected rows,
# execute post-processing steps, and run post-write reconciliation checks
# against the final target state before logging success.

# CELL ********************

try:
    set_current_step(9, "Write Data & Reconciliation")
    new_data = add_scd2_columns_for_dimensions(
        new_data = new_data,
        column_to_mark_source_data_deletion = watermark_config.column_to_mark_source_data_deletion,
        delete_rows_with_value = watermark_config.delete_rows_with_value,
        source_timestamp_column_name = dimension_config.source_timestamp_column_name,
        lakehouse_table_output = target_config.lakehouse_table_output,
        first_run = first_run,
        merge_type = watermark_config.merge_type,
    )

    stats_properties = build_statistics_columns_config(
        df = new_data,
        compute_statistics_on_columns = performance_config.compute_statistics_on_columns,
        compute_statistics_on_first_n_columns = performance_config.compute_statistics_on_first_n_columns,
        lakehouse_table_output = target_config.lakehouse_table_output
    )

    table_properties = build_table_properties(
        spark_config = spark_config,
        layer_config = layer_config,
        stats_properties = stats_properties
    )

    create_delta_table(
        df = new_data,
        liquid_clustering_columns = liquid_clustering_columns,
        target_table_name = target_config.target_table_name,
        output_external_location = target_config.output_external_location,
        lakehouse_table_output = target_config.lakehouse_table_output,
        target_table_exists = target_table_exists,
        table_properties = table_properties,
    )

    total_records_processed = write_data_orchestrator(
        df = new_data,
        first_run = first_run,
        target_config = target_config,
        watermark_config = watermark_config,
        primary_keys = primary_keys,
        dimension_config = dimension_config,
        stats_properties = stats_properties
    )

    quarantined_records = write_quarantined_data(
        quarantined_df = quarantined_data,
        target_quarantined_abfss_path = target_config.target_quarantined_abfss_path
    )

    post_processing_scd2_update(
        primary_keys = primary_keys,
        target_abfss_path = target_config.target_abfss_path,
        lakehouse_table_output = target_config.lakehouse_table_output,
        merge_type = watermark_config.merge_type,
        total_records_processed = total_records_processed
    )

    dq_warnings, dq_force_failures = handle_post_write_reconciliation(
        new_data = new_data,
        post_write_reconciliation_steps = advanced_steps_config.post_write_reconciliation_steps,
        target_config = target_config,
        watermark_config = watermark_config,
        primary_keys = primary_keys,
        dq_warnings = dq_warnings,
        dq_force_failures = dq_force_failures,
        source_details = source_details,
        lineage_info = lineage_info,
        session_globals = globals(),
        runtime_environment = runtime_environment
    )

    drop_external_table_for_shortcut(
        target_table_name = target_config.target_table_name,
        first_run = first_run,
        output_external_location = target_config.output_external_location,
        lakehouse_table_output = target_config.lakehouse_table_output
    )

    execute_vacuum_if_needed(
        lakehouse_table_output = target_config.lakehouse_table_output,
        target_table_name = target_config.target_table_name,
        target_abfss_path = target_config.target_abfss_path,
        min_operations_threshold = 50
    )

    cleanup_temporary_files(
        file_staging_path = custom_paths.file_staging_path,
        clean_up_temporary_path = custom_paths.clean_up_temporary_path
    )

except Exception as e:
    log_failure_and_cleanup(central_metadata_conn, log_entry, e, step="Step 9: Write Data")
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 10. Success Logging & Cleanup
# 
# Log the successful processing result to Data_Pipeline_Logs, record any
# schema changes to Schema_Logs, and close the logging connection.

# CELL ********************

try:
    log_success_and_cleanup(
        central_metadata_conn = central_metadata_conn,
        log_entry = log_entry,
        source_details = source_details,
        new_watermark_value = new_watermark_value,
        total_records_processed = total_records_processed,
        quarantined_records = quarantined_records,
        dq_warnings = dq_warnings,
        new_schema = new_schema,
        table_id = table_id,
        target_datastore = target_datastore,
        target_entity = target_entity,
        new_data_schema_hash = new_data_schema_hash,
        new_schema_details = new_schema_details,
        fabric_monitor_url = fabric_monitor_url
    )
finally:
    close_warehouse_connection(central_metadata_conn)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
