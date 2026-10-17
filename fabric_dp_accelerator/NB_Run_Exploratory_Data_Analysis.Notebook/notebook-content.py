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
# META     }
# META   }
# META }

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
# MAGIC     }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as f
#import com.microsoft.spark.fabric
#from com.microsoft.spark.fabric.Constants import Constants
from datetime import datetime, date
import json
import uuid
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# speed up EDA analysis using Native Execution Engine
spark.conf.set('spark.native.enabled', 'true')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

table_details = "[]"
data_product_key = "default"
trigger_name = ''
datastore_config = "[]"
metadata_lakehouse_id = ''  # Resolved by PL_01a from data_product_context
metadata_lakehouse_workspace_id = ''  # Resolved by PL_01a from data_product_context

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _parse_datastore_config(datastore_config):
    """
    Parse datastore configuration from pipeline lookup activity results.
    
    The pipeline passes the lookup activity result as JSON, like:
    '[{"Datastore_Name": "bronze", "Datastore_Type": "Lakehouse", ...}]'
    
    Args:
        datastore_config: Either a JSON string from the pipeline or an already-parsed list.
    
    Returns:
        list: Parsed list of datastore configuration dictionaries.
    """
    if isinstance(datastore_config, list):
        return datastore_config
    
    if isinstance(datastore_config, str):
        if not datastore_config.strip():
            return []
        try:
            return json.loads(datastore_config)
        except json.JSONDecodeError as e:
            raise ValueError(
                f"Failed to parse datastore_config as JSON. "
                f"Expected format: '[{{\"Datastore_Name\": \"bronze\", ...}}]'. Error: {e}"
            )
    
    raise TypeError(f"datastore_config must be str or list, got {type(datastore_config)}")

def _get_datastore_config(datastore_config, datastore_name, property_name):
    """
    Get a specific property value for a datastore from the configuration.
    
    Looks up the datastore by name (case-insensitive) and returns the requested property.
    
    Args:
        datastore_config (list): List of datastore configuration dictionaries
        datastore_name (str): Name of the datastore to look up (e.g., 'bronze', 'silver')
        property_name (str): Name of the property to retrieve. Valid properties:
            - Datastore_Name: The name identifier for the datastore
            - Datastore_Type: Type of datastore (Lakehouse or Warehouse)
            - Datastore_ID: The GUID of the datastore
            - Workspace_ID: The GUID of the workspace containing the datastore
            - Workspace_Name: The name of the workspace containing the datastore
            - Medallion_Layer: The medallion layer (Bronze, Silver, Gold)
            - Endpoint: SQL endpoint (only relevant for warehouses)
            - Connection_ID: Fabric Connection ID (can be used for any datastore)
    
    Returns:
        str: The property value, or empty string if not found
    
    Example:
        bronze_id = _get_datastore_config(datastore_config, 'bronze', 'Datastore_ID')
        silver_workspace = _get_datastore_config(datastore_config, 'silver', 'Workspace_Name')
    """
    for config in datastore_config:
        if config.get('Datastore_Name', '').lower() == datastore_name.lower():
            return config.get(property_name, '')
    return ''

table_details = json.loads(table_details)
datastore_config = _parse_datastore_config(datastore_config)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Constants for reusability
EDA_SCHEMA = "Data_Product_Key string, Table_ID int, Datastore_Name string, Target_Type string, Target_Medallion_Layer string, Table_Name string, Table_Last_Modified_Time timestamp, Column_Name string, Data_Type string, Total_Rows int, Total_Columns int, Approx_Distinct_Values int, Null_Count int, Null_Percent double, Mean double, Std_Dev double, Min string, Max string, Date_Key int"
EDA_OUTPUT_COLUMNS = [
    "Data_Product_Key", "Table_ID", "Datastore_Name", "Target_Type", "Target_Medallion_Layer",
    "Table_Name", "Table_Last_Modified_Time", "Column_Name", "Data_Type", "Total_Rows",
    "Total_Columns", "Approx_Distinct_Values", "Null_Count", "Null_Percent", "Mean", "Std_Dev",
    "Min", "Max", "Data_Profile_Execution_Time", "Date_Key"
]

# Data types that support statistical operations
NUMERIC_TYPES = ('decimal', 'int', 'smallint', 'bigint', 'tinyint', 'double', 'float')
TEMPORAL_TYPES = ('date', 'timestamp', 'timestamp_ntz', 'timestamp_ltz')
BRONZE_EXCLUDED_MEDALLION_LAYERS = ('bronze',)

def _create_empty_eda_dataframe():
    """Create an empty EDA DataFrame with the final output schema."""
    return (
        spark.createDataFrame([], EDA_SCHEMA)
        .withColumn("Data_Profile_Execution_Time", f.current_timestamp())
        .select(*EDA_OUTPUT_COLUMNS)
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _extract_table_metadata(table):
    """Extract and validate table metadata from table configuration."""
    return {
        'data_product_key': table.get('Data_Product_Key', data_product_key),
        'workspace_name': table.get('target_workspace_name'),
        'datastore_name': table.get('Target_Datastore'),
        'target_type': table.get('target_type', 'Lakehouse'),
        'target_medallion_layer': table.get('target_medallion_layer', ''),
        'table_name': f"{table.get('Target_Datastore')}.{table.get('Target_Entity')}",
        'table_id': int(table.get('Table_ID')),
        'path': table.get('path')
    }

def _get_table_last_modified(path):
    """Get the last modified timestamp for a Delta table."""
    deltaTable = DeltaTable.forPath(spark, path)
    return deltaTable.history(1).select("timestamp").collect()[0][0]

def _filter_columns(df):
    """Filter out system columns and return column metadata."""
    columns = [col for col in df.dtypes if not col[0].startswith('delta__')]
    return columns, len(columns)

def _get_base_data_type(data_type):
    """Return the top-level Spark data type name."""
    return data_type.strip().lower().split('(', 1)[0].split('<', 1)[0]

def _is_numeric_type(data_type):
    """Check if data type supports numeric statistics."""
    return _get_base_data_type(data_type) in NUMERIC_TYPES

def _is_temporal_type(data_type):
    """Check if data type is temporal (date/timestamp)."""
    return _get_base_data_type(data_type) in TEMPORAL_TYPES

def _column_ref(column_name):
    """Build a top-level Spark column reference, preserving dots/spaces in column names."""
    return f"`{column_name}`"

def _build_aggregation_expressions(columns):
    """Build aggregation expressions for single-pass EDA computation."""
    agg_exprs = [f.count("*").alias("total_rows")]
    
    for c, data_type in columns:
        column_ref = f.col(_column_ref(c))

        # Add distinct count and null count for all columns
        agg_exprs.extend([
            f.approx_count_distinct(column_ref).alias(f"distinct_{c}"),
            f.sum(column_ref.isNull().cast("int")).alias(f"null_{c}")
        ])
        
        # Add statistical measures for numeric columns
        if _is_numeric_type(data_type = data_type):
            agg_exprs.extend([
                f.mean(column_ref).cast("double").alias(f"mean_{c}"),
                f.stddev(column_ref).cast("double").alias(f"stddev_{c}"),
                # Cast Min/Max to string to match EDA_SCHEMA (Min/Max are string,
                # warehouse target is varchar) and to stay consistent with the
                # temporal branch below; otherwise numeric Min/Max reach the
                # string-typed schema as int/float/Decimal objects.
                f.min(column_ref).cast("string").alias(f"min_{c}"),
                f.max(column_ref).cast("string").alias(f"max_{c}")
            ])
        elif _is_temporal_type(data_type = data_type):
            agg_exprs.extend([
                f.min(column_ref).cast("string").alias(f"min_{c}"),
                f.max(column_ref).cast("string").alias(f"max_{c}")
            ])
    
    return agg_exprs

def _create_bronze_results(table_metadata, columns, total_rows, last_modified_timestamp):
    """Create EDA results for bronze tables (minimal computation)."""
    eda_results = []
    table_id = table_metadata['table_id']
    row_data_product_key = table_metadata['data_product_key']
    datastore_name = table_metadata['datastore_name']
    target_type = table_metadata['target_type']
    target_medallion_layer = table_metadata['target_medallion_layer']
    table_name = table_metadata['table_name']
    data_columns = len(columns)
    date_key = int(last_modified_timestamp.strftime("%Y%m%d"))
    
    for c, data_type in columns:
        eda_results.append((
            row_data_product_key,
            table_id, datastore_name, target_type, target_medallion_layer, table_name, last_modified_timestamp,
            c, data_type, total_rows, data_columns, None, None, None,
            None, None, None, None, date_key
        ))
    
    return eda_results

def _extract_column_stats(agg_result, column_name, data_type, total_rows):
    """Extract statistics for a single column from aggregation result."""
    approx_distinct_count = agg_result[f"distinct_{column_name}"]
    null_count = agg_result[f"null_{column_name}"]
    # Round to 4 dp to match the Null_Percent decimal(20,4) target (and the SQL
    # profiler). This is a 0-1 proportion, consistent with that profiler.
    null_percentage = float(round((null_count / total_rows) if total_rows > 0 else 0, 4))
    
    # Extract stats based on data type
    if _is_numeric_type(data_type):
        return {
            'approx_distinct': approx_distinct_count,
            'null_count': null_count,
            'null_percentage': null_percentage,
            'mean': agg_result[f"mean_{column_name}"],
            'stddev': agg_result[f"stddev_{column_name}"],
            'min': agg_result[f"min_{column_name}"],
            'max': agg_result[f"max_{column_name}"]
        }
    elif _is_temporal_type(data_type):
        return {
            'approx_distinct': approx_distinct_count,
            'null_count': null_count,
            'null_percentage': null_percentage,
            'mean': None,
            'stddev': None,
            'min': agg_result[f"min_{column_name}"],
            'max': agg_result[f"max_{column_name}"]
        }
    else:
        return {
            'approx_distinct': approx_distinct_count,
            'null_count': null_count,
            'null_percentage': null_percentage,
            'mean': None,
            'stddev': None,
            'min': None,
            'max': None
        }

def _create_detailed_results(table_metadata, columns, agg_result, last_modified_timestamp):
    """Create detailed EDA results for silver/gold tables."""
    eda_results = []
    table_id = table_metadata['table_id']
    row_data_product_key = table_metadata['data_product_key']
    datastore_name = table_metadata['datastore_name']
    target_type = table_metadata['target_type']
    target_medallion_layer = table_metadata['target_medallion_layer']
    table_name = table_metadata['table_name']
    data_columns = len(columns)
    total_rows = agg_result["total_rows"]
    date_key = int(last_modified_timestamp.strftime("%Y%m%d"))
    
    for c, data_type in columns:
        stats = _extract_column_stats(
            agg_result = agg_result, 
            column_name = c, 
            data_type = data_type, 
            total_rows = total_rows
        )
        
        eda_results.append((
            row_data_product_key,
            table_id, datastore_name, target_type, target_medallion_layer, table_name, last_modified_timestamp,
            c, data_type, total_rows, data_columns,
            stats['approx_distinct'], stats['null_count'], stats['null_percentage'],
            stats['mean'], stats['stddev'], stats['min'], stats['max'],
            date_key
        ))
    
    return eda_results

def _finalize_eda_dataframe(eda_results):
    """Convert results to DataFrame and apply final transformations."""
    if not eda_results:
        return _create_empty_eda_dataframe()

    try:
        eda_df = spark.createDataFrame(eda_results, EDA_SCHEMA)
    except Exception as e:
        print("Error creating DataFrame:", e)
        print("Results:", eda_results)
        raise

    eda_df = eda_df.withColumn("Data_Profile_Execution_Time", f.current_timestamp())

    # Convert long columns to int for consistency
    for column in eda_df.columns:
        if eda_df.schema[column].dataType.typeName() == 'long':
            eda_df = eda_df.withColumn(column, f.col(column).cast('int'))

    return eda_df.select(*EDA_OUTPUT_COLUMNS)

def pyspark_eda(table):
    """
    Perform exploratory data analysis on a table with optimized computation strategy.
    
    Uses different analysis strategies based on datastore type:
    - Bronze tables: Minimal analysis (row counts only) for performance
    - Silver/Gold tables: Full analysis with single-pass aggregation
    
    Args:
        table (dict): Table configuration containing metadata

    Returns:
        list: EDA results for this table
    """
    # Extract table metadata
    table_metadata = _extract_table_metadata(table = table)
    workspace_name = table_metadata['workspace_name']
    table_name = table_metadata['table_name']
    target_medallion_layer = table_metadata['target_medallion_layer']
    path = table_metadata['path']
    
    # Load table and get metadata
    df = spark.sql(f"SELECT * FROM `{workspace_name}`.{table_name}")
    last_modified_timestamp = _get_table_last_modified(path = path)
    columns, data_columns = _filter_columns(df = df)
    
    # Choose analysis strategy based on medallion layer
    if target_medallion_layer.lower() in BRONZE_EXCLUDED_MEDALLION_LAYERS:
        # Bronze tables: minimal analysis for performance
        total_rows = df.count()
        eda_results = _create_bronze_results(
            table_metadata = table_metadata, 
            columns = columns, 
            total_rows = total_rows, 
            last_modified_timestamp = last_modified_timestamp
        )
    else:
        # Silver/Gold tables: comprehensive single-pass analysis
        agg_exprs = _build_aggregation_expressions(columns = columns)
        agg_result = df.agg(*agg_exprs).collect()[0]
        eda_results = _create_detailed_results(
            table_metadata = table_metadata, 
            columns = columns, 
            agg_result = agg_result, 
            last_modified_timestamp = last_modified_timestamp
        )
    
    return eda_results

def _parse_table_schema(table_entity):
    """Parse table entity into schema and table name components."""
    parts = table_entity.split('.')
    if len(parts) < 2:
        raise ValueError(
            f"Target_Entity must be in 'schema.table' format, got: '{table_entity}'."
        )
    return parts[0], parts[1].lower()

def _build_table_path(datastore_config, datastore_name, schema_name, table_name):
    """Build the OneLake path for a table."""
    target_datastore_id = _get_datastore_config(datastore_config, datastore_name, 'Datastore_ID')
    target_datastore_workspace_id = _get_datastore_config(datastore_config, datastore_name, 'Workspace_ID')
    return f"abfss://{target_datastore_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_datastore_id}/Tables/{schema_name}/{table_name}"

def _build_full_table_name(target_workspace_name, datastore_name, schema_name, table_name):
    """Build fully qualified table name for Spark SQL."""
    return f"`{target_workspace_name}`.{datastore_name}.{schema_name}.{table_name}"

def _enrich_table_metadata(table, datastore_config):
    """Enrich table configuration with derived metadata."""
    datastore_name = table.get('Target_Datastore').lower()
    table_entity = table.get('Target_Entity')
    
    schema_name, table_name = _parse_table_schema(table_entity)
    target_workspace_name = _get_datastore_config(datastore_config, datastore_name, 'Workspace_Name')
    target_medallion_layer = _get_datastore_config(datastore_config, datastore_name, 'Medallion_Layer')
    target_type = _get_datastore_config(datastore_config, datastore_name, 'Datastore_Type') or 'Lakehouse'
    path = _build_table_path(
        datastore_config = datastore_config, 
        datastore_name = datastore_name, 
        schema_name = schema_name, 
        table_name = table_name
    )
    
    # Enrich table dictionary with computed values
    table['target_workspace_name'] = target_workspace_name
    table['target_medallion_layer'] = target_medallion_layer
    table['target_type'] = target_type
    table['path'] = path
    table['schema_name'] = schema_name
    table['table_name'] = table_name
    table['datastore_name'] = datastore_name
    
    return table

def _table_exists(target_workspace_name, datastore_name, schema_name, table_name):
    """Check if a table exists in the catalog."""
    full_table_name = _build_full_table_name(
        target_workspace_name = target_workspace_name, 
        datastore_name = datastore_name, 
        schema_name = schema_name, 
        table_name = table_name
    )
    return spark.catalog.tableExists(full_table_name)

def _apply_final_transformations(eda_df_full):
    """Apply final data type transformations to EDA results."""
    decimal_columns = ["Mean", "Std_Dev", "Null_Percent"]
    minmax_columns = ["Min", "Max"]

    NUMERIC_COLUMNS_OUTPUT_TYPE = "Decimal(20,4)"
    
    # Apply decimal casting to numeric columns
    for col_name in decimal_columns:
        eda_df_full = eda_df_full.withColumn(col_name, f.col(col_name).cast(NUMERIC_COLUMNS_OUTPUT_TYPE))
    
    # Apply special formatting to Min/Max columns (numeric formatting where possible, preserve strings otherwise)
    for col_name in minmax_columns:
        eda_df_full = eda_df_full.withColumn(
            col_name, 
            f.coalesce(
                f.format_number(f.expr(f"TRY_CAST({col_name} as {NUMERIC_COLUMNS_OUTPUT_TYPE})"), 2),
                f.col(col_name)
            )
        )
    
    return eda_df_full.select(*EDA_OUTPUT_COLUMNS)

def process_tables_for_eda(table_details, datastore_config):
    """
    Process multiple tables for exploratory data analysis.
    
    Args:
        table_details (list): List of table configurations
        datastore_config (list): Datastore configuration from Datastore_Configuration table
        
    Returns:
        DataFrame: Updated EDA results with final transformations applied
    """
    eda_results = []
    processed_count = 0
    failure_count = 0

    for table in table_details:
        # Isolate failures per table so one unsupported column or unreadable
        # table never aborts the whole batch (which would write no EDA results
        # for any table). Failures are logged as a warning and the loop continues.
        try:
            # Enrich table metadata
            enriched_table = _enrich_table_metadata(table = table, datastore_config = datastore_config)

            # Extract metadata for logging
            target_workspace_name = enriched_table['target_workspace_name']
            target_medallion_layer = enriched_table['target_medallion_layer']
            target_type = enriched_table['target_type']
            datastore_name = enriched_table['datastore_name']
            schema_name = enriched_table['schema_name']
            table_name = enriched_table['table_name']

            # Skip table if datastore_config is missing required datastore configuration
            if not target_workspace_name or not target_medallion_layer:
                print(f"Skipping datastore '{datastore_name}': Datastore_Configuration table needs to include this datastore (missing Workspace_Name or Medallion_Layer for '{datastore_name}')")
                continue

            # Skip warehouse tables - EDA only supported for Lakehouse tables
            if target_type.lower() == 'warehouse':
                print(f"Skipping '{datastore_name}.{schema_name}.{table_name}': EDA is not supported for Warehouse tables")
                continue

            # Check if table exists before processing
            if not _table_exists(
                target_workspace_name = target_workspace_name,
                datastore_name = datastore_name,
                schema_name = schema_name,
                table_name = table_name
            ):
                full_table_name = _build_full_table_name(
                    target_workspace_name = target_workspace_name,
                    datastore_name = datastore_name,
                    schema_name = schema_name,
                    table_name = table_name
                )
                print(f"{full_table_name} does not exist. Exploratory data analysis will not be run on data.")
            else:
                full_table_name = _build_full_table_name(
                    target_workspace_name = target_workspace_name,
                    datastore_name = datastore_name,
                    schema_name = schema_name,
                    table_name = table_name
                )
                print(f"Running exploratory data analysis on table: {full_table_name}")
                eda_results.extend(pyspark_eda(table = enriched_table))
                processed_count += 1
        except Exception as e:
            failure_count += 1
            print(f"WARNING: Skipping table '{table.get('Target_Entity')}' due to EDA failure: {e}")
            continue
    
    # If we attempted to profile at least one table and every attempt failed,
    # surface a hard failure instead of silently writing empty results.
    if failure_count > 0 and processed_count == 0:
        raise RuntimeError(
            f"Exploratory data analysis failed for all {failure_count} attempted table(s). "
            "See the per-table WARNING messages above for the underlying errors."
        )

    eda_df_full = _finalize_eda_dataframe(eda_results = eda_results)

    # Apply final transformations to the complete dataset
    eda_df_full = _apply_final_transformations(eda_df_full = eda_df_full)
    
    return eda_df_full

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Process all tables for EDA
eda_df_full = process_tables_for_eda(
    table_details = table_details, 
    datastore_config = datastore_config
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write the EDA results
# use the below proces when it's supported for service principals
# it does not as of July 4, 2025
#eda_df_full.write.mode("append").synapsesql("Metadata.dbo.Exploratory_Data_Analysis_Results")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

folder_name = str(uuid.uuid4())
folder_path = f"EDA/{trigger_name}/{folder_name}"

# Use the metadata lakehouse IDs passed from Variable Library
temp_path_for_eda_analysis_results = f"abfss://{metadata_lakehouse_workspace_id}@onelake.dfs.fabric.microsoft.com/{metadata_lakehouse_id}/Files/{folder_path}"

eda_df_full.coalesce(1).write.parquet(temp_path_for_eda_analysis_results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Exit notebook with comprehensive processing results
notebookutils.notebook.exit({
    "folder_path": folder_path
})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
