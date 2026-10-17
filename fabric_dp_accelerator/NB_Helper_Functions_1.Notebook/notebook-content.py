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

# MARKDOWN ********************

# # NB_Helper_Functions_1 - Core Data Engineering Utilities
# 
# ## Overview
# This notebook provides the primary suite of reusable helper functions for the Fabric Data Platform Accelerator. These functions are designed to streamline data engineering workflows, enforce best practices, and ensure robust, scalable, and maintainable pipelines across all medallion architecture layers (Bronze, Silver, Gold).
# 
# ### Key Functional Areas
# - **Surrogate Key Management**: Automated generation and assignment of surrogate keys for dimension tables.
# - **Foreign Key Integration**: Linking fact tables to dimensions using surrogate keys.
# - **Data Cleansing**: Standardization of column names, null handling, and duplicate removal.
# - **Metadata Enrichment**: Addition of system columns for audit, lineage, and change tracking.
# - **Data Quality Validation**: Configurable checks with support for warnings, quarantining, and failures.
# - **Transformation Pipeline**: Metadata-driven and custom transformation logic.
# - **Merge Operations**: Advanced merge patterns, including SCD Type 2 and partitioned overwrites.
# - **Table Management**: Dynamic DDL generation and Delta table optimization.
# - **Utility Functions**: Schema comparison, metadata handling, and API interactions.
# 
# > **Note:** All functions are designed for modularity, reusability, and performance in distributed Spark environments.


# CELL ********************

# Import all required libraries for data engineering, Spark, and Delta Lake operations.
# Each import is grouped and commented for clarity.

# --- Standard Python Libraries ---
import ast           # For safely evaluating Python literals from strings
import json          # For JSON serialization/deserialization
from datetime import timezone, timedelta, datetime, date # For date and time operations
import os            # For OS-level operations
import re            # For regular expressions
import sys           # For system-specific parameters and functions
import time          # For time-related operations (sleep, timing)
from concurrent.futures import ThreadPoolExecutor, as_completed  # For parallel DQ check execution

# --- PySpark and Delta Lake Libraries ---
from pyspark import errors, StorageLevel
from io import StringIO
from delta import DeltaTable
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.window import Window, WindowSpec
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    TimestampNTZType
)
from pyspark.sql import functions as f
from pyspark.sql import DataFrame

# --- PySpark ML Libraries (for entity resolution) ---
from pyspark.ml.feature import Tokenizer, HashingTF, MinHashLSH, NGram

# --- Additional Utilities ---
import uuid          # For generating unique identifiers
import pandas as pd  # For DataFrame operations outside Spark
import glob          # For file pattern matching
import importlib.util
from pathlib import Path
from decimal import Decimal
from typing import List, Optional, Union, Tuple, Any, Dict

# Core Python libraries
import requests                  # HTTP operations for API calls
import struct                    # Binary data operations
import pyodbc                    # Database connectivity
import hashlib                   # MD5 hashing for schema comparison

# Import libaries for Fabric Warehouse operations
import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants


# ===========================================================================================
# CUSTOM EXCEPTION CLASSES
# ===========================================================================================
# These exceptions allow helper functions to signal specific conditions to the calling
# notebook (NB_Batch_Processing) without directly performing logging or exit operations.
# The calling notebook catches these and handles logging/exit centrally.
# ===========================================================================================

class DataQualityFailureError(Exception):
    """
    Raised when critical data quality failures require notebook termination.
    
    Carries DQ failure/warning details so the calling notebook can log them
    via Log_Data_Movement before re-raising to fail the notebook.
    """
    def __init__(self, message: str, dq_warnings: list, dq_failures: list, source_details: str, lineage_info: dict):
        super().__init__(message)
        self.dq_warnings = dq_warnings
        self.dq_failures = dq_failures
        self.source_details = source_details
        self.lineage_info = lineage_info


class NoDataFoundError(Exception):
    """
    Raised when no new data is found on a non-first-run (graceful exit).
    
    Carries watermark and lineage details so the calling notebook can log
    a 'Processed' event with 0 records before stopping execution.
    """
    def __init__(self, message: str, watermark_value, source_details: str, lineage_info: dict):
        super().__init__(message)
        self.watermark_value = watermark_value
        self.source_details = source_details
        self.lineage_info = lineage_info

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Surrogate Key, Foreign Key, and Null Row Functions
# 
# These functions automate the creation and management of surrogate keys for dimension tables, establish foreign key relationships in fact tables, and ensure referential integrity by inserting standardized "unknown" member rows. This is essential for robust star schema modeling and analytics.

# CELL ********************

def _sk_prepare_existing_data(
    first_run: bool,
    new_data: bool,
    target_table_name: str
):
    """Load existing dimension data from target table."""
    if new_data and not first_run:
        prior = spark.sql(f"SELECT * FROM {target_table_name}")
    else:
        prior = None
    return prior

def _calculate_base_key(
    prior,
    dimension_table_key_column_name: str,
    start_at: int
) -> int:
    """Compute max existing key (or baseline from start_at)."""
    if prior is not None:
        max_sk = prior.agg(f.max(f.col(dimension_table_key_column_name)).alias("m")).select("m").first()[0]
        base = max(start_at - 1, int(max_sk) if max_sk is not None else start_at - 1)
    else:
        base = start_at - 1
    return base

def _prepare_prior_keys(
    prior,
    primary_keys: list,
    dimension_table_key_column_name: str
):
    """Prepare existing key mappings with SCD2 filtering and validation."""
    if prior is not None:
        # For SCD2 dimensions, only consider currently active records for key mapping
        if "scd_active" in prior.columns:
            prior = prior.filter(f.col("scd_active") == 1)

        # Backtick-quote keys so dots in flattened column names aren't read as nested fields.
        prior = prior.select(*[f.col(f"`{c}`") for c in primary_keys + [dimension_table_key_column_name]])

        # Ensure uniqueness in prior after SCD2 filtering to avoid fan-out joins
        prior_dup_cnt = (
            prior
            .groupBy(*[f.col(f"`{pk}`") for pk in primary_keys])
            .count()
            .filter(f.col("count") > 1)
            .limit(1)
            .count()
        )
        if prior_dup_cnt > 0:
            raise Exception(
                "Multiple active rows for the primary keys detected in existing data. "
                "Ensure at most one active row per key before assigning surrogate keys."
            )
    
    return prior

def _validate_input_duplicates(
    df,
    primary_keys: list
) -> None:
    """Guard: duplicates in incoming df on primary_keys.

    Rows where ANY primary-key column is NULL are excluded from duplicate
    detection because NULLs represent unknown values and should never be
    considered duplicates of each other.  This is consistent with the
    null-safe join in ``_join_with_existing_keys``.
    """
    # Exclude rows with null PKs — nulls are not true duplicates
    # Backtick-quote keys so dots in flattened column names aren't read as nested fields.
    non_null_condition = f.lit(True)
    for pk in primary_keys:
        non_null_condition = non_null_condition & f.col(f"`{pk}`").isNotNull()
    df_non_null = df.filter(non_null_condition)

    dupes = df_non_null.groupBy(*[f.col(f"`{pk}`") for pk in primary_keys]).count().filter(f.col("count") > 1)
    if dupes.take(1):
        raise Exception(f"Duplicate business keys detected on {primary_keys}. Deduplicate upstream before assigning surrogate keys.")

def _join_with_existing_keys(
    df,
    prior,
    primary_keys: list,
    dimension_table_key_column_name: str
):
    """Perform null-safe join to bring in existing surrogate keys."""
    if prior is not None:
        left_df = df.alias("left")
        right_df = prior.alias("right")
        join_cond = None
        # Backtick-quote identifiers so dots in flattened column names aren't read as nested fields.
        for k in primary_keys:
            cond_k = f.col(f"left.`{k}`").eqNullSafe(f.col(f"right.`{k}`"))
            join_cond = cond_k if join_cond is None else (join_cond & cond_k)

        # Select all original left columns plus the surrogate key from the right
        left_cols = [f.col(f"left.`{c}`") for c in df.columns]
        right_sk = f.col(f"right.`{dimension_table_key_column_name}`").alias(dimension_table_key_column_name)
        joined = left_df.join(right_df, join_cond, "left").select(*left_cols, right_sk)
    else:
        joined = df.withColumn(dimension_table_key_column_name, f.lit(None).cast("int"))

    return joined

def _assign_new_keys(
    joined,
    new_data: bool,
    primary_keys: list,
    dimension_table_key_column_name: str,
    base: int
):
    """Assign new keys only to rows missing a surrogate key."""
    new_rows = joined.filter(f.col(dimension_table_key_column_name).isNull())
    if new_data:
        # Backtick-quote keys so dots in flattened column names aren't read as nested fields.
        w = Window.orderBy(*[f.col(f"`{c}`") for c in primary_keys])
        new_rows_with_sk = (
            new_rows
            .withColumn("__rn", f.row_number().over(w))
            .withColumn(dimension_table_key_column_name, f.col("__rn") + f.lit(base))
            .drop("__rn")
        )
        # Keep the rows that already had a key and union the newly assigned
        assigned = joined.filter(f.col(dimension_table_key_column_name).isNotNull())
        out = assigned.unionByName(new_rows_with_sk, allowMissingColumns = True)
    else:
        out = joined
    
    return out

def _reorder_columns_with_sk_first(
    df: DataFrame,
    dimension_table_key_column_name: str
):
    """Put surrogate key first for convenience and ensure delta statistics run on column."""
    cols = [dimension_table_key_column_name] + [c for c in df.columns if c != dimension_table_key_column_name]
    # Backtick-quote identifiers so dots in flattened column names aren't read as nested fields.
    return df.select(*[f.col(f"`{c}`") for c in cols])

def surr_keys_dim(
    df,
    target_table_name: str,
    primary_keys: list,
    dimension_table_key_column_name: str,
    first_run: bool
):
    """
    Generate and assign surrogate keys for dimension table records.

    This function implements a robust surrogate key generation strategy that:
    - Preserves existing surrogate keys for unchanged records
    - Assigns new sequential keys for new/modified records
    - Handles both initial loads and incremental updates
    - Supports SCD2 dimensions by considering only active records

    Args:
        df (DataFrame): Input DataFrame containing dimension records to process
        target_table_name (str): Fully qualified name of the target dimension table
        primary_keys (list): List of column names forming the natural/business key
        dimension_table_key_column_name (str): Name of the surrogate key column
        first_run (bool): Flag indicating if this is the initial table load

    Returns:
        DataFrame: Enhanced DataFrame with surrogate keys assigned and positioned as first column

    Raises:
        Exception: If duplicate primary keys are detected in the input data
    """

    start_at = 1

    # Determine if there are rows to process
    new_data = df.isEmpty() == False

    # Handle incremental load - retrieve existing surrogate key mappings
    prior = _sk_prepare_existing_data(
        first_run = first_run, 
        new_data = new_data, 
        target_table_name = target_table_name
    )
        
    if not primary_keys:
        raise Exception("primary_keys is required and cannot be empty.")
        
    # Compute max existing key (or baseline from start_at)
    base = _calculate_base_key(
        prior = prior, 
        dimension_table_key_column_name = dimension_table_key_column_name, 
        start_at = start_at
    )
    
    # Prepare prior keys with SCD2 filtering and validation
    prior = _prepare_prior_keys(
        prior = prior, 
        primary_keys = primary_keys, 
        dimension_table_key_column_name = dimension_table_key_column_name
    )

    # Validate input for duplicates (always check, even on first run)
    _validate_input_duplicates(df = df, primary_keys = primary_keys)

    # Left-join to bring in any existing keys
    joined = _join_with_existing_keys(
        df = df, 
        prior = prior, 
        primary_keys = primary_keys, 
        dimension_table_key_column_name = dimension_table_key_column_name
    )

    # Assign new keys only to rows missing a surrogate key
    df = _assign_new_keys(
        joined = joined, 
        new_data = new_data, 
        primary_keys = primary_keys, 
        dimension_table_key_column_name = dimension_table_key_column_name, 
        base = base
    )

    # Put surrogate key first for convenience and ensure delta statistics run on column
    return _reorder_columns_with_sk_first(
        df = df, 
        dimension_table_key_column_name = dimension_table_key_column_name
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _extract_surrogate_key_configuration(
    surrogate_key_logic: dict, 
    dimension_table_key_column_name: str
) -> dict:
    """Extract and process surrogate key configuration."""
    dimension_table_name = surrogate_key_logic['dimension_table_name']
    dimension_join_logic = surrogate_key_logic['dimension_table_join_logic']
    
    dimension_join_logic = dimension_join_logic.replace('=', ' = ') \
                                        .replace('< = >', ' <=> ') \
                                        .replace('> = ', ' >= ') \
                                        .replace('< = ', ' <= ')

    dimension_table_key_column_name = surrogate_key_logic.get('dimension_table_key_column_name', dimension_table_key_column_name)

    dimension_key_output_column_name = surrogate_key_logic['dimension_key_output_column_name']
    
    dimension_columns_to_add_to_fact = surrogate_key_logic.get('dimension_columns_to_add_to_fact', [])
    
    return {
        'dimension_table_name': dimension_table_name,
        'dimension_join_logic': dimension_join_logic,
        'dimension_table_key_column_name': dimension_table_key_column_name,
        'dimension_key_output_column_name': dimension_key_output_column_name,
        'dimension_columns_to_add_to_fact': dimension_columns_to_add_to_fact
    }

def _setup_temp_views(
    df: DataFrame, 
    dimension_table_name: str,
    view_suffix: str = ""
) -> DataFrame:
    """Register temp views for SQL join operations."""
    df.createOrReplaceTempView(f"new_data_view{view_suffix}")
    dimension_table = spark.sql(f"select * from {dimension_table_name}")
    
    if "scd_active" in dimension_table.columns:
        dimension_table = dimension_table.filter(f.col("scd_active") == 1)
    dimension_table.createOrReplaceTempView(f"dim_view{view_suffix}")
    
    return dimension_table

def _extract_dimension_columns(
    dimension_join_logic: str
) -> str:
    """Extract dimension column names from join logic."""
    pattern = r'(b\.`[^`]+`|b\.[a-zA-Z0-9_\-]+)'
    dimension_columns = re.findall(pattern, dimension_join_logic, re.IGNORECASE)
    dimension_columns = [re.sub(r'(?i)^b\.', '', match) for match in dimension_columns]
    join_columns = ", ".join(dimension_columns)

    extracted_join_columns_log_info = f"Extracted dimension join columns are: {join_columns}."
    log_and_print(extracted_join_columns_log_info)

    return join_columns

def _prepare_additional_columns(
    dimension_columns_to_add_to_fact: list
) -> str:
    """Prepare additional dimension columns for fact table."""
    if dimension_columns_to_add_to_fact:
        adding_columns_log_info = f'Adding the following columns, {dimension_columns_to_add_to_fact}, from the dimension table to the fact table.'
        log_and_print(adding_columns_log_info)
        
        dimension_columns_to_add_to_fact = dimension_columns_to_add_to_fact.split(",")
        dimension_columns_to_add_to_fact = [f"b.`{col.strip()}`" for col in dimension_columns_to_add_to_fact]
        dimension_columns_to_add_to_fact_sql = ', ' + ', '.join(dimension_columns_to_add_to_fact)
    else:
        dimension_columns_to_add_to_fact_sql = ""
    
    return dimension_columns_to_add_to_fact_sql

def _validate_join_uniqueness(
    dimension_table_name: str, 
    join_columns: str, 
    dimension_join_logic: str
) -> None:
    """Validate uniqueness of join keys in dimension table."""
    duplicate_join_values = spark.sql(f"SELECT {join_columns} FROM {dimension_table_name} GROUP BY {join_columns} HAVING COUNT(*) > 1")
    if duplicate_join_values.count() > 0:
        display(duplicate_join_values)
        raise Exception(f"Many to many mappings for {dimension_join_logic} on this Dimension: {dimension_table_name}")

def _execute_surrogate_key_join(
    dimension_table_key_column_name: str,
    dimension_key_output_column_name: str,
    dimension_columns_to_add_to_fact_sql: str, 
    dimension_join_logic: str,
    view_suffix: str = ""
) -> DataFrame:
    """Execute the join to attach surrogate keys."""
    query = f"""    SELECT      a.*, b.{dimension_table_key_column_name} as {dimension_key_output_column_name} {dimension_columns_to_add_to_fact_sql}
                    FROM        new_data_view{view_suffix} a 
                    LEFT JOIN   dim_view{view_suffix} b on {dimension_join_logic}
            """
    new_data = spark.sql(query)
    
    new_data = new_data.withColumn(
        dimension_key_output_column_name, f.when(f.col(dimension_key_output_column_name).isNull(), -1).otherwise(f.col(dimension_key_output_column_name))
    )
    
    return new_data

def _attach_dimension_surrogate_key(
    df: DataFrame,
    surrogate_key_logic: dict,
    all_metadata: dict,
    dimension_table_key_column_name: str
) -> DataFrame:
    """
    Attach Surrogate Key for a DataFrame of a Fact Table linking it to a dimension table.

    Args:
        df (DataFrame): The input DataFrame of the Fact table.
        surrogate_key_logic (dict): The tables and joining conditions for the reference table (dimension_join_logic should include any SCD2 logic)
        all_metadata (dict): Metadata dictionary containing workspace variables and configuration
        dimension_table_key_column_name (str): The column name of the surrogate key in the table.
    Returns:
        DataFrame: df after addition of surrogate key column(s).
    """
    inserting_surrogate_key_log_info = "Inserting surrogate key from reference table."
    log_and_print(inserting_surrogate_key_log_info)

    # Extract configuration
    config = _extract_surrogate_key_configuration(
        surrogate_key_logic = surrogate_key_logic, 
        dimension_table_key_column_name = dimension_table_key_column_name
    )
    
    creating_surrogate_key_log_info = f"Creating surrogate key column, {config['dimension_key_output_column_name']}, in fact table from dimension table, {config['dimension_table_name']}, using column {config['dimension_table_key_column_name']}."
    log_and_print(creating_surrogate_key_log_info)

    join_logic_log_info = f"Join logic is: {config['dimension_join_logic']}."
    log_and_print(join_logic_log_info)

    dimension_datastore_name = config['dimension_table_name'].split('.')[0].lower()

    target_workspace_name = _get_datastore_config(all_metadata['datastore_config'], dimension_datastore_name, 'Workspace_Name')
    
    full_dimension_table_name = f"`{target_workspace_name}`.{config['dimension_table_name']}"

    # Check if fact key column already exists (case-insensitive check since Spark is case-insensitive)
    existing_columns_lower = [col.lower() for col in df.columns]
    if config['dimension_key_output_column_name'].lower() in existing_columns_lower:
        error_message = f"The column `{config['dimension_key_output_column_name']}` already exists in the DataFrame. Please rename the existing column or choose a different name for the surrogate key column."
        log_and_print(error_message, "error")
        raise ValueError(error_message)

    # Setup temp views (suffix with table_id for HC-mode isolation)
    table_id = all_metadata['orchestration_metadata']['Table_ID']
    # Negative table_ids (used by integration tests in the [-999999, -900000]
    # range) would produce invalid SQL identifiers like `new_data_view_-925901`
    # because of the leading dash. Map the sign to a letter prefix so the view
    # name is always a valid Spark SQL identifier.
    view_suffix = f"_{'n' if int(table_id) < 0 else 'p'}{abs(int(table_id))}"
    _setup_temp_views(
        df = df, 
        dimension_table_name = full_dimension_table_name,
        view_suffix = view_suffix
    )
    
    # Extract dimension columns
    join_columns = _extract_dimension_columns(
        dimension_join_logic = config['dimension_join_logic']
    )
    
    # Prepare additional columns
    dimension_columns_to_add_to_fact_sql = _prepare_additional_columns(
        dimension_columns_to_add_to_fact = config['dimension_columns_to_add_to_fact']
    )
    
    # Validate join uniqueness
    _validate_join_uniqueness(
        dimension_table_name = full_dimension_table_name, 
        join_columns = join_columns, 
        dimension_join_logic = config['dimension_join_logic']
    )
    
    # Execute surrogate key join (dimension_join_logic should include any SCD2 logic if needed)
    df = _execute_surrogate_key_join(
        dimension_table_key_column_name = config['dimension_table_key_column_name'],
        dimension_key_output_column_name = config['dimension_key_output_column_name'],
        dimension_columns_to_add_to_fact_sql = dimension_columns_to_add_to_fact_sql, 
        dimension_join_logic = config['dimension_join_logic'],
        view_suffix = view_suffix
    )
    
    # Reorder columns
    df = _reorder_columns_with_sk_first(
        df = df,
        dimension_table_key_column_name = config['dimension_key_output_column_name']
    )

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# This function creates the null row for a given Dataframe
def insert_null_row(
    new_data: DataFrame, 
    column_to_mark_source_data_deletion: str, 
    dimension_table_key_column_name: str
) -> DataFrame:
    """
    Create and append a standardized "unknown" member row to dimension tables.

    This function generates a null/unknown row with:
    - Surrogate key of -1 (standard unknown member identifier)
    - Default values for all non-nullable columns
    - NULL values for nullable columns
    - Active status for SCD tracking

    Args:
        new_data (DataFrame): The dimension table DataFrame
        column_to_mark_source_data_deletion (str): Column indicating deletion status
        dimension_table_key_column_name (str): Name of the surrogate key column

    Returns:
        DataFrame: Original data with appended null row

    Note:
        The null row ensures referential integrity when fact records 
        cannot be matched to dimension members.
    """

    schema = new_data.schema
    fields = new_data.schema.fields
    
    # Standard dates for unknown member
    old_timestamp = datetime(1900, 1, 1)
    old_date = date(1900, 1, 1)

    null_row = []
    
    for field in fields:
        field_name = field.name
        datatype = field.dataType.simpleString()
        nullable = field.nullable
        
        # Determine appropriate default value based on column type and nullability
        is_required_field = (
            not nullable or 
            field_name in ("scd_active", column_to_mark_source_data_deletion, dimension_table_key_column_name)
        )
        
        if is_required_field:
            # Handle special columns with specific values
            if field_name == "scd_active":
                null_row.append(1)  # Unknown member is always active
            elif field_name == column_to_mark_source_data_deletion:
                null_row.append(0)  # Unknown member is never deleted
            elif field_name == dimension_table_key_column_name:
                null_row.append(-1)  # Standard unknown member SK
            
            # Handle numeric types
            elif "double" in datatype:
                null_row.append(-1.0)
            elif "decimal" in datatype:
                null_row.append(Decimal(-1))
            elif datatype in ('tinyint', 'smallint', 'int', 'bigint'):
                null_row.append(-1)
            
            # Handle date/time types
            elif 'timestamp' in datatype:
                null_row.append(old_timestamp)
            elif 'date' in datatype:
                null_row.append(old_date)
            
            # Default to string representation for other types
            else:
                null_row.append('-1')
        else:
            # Nullable fields can be NULL for unknown member
            null_row.append(None)

    # Create and append the null row
    null_row = Row(*null_row)
    null_df = spark.createDataFrame([null_row], schema=schema)
    new_data = new_data.union(null_df)

    return new_data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Data Cleansing Functions
# 
# These functions standardize and clean data to ensure consistency across the platform. They handle column naming conventions, null value handling, and duplicate removal, which are critical for downstream data quality and analytics.

# CELL ********************

def _trim_column_names(
    columns: list,
    trim_enabled: bool
) -> list:
    """
    Trim leading and trailing whitespace from column names.
    
    Args:
        columns (list): List of column names to process
        trim_enabled (bool): Whether to apply trimming
    
    Returns:
        list: Column names with whitespace trimmed (system columns preserved)
    """
    if not trim_enabled:
        return columns
    
    trim_log_info = "Trimming leading and trailing whitespace from column names."
    log_and_print(trim_log_info)
    
    return [col if 'delta__' in col else col.strip() for col in columns]


def _exact_string_replacement(
    columns: list,
    find_strings: str,
    replace_string: str
) -> list:
    """
    Replace exact string matches in column names.
    
    Args:
        columns (list): List of column names to process
        find_strings (str): Comma-separated list of strings to find
        replace_string (str): Replacement string (single value to replace all matches) or 
                             comma-separated list of replacement strings (matched by position to find_strings)
    
    Returns:
        list: Column names with exact strings replaced (system columns preserved)
    
    Raises:
        ValueError: If replacement list has multiple items but count doesn't match find list count
    
    Notes:
        - If replace_string is a single value, all find_strings are replaced with that value
        - If replace_string is comma-separated, replacements MUST match the count of find_strings
        - System columns (containing 'delta__') are never modified
    """
    if not find_strings or not replace_string:
        return columns
    
    exact_find_list = [s.strip() for s in find_strings.split(',') if s.strip()]
    if not exact_find_list:
        return columns
    
    # Parse replacement string - could be single value or comma-separated list
    exact_replace_list = [s.strip() for s in replace_string.split(',') if s.strip()]
    if not exact_replace_list:
        return columns
    
    # Validate counts match for positional replacement
    if len(exact_replace_list) > 1 and len(exact_replace_list) != len(exact_find_list):
        error_message = (
            f"Exact string replacement count mismatch: "
            f"Found {len(exact_find_list)} strings to find ({exact_find_list}) "
            f"but {len(exact_replace_list)} replacement values ({exact_replace_list}). "
            f"Either provide a single replacement value for all finds, "
            f"or provide exactly one replacement value for each find string."
        )
        log_and_print(error_message)
        raise ValueError(error_message)
    
    # Determine if we have positional replacements or a single replacement for all
    if len(exact_replace_list) == 1:
        # Single replacement value for all find strings
        exact_replacement_log_info = f"Applying exact string replacement: {exact_find_list} → '{exact_replace_list[0]}'."
        log_and_print(exact_replacement_log_info)
        
        new_columns = []
        for col in columns:
            if 'delta__' in col:
                new_columns.append(col)
            else:
                new_col = col
                for find_str in exact_find_list:
                    new_col = new_col.replace(find_str, exact_replace_list[0])
                new_columns.append(new_col)
    else:
        # Positional replacements - match by index (already validated counts match)
        replacement_pairs = list(zip(exact_find_list, exact_replace_list))
        exact_replacement_log_info = f"Applying exact string replacement with positional matching: {replacement_pairs}."
        log_and_print(exact_replacement_log_info)
        
        new_columns = []
        for col in columns:
            if 'delta__' in col:
                new_columns.append(col)
            else:
                new_col = col
                for find_str, replace_str in replacement_pairs:
                    new_col = new_col.replace(find_str, replace_str)
                new_columns.append(new_col)
    
    return new_columns


def _regex_replacement(
    columns: list,
    pattern: str,
    replacement: str
) -> list:
    """
    Replace column name content using regex pattern.
    
    Args:
        columns (list): List of column names to process
        pattern (str): Regex pattern to find
        replacement (str): Replacement string for pattern matches
    
    Returns:
        list: Column names with regex replacements applied (system columns preserved)
    """
    if not pattern:
        return columns
    
    try:
        regex_replacement_log_info = f"Applying regex replacement: pattern='{pattern}', replacement='{replacement}'."
        log_and_print(regex_replacement_log_info)
        
        return [
            col if 'delta__' in col else re.sub(pattern, replacement, col)
            for col in columns
        ]
    except re.error as e:
        error_log_info = f"Invalid regex pattern '{pattern}': {str(e)}. Skipping regex replacement."
        log_and_print(error_log_info)
        return columns


def _replace_non_alphanumeric(
    columns: list,
    enabled: bool
) -> list:
    """
    Replace non-alphanumeric characters with underscores.
    
    Args:
        columns (list): List of column names to process
        enabled (bool): Whether to apply replacement
    
    Returns:
        list: Column names with non-alphanumeric chars replaced (system columns preserved)
    """
    if not enabled:
        return columns
    
    special_char_replacement_log_info = "Replacing non-alphanumeric characters with underscores using regex '[^A-Za-z0-9_]'."
    log_and_print(special_char_replacement_log_info)
    
    return [
        col if 'delta__' in col else re.sub(r'[^A-Za-z0-9_]', '_', col)
        for col in columns
    ]


def _collapse_underscores(
    columns: list
) -> list:
    """
    Collapse consecutive underscores to single underscore.
    
    Args:
        columns (list): List of column names to process
    
    Returns:
        list: Column names with consecutive underscores collapsed (system columns preserved)
    """
    collapse_underscores_log_info = "Collapsing consecutive underscores to single underscore."
    log_and_print(collapse_underscores_log_info)
    
    return [
        col if 'delta__' in col else re.sub(r'_+', '_', col)
        for col in columns
    ]

def _apply_case_conversion(
    columns: list,
    case_type: str
) -> list:
    """
    Apply case conversion to column names.
    
    Args:
        columns (list): List of column names to process
        case_type (str): Case type - 'title', 'lower', 'upper', or '' (no change)
    
    Returns:
        list: Column names with case conversion applied (system columns preserved)
    """
    if case_type == 'title':
        title_case_log_info = "Converting column names to title case."
        log_and_print(title_case_log_info)
        return [col if 'delta__' in col else col.title() for col in columns]
    
    elif case_type == 'lower':
        lower_case_log_info = "Converting column names to lowercase."
        log_and_print(lower_case_log_info)
        return [col if 'delta__' in col else col.lower() for col in columns]
    
    elif case_type == 'upper':
        upper_case_log_info = "Converting column names to uppercase."
        log_and_print(upper_case_log_info)
        return [col if 'delta__' in col else col.upper() for col in columns]
    
    else:
        column_names_kept_log_info = "Column name casing is kept the same."
        log_and_print(column_names_kept_log_info)
        return columns


def column_name_cleansing(
    columns,
    advanced_processing_config: dict
):
    """
    Standardize column names according to enterprise naming conventions.

    This function orchestrates a series of transformations to ensure:
    - Consistent naming patterns across all tables
    - Removal of problematic special characters
    - Compliance with Spark/Delta column naming rules
    - Preservation of system columns (delta__*)

    Args:
        columns (list): List of original column names
        advanced_processing_config (dict): Configuration dict containing column name standardization settings:
            - trim_column_names (bool): Trim leading/trailing whitespace from column names
            - apply_case_column_names (str): Case transformation to apply - 'lower', 'upper', 'title', or '' (no change)
            - replace_non_alphanumeric_chars_with_underscore_in_column_names (bool): Replace non-alphanumeric characters with underscores
            - regex_find_in_column_names (str): Regex pattern to find in column names
            - regex_replace_in_column_names (str): Replacement value for regex matches
            - exact_find_in_column_names (str): Comma-separated exact strings to find
            - exact_replace_in_column_names (str): Single replacement value for all finds OR comma-separated list with exact count match

    Returns:
        list: Cleansed column names following naming standards

    Transformations applied (in order):
        1. Trim leading/trailing whitespace (if enabled)
        2. Exact string replacement (if configured)
        3. Regex pattern replacement (if configured)
        4. Replace non-alphanumeric characters with underscores (if enabled)
        5. Collapse consecutive underscores to single underscore
        6. Apply case conversion (title, lower, upper, or none)
    """
    # Extract column name standardization settings from config
    trim_column_names = advanced_processing_config['trim_column_names']
    apply_case_column_names = advanced_processing_config['apply_case_column_names']
    replace_non_alphanumeric_chars_with_underscore_in_column_names = advanced_processing_config['replace_non_alphanumeric_chars_with_underscore_in_column_names']
    regex_find_in_column_names = advanced_processing_config['regex_find_in_column_names']
    regex_replace_in_column_names = advanced_processing_config['regex_replace_in_column_names']
    exact_find_in_column_names = advanced_processing_config['exact_find_in_column_names']
    exact_replace_in_column_names = advanced_processing_config['exact_replace_in_column_names']
    
    # Start with original columns
    cleansed_columns = columns
    
    # Step 1: Trim leading/trailing whitespace
    cleansed_columns = _trim_column_names(
        columns=cleansed_columns,
        trim_enabled=trim_column_names
    )
    
    # Step 2: Exact string replacement
    cleansed_columns = _exact_string_replacement(
        columns=cleansed_columns,
        find_strings=exact_find_in_column_names,
        replace_string=exact_replace_in_column_names
    )
    
    # Step 3: Regex pattern replacement
    cleansed_columns = _regex_replacement(
        columns=cleansed_columns,
        pattern=regex_find_in_column_names,
        replacement=regex_replace_in_column_names
    )
    
    # Step 4: Replace non-alphanumeric characters with underscores
    cleansed_columns = _replace_non_alphanumeric(
        columns=cleansed_columns,
        enabled=replace_non_alphanumeric_chars_with_underscore_in_column_names
    )
    
    # Step 5: Collapse consecutive underscores (only if step 4 was applied)
    if replace_non_alphanumeric_chars_with_underscore_in_column_names:
        cleansed_columns = _collapse_underscores(columns=cleansed_columns)
    
    # Step 6: Apply case conversion
    cleansed_columns = _apply_case_conversion(
        columns=cleansed_columns,
        case_type=apply_case_column_names
    )
    
    # Create column mapping for logging
    column_mapping = list(zip(columns, cleansed_columns))
    
    # Log first 10 column mappings for debugging
    first_ten_mappings = column_mapping[:10]
    column_mapping_log_info = f"First 10 column name mappings (original → cleansed): {first_ten_mappings}"
    log_and_print(column_mapping_log_info)
    
    return zip(columns, cleansed_columns)


def _apply_column_name_cleansing_to_list(
    columns: list,
    advanced_processing_config: dict,
    column_type_description: str
) -> list:
    """
    Helper function to apply column name cleansing to a list of columns.
    
    Args:
        columns (list): List of column names to cleanse
        advanced_processing_config (dict): Configuration dict containing column name standardization settings
        column_type_description (str): Description of column type for logging (e.g., "primary key", "liquid clustering")
    
    Returns:
        list: List of cleansed column names
    """
    if not columns:
        return []
    
    log_and_print(f"Applying column name standardization to {column_type_description} columns.")
    
    column_name_mapping_zip = column_name_cleansing(
        columns=columns,
        advanced_processing_config=advanced_processing_config
    )
    
    column_name_mapping = list(column_name_mapping_zip)
    cleansed_columns = [col[1] for col in column_name_mapping]
    
    return cleansed_columns


def cleanse_column_names(
    df: DataFrame,
    primary_keys: list, 
    liquid_clustering_columns: list, 
    advanced_processing_config: dict
):
    """
    Apply column name standardization to DataFrame and related metadata lists.

    This function serves as a wrapper that applies column name cleansing to:
    - The DataFrame column names
    - Primary key column references
    - Liquid clustering column references
    
    It intelligently determines the default standardization mode based on source type,
    allowing query-based sources to preserve their original naming conventions.

    Args:
        df (DataFrame): Input DataFrame with columns to be standardized
        primary_keys (list): List of primary key column names to be updated
        liquid_clustering_columns (list): List of clustering column names to be updated
        advanced_processing_config (dict): Dictionary containing processing configuration including:
            - lower_case_column_names (bool): Convert column names to lowercase
            - title_case_column_names (bool): Convert column names to title case
            - replace_non_alphanumeric_chars_with_underscore_in_column_names (bool): Replace non-alphanumeric characters with underscores

    Returns:
        tuple: (DataFrame with renamed columns, updated primary keys list, updated clustering columns list)

    Configuration:
        - Table sources default to false for all options (preserve original names)
        - File sources default to true for lowercase and replace_non_alphanumeric_chars (apply standardization)
        - Can be overridden via column_cleansing category configs: trim, apply_case, 
          replace_non_alphanumeric_with_underscore, regex_find, regex_replace, exact_find, exact_replace
    """
    # Apply column name standardization to DataFrame columns
    cleansed_column_names = _apply_column_name_cleansing_to_list(
        columns=df.columns,
        advanced_processing_config=advanced_processing_config,
        column_type_description="data"
    )
    
    # Apply column name standardization to primary key columns
    cleansed_primary_keys = _apply_column_name_cleansing_to_list(
        columns=primary_keys,
        advanced_processing_config=advanced_processing_config,
        column_type_description="primary key"
    )

    # Apply column name standardization to liquid clustering columns
    cleansed_liquid_clustering_columns = _apply_column_name_cleansing_to_list(
        columns=liquid_clustering_columns,
        advanced_processing_config=advanced_processing_config,
        column_type_description="liquid clustering"
    )

    # Rename DataFrame columns to match cleansed names
    df = df.toDF(*cleansed_column_names)

    return df, cleansed_primary_keys, cleansed_liquid_clustering_columns

def add_timestamp_metadata_columns(
    df: DataFrame,
    target_table_name: str
):
    """
    Add system-generated timestamp columns for data lineage and change tracking.

    This function adds metadata columns ONLY when writing to Delta tables (not file outputs):
    - `delta__created_datetime`: Captures when the record first lands in a Fabric Delta table.
    - `delta__modified_datetime`: Captures when the record is written/updated during the current batch.

    The function intelligently determines when to add timestamps:
    - Created datetime: Added if not already present to track initial ingestion
    - Modified datetime: Refreshed on every Delta table write so downstream consumers know when data was last updated

    Args:
        df (DataFrame): Input DataFrame to enhance with metadata
        target_table_name (str): Target Delta table name (empty string for file outputs)

    Returns:
        DataFrame: Enhanced DataFrame with timestamp columns (if target_table_name is provided), otherwise unchanged

    Notes:
        - Timestamps are ONLY added when target_table_name is truthy (Delta table writes)
        - For file outputs (target_table_name is empty/None), no timestamp columns are added
        - Modified datetime tracks when records were last written to the Delta table
        - Created datetime preserves initial Delta table ingestion time
        - Timestamps are generated from UTC wall-clock time to match run logging
        - Microsecond precision is maintained for accurate ordering
    """
    # Only add metadata columns if writing to a table (not a file)
            
    if target_table_name:
        date_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")

        # Add created datetime when the column does not exist, ensuring the initial load timestamp is preserved
        if 'delta__created_datetime' not in df.columns:
            adding_created_datetime_log_info = "Adding created datetime metadata column."
            log_and_print(adding_created_datetime_log_info)

            # Generate timestamp with microsecond precision
            df = df.withColumn('delta__created_datetime', f.to_timestamp(f.lit(date_time), 'yyyy-MM-dd HH:mm:ss.SSSSSS'))
        
        # Always refresh modified datetime so downstream tables know when the write occurred
        adding_modified_datetime_log_info = "Setting modified datetime metadata column."
        log_and_print(adding_modified_datetime_log_info)

        # Initialize modified datetime with created datetime value
        df = df.withColumn('delta__modified_datetime', f.to_timestamp(f.lit(date_time), 'yyyy-MM-dd HH:mm:ss.SSSSSS'))

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Data Quality Category Display Name Mapping
# This mapping converts internal category names to user-friendly display names
# for reporting in the Data_Quality_Notifications table
DQ_CATEGORY_DISPLAY_NAMES = {
    "validate_condition": "Validate Condition",
    "duplicates_on_pks": "Duplicate Primary Keys",
    "table_lookups": "Referential Integrity",  # Legacy name - kept for backward compatibility
    "validate_referential_integrity": "Referential Integrity",
    "pattern_match": "Pattern Validation",  # Legacy name - kept for backward compatibility
    "validate_pattern": "Pattern Validation",
    "validate_batch_size": "Batch Size Validation",
    "validate_not_null": "Not Null Validation",
    "validate_unique": "Uniqueness Validation",
    "validate_range": "Range Validation",
    "validate_freshness": "Freshness Validation",
    "validate_completeness": "Completeness Validation",
    "validate_anomaly": "Anomaly Detection",
    "reconcile_row_count": "Reconcile Row Count",
    "reconcile_distinct_count": "Reconcile Distinct Count",
    "reconcile_missing_keys": "Reconcile Missing Keys",
    "reconcile_aggregate": "Reconcile Aggregate",
    "File Corruption": "File Corruption"  # Already formatted
}

def _get_dq_category_display_name(category: str) -> str:
    """Get user-friendly display name for a data quality category.
    
    Args:
        category: Internal category name (e.g., 'validate_condition', 'validate_not_null')
    
    Returns:
        str: User-friendly display name for reporting (e.g., 'Validate Condition', 'Not Null Validation')
    """
    return DQ_CATEGORY_DISPLAY_NAMES.get(category, category.replace('_', ' ').title())


DQ_FAILURE_DETAILS_COLUMN = "delta__dq_failure_details"
QUARANTINE_REASON_COLUMN = "delta__quarantine_reason"
DQ_ROW_ID_COLUMN = "delta__idx"
DQ_STEP_ORDER_COLUMN = "__dq_step_order"


def _agent_failure_routing_hint(failure_type: str, table_id=None) -> str:
    table_reference = f"Table_ID={table_id}" if table_id not in (None, "") else "the exact Table_ID"
    return (
        f"Agent follow-up: route this {failure_type} failure to /fdp-06-troubleshoot "
        f"or /fdp-06-investigate with {table_reference} and the current environment."
    )


def _missing_required_columns(df: DataFrame, required_columns: set) -> set:
    """Return required columns that are absent from the DataFrame schema."""
    return required_columns - set(df.columns)


def _is_mark_or_quarantine_action(action: str) -> bool:
    """Return True when DQ action reuses row-level violation DataFrames."""
    return action in ("mark", "quarantine")


def _build_out_of_range_condition(col_name: str, min_value: Any, max_value: Any):
    """Build an out-of-range predicate for a single column."""
    col_conditions = []
    if min_value is not None:
        col_conditions.append(f.col(col_name) < f.lit(min_value))
    if max_value is not None:
        col_conditions.append(f.col(col_name) > f.lit(max_value))

    out_of_range_condition = col_conditions[0]
    for cond in col_conditions[1:]:
        out_of_range_condition = out_of_range_condition | cond

    return out_of_range_condition


def _build_nullability_count_aggregates(
    column_names: list,
    count_nulls: bool,
    alias_prefix: str
) -> tuple:
    """Build alias map and aggregate expressions for null/non-null counts."""
    count_aliases = {}
    count_exprs = []

    for i, col_name in enumerate(column_names):
        alias = f"{alias_prefix}{i}"
        count_aliases[col_name] = alias

        if count_nulls:
            count_condition = f.col(col_name).isNull()
        else:
            count_condition = f.col(col_name).isNotNull()

        count_exprs.append(
            f.sum(
                f.when(count_condition, f.lit(1)).otherwise(f.lit(0))
            ).cast("long").alias(alias)
        )

    return count_aliases, count_exprs


def _empty_dq_marker_df(df: DataFrame) -> DataFrame:
    """Create an empty DataFrame with marker schema for row-level DQ failures.
    
    Markers are lightweight DataFrames that carry (delta__idx, detail_message) pairs.
    They are accumulated across all DQ checks and later joined back onto the main
    DataFrame to populate the delta__dq_failure_details column. This function
    creates a schema-compatible empty marker DF so unionByName always works.
    """
    # Build a zero-row DF with exactly the two columns every marker must have:
    #   delta__idx  (long)   – row identifier assigned at the start of _data_quality_checks
    #   delta__dq_failure_details (string) – human-readable failure message
    return df.limit(0).select(
        f.lit(None).cast("long").alias("delta__idx"),
        f.lit(None).cast("string").alias(DQ_FAILURE_DETAILS_COLUMN)
    ).where("1 = 0")


def _build_dq_marker_df(
    df: DataFrame,
    df_violations: DataFrame,
    detail_message: str,
    join_keys: Union[str, list] = "delta__idx"
) -> DataFrame:
    """Build row-level DQ marker rows (delta__idx + detail message) for violating records.
    
    This is used by the 'mark' action: instead of quarantining rows, we tag each
    failing row with a detail message so it continues downstream with an annotation
    in delta__dq_failure_details.  The resulting DF has exactly two columns:
      - delta__idx: identifies the row
      - delta__dq_failure_details: the human-readable failure detail
    """
    # Guard: if the main DF doesn't have delta__idx or there are no violations, return empty
    if "delta__idx" not in df.columns or df_violations is None:
        return _empty_dq_marker_df(df)

    # Normalize join_keys to a list so the logic below works uniformly
    normalized_join_keys = [join_keys] if isinstance(join_keys, str) else join_keys

    # Fast path: when joining on delta__idx and it's already in violations, just select it
    if normalized_join_keys == ["delta__idx"] and "delta__idx" in df_violations.columns:
        marker_ids = df_violations.select("delta__idx").distinct()
    else:
        # Slow path: join violations back to the main DF on business keys to resolve delta__idx
        marker_ids = df.join(df_violations, normalized_join_keys, "inner").select(df["delta__idx"]).distinct()

    # Stamp the detail message onto every violation row
    return marker_ids.withColumn(DQ_FAILURE_DETAILS_COLUMN, f.lit(detail_message))


def _append_marker_df(base_markers: DataFrame, additional_markers: DataFrame) -> DataFrame:
    """Union marker rows while preserving schema.
    
    Markers accumulate across every DQ check. This helper safely stacks them
    using unionByName (allowMissingColumns handles any schema drift).
    Later, all markers are grouped by delta__idx and their messages are
    concatenated into a single pipe-delimited string by _apply_dq_failure_markers.
    """
    if additional_markers is None:
        return base_markers

    return base_markers.unionByName(additional_markers, allowMissingColumns=True)


def _apply_dq_failure_markers(
    df: DataFrame,
    df_quarantined_all: DataFrame,
    marker_candidates: DataFrame
) -> tuple:
    """Attach aggregated DQ failure details to active and quarantined outputs.
    
    This is the final step of the 'mark' action flow:
      1. Individual validators created per-row marker DFs via _build_dq_marker_df
      2. Markers were accumulated across all checks via _append_marker_df
      3. Markers for quarantined rows were removed by _exclude_quarantined_rows_from_markers
      4. HERE: remaining markers are grouped per-row and joined back as a new column
    
    The result is that rows with 'mark' failures get a delta__dq_failure_details column
    containing all failure messages separated by '; ', e.g.:
      "Customer ID required; Age must be positive"
    """
    # Safety: if no delta__idx tracking column, nothing to join on
    if "delta__idx" not in df.columns:
        return df, df_quarantined_all

    # Safety: if there are no markers at all, skip the join entirely
    if (
        marker_candidates is None
        or DQ_FAILURE_DETAILS_COLUMN not in marker_candidates.columns
        or marker_candidates.limit(1).count() == 0
    ):
        return df, df_quarantined_all

    # Aggregate: one row may have markers from multiple checks.
    # collect_set + sort_array ensures deterministic, deduplicated ordering.
    # concat_ws joins them with '; ' into a single human-readable string.
    aggregated_markers = marker_candidates.groupBy("delta__idx").agg(
        f.concat_ws(
            "; ",
            f.sort_array(f.collect_set(f.col(DQ_FAILURE_DETAILS_COLUMN)))
        ).alias(DQ_FAILURE_DETAILS_COLUMN)
    )

    # Left join: only active rows get markers. Quarantined rows should NOT get
    # delta__dq_failure_details — they already have delta__quarantine_reason.
    df = df.join(aggregated_markers, "delta__idx", "left")
    return df, df_quarantined_all


def _exclude_quarantined_rows_from_markers(
    marker_candidates: DataFrame,
    df_quarantined_all: DataFrame
) -> DataFrame:
    """Remove marker candidates for rows that were ultimately quarantined.
    
    A row can have BOTH a 'mark' result from one check AND a 'quarantine' result
    from another check.  Since quarantine wins (the row is removed from the main
    DF entirely), we must drop its markers so we don't try to join them onto a
    row that no longer exists in the active output.  This left_anti join removes
    any marker whose delta__idx appears in the quarantined set.
    """
    if (
        marker_candidates is None
        or "delta__idx" not in marker_candidates.columns
        or df_quarantined_all is None
        or "delta__idx" not in df_quarantined_all.columns
    ):
        return marker_candidates

    # Get the set of row IDs that were quarantined
    quarantined_ids = df_quarantined_all.select("delta__idx").distinct()
    # left_anti: keep only markers whose delta__idx is NOT in the quarantined set
    return marker_candidates.join(quarantined_ids, "delta__idx", "left_anti")


def _record_dq_result(
    violation_count: int,
    dq_category: str,
    dq_message: str,
    if_not_compliant: str,
    warnings: list,
    force_failures: list,
    pass_message: str = None,
    column_name: str = None
) -> tuple:
    """
    Record data quality check results to the appropriate list based on compliance action.
    
    This centralized function handles the common pattern of recording DQ results:
    - 'warn' (default): Add warning with row impact count
    - 'mark': Add warning with row impact count (without quarantine)
    - 'quarantine': Add warning indicating rows were quarantined
    - 'fail': Add to force_failures list
    - Pass (violation_count == 0): Record successful validation
    
    Args:
        violation_count (int): Number of records that violated the DQ rule
        dq_category (str): Internal category name (will be converted to display name)
        dq_message (str): Message describing the violation (used for failures and warnings)
        if_not_compliant (str): Action to take - 'warn', 'mark', 'quarantine', or 'fail'
        warnings (list): List to append warning/pass results to
        force_failures (list): List to append fail results to
        pass_message (str, optional): Custom message for pass outcome. If None, uses dq_message.
        column_name (str, optional): Column name for column-specific validations (adds 'column' key)
    
    Returns:
        tuple: (warnings, force_failures) - Updated lists after recording the result
    
    Note:
        This function only records results - it does NOT handle DataFrame quarantine operations.
        Callers must handle quarantine DataFrame logic separately before calling this function.
    """
    category_display = _get_dq_category_display_name(dq_category)
    
    if violation_count > 0:
        if if_not_compliant in ('warn', 'mark'):
            json_to_append = {
                "outcome": "Warning", 
                "category": category_display, 
                "message": dq_message, 
                "quarantined": "No", 
                "rows_impacted": violation_count
            }
            if column_name:
                json_to_append["column"] = column_name
            warnings.append(json_to_append)
        elif if_not_compliant == 'quarantine':
            json_to_append = {
                "outcome": "Warning", 
                "category": category_display, 
                "message": dq_message, 
                "quarantined": "Yes", 
                "rows_quarantined": violation_count
            }
            if column_name:
                json_to_append["column"] = column_name
            warnings.append(json_to_append)
        else:
            # 'fail' or any unrecognized value - add to force_failures list
            json_to_append = {
                "outcome": "Failure",
                "category": category_display, 
                "message": dq_message, 
                "rows_impacted": violation_count
            }
            if column_name:
                json_to_append["column"] = column_name
            force_failures.append(json_to_append)
    else:
        # Pass - no violations found
        json_to_append = {
            "outcome": "Pass", 
            "category": category_display, 
            "message": pass_message if pass_message else dq_message
        }
        if column_name:
            json_to_append["column"] = column_name
        warnings.append(json_to_append)
    
    return warnings, force_failures


def _quarantine_rows(
    df: DataFrame,
    df_quarantined_all: DataFrame,
    df_violations: DataFrame,
    quarantine_reason: str,
    join_keys: Union[str, list],
    join_type: str = 'inner'
) -> tuple:
    """
    Quarantine violating rows from the main DataFrame.
    
    This centralized function handles the common pattern of:
    1. Joining main df with violations to identify rows to quarantine
    2. Adding quarantine reason column (delta__quarantine_reason)
    3. Appending to accumulated quarantine DataFrame
    4. Removing quarantined rows from main DataFrame via left_anti join
    
    The quarantined rows end up in a separate output table so they can be
    reviewed and re-processed later. The main DataFrame continues downstream
    without those rows.
    
    Args:
        df (DataFrame): Main DataFrame to remove quarantined rows from
        df_quarantined_all (DataFrame): Accumulated quarantined records DataFrame
        df_violations (DataFrame): DataFrame containing the violating rows/keys
        quarantine_reason (str): Reason message to add to quarantined records
        join_keys (str or list): Column name(s) to join on for identifying rows
        join_type (str): Type of join for quarantine identification ('inner' default)
                        Use 'semi' when df_violations has fewer columns than df
    
    Returns:
        tuple: (cleaned_df, updated_quarantined_df)
            - cleaned_df: Main DataFrame with violating rows removed
            - updated_quarantined_df: Accumulated quarantine DataFrame with new rows
    """
    # Step 1: Identify the full rows to quarantine by joining violations back to the main DF.
    # 'semi' is used when violations DF has fewer columns (avoids duplicate columns from inner join).
    # 'inner' is used when violations already share the same schema.
    # select(df["*"]) ensures we keep only the original columns from df.
    if join_type == 'semi':
        df_quarantined = df.join(df_violations, join_keys, 'semi').select(df["*"])
    else:
        df_quarantined = df.join(df_violations, join_keys, 'inner').select(df["*"])
    
    # Step 2: Stamp the quarantine reason onto these rows so reviewers know why they were removed.
    # This becomes the delta__quarantine_reason column in the quarantine output table.
    df_quarantined = df_quarantined.withColumn(QUARANTINE_REASON_COLUMN, f.lit(quarantine_reason))
    
    # Step 3: Accumulate quarantined rows — multiple checks can each quarantine different rows,
    # so we unionByName (allowMissingColumns handles schema differences across check types).
    df_quarantined_all = df_quarantined_all.unionByName(
        df_quarantined, 
        allowMissingColumns=True
    )
    
    # Step 4: Remove quarantined rows from the main DF using left_anti join.
    # left_anti keeps only rows from df whose join keys do NOT appear in df_violations.
    df = df.join(df_violations, join_keys, 'left_anti').select(df["*"])
    
    return df, df_quarantined_all


def _handle_corrupt_records(
    df: DataFrame, 
    warnings: list
) -> tuple:
    """Handle corrupt records from schema enforcement."""
    df_quarantined_all = df.limit(0)
    dq_markers = _empty_dq_marker_df(df)
    
    if 'delta__corrupt_record' in df.columns:
        identifying_corrupt_records_log_info = "Identifying corrupt records."
        log_and_print(identifying_corrupt_records_log_info)
        df_corrupt_records = df.where(f.col("delta__corrupt_record").isNotNull())
        # Reused for quarantine union, anti-join, and count; persist to avoid re-scans.
        df_corrupt_records = df_corrupt_records.persist(StorageLevel.MEMORY_AND_DISK)

        try:
            quarantine_message = "File Corruption check failed: row data does not conform to input schema in metadata."
            df_quarantined = df_corrupt_records.withColumn(QUARANTINE_REASON_COLUMN, f.lit(quarantine_message))
            df_quarantined_all = df_quarantined_all.unionByName(
                df_quarantined, 
                allowMissingColumns = True
            )
            
            df = df.join(df_corrupt_records, "delta__idx", 'left_anti').select(df["*"])
            df = df.drop("delta__corrupt_record")

            number_of_corrupt_records = df_corrupt_records.count()
            dq_message = f"There are {number_of_corrupt_records} rows from the input file in the Bronze files did not match the provided schema."
            json_to_append = {
                "outcome": "Warning", 
                "category": "File Corruption", 
                "message": dq_message, 
                "quarantined": "Yes", 
                "rows_quarantined": number_of_corrupt_records
            }
            warnings.append(json_to_append)
        finally:
            df_corrupt_records.unpersist()
    
    return df, df_quarantined_all, warnings, dq_markers

def _validate_primary_keys(
    df: DataFrame, 
    df_quarantined_all: DataFrame, 
    primary_keys: list, 
    if_duplicate_primary_keys: str, 
    columns_to_add_to_pks: list, 
    target_table_name: str, 
    warnings: list, 
    force_failures: list,
    table_id=None
) -> tuple:
    """Validate primary key uniqueness."""
    dq_markers = _empty_dq_marker_df(df)

    # Rule 89 is now checked upstream in parse_primary_key_configuration() in HF3
    # where it can inspect the RAW primary_config value before the "fail" default is applied.

    if primary_keys:
        validating_primary_keys_log_info = "Validating primary keys."
        log_and_print(validating_primary_keys_log_info)

        data_quality_category = "duplicates_on_pks"

        if columns_to_add_to_pks and target_table_name:
            primary_keys = primary_keys + columns_to_add_to_pks

        # Validate that all primary keys exist
        _validate_columns_exist(
            df = df, 
            columns_to_check = primary_keys, 
            operation_name = "primary_key_validation"
        )

        # Backtick-quote keys so dots in flattened column names aren't read as nested fields.
        dupes = df.groupby(*[f.col(f"`{primary_key}`") for primary_key in primary_keys]) \
                    .count() \
                    .filter(f.col('count') > 1) 
        # Persist only when duplicate keys are reused after count() by mark/quarantine.
        reuse_dupes_df = if_duplicate_primary_keys in ('mark', 'quarantine')
        if reuse_dupes_df:
            dupes = dupes.persist(StorageLevel.MEMORY_AND_DISK)

        try:
            number_of_dupes = dupes.count()
            primary_keys_str = ', '.join(primary_keys)
            message = f"Duplicate primary keys ({primary_keys_str})"
            dq_message = message
            if number_of_dupes > 0 and if_duplicate_primary_keys not in ('warn', 'mark', 'quarantine'):
                dq_message = f"{message}. {_agent_failure_routing_hint('duplicate primary keys', table_id)}"

            if number_of_dupes > 0 and if_duplicate_primary_keys == 'mark':
                dq_markers = _build_dq_marker_df(
                    df=df,
                    df_violations=dupes,
                    detail_message=message,
                    join_keys=primary_keys
                )
            
            # Handle quarantine if needed
            if number_of_dupes > 0 and if_duplicate_primary_keys == 'quarantine':
                df, df_quarantined_all = _quarantine_rows(
                    df=df,
                    df_quarantined_all=df_quarantined_all,
                    df_violations=dupes,
                    quarantine_reason=message,
                    join_keys=primary_keys
                )
        finally:
            if reuse_dupes_df:
                dupes.unpersist()
        
        # Record the DQ result
        _record_dq_result(
            violation_count=number_of_dupes,
            dq_category=data_quality_category,
            dq_message=dq_message,
            if_not_compliant=if_duplicate_primary_keys,
            warnings=warnings,
            force_failures=force_failures
        )
    
    return df, df_quarantined_all, warnings, force_failures, dq_markers

def _validate_condition(
    df: DataFrame, 
    df_quarantined_all: DataFrame, 
    data_quality_step: dict, 
    warnings: list, 
    force_failures: list
) -> tuple:
    """Validate data using a SQL condition expression.
    
    Note: The condition should define what VALID data looks like. Records that DON'T match
    this condition will be flagged/quarantined as data quality issues.
    
    Example: condition = "amount > 0 AND status = 'Active'"
    This will flag/quarantine records where amount <= 0 OR status != 'Active'
    """
    data_quality_category = data_quality_step["Category"]
    dq_result_if_not_compliant = data_quality_step.get('if_not_compliant', 'warn').lower()
    condition = data_quality_step["condition"]
    message = data_quality_step['message']

    validating_condition_log_info = f"Validating data with expected rule: ({condition}). Rows that fail this rule will be flagged."
    log_and_print(validating_condition_log_info)
    
    # Rows are invalid when they do not satisfy the configured condition.
    condition_not_matches = df.filter(f"NOT ({condition})")
    # Persist only when the violations DF is reused (count + mark/quarantine path)
    # to avoid re-running the same filter plan multiple times.
    reuse_violations_df = _is_mark_or_quarantine_action(dq_result_if_not_compliant)
    if reuse_violations_df:
        condition_not_matches = condition_not_matches.persist(StorageLevel.MEMORY_AND_DISK)

    try:
        number_of_condition_not_matches = condition_not_matches.count()
        dq_message = message
        dq_markers = _empty_dq_marker_df(df)

        if number_of_condition_not_matches > 0 and dq_result_if_not_compliant == 'mark':
            dq_markers = _build_dq_marker_df(
                df=df,
                df_violations=condition_not_matches,
                detail_message=message,
                join_keys="delta__idx"
            )
        
        # Handle quarantine if needed
        if number_of_condition_not_matches > 0 and dq_result_if_not_compliant == 'quarantine':
            quarantine_message = message
            df, df_quarantined_all = _quarantine_rows(
                df=df,
                df_quarantined_all=df_quarantined_all,
                df_violations=condition_not_matches,
                quarantine_reason=quarantine_message,
                join_keys='delta__idx'
            )
    finally:
        if reuse_violations_df:
            # Release temporary persisted data immediately after this check completes.
            condition_not_matches.unpersist()
    
    # Record the DQ result
    _record_dq_result(
        violation_count=number_of_condition_not_matches,
        dq_category=data_quality_category,
        dq_message=dq_message,
        if_not_compliant=dq_result_if_not_compliant,
        warnings=warnings,
        force_failures=force_failures
    )
    
    return df, df_quarantined_all, warnings, force_failures, dq_markers

def _validate_batch_size(
    df: DataFrame, 
    data_quality_step: dict, 
    warnings: list, 
    force_failures: list
) -> tuple:
    """Validate that the batch meets a minimum row count threshold."""
    data_quality_category = data_quality_step["Category"]
    dq_result_if_not_compliant = data_quality_step.get('if_not_compliant', 'warn').lower()
    min_rows = data_quality_step["min_rows"]
    min_rows_int = int(min_rows)
    message = data_quality_step['message']
    records = df.count()

    validating_batch_size_log_info = f"Validating batch size meets minimum threshold of {min_rows} rows"
    log_and_print(validating_batch_size_log_info)

    # Determine violation count: report the shortfall (how many rows are missing)
    # so that warnings/failures show "rows_impacted: 3" meaning "3 more rows needed".
    is_below_threshold = records < min_rows_int
    violation_count = (min_rows_int - records) if is_below_threshold else 0
    
    dq_message = message
    pass_message = message
    
    # Record the DQ result
    _record_dq_result(
        violation_count=violation_count,
        dq_category=data_quality_category,
        dq_message=dq_message,
        if_not_compliant=dq_result_if_not_compliant,
        warnings=warnings,
        force_failures=force_failures,
        pass_message=pass_message
    )
    
    return warnings, force_failures

def _validate_referential_integrity(
    df: DataFrame, 
    df_quarantined_all: DataFrame, 
    data_quality_step: dict, 
    datastore_config: str | list, 
    warnings: list, 
    force_failures: list
) -> tuple:
    """Validate referential integrity by checking values exist in a reference table.
    
    This function validates that values in the current table's column exist in a 
    reference/master table. This is essential for maintaining data integrity in 
    fact-dimension relationships and foreign key validations.
    
    Args:
        df (DataFrame): Input DataFrame to validate
        df_quarantined_all (DataFrame): Accumulated quarantined records
        data_quality_step (dict): Configuration containing:
            - current_table_column_name: Single column in the current table to validate
            - reference_table_name: Reference table name (datastore.schema.table)
            - reference_table_column_name: Single corresponding column in the reference table
            - if_not_compliant: Action when values not found ('warn', 'quarantine', 'fail')
        datastore_config (str | list): Datastore configuration from Datastore_Configuration table
        warnings (list): Accumulated warning messages
        force_failures (list): Accumulated failure messages
    
    Returns:
        tuple: (cleaned_df, quarantined_df, warnings, force_failures)
    """
    data_quality_category = data_quality_step["Category"]
    dq_result_if_not_compliant = data_quality_step.get('if_not_compliant', 'warn').lower()
    current_table_column_name = data_quality_step['current_table_column_name']
    reference_table_name = data_quality_step['reference_table_name']
    reference_table_column_name = data_quality_step['reference_table_column_name']
    message = data_quality_step['message']

    target_datastore_name = reference_table_name.split('.')[0].lower()
    target_workspace_name = _get_datastore_config(datastore_config, target_datastore_name, 'Workspace_Name')
    full_reference_table_name = f"`{target_workspace_name}`.{reference_table_name}"

    validating_referential_integrity_log_info = f"Validating referential integrity: {current_table_column_name} values exist in {full_reference_table_name}"
    log_and_print(validating_referential_integrity_log_info)

    df_primary_table = df.select(current_table_column_name).distinct()
    df_reference_table = spark.sql(f"SELECT {reference_table_column_name} as {current_table_column_name} FROM {full_reference_table_name}").distinct()

    # left_anti avoids subtract's extra set-operation overhead and is easier to optimize.
    missing_values = df_primary_table.join(df_reference_table, current_table_column_name, "left_anti")
    # Persist only when the violations DF is reused after count() by mark/quarantine.
    reuse_violations_df = _is_mark_or_quarantine_action(dq_result_if_not_compliant)
    if reuse_violations_df:
        missing_values = missing_values.persist(StorageLevel.MEMORY_AND_DISK)

    try:
        number_of_missing_values = missing_values.count()
        dq_message = message
        dq_markers = _empty_dq_marker_df(df)

        if number_of_missing_values > 0 and dq_result_if_not_compliant == 'mark':
            marker_message = message
            dq_markers = _build_dq_marker_df(
                df=df,
                df_violations=missing_values,
                detail_message=marker_message,
                join_keys=current_table_column_name
            )
        
        # Handle quarantine if needed
        if number_of_missing_values > 0 and dq_result_if_not_compliant == "quarantine":
            quarantine_message = message
            df, df_quarantined_all = _quarantine_rows(
                df=df,
                df_quarantined_all=df_quarantined_all,
                df_violations=missing_values,
                quarantine_reason=quarantine_message,
                join_keys=current_table_column_name
            )
    finally:
        if reuse_violations_df:
            # Ensure temporary cache is released even on exceptions.
            missing_values.unpersist()
    
    # Record the DQ result
    _record_dq_result(
        violation_count=number_of_missing_values,
        dq_category=data_quality_category,
        dq_message=dq_message,
        if_not_compliant=dq_result_if_not_compliant,
        warnings=warnings,
        force_failures=force_failures
    )
    
    return df, df_quarantined_all, warnings, force_failures, dq_markers


def _record_not_null_warn_fail_results(
    null_counts: dict,
    null_count_aliases: dict,
    column_names: list,
    message: str,
    data_quality_category: str,
    dq_result_if_not_compliant: str,
    warnings: list,
    force_failures: list
) -> tuple:
    """Record NOT NULL validation outcomes for warn/fail action paths."""
    for col_name in column_names:
        col_message = f"{message} ({col_name})" if len(column_names) > 1 else message
        null_count = int(null_counts.get(null_count_aliases[col_name], 0) or 0)

        # Build a descriptive message that includes 'null' context when violations found
        dq_message = f"{col_message} - {null_count} null value(s) found" if null_count > 0 else col_message

        _record_dq_result(
            violation_count=null_count,
            dq_category=data_quality_category,
            dq_message=dq_message,
            if_not_compliant=dq_result_if_not_compliant,
            warnings=warnings,
            force_failures=force_failures,
            pass_message=col_message,
            column_name=col_name
        )

    return warnings, force_failures


def _process_not_null_column_mark_or_quarantine(
    df: DataFrame,
    df_quarantined_all: DataFrame,
    dq_markers: DataFrame,
    col_name: str,
    message: str,
    total_columns: int,
    dq_result_if_not_compliant: str
) -> tuple:
    """Process row-level NOT NULL handling for a single column (mark/quarantine)."""
    col_message = f"{message} ({col_name})" if total_columns > 1 else message
    null_condition = f.col(col_name).isNull()
    df_nulls = df.filter(null_condition)

    reuse_violations_df = _is_mark_or_quarantine_action(dq_result_if_not_compliant)
    if reuse_violations_df:
        df_nulls = df_nulls.persist(StorageLevel.MEMORY_AND_DISK)

    try:
        null_count = df_nulls.count()

        if null_count > 0 and dq_result_if_not_compliant == "quarantine":
            quarantine_message = col_message
            df, df_quarantined_all = _quarantine_rows(
                df=df,
                df_quarantined_all=df_quarantined_all,
                df_violations=df_nulls,
                quarantine_reason=quarantine_message,
                join_keys='delta__idx'
            )

        if null_count > 0 and dq_result_if_not_compliant == 'mark':
            dq_markers = _append_marker_df(
                dq_markers,
                _build_dq_marker_df(
                    df=df,
                    df_violations=df_nulls,
                    detail_message=col_message,
                    join_keys="delta__idx"
                )
            )
    finally:
        if reuse_violations_df:
            df_nulls.unpersist()

    return df, df_quarantined_all, dq_markers, null_count, col_message

def _validate_not_null(
    df: DataFrame,
    df_quarantined_all: DataFrame,
    data_quality_step: dict,
    warnings: list,
    force_failures: list
) -> tuple:
    """Validate that specified columns do not contain NULL values.
    
    This function checks for NULL values in one or more columns and takes
    appropriate action based on configuration. Each column generates its own
    separate warning/failure message for granular tracking.
    
    Args:
        df (DataFrame): Input DataFrame to validate
        df_quarantined_all (DataFrame): Accumulated quarantined records
        data_quality_step (dict): Configuration containing:
            - column_name: Column(s) to check for NULLs (comma-separated for multiple)
            - if_not_compliant: Action when NULLs found ('warn', 'quarantine', 'fail')
        warnings (list): Accumulated warning messages
        force_failures (list): Accumulated failure messages
    
    Returns:
        tuple: (cleaned_df, quarantined_df, warnings, force_failures)
    """
    data_quality_category = data_quality_step["Category"]
    dq_result_if_not_compliant = data_quality_step.get('if_not_compliant', 'warn').lower()
    column_names_raw = data_quality_step['column_name']
    message = data_quality_step['message']
    
    # Parse comma-separated column names
    column_names = [col.strip() for col in column_names_raw.split(',')]
    dq_markers = _empty_dq_marker_df(df)
    
    log_and_print(f"Validating NOT NULL constraint on columns: {column_names}")

    # For warn/fail, we can compute all per-column null counts in one pass because
    # row-level mutation (mark/quarantine) is not required.
    if dq_result_if_not_compliant in ('warn', 'fail'):
        pass  # fall through to warn/fail aggregation below
    elif "delta__idx" not in df.columns:
        # Ensure delta__idx exists for row-level operations (mark/quarantine)
        df = df.withColumn("delta__idx", f.monotonically_increasing_id())
        dq_markers = _empty_dq_marker_df(df)

    if dq_result_if_not_compliant in ('warn', 'fail'):
        null_count_aliases, null_count_exprs = _build_nullability_count_aggregates(
            column_names=column_names,
            count_nulls=True,
            alias_prefix="__null_count_"
        )

        null_counts = df.agg(*null_count_exprs).first().asDict()

        warnings, force_failures = _record_not_null_warn_fail_results(
            null_counts=null_counts,
            null_count_aliases=null_count_aliases,
            column_names=column_names,
            message=message,
            data_quality_category=data_quality_category,
            dq_result_if_not_compliant=dq_result_if_not_compliant,
            warnings=warnings,
            force_failures=force_failures
        )

        return df, df_quarantined_all, warnings, force_failures, dq_markers
    
    # Process each column separately for granular tracking
    # For mark/quarantine, keep sequential evaluation so each column sees the
    # current post-removal df and preserves first-match row-removal semantics.
    for col_name in column_names:
        df, df_quarantined_all, dq_markers, null_count, col_message = _process_not_null_column_mark_or_quarantine(
            df=df,
            df_quarantined_all=df_quarantined_all,
            dq_markers=dq_markers,
            col_name=col_name,
            message=message,
            total_columns=len(column_names),
            dq_result_if_not_compliant=dq_result_if_not_compliant
        )

        dq_message = col_message
        pass_message = col_message
        
        # Record the DQ result
        _record_dq_result(
            violation_count=null_count,
            dq_category=data_quality_category,
            dq_message=dq_message,
            if_not_compliant=dq_result_if_not_compliant,
            warnings=warnings,
            force_failures=force_failures,
            pass_message=pass_message,
            column_name=col_name
        )
    
    return df, df_quarantined_all, warnings, force_failures, dq_markers


def _validate_unique(
    df: DataFrame,
    df_quarantined_all: DataFrame,
    data_quality_step: dict,
    warnings: list,
    force_failures: list
) -> tuple:
    """Validate that specified columns contain unique values (no duplicates).
    
    This function checks for duplicate values in one or more columns that are
    not part of the primary key validation.
    
    Args:
        df (DataFrame): Input DataFrame to validate
        df_quarantined_all (DataFrame): Accumulated quarantined records
        data_quality_step (dict): Configuration containing:
            - column_name: Column(s) that should be unique (comma-separated for multiple)
            - if_not_compliant: Action when duplicates found ('warn', 'quarantine', 'fail')
        warnings (list): Accumulated warning messages
        force_failures (list): Accumulated failure messages
    
    Returns:
        tuple: (cleaned_df, quarantined_df, warnings, force_failures)
    """
    data_quality_category = data_quality_step["Category"]
    dq_result_if_not_compliant = data_quality_step.get('if_not_compliant', 'warn').lower()
    column_names_raw = data_quality_step['column_name']
    message = data_quality_step['message']
    
    # Parse comma-separated column names
    column_names = [col.strip() for col in column_names_raw.split(',')]
    
    log_and_print(f"Validating uniqueness on columns: {column_names}")
    
    # Find duplicates
    window_spec = Window.partitionBy([f.col(c) for c in column_names])
    df_with_count = df.withColumn("_dup_count", f.count("*").over(window_spec))
    # Persist only when the scored DF is reused across count() and mark/quarantine logic.
    reuse_scored_df = _is_mark_or_quarantine_action(dq_result_if_not_compliant)
    if reuse_scored_df:
        df_with_count = df_with_count.persist(StorageLevel.MEMORY_AND_DISK)

    dq_markers = _empty_dq_marker_df(df)
    duplicate_count = 0
    pass_message = message

    try:
        df_duplicates = df_with_count.filter(f.col("_dup_count") > 1)
        duplicate_count = df_duplicates.count()
        
        # Build descriptive message including 'duplicate' context when violations found
        dq_message = f"{message} - {duplicate_count} duplicate value(s) found" if duplicate_count > 0 else message

        # Handle quarantine if needed
        if duplicate_count > 0 and dq_result_if_not_compliant == "quarantine":
            quarantine_message = message
            df_quarantined = df_duplicates.drop("_dup_count").withColumn(QUARANTINE_REASON_COLUMN, f.lit(quarantine_message))
            df_quarantined_all = df_quarantined_all.unionByName(df_quarantined, allowMissingColumns=True)
            df = df_with_count.filter(f.col("_dup_count") == 1).drop("_dup_count")
        else:
            df = df_with_count.drop("_dup_count")

        if duplicate_count > 0 and dq_result_if_not_compliant == 'mark':
            dq_markers = _build_dq_marker_df(
                df=df_with_count.drop("_dup_count"),
                df_violations=df_duplicates.drop("_dup_count"),
                detail_message=message,
                join_keys="delta__idx"
            )
    finally:
        if reuse_scored_df:
            # Ensure temporary cache is released even on exceptions.
            df_with_count.unpersist()
    
    # Record the DQ result
    _record_dq_result(
        violation_count=duplicate_count,
        dq_category=data_quality_category,
        dq_message=dq_message,
        if_not_compliant=dq_result_if_not_compliant,
        warnings=warnings,
        force_failures=force_failures,
        pass_message=pass_message
    )
    
    return df, df_quarantined_all, warnings, force_failures, dq_markers


def _build_range_count_aggregates(
    column_names: list,
    min_value: Any,
    max_value: Any
) -> tuple:
    """Build alias map and aggregate expressions for range violation counts."""
    out_of_range_count_aliases = {}
    out_of_range_count_exprs = []

    for i, col_name in enumerate(column_names):
        out_of_range_condition = _build_out_of_range_condition(
            col_name=col_name,
            min_value=min_value,
            max_value=max_value
        )

        alias = f"__out_of_range_count_{i}"
        out_of_range_count_aliases[col_name] = alias
        out_of_range_count_exprs.append(
            f.sum(
                f.when(out_of_range_condition, f.lit(1)).otherwise(f.lit(0))
            ).cast("long").alias(alias)
        )

    return out_of_range_count_aliases, out_of_range_count_exprs


def _record_range_warn_fail_results(
    out_of_range_counts: dict,
    out_of_range_count_aliases: dict,
    column_names: list,
    message: str,
    data_quality_category: str,
    dq_result_if_not_compliant: str,
    warnings: list,
    force_failures: list
) -> tuple:
    """Record range validation outcomes for warn/fail action paths."""
    for col_name in column_names:
        col_message = f"{message} ({col_name})" if len(column_names) > 1 else message
        out_of_range_count = int(out_of_range_counts.get(out_of_range_count_aliases[col_name], 0) or 0)

        _record_dq_result(
            violation_count=out_of_range_count,
            dq_category=data_quality_category,
            dq_message=col_message,
            if_not_compliant=dq_result_if_not_compliant,
            warnings=warnings,
            force_failures=force_failures,
            pass_message=col_message,
            column_name=col_name
        )

    return warnings, force_failures


def _process_range_column_mark_or_quarantine(
    df: DataFrame,
    df_quarantined_all: DataFrame,
    dq_markers: DataFrame,
    col_name: str,
    message: str,
    total_columns: int,
    min_value: Any,
    max_value: Any,
    dq_result_if_not_compliant: str
) -> tuple:
    """Process row-level range handling for a single column (mark/quarantine)."""
    col_message = f"{message} ({col_name})" if total_columns > 1 else message
    out_of_range_condition = _build_out_of_range_condition(
        col_name=col_name,
        min_value=min_value,
        max_value=max_value
    )

    df_out_of_range = df.filter(out_of_range_condition)
    reuse_violations_df = _is_mark_or_quarantine_action(dq_result_if_not_compliant)
    if reuse_violations_df:
        df_out_of_range = df_out_of_range.persist(StorageLevel.MEMORY_AND_DISK)

    try:
        out_of_range_count = df_out_of_range.count()

        if out_of_range_count > 0 and dq_result_if_not_compliant == "quarantine":
            quarantine_message = col_message
            df_quarantined = df_out_of_range.withColumn(QUARANTINE_REASON_COLUMN, f.lit(quarantine_message))
            df_quarantined_all = df_quarantined_all.unionByName(df_quarantined, allowMissingColumns=True)
            df = df.filter(~out_of_range_condition)

        if out_of_range_count > 0 and dq_result_if_not_compliant == 'mark':
            dq_markers = _append_marker_df(
                dq_markers,
                _build_dq_marker_df(
                    df=df,
                    df_violations=df_out_of_range,
                    detail_message=col_message,
                    join_keys="delta__idx"
                )
            )
    finally:
        if reuse_violations_df:
            df_out_of_range.unpersist()

    return df, df_quarantined_all, dq_markers, out_of_range_count, col_message


def _validate_range(
    df: DataFrame,
    df_quarantined_all: DataFrame,
    data_quality_step: dict,
    warnings: list,
    force_failures: list
) -> tuple:
    """Validate that numeric or date column values fall within a specified range.
    
    This function checks if values in the specified column(s) are within the 
    configured minimum and/or maximum bounds. Each column generates its own
    separate warning/failure message for granular tracking.
    
    Args:
        df (DataFrame): Input DataFrame to validate
        df_quarantined_all (DataFrame): Accumulated quarantined records
        data_quality_step (dict): Configuration containing:
            - column_name: Column to validate (supports comma-separated for multiple)
            - min_value: Optional minimum allowed value (inclusive)
            - max_value: Optional maximum allowed value (inclusive)
            - if_not_compliant: Action when out of range ('warn', 'quarantine', 'fail')
        warnings (list): Accumulated warning messages
        force_failures (list): Accumulated failure messages
    
    Returns:
        tuple: (cleaned_df, quarantined_df, warnings, force_failures)
    """
    data_quality_category = data_quality_step["Category"]
    dq_result_if_not_compliant = data_quality_step.get('if_not_compliant', 'warn').lower()
    column_names_raw = data_quality_step['column_name']
    min_value = data_quality_step.get('min_value')
    max_value = data_quality_step.get('max_value')
    message = data_quality_step['message']
    
    # Parse comma-separated column names
    column_names = [col.strip() for col in column_names_raw.split(',')]
    dq_markers = _empty_dq_marker_df(df)
    
    log_and_print(f"Validating range on columns: {column_names} (min: {min_value}, max: {max_value})")
    
    if min_value is None and max_value is None:
        log_and_print("No min_value or max_value specified, skipping range validation", "warn")
        return df, df_quarantined_all, warnings, force_failures, dq_markers

    # For warn/fail, compute out-of-range counts for all columns in one pass because
    # row-level mutation (mark/quarantine) is not required.
    if dq_result_if_not_compliant in ('warn', 'fail'):
        out_of_range_count_aliases, out_of_range_count_exprs = _build_range_count_aggregates(
            column_names=column_names,
            min_value=min_value,
            max_value=max_value
        )

        out_of_range_counts = df.agg(*out_of_range_count_exprs).first().asDict()
        warnings, force_failures = _record_range_warn_fail_results(
            out_of_range_counts=out_of_range_counts,
            out_of_range_count_aliases=out_of_range_count_aliases,
            column_names=column_names,
            message=message,
            data_quality_category=data_quality_category,
            dq_result_if_not_compliant=dq_result_if_not_compliant,
            warnings=warnings,
            force_failures=force_failures
        )

        return df, df_quarantined_all, warnings, force_failures, dq_markers
    
    # Process each column separately for granular tracking
    for col_name in column_names:
        df, df_quarantined_all, dq_markers, out_of_range_count, col_message = _process_range_column_mark_or_quarantine(
            df=df,
            df_quarantined_all=df_quarantined_all,
            dq_markers=dq_markers,
            col_name=col_name,
            message=message,
            total_columns=len(column_names),
            min_value=min_value,
            max_value=max_value,
            dq_result_if_not_compliant=dq_result_if_not_compliant
        )

        dq_message = col_message
        pass_message = col_message
        
        # Record the DQ result
        _record_dq_result(
            violation_count=out_of_range_count,
            dq_category=data_quality_category,
            dq_message=dq_message,
            if_not_compliant=dq_result_if_not_compliant,
            warnings=warnings,
            force_failures=force_failures,
            pass_message=pass_message,
            column_name=col_name
        )
    
    return df, df_quarantined_all, warnings, force_failures, dq_markers


def _current_utc_datetime() -> datetime:
    """Return the current UTC timestamp for freshness checks."""
    return datetime.now(timezone.utc)


def _normalize_collected_timestamp_to_utc(value) -> datetime:
    """Normalize Spark-collected timestamp/date values to timezone-aware UTC datetimes."""
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)

    return datetime.combine(value, datetime.min.time(), tzinfo=timezone.utc)


def _validate_freshness(
    df: DataFrame,
    df_quarantined_all: DataFrame,
    data_quality_step: dict,
    warnings: list,
    force_failures: list
) -> tuple:
    """Validate that data is fresh (within expected time threshold).
    
    This function checks if the maximum timestamp in a date/datetime column
    is within the configured freshness threshold from current time.
    
    Args:
        df (DataFrame): Input DataFrame to validate
        df_quarantined_all (DataFrame): Accumulated quarantined records
        data_quality_step (dict): Configuration containing:
            - column_name: Timestamp/date column to check for freshness
            - max_age: Maximum allowed age value (numeric)
            - unit_of_measure: Unit for max_age - 'minute', 'hour', 'day', 'week', 'month' (default: 'hour')
            - if_not_compliant: Action when data is stale ('warn', 'fail')
        warnings (list): Accumulated warning messages
        force_failures (list): Accumulated failure messages
    
    Returns:
        tuple: (cleaned_df, quarantined_df, warnings, force_failures, dq_markers)
    
    Note:
        This check validates the overall dataset freshness, not individual records.
        It's useful for alerting when data pipelines haven't run recently.
        Row-level actions (mark, quarantine) are not applicable; the validator
        blocks them, but this function falls back to 'warn' defensively.
    """
    data_quality_category = data_quality_step["Category"]
    dq_result_if_not_compliant = data_quality_step.get('if_not_compliant', 'warn').lower()
    dq_markers = _empty_dq_marker_df(df)
    column_name = data_quality_step['column_name'].strip()
    max_age = data_quality_step.get('max_age')
    unit_of_measure = data_quality_step.get('unit_of_measure', 'hour').lower()
    message = data_quality_step['message']
    
    log_and_print(f"Validating data freshness on column: {column_name}")
    
    # Calculate threshold based on unit_of_measure
    unit_to_seconds = {
        'minute': 60,
        'hour': 60 * 60,
        'day': 24 * 60 * 60,
        'week': 7 * 24 * 60 * 60,
        'month': 30 * 24 * 60 * 60  # Approximate 30 days
    }
    
    if max_age is not None:
        if unit_of_measure not in unit_to_seconds:
            raise ValueError(f"Invalid unit_of_measure '{unit_of_measure}'. Must be one of: minute, hour, day, week, month")
        max_age_seconds = float(max_age) * unit_to_seconds[unit_of_measure]
    else:
        max_age_seconds = 24 * 60 * 60  # Default 24 hours
    
    # Get the maximum timestamp from the data
    max_timestamp_row = df.agg(f.max(f.col(column_name)).alias("max_ts")).collect()[0]
    max_timestamp = max_timestamp_row["max_ts"]
    
    if max_timestamp is None:
        dq_message = f"No timestamp values found in column '{column_name}' - cannot validate freshness"
        _record_dq_result(
            violation_count=0,
            dq_category=data_quality_category,
            dq_message=dq_message,
            if_not_compliant='warn',
            warnings=warnings,
            force_failures=force_failures
        )
        return df, df_quarantined_all, warnings, force_failures, dq_markers
    
    # Spark returns collected TimestampType values as naive UTC datetimes.
    current_time = _current_utc_datetime()
    max_datetime = _normalize_collected_timestamp_to_utc(max_timestamp)

    data_age = current_time - max_datetime
    age_seconds = data_age.total_seconds()
    
    is_stale = age_seconds > max_age_seconds
    
    if is_stale:
        # Build descriptive message; include 'batch-level' note when falling back from mark/quarantine to warn
        if dq_result_if_not_compliant in ('quarantine', 'mark'):
            dq_message = f"{message} (batch-level metric; falling back to warn)"
            effective_action = 'warn'
        else:
            dq_message = message
            effective_action = dq_result_if_not_compliant
        
        _record_dq_result(
            violation_count=1,  # Freshness is a batch-level metric, use 1 to indicate violation
            dq_category=data_quality_category,
            dq_message=dq_message,
            if_not_compliant=effective_action,
            warnings=warnings,
            force_failures=force_failures
        )
    else:
        pass_message = message
        _record_dq_result(
            violation_count=0,
            dq_category=data_quality_category,
            dq_message=pass_message,
            if_not_compliant=dq_result_if_not_compliant,
            warnings=warnings,
            force_failures=force_failures
        )
    
    return df, df_quarantined_all, warnings, force_failures, dq_markers


def _validate_completeness(
    df: DataFrame,
    df_quarantined_all: DataFrame,
    data_quality_step: dict,
    warnings: list,
    force_failures: list
) -> tuple:
    """Validate that columns meet minimum completeness (non-null percentage) thresholds.
    
    This function calculates the percentage of non-null values in specified columns
    and validates against configured thresholds. Each column generates its own
    separate warning/failure message for granular tracking.
    
    Args:
        df (DataFrame): Input DataFrame to validate
        df_quarantined_all (DataFrame): Accumulated quarantined records
        data_quality_step (dict): Configuration containing:
            - column_name: Column(s) to check completeness (comma-separated for multiple)
            - min_completeness_percent: Minimum required non-null percentage (0-100, default: 95)
            - if_not_compliant: Action when below threshold ('warn', 'fail')
        warnings (list): Accumulated warning messages
        force_failures (list): Accumulated failure messages
    
    Returns:
        tuple: (cleaned_df, quarantined_df, warnings, force_failures, dq_markers)
    
    Note:
        This check validates overall column completeness, not individual records.
        Useful for monitoring data quality trends and SLA compliance.
        Row-level actions (mark, quarantine) are not applicable; the validator
        blocks them, but this function falls back to 'warn' defensively.
    """
    data_quality_category = data_quality_step["Category"]
    dq_result_if_not_compliant = data_quality_step.get('if_not_compliant', 'warn').lower()
    dq_markers = _empty_dq_marker_df(df)
    column_names_raw = data_quality_step['column_name']
    min_completeness = float(data_quality_step.get('min_completeness_percent', 95))
    message = data_quality_step['message']
    
    # Parse comma-separated column names
    column_names = [col.strip() for col in column_names_raw.split(',')]
    
    log_and_print(f"Validating completeness on columns: {column_names} (min: {min_completeness}%)")
    
    total_count_alias = "__total_count"
    non_null_count_aliases, non_null_count_exprs = _build_nullability_count_aggregates(
        column_names=column_names,
        count_nulls=False,
        alias_prefix="__non_null_count_"
    )

    # Compute total row count and all non-null counts in one aggregation pass.
    aggregate_counts = df.agg(
        f.count(f.lit(1)).cast("long").alias(total_count_alias),
        *non_null_count_exprs
    ).first().asDict()

    total_count = int(aggregate_counts.get(total_count_alias, 0) or 0)
    if total_count == 0:
        _record_dq_result(
            violation_count=0,
            dq_category=data_quality_category,
            dq_message="No records to validate completeness",
            if_not_compliant='warn',
            warnings=warnings,
            force_failures=force_failures
        )
        return df, df_quarantined_all, warnings, force_failures, dq_markers

    non_null_counts = aggregate_counts
    
    # Process each column separately for granular tracking
    for col_name in column_names:
        # When checking multiple columns, suffix the message with the column name for clarity
        col_message = f"{message} ({col_name})" if len(column_names) > 1 else message

        non_null_count = int(non_null_counts.get(non_null_count_aliases[col_name], 0) or 0)
        completeness_pct = (non_null_count / total_count) * 100
        
        if completeness_pct < min_completeness:
            # Build descriptive message including actual/threshold completeness percentages
            # Include 'batch-level' note when falling back from mark/quarantine to warn
            if dq_result_if_not_compliant in ('quarantine', 'mark'):
                dq_message = f"{col_message} (batch-level metric; falling back to warn) - completeness {completeness_pct:.0f}% below threshold {min_completeness:.0f}%"
                effective_action = 'warn'
            else:
                dq_message = f"{col_message} - completeness {completeness_pct:.0f}% below threshold {min_completeness:.0f}%"
                effective_action = dq_result_if_not_compliant
            
            _record_dq_result(
                violation_count=1,  # Completeness is a batch-level metric, use 1 to indicate violation
                dq_category=data_quality_category,
                dq_message=dq_message,
                if_not_compliant=effective_action,
                warnings=warnings,
                force_failures=force_failures,
                column_name=col_name
            )
        else:
            pass_message = col_message
            _record_dq_result(
                violation_count=0,
                dq_category=data_quality_category,
                dq_message=pass_message,
                if_not_compliant=dq_result_if_not_compliant,
                warnings=warnings,
                force_failures=force_failures,
                column_name=col_name
            )
    
    return df, df_quarantined_all, warnings, force_failures, dq_markers


def _load_anomaly_baseline_data(
    full_baseline_table_name: str,
    column_name: str,
    minimum_baseline_records: int,
    data_quality_category: str,
    warnings: list
) -> tuple:
    """Load baseline data and validate it has sufficient records.
    
    Args:
        full_baseline_table_name (str): Fully qualified baseline table name
        column_name (str): Column to load for statistics calculation
        minimum_baseline_records (int): Minimum records required
        data_quality_category (str): Category for logging
        warnings (list): Accumulated warnings
    
    Returns:
        tuple: (baseline_df, should_skip, warnings) where should_skip is True if detection should be skipped
    """
    try:
        baseline_df = spark.sql(f"SELECT `{column_name}` FROM {full_baseline_table_name} WHERE `{column_name}` IS NOT NULL")
        baseline_count = baseline_df.count()
    except Exception as e:
        skip_message = f"Skipping anomaly detection: baseline table not accessible or column '{column_name}' not found. Error: {str(e)[:100]}"
        log_and_print(skip_message, "warn")
        json_to_append = {
            "outcome": "Skipped",
            "category": _get_dq_category_display_name(data_quality_category),
            "message": skip_message
        }
        warnings.append(json_to_append)
        return None, True, warnings
    
    if baseline_count < minimum_baseline_records:
        skip_message = f"Skipping anomaly detection: baseline table has {baseline_count} records, minimum required is {minimum_baseline_records}."
        log_and_print(skip_message, "warn")
        json_to_append = {
            "outcome": "Skipped",
            "category": _get_dq_category_display_name(data_quality_category),
            "message": skip_message
        }
        warnings.append(json_to_append)
        return None, True, warnings
    
    return baseline_df, False, warnings


def _calculate_zscore_statistics(
    baseline_df: DataFrame,
    column_name: str,
    data_quality_category: str,
    warnings: list
) -> tuple:
    """Calculate mean and standard deviation for z-score anomaly detection.
    
    Args:
        baseline_df (DataFrame): Baseline data for statistics calculation
        column_name (str): Column to calculate statistics for
        data_quality_category (str): Category for logging
        warnings (list): Accumulated warnings
    
    Returns:
        tuple: (mean_val, stddev_val, should_skip, warnings)
    """
    stats = baseline_df.agg(
        f.mean(column_name).alias("mean_val"),
        f.stddev(column_name).alias("stddev_val")
    ).first()
    
    mean_val = stats["mean_val"]
    stddev_val = stats["stddev_val"]
    
    if stddev_val is None or stddev_val == 0:
        skip_message = f"Skipping anomaly detection: standard deviation is zero or null for column '{column_name}'."
        log_and_print(skip_message, "warn")
        json_to_append = {
            "outcome": "Skipped",
            "category": _get_dq_category_display_name(data_quality_category),
            "message": skip_message
        }
        warnings.append(json_to_append)
        return None, None, True, warnings
    
    stats_log_info = f"Baseline statistics: mean={mean_val:.4f}, stddev={stddev_val:.4f}"
    log_and_print(stats_log_info)
    
    return mean_val, stddev_val, False, warnings


def _calculate_iqr_statistics(
    baseline_df: DataFrame,
    column_name: str,
    threshold: float
) -> tuple:
    """Calculate IQR bounds for anomaly detection.
    
    Args:
        baseline_df (DataFrame): Baseline data for statistics calculation
        column_name (str): Column to calculate statistics for
        threshold (float): IQR multiplier for bounds
    
    Returns:
        tuple: (lower_bound, upper_bound, q1, q3, iqr)
    """
    quantiles = baseline_df.approxQuantile(column_name, [0.25, 0.75], 0.01)
    q1 = quantiles[0]
    q3 = quantiles[1]
    iqr = q3 - q1
    
    lower_bound = q1 - threshold * iqr
    upper_bound = q3 + threshold * iqr
    
    stats_log_info = f"Baseline statistics: Q1={q1:.4f}, Q3={q3:.4f}, IQR={iqr:.4f}, bounds=[{lower_bound:.4f}, {upper_bound:.4f}]"
    log_and_print(stats_log_info)
    
    return lower_bound, upper_bound, q1, q3, iqr


def _find_zscore_anomalies(
    df: DataFrame,
    column_name: str,
    mean_val: float,
    stddev_val: float,
    threshold: float
) -> tuple:
    """Find anomalies using z-score method.
    
    Args:
        df (DataFrame): Input data to check
        column_name (str): Column to analyze
        mean_val (float): Mean from baseline
        stddev_val (float): Standard deviation from baseline
        threshold (float): Z-score threshold
    
    Returns:
        DataFrame: Anomalies DataFrame containing rows that exceed the z-score threshold
    """
    anomalies = df.filter(
        (f.col(column_name).isNotNull()) &
        (f.abs(f.col(column_name) - mean_val) / stddev_val > threshold)
    )
    
    return anomalies


def _find_iqr_anomalies(
    df: DataFrame,
    column_name: str,
    lower_bound: float,
    upper_bound: float
) -> tuple:
    """Find anomalies using IQR method.
    
    Args:
        df (DataFrame): Input data to check
        column_name (str): Column to analyze
        lower_bound (float): Lower IQR bound
        upper_bound (float): Upper IQR bound
    
    Returns:
        DataFrame: Anomalies DataFrame containing rows outside the IQR bounds
    """
    anomalies = df.filter(
        (f.col(column_name).isNotNull()) &
        ((f.col(column_name) < lower_bound) | (f.col(column_name) > upper_bound))
    )
    
    return anomalies


def _handle_anomaly_results(
    df: DataFrame,
    df_quarantined_all: DataFrame,
    anomalies: DataFrame,
    column_name: str,
    data_quality_category: str,
    dq_result_if_not_compliant: str,
    message: str,
    warnings: list,
    force_failures: list
) -> tuple:
    """Handle anomaly detection results based on compliance action.
    
    This function handles quarantine logic (using delta__idx join) and delegates
    result recording to _record_dq_result.
    
    Args:
        df (DataFrame): Input data
        df_quarantined_all (DataFrame): Accumulated quarantined records
        anomalies (DataFrame): Detected anomalies
        column_name (str): Column that was analyzed
        data_quality_category (str): Category for logging
        dq_result_if_not_compliant (str): Action to take ('warn', 'quarantine', 'fail')
        warnings (list): Accumulated warnings
        force_failures (list): Accumulated failures
    
    Returns:
        tuple: (df, df_quarantined_all, warnings, force_failures)
    """
    # Persist only when anomalies are reused by mark/quarantine logic after count().
    reuse_violations_df = _is_mark_or_quarantine_action(dq_result_if_not_compliant)
    if reuse_violations_df:
        anomalies = anomalies.persist(StorageLevel.MEMORY_AND_DISK)

    try:
        number_of_anomalies = anomalies.count()
        # Build descriptive message including 'anomal' context when violations found
        dq_message = f"{message} - {number_of_anomalies} anomalous value(s) detected" if number_of_anomalies > 0 else message
        dq_markers = _empty_dq_marker_df(df)

        if number_of_anomalies > 0 and dq_result_if_not_compliant == 'mark':
            dq_markers = _build_dq_marker_df(
                df=df,
                df_violations=anomalies,
                detail_message=message,
                join_keys="delta__idx"
            )
        
        # Handle quarantine if needed (must happen before recording result)
        if number_of_anomalies > 0 and dq_result_if_not_compliant == 'quarantine':
            quarantine_message = message
            df, df_quarantined_all = _quarantine_rows(
                df=df,
                df_quarantined_all=df_quarantined_all,
                df_violations=anomalies,
                quarantine_reason=quarantine_message,
                join_keys='delta__idx'
            )
    finally:
        if reuse_violations_df:
            # Keep lifecycle local to this function to avoid unnecessary cache pressure.
            anomalies.unpersist()
    
    # Record the DQ result
    _record_dq_result(
        violation_count=number_of_anomalies,
        dq_category=data_quality_category,
        dq_message=dq_message,
        if_not_compliant=dq_result_if_not_compliant,
        warnings=warnings,
        force_failures=force_failures,
        column_name=column_name
    )
    
    return df, df_quarantined_all, warnings, force_failures, dq_markers


def _validate_anomaly_single_column(
    df: DataFrame,
    df_quarantined_all: DataFrame,
    data_quality_step: dict,
    column_name: str,
    full_baseline_table_name: str,
    warnings: list,
    force_failures: list
) -> tuple:
    """Process anomaly detection for a single column.
    
    Args:
        df (DataFrame): Input data to check for anomalies
        df_quarantined_all (DataFrame): Accumulated quarantined records
        data_quality_step (dict): Configuration containing method, threshold, if_not_compliant, etc.
        column_name (str): Single column name to analyze
        full_baseline_table_name (str): Fully qualified baseline table name
        warnings (list): Accumulated warnings
        force_failures (list): Accumulated failures
    
    Returns:
        tuple: (df, df_quarantined_all, warnings, force_failures)
    """
    # Parse configuration from data_quality_step
    data_quality_category = data_quality_step.get("Category", "validate_anomaly")
    dq_result_if_not_compliant = data_quality_step.get('if_not_compliant', 'warn').lower()
    method = data_quality_step.get('method', 'z_score').lower()
    threshold = float(data_quality_step.get('threshold', '3' if method == 'z_score' else '1.5'))
    minimum_baseline_records = int(data_quality_step.get('minimum_baseline_records', '30'))
    message = data_quality_step['message']
    
    detecting_anomaly_log_info = f"Detecting anomalies in column '{column_name}' using {method} method (threshold: {threshold})."
    log_and_print(detecting_anomaly_log_info)
    
    # Load baseline data for this column
    baseline_df, should_skip, warnings = _load_anomaly_baseline_data(
        full_baseline_table_name=full_baseline_table_name,
        column_name=column_name,
        minimum_baseline_records=minimum_baseline_records,
        data_quality_category=data_quality_category,
        warnings=warnings
    )
    
    if should_skip:
        return df, df_quarantined_all, warnings, force_failures, _empty_dq_marker_df(df)
    
    # Calculate statistics and find anomalies based on method
    if method == 'z_score':
        mean_val, stddev_val, should_skip, warnings = _calculate_zscore_statistics(
            baseline_df=baseline_df,
            column_name=column_name,
            data_quality_category=data_quality_category,
            warnings=warnings
        )
        
        if should_skip:
            return df, df_quarantined_all, warnings, force_failures, _empty_dq_marker_df(df)
        
        anomalies = _find_zscore_anomalies(
            df=df,
            column_name=column_name,
            mean_val=mean_val,
            stddev_val=stddev_val,
            threshold=threshold
        )
        
    elif method == 'iqr':
        lower_bound, upper_bound, q1, q3, iqr = _calculate_iqr_statistics(
            baseline_df=baseline_df,
            column_name=column_name,
            threshold=threshold
        )
        
        anomalies = _find_iqr_anomalies(
            df=df,
            column_name=column_name,
            lower_bound=lower_bound,
            upper_bound=upper_bound
        )
        
    else:
        raise ValueError(f"Unknown anomaly detection method '{method}'. Supported methods: 'z_score', 'iqr'")
    
    # Handle results
    return _handle_anomaly_results(
        df=df,
        df_quarantined_all=df_quarantined_all,
        anomalies=anomalies,
        column_name=column_name,
        data_quality_category=data_quality_category,
        dq_result_if_not_compliant=dq_result_if_not_compliant,
        message=message,
        warnings=warnings,
        force_failures=force_failures
    )


def _validate_anomaly(
    df: DataFrame,
    df_quarantined_all: DataFrame,
    data_quality_step: dict,
    target_table_name: str,
    warnings: list,
    force_failures: list
) -> tuple:
    """Detect statistical anomalies in numeric columns using z-score or IQR methods.
    
    This function identifies outliers by comparing incoming data against the target table's
    historical statistical distribution. It supports two detection methods:
    - Z-Score: Flags values more than N standard deviations from the mean
    - IQR (Interquartile Range): Flags values outside Q1 - k*IQR or Q3 + k*IQR
    
    The target table provides baseline data for calculating statistics. If the target table
    doesn't exist (first load) or has insufficient records, anomaly detection is gracefully skipped.
    
    Supports multiple columns via comma-separated values in column_name.
    
    Args:
        df (DataFrame): Input data to check for anomalies
        df_quarantined_all (DataFrame): Accumulated quarantined records
        data_quality_step (dict): Configuration with:
            - column_name: Numeric column(s) to analyze (comma-separated for multiple)
            - method: 'z_score' or 'iqr'
            - threshold: Z-score threshold (default 3) or IQR multiplier (default 1.5)
            - if_not_compliant: 'warn', 'quarantine', or 'fail'
            - minimum_baseline_records: Minimum records needed (default 30)
        target_table_name (str): Fully qualified target table name (`workspace`.lakehouse.schema.table)
            used as baseline for statistics
        warnings (list): Accumulated warnings
        force_failures (list): Accumulated failures
    
    Returns:
        tuple: (df, df_quarantined_all, warnings, force_failures)
    
    Detection Methods:
        - z_score: |value - mean| / stddev > threshold → anomaly
        - iqr: value < Q1 - threshold*IQR OR value > Q3 + threshold*IQR → anomaly
    """
    # Extract only the configuration needed at this level
    column_names_raw = data_quality_step['column_name']
    method = data_quality_step.get('method', 'z_score').lower()
    threshold = float(data_quality_step.get('threshold', '3' if method == 'z_score' else '1.5'))
    message = data_quality_step['message']
    
    # Parse comma-separated column names
    column_names = [col.strip() for col in column_names_raw.split(',') if col.strip()]
    
    detecting_anomaly_log_info = f"Detecting anomalies in column(s) '{column_names}' using {method} method (threshold: {threshold}). Baseline: {target_table_name}"
    log_and_print(detecting_anomaly_log_info)
    
    # Validate all columns exist in incoming data
    _validate_columns_exist(
        df=df,
        columns_to_check=column_names,
        operation_name="validate_anomaly"
    )
    
    # target_table_name is already fully qualified (`workspace`.lakehouse.schema.table)
    full_baseline_table_name = target_table_name
    
    # Process each column
    dq_markers = _empty_dq_marker_df(df)
    for column_name in column_names:
        # When checking multiple columns, suffix the message with the column name for clarity
        col_message = f"{message} ({column_name})" if len(column_names) > 1 else message
        col_step = {**data_quality_step, 'message': col_message}

        column_result = _validate_anomaly_single_column(
            df=df,
            df_quarantined_all=df_quarantined_all,
            data_quality_step=col_step,
            column_name=column_name,
            full_baseline_table_name=full_baseline_table_name,
            warnings=warnings,
            force_failures=force_failures
        )
        df, df_quarantined_all, warnings, force_failures, col_markers = column_result
        dq_markers = _append_marker_df(dq_markers, col_markers)
    
    return df, df_quarantined_all, warnings, force_failures, dq_markers


def _get_pattern_validation_regex(pattern_type: str) -> str:
    """
    Get regex pattern for common data validation patterns.
    
    Args:
        pattern_type (str): The type of pattern to validate against
    
    Returns:
        str: Regex pattern string for the specified pattern type
    
    Raises:
        ValueError: If pattern_type is not recognized
    
    Notes:
        - All patterns use ^ and $ anchors for full string matching
        - ip_address pattern accepts both IPv4 and IPv6 formats
        - Date patterns validate format but not actual date validity (e.g., allows 02/31/2024)
        - Credit card pattern validates structure only, not actual card validity
        - Email pattern is permissive and follows common format rules
    """
    patterns = {
        # US Phone: Supports (123) 456-7890, 123-456-7890, 123.456.7890, or 1234567890
        'us_phone': r'^\(?([0-9]{3})\)?[-. ]?([0-9]{3})[-. ]?([0-9]{4})$',
        
        # Email: Standard email format (allows letters, numbers, and common special chars)
        'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
        
        # US ZIP: 5 digits or 5+4 format (12345 or 12345-6789)
        'us_zip': r'^\d{5}(-\d{4})?$',
        
        # US SSN: Format XXX-XX-XXXX (requires hyphens)
        'us_ssn': r'^\d{3}-\d{2}-\d{4}$',
        
        # Date MM/DD/YYYY: Validates format only, not actual date validity
        'date_mmddyyyy': r'^(0[1-9]|1[0-2])\/(0[1-9]|[12][0-9]|3[01])\/\d{4}$',
        
        # Date YYYY-MM-DD: Validates format only, not actual date validity
        'date_yyyymmdd': r'^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$',
        
        # URL: HTTP/HTTPS URLs with optional www prefix
        'url': r'^https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()@:%_\+.~#?&/=]*)$',
        
        # Credit Card: 16 digits with optional hyphens or spaces between groups of 4
        'credit_card': r'^\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}$',
        
        # IP Address: Accepts both IPv4 and IPv6 addresses
        # IPv4: 192.168.1.1 (validates octets 0-255)
        # IPv6: 2001:db8::8a2e:370:7334 (full and compressed formats)
        'ip_address': r'^(((25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])|(([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])))$',
        
        # IPv4 Address Only: Validates each octet is 0-255
        'ipv4_address': r'^((25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])$',
        
        # IPv6 Address Only: Full and compressed formats
        'ipv6_address': r'^(([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))$',
        
        # Alpha Only: Only alphabetic characters (uppercase and lowercase)
        'alpha_only': r'^[A-Za-z]+$',
        
        # Alphanumeric Only: Letters and numbers only
        'alphanumeric_only': r'^[A-Za-z0-9]+$',
        
        # Numeric Only: Digits only (no decimals or negative signs)
        'numeric_only': r'^\d+$',
        
        # GUID/UUID: Standard format with hyphens (8-4-4-4-12)
        'guid': r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$',

        # UK Phone: Supports 01612949234, +447895891235 (including any spaces variations after the first 2 digits)
        'uk_phone': r'^(?:0|\+?44\s?)(?:\d\s?){9,10}$',

        # UK Postcode: 6 to 8 characters, with or without central space and case-insensitive (EC1A 1BB, SK12TN, m20 1hs)
        'uk_postcode': r'(?i)^[A-Z]{1,2}[0-9][A-Z0-9]?\s?[0-9][A-Z]{2}$',
    }
    
    if pattern_type not in patterns:
        available_patterns = ', '.join(patterns.keys())
        raise ValueError(f"Unknown pattern type '{pattern_type}'. Available patterns: {available_patterns}")
    
    return patterns[pattern_type]

def _validate_pattern_single_column(
    df: DataFrame,
    df_quarantined_all: DataFrame,
    column_name: str,
    pattern_type: str,
    regex_pattern: str,
    allow_null: bool,
    data_quality_category: str,
    dq_result_if_not_compliant: str,
    message: str,
    warnings: list,
    force_failures: list
) -> tuple:
    """Validate a single column against a pattern.
    
    Args:
        df (DataFrame): Input data to validate
        df_quarantined_all (DataFrame): Accumulated quarantined records
        column_name (str): Single column name to validate
        pattern_type (str): Type of pattern for logging
        regex_pattern (str): Regex pattern to match against
        allow_null (bool): Whether null values are allowed
        data_quality_category (str): Category for logging
        dq_result_if_not_compliant (str): Action to take
        warnings (list): Accumulated warnings
        force_failures (list): Accumulated failures
    
    Returns:
        tuple: (df, df_quarantined_all, warnings, force_failures)
    """
    validating_pattern_log_info = f"Validating column '{column_name}' matches pattern '{pattern_type}'. Allow null: {allow_null}."
    log_and_print(validating_pattern_log_info)
    
    # Find records that DON'T match the pattern
    # Convert column to string and check against regex
    if allow_null:
        # Records that fail: non-null values that don't match the pattern
        pattern_not_matches = df.filter(
            (f.col(column_name).isNotNull()) & 
            (~f.col(column_name).cast("string").rlike(regex_pattern))
        )
    else:
        # Records that fail: null values OR values that don't match the pattern
        pattern_not_matches = df.filter(
            (f.col(column_name).isNull()) | 
            (~f.col(column_name).cast("string").rlike(regex_pattern))
        )
    
    # Persist only for reuse-heavy paths (count + mark/quarantine against same DF).
    reuse_violations_df = _is_mark_or_quarantine_action(dq_result_if_not_compliant)
    if reuse_violations_df:
        pattern_not_matches = pattern_not_matches.persist(StorageLevel.MEMORY_AND_DISK)

    try:
        number_of_pattern_not_matches = pattern_not_matches.count()
        dq_message = message
        dq_markers = _empty_dq_marker_df(df)

        if number_of_pattern_not_matches > 0 and dq_result_if_not_compliant == 'mark':
            dq_markers = _build_dq_marker_df(
                df=df,
                df_violations=pattern_not_matches,
                detail_message=message,
                join_keys="delta__idx"
            )
        
        # Handle quarantine if needed (must happen before recording result)
        if number_of_pattern_not_matches > 0 and dq_result_if_not_compliant == 'quarantine':
            quarantine_message = message
            df, df_quarantined_all = _quarantine_rows(
                df=df,
                df_quarantined_all=df_quarantined_all,
                df_violations=pattern_not_matches,
                quarantine_reason=quarantine_message,
                join_keys='delta__idx'
            )
    finally:
        if reuse_violations_df:
            # Unpersist defensively in finally so cleanup happens even on exceptions.
            pattern_not_matches.unpersist()
    
    # Record the DQ result
    _record_dq_result(
        violation_count=number_of_pattern_not_matches,
        dq_category=data_quality_category,
        dq_message=dq_message,
        if_not_compliant=dq_result_if_not_compliant,
        warnings=warnings,
        force_failures=force_failures,
        column_name=column_name
    )
    
    return df, df_quarantined_all, warnings, force_failures, dq_markers


def _validate_pattern(
    df: DataFrame, 
    df_quarantined_all: DataFrame, 
    data_quality_step: dict, 
    warnings: list, 
    force_failures: list
) -> tuple:
    """Validate data using templated pattern matching or custom regex.
    
    This function provides prescriptive validation for common data patterns like
    phone numbers, emails, dates, etc. using predefined regex patterns.
    
    Supports multiple columns via comma-separated values in column_name.
    
    Args:
        df (DataFrame): Input data to validate
        df_quarantined_all (DataFrame): Accumulated quarantined records
        data_quality_step (dict): Configuration with:
            - column_name: Column(s) to validate (comma-separated for multiple)
            - pattern_type: A predefined pattern type (see supported patterns below)
            - if_not_compliant: Action to take ('warn', 'quarantine', 'fail')
            - allow_null: Whether null values are allowed (default: True)
        warnings (list): Accumulated warnings
        force_failures (list): Accumulated failures
    
    Returns:
        tuple: (df, df_quarantined_all, warnings, force_failures)
    
    Supported Pattern Types:
        - us_phone: US phone numbers (formats: 123-456-7890, (123) 456-7890, 1234567890)
        - uk_phone: UK phone numbers (formats: 0161 2949234, +447895891235, 07896789812)
        - email: Email addresses
        - us_zip: US ZIP codes (5 or 9 digit)
        - uk_postcode: UK Postcodes (formats: EC1A 1BB, M201HS)
        - us_ssn: US Social Security Numbers (XXX-XX-XXXX)
        - date_mmddyyyy: Dates in MM/DD/YYYY format
        - date_yyyymmdd: Dates in YYYY-MM-DD format
        - url: HTTP/HTTPS URLs
        - credit_card: Credit card numbers (with or without spaces/hyphens)
        - ip_address: IPv4 or IPv6 addresses (supports both formats)
        - ipv4_address: IPv4 addresses only (e.g., 192.168.1.1)
        - ipv6_address: IPv6 addresses only (e.g., 2001:db8::8a2e:370:7334)
        - alpha_only: Alphabetic characters only
        - alphanumeric_only: Alphanumeric characters only
        - numeric_only: Numeric characters only
        - guid: GUIDs/UUIDs
    """
    data_quality_category = data_quality_step.get("Category", "validate_pattern")
    dq_result_if_not_compliant = data_quality_step.get('if_not_compliant', 'warn').lower()
    column_names_raw = data_quality_step['column_name']
    pattern_type = data_quality_step['pattern_type']
    allow_null = data_quality_step.get('allow_null', True)
    message = data_quality_step['message']
    
    # Parse comma-separated column names
    column_names = [col.strip() for col in column_names_raw.split(',') if col.strip()]
    
    validating_pattern_log_info = f"Validating column(s) '{column_names}' match pattern '{pattern_type}'. Allow null: {allow_null}."
    log_and_print(validating_pattern_log_info)
    
    # Validate all columns exist
    _validate_columns_exist(
        df=df,
        columns_to_check=column_names,
        operation_name="pattern_match_validation"
    )
    
    # Get the regex pattern from predefined patterns
    regex_pattern = _get_pattern_validation_regex(pattern_type)
    
    # Process each column
    dq_markers = _empty_dq_marker_df(df)
    for column_name in column_names:
        # When checking multiple columns, suffix the message with the column name for clarity
        col_message = f"{message} ({column_name})" if len(column_names) > 1 else message

        column_result = _validate_pattern_single_column(
            df=df,
            df_quarantined_all=df_quarantined_all,
            column_name=column_name,
            pattern_type=pattern_type,
            regex_pattern=regex_pattern,
            allow_null=allow_null,
            data_quality_category=data_quality_category,
            dq_result_if_not_compliant=dq_result_if_not_compliant,
            message=col_message,
            warnings=warnings,
            force_failures=force_failures
        )
        df, df_quarantined_all, warnings, force_failures, col_markers = column_result
        dq_markers = _append_marker_df(dq_markers, col_markers)
    
    return df, df_quarantined_all, warnings, force_failures, dq_markers


# ---------------------------------------------------------------------------
# DQ dispatch infrastructure (module-level for reuse and testability)
# Defined after all _validate_* functions so they are in scope.
# ---------------------------------------------------------------------------

# Maps DQ category name → validation function.
_DQ_DISPATCH = {
    "validate_condition":             _validate_condition,
    "validate_pattern":               _validate_pattern,
    "validate_batch_size":            _validate_batch_size,
    "validate_referential_integrity": _validate_referential_integrity,
    "validate_anomaly":               _validate_anomaly,
    "validate_not_null":              _validate_not_null,
    "validate_unique":                _validate_unique,
    "validate_range":                 _validate_range,
    "validate_freshness":             _validate_freshness,
    "validate_completeness":          _validate_completeness,
}


def _build_dq_extra_args(
    datastore_config: str | list,
    target_table_name: str
) -> dict:
    """Build per-category extra keyword args for DQ dispatch.
    
    Most DQ checks share a uniform signature:
        (df, df_quarantined_all, data_quality_step, warnings, force_failures)
    
    But some checks need additional context:
      - validate_referential_integrity needs datastore_config to resolve lookup tables
      - validate_anomaly needs target_table_name (already fully qualified) as the baseline
    
    This function pre-builds a dict of {category: extra_kwargs} so that
    _dispatch_dq_check can merge the right extras without knowing which
    categories need what. If a category isn't in this dict, it gets no extras.

    Args:
        datastore_config: Datastore configuration from Datastore_Configuration table.
        target_table_name: Fully qualified target table name (`workspace`.lakehouse.schema.table).

    Returns:
        dict: Mapping of category → extra kwargs dict.
    """
    return {
        "validate_referential_integrity": {"datastore_config": datastore_config},
        "validate_anomaly":               {"target_table_name": target_table_name},
    }


def _dispatch_dq_check(
    data_quality_step: dict,
    check_df: DataFrame,
    check_quarantined: DataFrame,
    check_warnings: list,
    check_failures: list,
    extra_args: dict
) -> tuple:
    """Route a single DQ step to the appropriate validation function.

    Args:
        data_quality_step: Configuration dict for this quality check.
        check_df: DataFrame to validate.
        check_quarantined: Accumulated quarantined records.
        check_warnings: Accumulated warnings.
        check_failures: Accumulated failures.
        extra_args: Per-category extra kwargs (from _build_dq_extra_args).

    Returns:
        tuple: (df, quarantined_df, warnings, failures, dq_markers).

    Raises:
        Exception: If the check category is not recognised.
    """
    # Look up the category string (e.g. "validate_condition") from the metadata step dict
    category = data_quality_step["Category"]

    # Resolve the actual Python function from the _DQ_DISPATCH lookup table.
    # This table maps category names → functions and is defined at module level
    # after all _validate_* functions so they are in scope.
    fn = _DQ_DISPATCH.get(category)
    if fn is None:
        raise Exception(f"No logic currently exists to run the data quality check, {category}")

    # Build the keyword arguments dict dynamically.
    # All checks share these four base parameters:
    kwargs = {
        "df": check_df,                      # The DataFrame to validate
        "data_quality_step": data_quality_step,  # Full metadata config for this check
        "warnings": check_warnings,          # Accumulator list for warning outcomes
        "force_failures": check_failures,    # Accumulator list for fail outcomes
    }

    # validate_batch_size is the only check that doesn't accept df_quarantined_all
    # because it validates the overall batch count, not individual rows — it can't
    # quarantine specific rows.
    if category != "validate_batch_size":
        kwargs["df_quarantined_all"] = check_quarantined

    # Merge any category-specific extra args (e.g. datastore_config for referential integrity)
    kwargs.update(extra_args.get(category, {}))

    # Call the actual validation function with the assembled kwargs
    result = fn(**kwargs)
    check_markers = _empty_dq_marker_df(check_df)

    # Normalise the return value into a consistent 5-tuple.
    # validate_batch_size returns only (warnings, failures) because it can't
    # modify rows or produce markers. All other checks return the full 5-tuple:
    #   (df, quarantined_df, warnings, failures, markers)
    if category == "validate_batch_size":
        check_warnings, check_failures = result
    else:
        check_df, check_quarantined, check_warnings, check_failures, check_markers = result

    return check_df, check_quarantined, check_warnings, check_failures, check_markers


def _apply_deterministic_quarantine_candidates(
    df: DataFrame,
    df_quarantined_all: DataFrame,
    quarantine_candidates: DataFrame
) -> tuple:
    """Apply deterministic first-match quarantine from parallel-phase candidate rows.
    
    Why this exists:
    During the parallel DQ phase, all checks run against the SAME snapshot of df.
    That means multiple checks can independently decide to quarantine the same row.
    If we applied each check's quarantine eagerly, results would depend on thread
    execution order — which is non-deterministic.
    
    Instead, each parallel check only *proposes* quarantine candidates and tags them
    with a __dq_step_order (the check's position in the metadata-defined order).
        THIS function then resolves conflicts deterministically:
            - For each row (delta__idx), the LOWEST __dq_step_order defines precedence.
            - All matching quarantine reasons are concatenated in metadata order.
            - The row is removed from the active DF and added to quarantine output.
    
    This guarantees the same quarantine reason regardless of thread scheduling.
    """
    # Guard: if no quarantine candidates were proposed (no quarantine-action checks, or
    # none had violations), there's nothing to apply.
    required_columns = {DQ_ROW_ID_COLUMN, DQ_STEP_ORDER_COLUMN, QUARANTINE_REASON_COLUMN}
    missing_columns = _missing_required_columns(quarantine_candidates, required_columns)
    if missing_columns:
        return df, df_quarantined_all

    # Window: partition by row ID, order by metadata-defined step position (ascending).
    step_precedence_window = Window.partitionBy(DQ_ROW_ID_COLUMN).orderBy(f.col(DQ_STEP_ORDER_COLUMN).asc())

    # Full-frame window to collect ALL quarantine reasons for a row, ordered by step position.
    # This ensures every matching check's reason is preserved, not just the winner's.
    all_reasons_window = step_precedence_window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    selected_quarantine_rows = quarantine_candidates.withColumn(
        # Concatenate ALL quarantine reasons for this row (ordered by step position)
        "__all_reasons",
        f.concat_ws("; ", f.collect_list(QUARANTINE_REASON_COLUMN).over(all_reasons_window))
    ).withColumn(
        "__row_priority",
        f.row_number().over(step_precedence_window)
    ).filter(
        # Keep only the winning (first-match) proposal for each row's data columns
        f.col("__row_priority") == 1
    ).drop(
        # Replace single reason with concatenated all-reasons, clean up helper columns
        QUARANTINE_REASON_COLUMN
    ).withColumnRenamed(
        "__all_reasons", QUARANTINE_REASON_COLUMN
    ).drop(
        "__row_priority", DQ_STEP_ORDER_COLUMN
    )

    # Extract the set of row IDs that will be quarantined
    quarantined_row_ids = selected_quarantine_rows.select(DQ_ROW_ID_COLUMN).distinct()

    # Append the winning quarantine rows (with their quarantine_reason) to the output
    df_quarantined_all = df_quarantined_all.unionByName(
        selected_quarantine_rows,
        allowMissingColumns=True
    )

    # Remove quarantined rows from the active DF (left_anti = keep only non-matching rows)
    df = df.join(quarantined_row_ids, DQ_ROW_ID_COLUMN, "left_anti").select(df["*"])
    return df, df_quarantined_all


def _run_all_dq_checks_parallel(
    data_quality_steps: list,
    df: DataFrame,
    extra_args: dict
) -> tuple:
    """Execute all DQ checks in a single parallel pass on the same df snapshot.

    Each check is evaluated against the same immutable DataFrame snapshot. Quarantine
    candidates are accumulated with metadata-order precedence and can be applied later
    in one deterministic pass.

    Args:
        data_quality_steps: All configured DQ steps (warn/fail/quarantine).
        df: DataFrame snapshot to validate.
        extra_args: Per-category extra kwargs (from _build_dq_extra_args).

    Returns:
        tuple: (warnings, force_failures, quarantine_candidates, dq_markers).
    """
    # Initialise empty accumulators for warnings, failures, quarantine proposals, and markers
    warnings, force_failures = [], []
    quarantine_candidates = df.limit(0)       # Empty DF with same schema — will collect quarantine proposals
    dq_markers = _empty_dq_marker_df(df)      # Empty marker DF — will collect 'mark' action results

    # Early exit: no configured DQ steps means nothing to check
    if not data_quality_steps:
        return warnings, force_failures, quarantine_candidates, dq_markers

    log_and_print(f"Running {len(data_quality_steps)} data quality check(s) in one parallel phase.")

    def _run_check(step_order_and_step):
        """Execute a single DQ check against the shared DF snapshot.
        
        Each check gets its own local warning/failure lists (thread-safe since
        Python lists are only appended to within one thread). The check_quarantined
        DF starts empty — the check populates it if quarantine is needed.
        
        After the check runs, we tag quarantined rows with __dq_step_order so
        _apply_deterministic_quarantine_candidates can resolve conflicts later.
        """
        step_order, step = step_order_and_step
        local_w, local_f = [], []  # Thread-local accumulators — NOT shared across threads

        # Dispatch to the appropriate _validate_* function via _dispatch_dq_check.
        # We pass df.limit(0) as check_quarantined because in the parallel phase,
        # quarantine is only *proposed*, not applied — the actual removal happens
        # later in _apply_deterministic_quarantine_candidates.
        _, step_quarantined, local_w, local_f, step_markers = _dispatch_dq_check(
            data_quality_step=step,
            check_df=df,                # Shared immutable snapshot (persisted by caller)
            check_quarantined=df.limit(0),  # Empty quarantine — each check proposes independently
            check_warnings=local_w,
            check_failures=local_f,
            extra_args=extra_args
        )

        # Tag quarantine candidates with their metadata-defined order position.
        # This is how we resolve "which check wins" when two checks both want
        # to quarantine the same row — the lower step_order wins.
        if not _missing_required_columns(step_quarantined, {DQ_ROW_ID_COLUMN}):
            step_quarantined = step_quarantined.withColumn(DQ_STEP_ORDER_COLUMN, f.lit(step_order))

        return step_order, local_w, local_f, step_quarantined, step_markers

    # Run checks concurrently — capped at 8 threads to avoid overwhelming the Spark driver.
    # Each thread submits Spark jobs against the same persisted DF.
    max_workers = min(len(data_quality_steps), 8)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_run_check, (step_order, step)): step_order
            for step_order, step in enumerate(data_quality_steps)
        }
        # as_completed returns futures as they finish (non-deterministic order)
        ordered_results = [future.result() for future in as_completed(futures)]

    # Sort by step_order so that merging results is deterministic regardless
    # of which thread finished first. This ensures warnings/failures appear
    # in the same order as defined in metadata.
    ordered_results = sorted(ordered_results, key=lambda result: result[0])

    # Merge each check's results into the shared accumulators
    for _, step_warnings, step_failures, step_quarantined, step_markers in ordered_results:
        warnings.extend(step_warnings)        # Append this check's warnings to the master list
        force_failures.extend(step_failures)  # Append this check's failures to the master list
        dq_markers = _append_marker_df(dq_markers, step_markers)  # Stack marker DFs

        # Accumulate quarantine proposals — these are NOT applied yet.
        # _apply_deterministic_quarantine_candidates will resolve conflicts.
        if not _missing_required_columns(step_quarantined, {DQ_ROW_ID_COLUMN}):
            quarantine_candidates = quarantine_candidates.unionByName(
                step_quarantined,
                allowMissingColumns=True
            )

    return warnings, force_failures, quarantine_candidates, dq_markers


CUSTOM_DQ_FUNCTION_CATEGORY = "custom_data_quality_function"
CUSTOM_DQ_VALID_ACTIONS = {'warn', 'mark', 'quarantine', 'fail'}
RUN_IN_ENVIRONMENTS_ATTRIBUTE = "run_in_environments"
VALID_RUNTIME_ENVIRONMENTS = {'dev', 'qa', 'prod'}


def _normalize_runtime_environment_label(
    runtime_environment: str | None
) -> str | None:
    """Normalize the current runtime environment label for environment-scoped checks."""
    normalized_value = str(runtime_environment or '').strip().lower()
    return normalized_value or None


def _parse_run_in_environments(
    step: dict
) -> set | None:
    """Parse and validate the optional run_in_environments allow-list on a step."""
    raw_value = step.get(RUN_IN_ENVIRONMENTS_ATTRIBUTE)
    if raw_value is None:
        return None

    if isinstance(raw_value, str):
        values = [value.strip().lower() for value in raw_value.split(',') if value.strip()]
    elif isinstance(raw_value, list):
        values = [str(value).strip().lower() for value in raw_value if str(value).strip()]
    else:
        raise ValueError(
            f"'{RUN_IN_ENVIRONMENTS_ATTRIBUTE}' must be a comma-separated string or list. "
            f"Got {type(raw_value).__name__}."
        )

    if not values:
        return None

    invalid_values = sorted({
        value for value in values if value not in VALID_RUNTIME_ENVIRONMENTS
    })
    if invalid_values:
        raise ValueError(
            f"Invalid {RUN_IN_ENVIRONMENTS_ATTRIBUTE} value(s): {invalid_values}. "
            f"Valid options are: {sorted(VALID_RUNTIME_ENVIRONMENTS)}."
        )

    return set(values)


def _describe_environment_scoped_step(
    step: dict
) -> str:
    """Build a compact label for environment-scoped skip and error messages."""
    category = (step.get('Category') or 'step').strip()
    function_name = (step.get('function_name') or '').strip()
    message = (step.get('message') or '').strip()

    if function_name:
        return f"{category} ({function_name})"
    if message:
        return f"{category} ({message})"
    return category


def _filter_steps_by_runtime_environment(
    steps: list,
    runtime_environment: str | None,
    step_group_name: str
) -> list:
    """Filter DQ or reconciliation steps based on the current runtime environment."""
    if not steps:
        return []

    filtered_steps = []
    normalized_runtime_environment = _normalize_runtime_environment_label(runtime_environment)

    for step in steps:
        allowed_environments = _parse_run_in_environments(step)
        if allowed_environments is None:
            filtered_steps.append(step)
            continue

        if normalized_runtime_environment is None:
            raise ValueError(
                f"{step_group_name} step '{_describe_environment_scoped_step(step)}' uses "
                f"'{RUN_IN_ENVIRONMENTS_ATTRIBUTE}' but runtime_environment was not provided. "
                f"Set the Variable Library value 'runtime_environment' to one of: "
                f"{sorted(VALID_RUNTIME_ENVIRONMENTS)}."
            )

        if normalized_runtime_environment not in VALID_RUNTIME_ENVIRONMENTS:
            raise ValueError(
                f"Invalid runtime_environment='{runtime_environment}'. Expected one of: "
                f"{sorted(VALID_RUNTIME_ENVIRONMENTS)}."
            )

        if normalized_runtime_environment in allowed_environments:
            filtered_steps.append(step)
            continue

        log_and_print(
            f"Skipping {step_group_name} step '{_describe_environment_scoped_step(step)}' because "
            f"runtime_environment='{normalized_runtime_environment}' is not in "
            f"'{RUN_IN_ENVIRONMENTS_ATTRIBUTE}' ({', '.join(sorted(allowed_environments))})."
        )

    return filtered_steps


def _validate_custom_dq_action(
    function_name: str,
    action: str,
    action_source: str
) -> str:
    """Validate and normalize a custom DQ action value."""
    normalized_action = (action or 'warn').strip().lower()
    valid_actions_display = ', '.join(sorted(CUSTOM_DQ_VALID_ACTIONS))

    if normalized_action in CUSTOM_DQ_VALID_ACTIONS:
        return normalized_action

    if action_source == 'step':
        raise ValueError(
            f"custom_data_quality_function '{function_name}' has invalid "
            f"if_not_compliant='{normalized_action}'. Expected one of: "
            f"{valid_actions_display}."
        )

    raise ValueError(
        f"Custom DQ function '{function_name}' returned invalid "
        f"if_not_compliant='{normalized_action}'. Expected one of: "
        f"{valid_actions_display}."
    )


def _resolve_custom_dq_step(
    step: dict,
    target_table_name: str,
    datastore_config: str | list,
    session_globals: dict,
) -> tuple:
    """Validate one custom DQ step, load its callable, and build metadata payload."""
    function_name = (step.get('function_name') or '').strip()
    notebook_name = (step.get('notebook_name') or '').strip()
    step_if_not_compliant = _validate_custom_dq_action(
        function_name=function_name,
        action=step.get('if_not_compliant') or 'warn',
        action_source='step'
    )
    message = step.get('message') or f"Custom DQ function {function_name}"

    if not function_name or not notebook_name:
        raise ValueError(
            "custom_data_quality_function requires both 'function_name' and 'notebook_name'."
        )

    instantiate_notebook(
        notebook_name=notebook_name,
        function_scope=step.get('function_scope', 'local'),
    )
    fn = session_globals.get(function_name)
    if fn is None:
        raise KeyError(
            f"Custom DQ function '{function_name}' not found after loading "
            f"notebook '{notebook_name}'. Confirm the function is defined at "
            f"module level in the notebook."
        )

    metadata = {
        "function_config": {k: v for k, v in step.items() if k != 'Category'},
        "target_table_name": target_table_name,
        "datastore_config": datastore_config,
    }
    return fn, function_name, notebook_name, step_if_not_compliant, message, metadata


def _normalize_custom_dq_results(
    function_name: str,
    results: Any
) -> list:
    """Normalize custom DQ function output to a list of result dicts."""
    if results is None:
        return []

    if not isinstance(results, list):
        raise TypeError(
            f"Custom DQ function '{function_name}' must return a list of dicts. "
            f"Got {type(results).__name__}."
        )

    return results


def _normalize_custom_dq_result_entry(
    entry: Any,
    function_name: str,
    step_if_not_compliant: str,
    default_message: str,
    default_category: str
) -> dict:
    """Validate and normalize one custom DQ result entry."""
    if not isinstance(entry, dict):
        raise TypeError(
            f"Custom DQ function '{function_name}' returned a non-dict result: "
            f"{type(entry).__name__}."
        )

    return {
        "message": entry.get('message') or default_message,
        "category": entry.get('category') or default_category,
        "if_not_compliant": _validate_custom_dq_action(
            function_name=function_name,
            action=entry.get('if_not_compliant') or step_if_not_compliant,
            action_source='result'
        ),
        "rows_impacted": int(entry.get('rows_impacted') or 0),
        "quarantine_df": entry.get('quarantine_df'),
        "marker_df": entry.get('marker_df'),
    }


def _apply_custom_dq_result_entry(
    df: DataFrame,
    df_quarantined_all: DataFrame,
    dq_markers_all: DataFrame,
    function_name: str,
    normalized_entry: dict
) -> tuple:
    """Apply row-level side effects for one normalized custom DQ result entry."""
    entry_message = normalized_entry['message']
    entry_category = normalized_entry['category']
    entry_action = normalized_entry['if_not_compliant']
    rows_impacted = normalized_entry['rows_impacted']
    quarantine_df = normalized_entry['quarantine_df']
    marker_df = normalized_entry['marker_df']

    if entry_action == 'quarantine':
        if quarantine_df is None:
            raise ValueError(
                f"Custom DQ function '{function_name}' returned a quarantine result "
                f"for category '{entry_category}' but did not supply 'quarantine_df'."
            )
        if 'delta__idx' not in quarantine_df.columns:
            raise ValueError(
                f"Custom DQ function '{function_name}' requested quarantine but its "
                f"quarantine_df is missing the required 'delta__idx' column."
            )

        quarantine_idx = quarantine_df.select('delta__idx').distinct()
        rows_impacted = quarantine_idx.count() if rows_impacted == 0 else rows_impacted

        df, df_quarantined_all = _quarantine_rows(
            df=df,
            df_quarantined_all=df_quarantined_all,
            df_violations=quarantine_idx,
            quarantine_reason=entry_message,
            join_keys='delta__idx',
            join_type='semi'
        )

    if entry_action == 'mark':
        if marker_df is None:
            raise ValueError(
                f"Custom DQ function '{function_name}' returned a mark result "
                f"for category '{entry_category}' but did not supply 'marker_df'."
            )
        if 'delta__idx' not in marker_df.columns:
            raise ValueError(
                f"Custom DQ function '{function_name}' requested mark but its "
                f"marker_df is missing the required 'delta__idx' column."
            )

        new_markers = _build_dq_marker_df(
            df=df,
            df_violations=marker_df.select('delta__idx').distinct(),
            detail_message=entry_message,
            join_keys='delta__idx'
        )
        rows_impacted = new_markers.count() if rows_impacted == 0 else rows_impacted
        dq_markers_all = _append_marker_df(dq_markers_all, new_markers)

    return df, df_quarantined_all, dq_markers_all, rows_impacted


def _execute_custom_dq_functions(
    df: DataFrame,
    df_quarantined_all: DataFrame,
    dq_markers_all: DataFrame,
    custom_dq_steps: list,
    target_table_name: str,
    datastore_config: str | list,
    session_globals: dict,
    warnings: list,
    force_failures: list
) -> tuple:
    """Execute custom DQ functions sequentially and funnel results through shared DQ handlers."""
    if not custom_dq_steps:
        return df, df_quarantined_all, dq_markers_all, warnings, force_failures

    if session_globals is None:
        raise ValueError(
            "Custom data quality functions require session_globals; "
            "pass globals() from the calling notebook to execute_data_quality_checks()."
        )

    for step in custom_dq_steps:
        fn, function_name, notebook_name, step_if_not_compliant, message, metadata = _resolve_custom_dq_step(
            step=step,
            target_table_name=target_table_name,
            datastore_config=datastore_config,
            session_globals=session_globals,
        )

        log_and_print(f"Executing custom DQ function: {function_name} (from {notebook_name})")
        results = _normalize_custom_dq_results(
            function_name=function_name,
            results=fn(new_data=df, metadata=metadata, spark=spark)
        )

        default_category = f"custom:{function_name}"

        for entry in results:
            normalized_entry = _normalize_custom_dq_result_entry(
                entry=entry,
                function_name=function_name,
                step_if_not_compliant=step_if_not_compliant,
                default_message=message,
                default_category=default_category
            )

            df, df_quarantined_all, dq_markers_all, rows_impacted = _apply_custom_dq_result_entry(
                df=df,
                df_quarantined_all=df_quarantined_all,
                dq_markers_all=dq_markers_all,
                function_name=function_name,
                normalized_entry=normalized_entry
            )

            # Funnel the result through the shared recorder so warnings/failures are
            # formatted identically to built-in checks and surface in Data_Quality_Notifications.
            warnings, force_failures = _record_dq_result(
                violation_count=rows_impacted,
                dq_category=normalized_entry['category'],
                dq_message=normalized_entry['message'],
                if_not_compliant=normalized_entry['if_not_compliant'],
                warnings=warnings,
                force_failures=force_failures,
                pass_message=normalized_entry['message']
            )

    return df, df_quarantined_all, dq_markers_all, warnings, force_failures


CUSTOM_POST_WRITE_RECONCILIATION_FUNCTION_CATEGORY = "custom_post_write_reconciliation_function"
POST_WRITE_RECONCILIATION_VALID_ACTIONS = {'warn', 'fail'}
POST_WRITE_RECONCILIATION_NUMERIC_TOLERANCE_AGGREGATIONS = {'sum', 'avg', 'count', 'count_distinct'}


def _validate_post_write_reconciliation_action(
    category_name: str,
    action: str,
    action_source: str
) -> str:
    """Validate and normalize warn/fail actions for post-write reconciliation."""
    normalized_action = (action or 'warn').strip().lower()
    valid_actions_display = ', '.join(sorted(POST_WRITE_RECONCILIATION_VALID_ACTIONS))

    if normalized_action in POST_WRITE_RECONCILIATION_VALID_ACTIONS:
        return normalized_action

    if action_source == 'step':
        raise ValueError(
            f"{category_name} has invalid if_not_compliant='{normalized_action}'. "
            f"Expected one of: {valid_actions_display}."
        )

    raise ValueError(
        f"Post-write reconciliation result '{category_name}' returned invalid "
        f"if_not_compliant='{normalized_action}'. Expected one of: {valid_actions_display}."
    )


def _split_post_write_reconciliation_columns(value: Any) -> list:
    """Normalize comma-separated or list-based column config to a clean list."""
    if value is None:
        return []

    if isinstance(value, (list, tuple)):
        return [str(column).strip() for column in value if str(column).strip()]

    return [column.strip() for column in str(value).split(',') if column.strip()]


def _resolve_post_write_reconciliation_scope_columns(
    step: dict,
    target_config: dict,
    watermark_config: dict,
    primary_keys: list
) -> list:
    """Resolve the target slice columns used to isolate just-written records."""
    scope_columns = _split_post_write_reconciliation_columns(step.get('scope_columns'))
    if scope_columns:
        return scope_columns

    group_by_columns = _split_post_write_reconciliation_columns(step.get('group_by_columns'))
    if group_by_columns:
        return group_by_columns

    merge_batch_columns = _split_post_write_reconciliation_columns(
        watermark_config.get('merge_in_batches_with_columns')
    )
    if merge_batch_columns:
        return merge_batch_columns

    replace_where_column = (target_config.get('replace_where_column') or '').strip()
    if watermark_config.get('merge_type') == 'replace_where' and replace_where_column:
        return [replace_where_column]

    return list(primary_keys or [])


def _read_post_write_reconciliation_target_data(
    target_config: dict,
    watermark_config: dict
) -> DataFrame:
    """Load the written target so reconciliation checks can compare source vs target state."""
    merge_type = watermark_config.get('merge_type')

    if target_config.get('lakehouse_table_output'):
        target_abfss_path = target_config.get('target_abfss_path')
        if not target_abfss_path:
            raise ValueError(
                "Post-write reconciliation requires target_abfss_path for lakehouse table outputs."
            )

        target_df = spark.read.format("delta").load(target_abfss_path)
    elif merge_type == 'warehouse_spark_connector':
        target_table_name = target_config.get('target_table_name')
        if not target_table_name:
            raise ValueError(
                "Post-write reconciliation requires target_table_name for warehouse targets."
            )

        target_df = spark.sql(f"SELECT * FROM {target_table_name}")
    else:
        raise ValueError(
            "Post-write reconciliation is currently supported only for lakehouse tables "
            "and warehouse_spark_connector targets."
        )

    if merge_type == 'scd2' and 'scd_active' in target_df.columns:
        target_df = target_df.filter(f.col('scd_active') == 1)

    return target_df


def _load_post_write_reconciliation_target_slice(
    new_data: DataFrame,
    step: dict,
    target_config: dict,
    watermark_config: dict,
    primary_keys: list,
    base_target_data: DataFrame | None = None,
    target_slice_cache: dict | None = None
) -> tuple:
    """Load the target slice that corresponds to the batch being written."""
    scope_columns = _resolve_post_write_reconciliation_scope_columns(
        step=step,
        target_config=target_config,
        watermark_config=watermark_config,
        primary_keys=primary_keys
    )
    scope_key = tuple(scope_columns)

    if target_slice_cache is not None and scope_key in target_slice_cache:
        return target_slice_cache[scope_key], scope_columns

    target_data = base_target_data
    if target_data is None:
        target_data = _read_post_write_reconciliation_target_data(
            target_config=target_config,
            watermark_config=watermark_config
        )

    if scope_columns:
        _validate_columns_exist(new_data, scope_columns, "post_write_reconciliation (source scope columns)")
        _validate_columns_exist(target_data, scope_columns, "post_write_reconciliation (target scope columns)")

        scope_values = new_data.select(*scope_columns).dropDuplicates()
        target_slice = target_data.join(f.broadcast(scope_values), on=scope_columns, how='inner')
    elif watermark_config.get('merge_type') == 'overwrite':
        target_slice = target_data
    else:
        raise ValueError(
            "Post-write reconciliation could not determine the target slice for merge_type='"
            f"{watermark_config.get('merge_type')}'. Provide scope_columns, configure "
            "merge_in_batches_with_columns, use replace_where_column, or define primary_keys."
        )

    if target_slice_cache is not None:
        if target_slice is not target_data:
            target_slice = target_slice.persist(StorageLevel.MEMORY_AND_DISK)
        target_slice_cache[scope_key] = target_slice

    return target_slice, scope_columns


def _release_post_write_reconciliation_target_slices(
    base_target_data: DataFrame | None,
    target_slice_cache: dict | None
) -> None:
    """Release persisted target slices after reconciliation completes."""
    if target_slice_cache:
        for cached_slice in target_slice_cache.values():
            if base_target_data is not None and cached_slice is base_target_data:
                continue
            cached_slice.unpersist()

    if base_target_data is not None:
        base_target_data.unpersist()


def _get_post_write_reconciliation_distinct_count(df: DataFrame, columns: list) -> int:
    """Count distinct values for one or more columns."""
    if len(columns) == 1:
        return df.select(f.countDistinct(f.col(columns[0])).alias('distinct_count')).collect()[0]['distinct_count']

    return df.select(*columns).dropDuplicates().count()


def _build_post_write_reconciliation_aggregation_expr(
    aggregation: str,
    column_name: str
):
    """Map configured aggregation names to Spark expressions."""
    if aggregation == 'sum':
        return f.sum(f.col(column_name))
    if aggregation == 'avg':
        return f.avg(f.col(column_name))
    if aggregation == 'count':
        if column_name == '*':
            return f.count(f.lit(1))
        return f.count(f.col(column_name))
    if aggregation == 'min':
        return f.min(f.col(column_name))
    if aggregation == 'max':
        return f.max(f.col(column_name))
    if aggregation == 'count_distinct':
        return f.countDistinct(f.col(column_name))

    raise ValueError(
        f"Invalid aggregation function '{aggregation}' for post-write reconciliation. "
        f"Valid options are: {_VALID_AGGREGATION_FUNCTIONS}"
    )


def _build_post_write_reconciliation_aggregate_frame(
    df: DataFrame,
    group_by_columns: list,
    aggregation: str,
    column_name: str,
    metric_alias: str
) -> DataFrame:
    """Aggregate a DataFrame for group-level reconciliation comparisons."""
    aggregated_df = df.groupBy(*group_by_columns) if group_by_columns else df.groupBy()
    return aggregated_df.agg(
        _build_post_write_reconciliation_aggregation_expr(aggregation, column_name).alias(metric_alias)
    )


def _build_post_write_reconciliation_metric_mismatch_condition(
    source_metric_column,
    target_metric_column,
    aggregation: str,
    tolerance: float
):
    """Build a mismatch predicate for aggregate comparisons."""
    if tolerance > 0:
        if aggregation not in POST_WRITE_RECONCILIATION_NUMERIC_TOLERANCE_AGGREGATIONS:
            raise ValueError(
                f"Tolerance is only supported for numeric aggregations: "
                f"{sorted(POST_WRITE_RECONCILIATION_NUMERIC_TOLERANCE_AGGREGATIONS)}. "
                f"Received '{aggregation}'."
            )

        null_mismatch = (
            (source_metric_column.isNull() & target_metric_column.isNotNull()) |
            (source_metric_column.isNotNull() & target_metric_column.isNull())
        )
        value_mismatch = f.abs(
            source_metric_column.cast('double') - target_metric_column.cast('double')
        ) > f.lit(tolerance)
        return null_mismatch | value_mismatch

    return ~source_metric_column.eqNullSafe(target_metric_column)


def _execute_reconcile_row_count(
    new_data: DataFrame,
    target_data: DataFrame,
    step: dict,
    warnings: list,
    force_failures: list
) -> tuple:
    """Compare source and target row counts for the written slice."""
    action = _validate_post_write_reconciliation_action(
        category_name='reconcile_row_count',
        action=step.get('if_not_compliant') or 'warn',
        action_source='step'
    )
    message = step.get('message') or 'Post-write row count reconciliation failed'

    source_count = new_data.count()
    target_count = target_data.count()
    detail_message = f"{message}. Source rows: {source_count}, target rows: {target_count}."
    pass_message = (
        f"Post-write row count reconciliation passed. "
        f"Source rows: {source_count}, target rows: {target_count}."
    )

    return _record_dq_result(
        violation_count=abs(source_count - target_count),
        dq_category='reconcile_row_count',
        dq_message=detail_message,
        if_not_compliant=action,
        warnings=warnings,
        force_failures=force_failures,
        pass_message=pass_message
    )


def _execute_reconcile_distinct_count(
    new_data: DataFrame,
    target_data: DataFrame,
    step: dict,
    warnings: list,
    force_failures: list
) -> tuple:
    """Compare distinct counts for one or more columns."""
    action = _validate_post_write_reconciliation_action(
        category_name='reconcile_distinct_count',
        action=step.get('if_not_compliant') or 'warn',
        action_source='step'
    )
    columns = _split_post_write_reconciliation_columns(step.get('column_name'))
    if not columns:
        raise ValueError("reconcile_distinct_count requires 'column_name'.")

    _validate_columns_exist(new_data, columns, "reconcile_distinct_count (source columns)")
    _validate_columns_exist(target_data, columns, "reconcile_distinct_count (target columns)")

    source_count = _get_post_write_reconciliation_distinct_count(new_data, columns)
    target_count = _get_post_write_reconciliation_distinct_count(target_data, columns)
    columns_display = ', '.join(columns)
    message = step.get('message') or f"Distinct count mismatch for columns {columns_display}"
    detail_message = (
        f"{message}. Source distinct count: {source_count}, "
        f"target distinct count: {target_count}."
    )
    pass_message = (
        f"Distinct counts matched for columns {columns_display}. "
        f"Source distinct count: {source_count}, target distinct count: {target_count}."
    )

    return _record_dq_result(
        violation_count=abs(source_count - target_count),
        dq_category='reconcile_distinct_count',
        dq_message=detail_message,
        if_not_compliant=action,
        warnings=warnings,
        force_failures=force_failures,
        pass_message=pass_message
    )


def _execute_reconcile_missing_keys(
    new_data: DataFrame,
    target_data: DataFrame,
    step: dict,
    warnings: list,
    force_failures: list
) -> tuple:
    """Verify that every source key combination exists in the written target slice."""
    action = _validate_post_write_reconciliation_action(
        category_name='reconcile_missing_keys',
        action=step.get('if_not_compliant') or 'warn',
        action_source='step'
    )
    key_columns = _split_post_write_reconciliation_columns(step.get('column_name'))
    if not key_columns:
        raise ValueError("reconcile_missing_keys requires 'column_name'.")

    _validate_columns_exist(new_data, key_columns, "reconcile_missing_keys (source columns)")
    _validate_columns_exist(target_data, key_columns, "reconcile_missing_keys (target columns)")

    source_keys = new_data.select(*key_columns).dropDuplicates()
    target_keys = target_data.select(*key_columns).dropDuplicates()
    missing_key_count = source_keys.join(target_keys, on=key_columns, how='left_anti').count()
    keys_display = ', '.join(key_columns)
    message = step.get('message') or f"Missing keys detected in target for columns {keys_display}"
    detail_message = f"{message}. Missing key combinations in target slice: {missing_key_count}."
    pass_message = (
        f"All source keys were found in the target slice for columns {keys_display}. "
        f"Missing key combinations in target slice: {missing_key_count}."
    )

    return _record_dq_result(
        violation_count=missing_key_count,
        dq_category='reconcile_missing_keys',
        dq_message=detail_message,
        if_not_compliant=action,
        warnings=warnings,
        force_failures=force_failures,
        pass_message=pass_message
    )


def _execute_reconcile_aggregate(
    new_data: DataFrame,
    target_data: DataFrame,
    step: dict,
    warnings: list,
    force_failures: list
) -> tuple:
    """Compare source and target aggregates, optionally per group."""
    action = _validate_post_write_reconciliation_action(
        category_name='reconcile_aggregate',
        action=step.get('if_not_compliant') or 'warn',
        action_source='step'
    )
    aggregation = (step.get('aggregation') or '').strip().lower()
    column_name = (step.get('column_name') or '').strip()
    group_by_columns = _split_post_write_reconciliation_columns(step.get('group_by_columns'))

    if aggregation not in _VALID_AGGREGATION_FUNCTIONS:
        raise ValueError(
            f"reconcile_aggregate has invalid aggregation='{aggregation}'. "
            f"Valid options are: {_VALID_AGGREGATION_FUNCTIONS}"
        )

    if not column_name:
        raise ValueError(
            "reconcile_aggregate requires 'column_name'. Use '*' with aggregation='count' "
            "for count(*) semantics."
        )

    is_count_all = aggregation == 'count' and column_name == '*'
    if not is_count_all:
        _validate_columns_exist(new_data, [column_name], "reconcile_aggregate (source column)")
        _validate_columns_exist(target_data, [column_name], "reconcile_aggregate (target column)")

    if group_by_columns:
        _validate_columns_exist(new_data, group_by_columns, "reconcile_aggregate (source group_by_columns)")
        _validate_columns_exist(target_data, group_by_columns, "reconcile_aggregate (target group_by_columns)")

    tolerance_raw = step.get('tolerance') or 0
    try:
        tolerance = float(tolerance_raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"reconcile_aggregate has invalid tolerance='{tolerance_raw}'. Expected a numeric value."
        ) from exc

    source_aggregate = _build_post_write_reconciliation_aggregate_frame(
        df=new_data,
        group_by_columns=group_by_columns,
        aggregation=aggregation,
        column_name=column_name,
        metric_alias='source_metric'
    )
    target_aggregate = _build_post_write_reconciliation_aggregate_frame(
        df=target_data,
        group_by_columns=group_by_columns,
        aggregation=aggregation,
        column_name=column_name,
        metric_alias='target_metric'
    )

    if group_by_columns:
        comparison_df = source_aggregate.join(target_aggregate, on=group_by_columns, how='full_outer')
    else:
        comparison_df = source_aggregate.crossJoin(target_aggregate)

    mismatch_condition = _build_post_write_reconciliation_metric_mismatch_condition(
        source_metric_column=f.col('source_metric'),
        target_metric_column=f.col('target_metric'),
        aggregation=aggregation,
        tolerance=tolerance
    )
    mismatch_count = comparison_df.filter(mismatch_condition).count()
    metric_display = f"{aggregation}(*)" if is_count_all else f"{aggregation}({column_name})"
    message = step.get('message') or f"Aggregate mismatch for {metric_display}"

    if group_by_columns:
        group_display = ', '.join(group_by_columns)
        detail_message = (
            f"{message}. Mismatched groups for {metric_display} by "
            f"{group_display}: {mismatch_count}. Tolerance: {tolerance}."
        )
        pass_message = (
            f"Aggregate comparison matched for {metric_display} by {group_display}. "
            f"Mismatched groups: {mismatch_count}. Tolerance: {tolerance}."
        )
    else:
        detail_message = (
            f"{message}. Source and target {metric_display} comparison "
            f"mismatch count: {mismatch_count}. Tolerance: {tolerance}."
        )
        pass_message = (
            f"Aggregate comparison matched for {metric_display}. "
            f"Source and target {metric_display} comparison mismatch count: {mismatch_count}. "
            f"Tolerance: {tolerance}."
        )

    return _record_dq_result(
        violation_count=mismatch_count,
        dq_category='reconcile_aggregate',
        dq_message=detail_message,
        if_not_compliant=action,
        warnings=warnings,
        force_failures=force_failures,
        pass_message=pass_message
    )


def _execute_builtin_post_write_reconciliation_step(
    new_data: DataFrame,
    step: dict,
    target_config: dict,
    watermark_config: dict,
    primary_keys: list,
    warnings: list,
    force_failures: list,
    base_target_data: DataFrame | None = None,
    target_slice_cache: dict | None = None
) -> tuple:
    """Execute one built-in post-write reconciliation step."""
    category = step.get('Category')
    target_data, _ = _load_post_write_reconciliation_target_slice(
        new_data=new_data,
        step=step,
        target_config=target_config,
        watermark_config=watermark_config,
        primary_keys=primary_keys,
        base_target_data=base_target_data,
        target_slice_cache=target_slice_cache
    )

    if category == 'reconcile_row_count':
        return _execute_reconcile_row_count(
            new_data=new_data,
            target_data=target_data,
            step=step,
            warnings=warnings,
            force_failures=force_failures
        )

    if category == 'reconcile_distinct_count':
        return _execute_reconcile_distinct_count(
            new_data=new_data,
            target_data=target_data,
            step=step,
            warnings=warnings,
            force_failures=force_failures
        )

    if category == 'reconcile_missing_keys':
        return _execute_reconcile_missing_keys(
            new_data=new_data,
            target_data=target_data,
            step=step,
            warnings=warnings,
            force_failures=force_failures
        )

    if category == 'reconcile_aggregate':
        return _execute_reconcile_aggregate(
            new_data=new_data,
            target_data=target_data,
            step=step,
            warnings=warnings,
            force_failures=force_failures
        )

    raise ValueError(f"Unsupported post_write_reconciliation category: {category}")


def _resolve_custom_post_write_reconciliation_step(
    step: dict,
    new_data: DataFrame,
    target_config: dict,
    watermark_config: dict,
    primary_keys: list,
    session_globals: dict,
    base_target_data: DataFrame | None = None,
    target_slice_cache: dict | None = None
) -> tuple:
    """Validate one custom reconciliation step, load its callable, and build metadata."""
    function_name = (step.get('function_name') or '').strip()
    notebook_name = (step.get('notebook_name') or '').strip()
    step_if_not_compliant = _validate_post_write_reconciliation_action(
        category_name=function_name or CUSTOM_POST_WRITE_RECONCILIATION_FUNCTION_CATEGORY,
        action=step.get('if_not_compliant') or 'warn',
        action_source='step'
    )
    message = step.get('message') or f"Custom post-write reconciliation function {function_name}"

    if not function_name or not notebook_name:
        raise ValueError(
            "custom_post_write_reconciliation_function requires both 'function_name' and 'notebook_name'."
        )

    instantiate_notebook(
        notebook_name=notebook_name,
        function_scope=step.get('function_scope', 'local'),
    )
    fn = session_globals.get(function_name)
    if fn is None:
        raise KeyError(
            f"Custom post-write reconciliation function '{function_name}' not found after "
            f"loading notebook '{notebook_name}'. Confirm the function is defined at module level."
        )

    target_data, scope_columns = _load_post_write_reconciliation_target_slice(
        new_data=new_data,
        step=step,
        target_config=target_config,
        watermark_config=watermark_config,
        primary_keys=primary_keys,
        base_target_data=base_target_data,
        target_slice_cache=target_slice_cache
    )

    metadata = {
        "function_config": {k: v for k, v in step.items() if k != 'Category'},
        "merge_type": watermark_config.get('merge_type'),
        "primary_keys": list(primary_keys or []),
        "scope_columns": scope_columns,
        "target_abfss_path": target_config.get('target_abfss_path'),
        "target_data": target_data,
        "target_table_name": target_config.get('target_table_name'),
    }
    return fn, function_name, notebook_name, step_if_not_compliant, message, metadata


def _normalize_custom_post_write_reconciliation_results(
    function_name: str,
    results: Any
) -> list:
    """Normalize custom post-write reconciliation output to a list of result dicts."""
    if results is None:
        return []

    if not isinstance(results, list):
        raise TypeError(
            f"Custom post-write reconciliation function '{function_name}' must return "
            f"a list of dicts. Got {type(results).__name__}."
        )

    return results


def _normalize_custom_post_write_reconciliation_result_entry(
    entry: Any,
    function_name: str,
    step_if_not_compliant: str,
    default_message: str,
    default_category: str
) -> dict:
    """Validate and normalize one custom post-write reconciliation result entry."""
    if not isinstance(entry, dict):
        raise TypeError(
            f"Custom post-write reconciliation function '{function_name}' returned "
            f"a non-dict result: {type(entry).__name__}."
        )

    return {
        "message": entry.get('message') or default_message,
        "category": entry.get('category') or default_category,
        "if_not_compliant": _validate_post_write_reconciliation_action(
            category_name=entry.get('category') or default_category,
            action=entry.get('if_not_compliant') or step_if_not_compliant,
            action_source='result'
        ),
        "rows_impacted": int(entry.get('rows_impacted') or 0),
    }


def _execute_custom_post_write_reconciliation_functions(
    new_data: DataFrame,
    custom_reconciliation_steps: list,
    target_config: dict,
    watermark_config: dict,
    primary_keys: list,
    session_globals: dict,
    warnings: list,
    force_failures: list,
    base_target_data: DataFrame | None = None,
    target_slice_cache: dict | None = None
) -> tuple:
    """Execute custom post-write reconciliation functions and record warn/fail results."""
    if not custom_reconciliation_steps:
        return warnings, force_failures

    if session_globals is None:
        raise ValueError(
            "Custom post-write reconciliation functions require session_globals; "
            "pass globals() from the calling notebook."
        )

    for step in custom_reconciliation_steps:
        fn, function_name, notebook_name, step_if_not_compliant, message, metadata = _resolve_custom_post_write_reconciliation_step(
            step=step,
            new_data=new_data,
            target_config=target_config,
            watermark_config=watermark_config,
            primary_keys=primary_keys,
            session_globals=session_globals,
            base_target_data=base_target_data,
            target_slice_cache=target_slice_cache
        )

        log_and_print(
            f"Executing custom post-write reconciliation function: {function_name} "
            f"(from {notebook_name})"
        )
        results = _normalize_custom_post_write_reconciliation_results(
            function_name=function_name,
            results=fn(new_data=new_data, metadata=metadata, spark=spark)
        )
        default_category = f"custom:{function_name}"

        for entry in results:
            normalized_entry = _normalize_custom_post_write_reconciliation_result_entry(
                entry=entry,
                function_name=function_name,
                step_if_not_compliant=step_if_not_compliant,
                default_message=message,
                default_category=default_category
            )

            warnings, force_failures = _record_dq_result(
                violation_count=normalized_entry['rows_impacted'],
                dq_category=normalized_entry['category'],
                dq_message=normalized_entry['message'],
                if_not_compliant=normalized_entry['if_not_compliant'],
                warnings=warnings,
                force_failures=force_failures,
                pass_message=normalized_entry['message']
            )

    return warnings, force_failures


def execute_post_write_reconciliation_checks(
    new_data: DataFrame,
    post_write_reconciliation_steps: list,
    target_config: dict,
    watermark_config: dict,
    primary_keys: list,
    session_globals: dict = None,
    runtime_environment: str | None = None
) -> tuple:
    """Run built-in and custom post-write reconciliation checks after the target write completes."""
    if not post_write_reconciliation_steps:
        return [], []

    warnings = []
    force_failures = []
    built_in_steps = []
    custom_steps = []

    scoped_reconciliation_steps = _filter_steps_by_runtime_environment(
        steps=post_write_reconciliation_steps,
        runtime_environment=runtime_environment,
        step_group_name='post-write reconciliation'
    )

    for step in scoped_reconciliation_steps:
        if step.get('Category') == CUSTOM_POST_WRITE_RECONCILIATION_FUNCTION_CATEGORY:
            custom_steps.append(step)
        else:
            built_in_steps.append(step)

    if not built_in_steps and not custom_steps:
        return warnings, force_failures

    base_target_data = _read_post_write_reconciliation_target_data(
        target_config=target_config,
        watermark_config=watermark_config
    ).persist(StorageLevel.MEMORY_AND_DISK)
    target_slice_cache = {}

    try:
        for step in built_in_steps:
            warnings, force_failures = _execute_builtin_post_write_reconciliation_step(
                new_data=new_data,
                step=step,
                target_config=target_config,
                watermark_config=watermark_config,
                primary_keys=primary_keys,
                warnings=warnings,
                force_failures=force_failures,
                base_target_data=base_target_data,
                target_slice_cache=target_slice_cache
            )

        warnings, force_failures = _execute_custom_post_write_reconciliation_functions(
            new_data=new_data,
            custom_reconciliation_steps=custom_steps,
            target_config=target_config,
            watermark_config=watermark_config,
            primary_keys=primary_keys,
            session_globals=session_globals,
            warnings=warnings,
            force_failures=force_failures,
            base_target_data=base_target_data,
            target_slice_cache=target_slice_cache
        )
    finally:
        _release_post_write_reconciliation_target_slices(
            base_target_data=base_target_data,
            target_slice_cache=target_slice_cache
        )

    return warnings, force_failures


def handle_post_write_reconciliation(
    new_data: DataFrame,
    post_write_reconciliation_steps: list,
    target_config: dict,
    watermark_config: dict,
    primary_keys: list,
    dq_warnings: list,
    dq_force_failures: list,
    source_details: str,
    lineage_info: dict,
    session_globals: dict = None,
    runtime_environment: str | None = None
) -> tuple:
    """Execute post-write reconciliation and funnel failures through the shared DQ exception path."""
    reconciliation_warnings, reconciliation_force_failures = execute_post_write_reconciliation_checks(
        new_data=new_data,
        post_write_reconciliation_steps=post_write_reconciliation_steps,
        target_config=target_config,
        watermark_config=watermark_config,
        primary_keys=primary_keys,
        session_globals=session_globals,
        runtime_environment=runtime_environment
    )

    dq_warnings.extend(reconciliation_warnings)
    dq_force_failures.extend(reconciliation_force_failures)

    if reconciliation_force_failures:
        raise DataQualityFailureError(
            message="One or more post-write reconciliation checks triggered notebook failure. Please review logging record for details.",
            dq_warnings=dq_warnings,
            dq_failures=dq_force_failures,
            source_details=source_details,
            lineage_info=lineage_info
        )

    return dq_warnings, dq_force_failures


def _data_quality_checks(
    df: DataFrame, 
    data_quality_steps: list, 
    primary_keys: list, 
    if_duplicate_primary_keys: str, 
    columns_to_add_to_pks: list, 
    target_table_name: str, 
    datastore_config: str | list,
    session_globals: dict = None,
    runtime_environment: str | None = None,
    table_id=None
) -> tuple:
    """
    Execute comprehensive data quality validation with configurable actions.

    Orchestrates a data quality framework that evaluates all checks in one parallel phase:
    1. Adds a tracking index and initialises result accumulators
    2. Checks for corrupt records and primary key violations
    3. Evaluates all remaining checks in a single parallel pass over the same snapshot
    4. Applies deterministic quarantine precedence in one pass and aggregates markers

    Args:
        df (DataFrame): Input data to validate
        data_quality_steps (list): List of configured quality check definitions
        primary_keys (list): Primary key columns for uniqueness validation
        if_duplicate_primary_keys (str): Action for duplicates ('warn', 'quarantine', or fail)
        columns_to_add_to_pks (list): Additional columns for composite key validation
        target_table_name (str): Target table name (used for anomaly baselines and PK file checks)
        datastore_config (str | list): Datastore configuration from Datastore_Configuration table

    Returns:
        tuple: (warnings list, failures list, cleaned DataFrame, quarantined DataFrame)

    See Also:
        _run_all_dq_checks_parallel: Concurrent evaluation of all DQ checks.
        _apply_deterministic_quarantine_candidates: Deterministic quarantine application.
        _dispatch_dq_check: Routes a single check to its validation function.
    """
    # Every DQ pass owns these managed system columns. Drop any carried-forward
    # versions from upstream layers so current-run markers/quarantine reasons
    # cannot collide with or append onto prior-run/system values.
    managed_dq_columns = [
        column_name
        for column_name in (
            DQ_FAILURE_DETAILS_COLUMN,
            QUARANTINE_REASON_COLUMN,
            DQ_ROW_ID_COLUMN,
            DQ_STEP_ORDER_COLUMN,
        )
        if column_name in df.columns
    ]
    if managed_dq_columns:
        df = df.drop(*managed_dq_columns)

    # ── PHASE 0: INITIALISATION ──────────────────────────────────────────────
    # Assign a unique row ID (delta__idx) to every row. This integer, generated by
    # monotonically_increasing_id(), is the join key used throughout DQ processing
    # to track which rows are quarantined, marked, or clean. It is dropped at the end.
    df = df.withColumn("delta__idx", f.monotonically_increasing_id())

    # Empty marker accumulator — will collect (delta__idx, detail_message) pairs
    # from every check that uses the 'mark' action.
    dq_markers_all = _empty_dq_marker_df(df)
    
    # Warning and failure accumulators — JSON dicts are appended here by each check.
    # Warnings are informational (continues processing); failures halt the pipeline.
    warnings = []
    force_failures = []

    # ── PHASE 1: CORRUPT RECORDS ────────────────────────────────────────────
    # Check for rows with _corrupt_record column (comes from Spark's schema
    # enforcement when reading files). These rows failed to parse and are
    # quarantined unconditionally. This runs first because corrupt rows
    # should not pass through any downstream checks.
    df, df_quarantined_all, warnings, corrupt_markers = _handle_corrupt_records(
        df=df, 
        warnings=warnings
    )
    dq_markers_all = _append_marker_df(dq_markers_all, corrupt_markers)

    # ── PHASE 2: PRIMARY KEY VALIDATION ─────────────────────────────────────
    # Check for duplicate primary keys. This runs before the parallel phase
    # because PK violations can quarantine rows, and downstream checks should
    # not evaluate rows that have already been removed for PK issues.
    # columns_to_add_to_pks supports file-batched ingestion where the batch
    # column is temporarily included in the PK for uniqueness within a file.
    df, df_quarantined_all, warnings, force_failures, pk_markers = _validate_primary_keys(
        df=df, 
        df_quarantined_all=df_quarantined_all, 
        primary_keys=primary_keys, 
        if_duplicate_primary_keys=if_duplicate_primary_keys, 
        columns_to_add_to_pks=columns_to_add_to_pks, 
        target_table_name=target_table_name, 
        warnings=warnings, 
        force_failures=force_failures,
        table_id=table_id
    )
    dq_markers_all = _append_marker_df(dq_markers_all, pk_markers)

    # Split custom DQ function steps from built-in checks. Custom steps run
    # sequentially (Phase 3.5) — user code may perform external I/O or hold
    # non-thread-safe state, so they are not safe inside the parallel phase.
    scoped_data_quality_steps = _filter_steps_by_runtime_environment(
        steps=data_quality_steps or [],
        runtime_environment=runtime_environment,
        step_group_name='data quality'
    )

    builtin_dq_steps = [
        step for step in scoped_data_quality_steps
        if (step.get("Category") or "").strip() != CUSTOM_DQ_FUNCTION_CATEGORY
    ]
    custom_dq_steps = [
        step for step in scoped_data_quality_steps
        if (step.get("Category") or "").strip() == CUSTOM_DQ_FUNCTION_CATEGORY
    ]

    # ── PHASE 3: PARALLEL DQ CHECKS ─────────────────────────────────────────
    # All remaining metadata-configured checks (validate_condition, validate_not_null,
    # validate_unique, validate_range, validate_pattern, validate_anomaly,
    # validate_freshness, validate_completeness, validate_batch_size,
    # validate_referential_integrity) run concurrently against the SAME snapshot.
    #
    # Key design decisions:
    #   - Checks do NOT mutate df — they only *propose* quarantine candidates
    #   - Each check gets its own warning/failure lists (merged after)
    #   - Quarantine candidates are tagged with step_order for deterministic resolution
    #   - The df is persisted to avoid redundant recomputation per thread
    extra_args = _build_dq_extra_args(datastore_config, target_table_name)

    # Keep one shared persisted snapshot alive from parallel check evaluation through
    # deterministic quarantine removal so the same delta__idx mapping is used end-to-end
    # for rows touched by DQ checks, without forcing an additional full-table pass.
    #
    # Important: only persist when there are actual metadata DQ steps to run in
    # parallel. If no DQ steps are configured, skip parallel evaluation entirely.
    if builtin_dq_steps:
        df.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            p_warnings, p_failures, quarantine_candidates, p_markers = _run_all_dq_checks_parallel(
                data_quality_steps=builtin_dq_steps,
                df=df,
                extra_args=extra_args
            )
            # Merge parallel results into the master accumulators
            warnings.extend(p_warnings)
            force_failures.extend(p_failures)
            dq_markers_all = _append_marker_df(dq_markers_all, p_markers)

            # ── PHASE 4: DETERMINISTIC QUARANTINE APPLICATION ───────────────────────
            # Resolve quarantine conflicts: if multiple checks proposed quarantining the
            # same row, the check with the lowest metadata-defined order wins. This
            # ensures the quarantine reason is deterministic regardless of thread timing.
            # After this, quarantined rows are removed from df and added to df_quarantined_all.
            df, df_quarantined_all = _apply_deterministic_quarantine_candidates(
                df=df,
                df_quarantined_all=df_quarantined_all,
                quarantine_candidates=quarantine_candidates
            )
        finally:
            df.unpersist()

    # ── PHASE 3.5: CUSTOM DQ FUNCTIONS ──────────────────────────────────────
    # Executed after built-in checks so user code sees the post-parallel state
    # (built-in quarantines already removed) and before marker cleanup. Runs
    # sequentially in metadata-defined instance order.
    if custom_dq_steps:
        df, df_quarantined_all, dq_markers_all, warnings, force_failures = _execute_custom_dq_functions(
            df=df,
            df_quarantined_all=df_quarantined_all,
            dq_markers_all=dq_markers_all,
            custom_dq_steps=custom_dq_steps,
            target_table_name=target_table_name,
            datastore_config=datastore_config,
            session_globals=session_globals,
            warnings=warnings,
            force_failures=force_failures
        )

    # ── PHASE 5: MARKER CLEANUP & APPLICATION ───────────────────────────────
    # Step 5a: Remove markers for rows that ended up quarantined.
    # A row might have been marked by one check and quarantined by another.
    # Since quarantine removes the row entirely, its markers are meaningless.
    dq_markers_all = _exclude_quarantined_rows_from_markers(
        marker_candidates=dq_markers_all,
        df_quarantined_all=df_quarantined_all
    )

    # Step 5b: Join aggregated markers back onto the active and quarantined DFs.
    # This adds the delta__dq_failure_details column with pipe-delimited messages
    # for any row that was flagged by a 'mark' action.
    df, df_quarantined_all = _apply_dq_failure_markers(
        df=df,
        df_quarantined_all=df_quarantined_all,
        marker_candidates=dq_markers_all
    )

    # ── PHASE 6: CLEANUP ────────────────────────────────────────────────────
    # Remove the internal tracking column — it's an implementation detail that
    # should never appear in downstream tables.
    df = df.drop("delta__idx")
    df_quarantined_all = df_quarantined_all.drop("delta__idx")

    # Log accumulated warnings and failures for pipeline observability
    if warnings:
        data_quality_warnings_log_warn = f"Data Quality Warnings: {warnings}"
        log_and_print(data_quality_warnings_log_warn, "warn")
    
    if force_failures:
        data_quality_failures_log_error = f"Data Quality Failures: {force_failures}"
        log_and_print(data_quality_failures_log_error, "error")

    # Return the four outputs:
    #   warnings       — list of JSON dicts for pipeline logging / notifications
    #   force_failures — list of JSON dicts; if non-empty, the pipeline will halt
    #   df             — cleaned DataFrame (quarantined + corrupt rows removed)
    #   df_quarantined_all — all quarantined rows with delta__quarantine_reason
    return warnings, force_failures, df, df_quarantined_all

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def reorder_columns_for_output(
    new_data: DataFrame
) -> DataFrame:
    """
    Reorder DataFrame columns by moving system columns to the end.
    
    This function organizes columns into three groups for improved readability:
    1. Business/data columns (first)
    2. SCD2 tracking columns (middle)
    3. Delta/system metadata columns (last)
    
    Args:
        new_data (DataFrame): Input DataFrame with mixed column types
    
    Returns:
        DataFrame: DataFrame with reordered columns
    """
    # move scd2 and delta columns to back
    data_columns = [col for col in new_data.columns if not col.startswith('delta__') and not col.startswith('scd_')]
    scd_columns = [col for col in new_data.columns if col.startswith('scd_')]
    delta_columns = [col for col in new_data.columns if col.startswith('delta__')]

    # Combine all lists
    all_columns = data_columns + scd_columns + delta_columns

    # Quote literal identifiers so dots in flattened source column names are not
    # interpreted as nested-field access by Spark.
    new_data = new_data.select(*[f.col(f"`{col_name}`") for col_name in all_columns])
    return new_data

def execute_data_quality_checks(
    new_data: DataFrame, 
    data_quality_steps: list, 
    if_duplicate_primary_keys: str, 
    primary_keys: list, 
    target_table_name: str, 
    datastore_config: str | list, 
    merge_in_batches_with_columns: List[str],
    session_globals: dict = None,
    runtime_environment: str | None = None,
    table_id=None
) -> tuple:
    """
    Execute data quality validation with optional batch-specific primary key handling.
    
    This wrapper function orchestrates data quality checks and adjusts primary key
    validation when data is being written in batches.
    
    Args:
        new_data (DataFrame): Data to validate
        data_quality_steps (list): List of quality check configurations
        if_duplicate_primary_keys (str): Action for duplicates ('warn', 'quarantine', or fail)
        primary_keys (list): Primary key columns
        target_table_name (str): Target table name
        datastore_config (str | list): Datastore configuration from Datastore_Configuration table
        merge_in_batches_with_columns (List[str]): Batch columns to include in PK validation
        session_globals (dict, optional): globals() from the calling notebook, required when
            data_quality_steps includes any `custom_data_quality_function` steps. Used to look
            up the user's DQ function after instantiate_notebook() loads it into the session.
        runtime_environment (str, optional): Logical environment label (`dev`, `qa`, `prod`) used
            to evaluate `run_in_environments` allow-lists on DQ steps.
        table_id (optional): Metadata Table_ID used in failure guidance for agent follow-up.
    
    Returns:
        tuple: (warnings list, failures list, cleaned DataFrame, quarantined DataFrame)
    """
    # If data is being batched by file
    if merge_in_batches_with_columns:
        primary_key_update_log_info = f"Writing data in batches with columns: {merge_in_batches_with_columns}. These columns will be included in primary key validation."
        log_and_print(primary_key_update_log_info)
        columns_to_add_to_pks = merge_in_batches_with_columns
    else:
        columns_to_add_to_pks = []

    # Execute data quality checks and capture results
    dq_warnings, dq_force_failures, new_data, quarantined_data = _data_quality_checks(
        df = new_data,
        data_quality_steps = data_quality_steps,
        if_duplicate_primary_keys = if_duplicate_primary_keys,
        primary_keys = primary_keys,
        columns_to_add_to_pks = columns_to_add_to_pks,
        target_table_name = target_table_name,
        datastore_config = datastore_config,
        session_globals = session_globals,
        runtime_environment = runtime_environment,
        table_id = table_id
    )
    return dq_warnings, dq_force_failures, new_data, quarantined_data

def analyze_schema_changes(
    new_data: DataFrame, 
    last_schema_id: str, 
    last_schema_details: list
) -> tuple:
    """
    Analyze schema changes between current and previous DataFrame versions.
    
    Compares the current DataFrame schema against the last known schema to detect:
    - Schema hash changes
    - Column additions
    - Column removals
    - Data type modifications
    
    Args:
        new_data (DataFrame): Current DataFrame to analyze
        last_schema_id (str): MD5 hash of the previous schema
        last_schema_details (list): Previous schema definition as list of (column, datatype) tuples
    
    Returns:
        tuple: (new_schema flag, schema details dict, schema hash, change summary, 
                type update summary, changed column types list)
    """
    # Extract and analyze schema information
    new_data_schema = [(field.name, field.dataType.simpleString()) for field in new_data.schema.fields]
    new_data_schema_string = str(new_data_schema)          # Python repr — used ONLY for hash stability
    new_data_schema_json = json.dumps(new_data_schema)     # JSON — used for storage / downstream parsing
    new_data_schema_hash = get_md5_of_string(new_data_schema_string)

    # Determine if schema has changed
    if new_data_schema_hash == last_schema_id:
        new_schema = False
    else:
        new_schema = True

    # Calculate schema differences
    schema_changes = get_schema_changes(new_schema = new_data_schema, old_schema = last_schema_details)

    # Prepare schema details for logging
    if new_schema and last_schema_id:
        new_schema_details = {"schema_details": new_data_schema_json, "schema_changes": schema_changes }
        column_types_that_changed = [column_update for column_update in schema_changes if column_update['change_type'] == 'Column Type Changed']
        columns_added = [column_update for column_update in schema_changes if column_update['change_type'] == 'Column Added']
        columns_dropped = [column_update for column_update in schema_changes if column_update['change_type'] == 'Column Dropped']

        schema_change_summary = ""
        schema_type_updates_summary = ""

        if columns_added:
            schema_change_summary += "Columns Added - "
            schema_change_summary += "; ".join(f"{col['column']} ({col['data_type']})" for col in columns_added)
            schema_change_summary += ". "

        if columns_dropped:
            schema_change_summary += "Columns Dropped - "
            schema_change_summary += "; ".join(f"{col['column']} ({col['data_type']})" for col in columns_dropped)
            schema_change_summary += ". "

        if column_types_that_changed:
            schema_type_updates_summary += "Column Types Changed - "
            schema_type_updates_summary += "; ".join(f"{col['column']} → {col['data_type']}" for col in column_types_that_changed)
            schema_type_updates_summary += ". "
            schema_change_summary += schema_type_updates_summary

    else:
        new_schema_details = {"schema_details": new_data_schema_json, "schema_changes": ""}
        schema_change_summary = ""
        schema_type_updates_summary = ""
        column_types_that_changed = ""
        
    return new_schema, new_schema_details, new_data_schema_hash, schema_change_summary, schema_type_updates_summary, column_types_that_changed

def handle_schema_change_failures(
    new_schema: bool, 
    fail_on_new_schema: bool, 
    first_run: bool, 
    last_schema_id: str, 
    schema_change_summary: str, 
    fail_on_column_data_type_change: bool, 
    column_types_that_changed: list, 
    schema_type_updates_summary: str, 
    dq_warnings: list, 
    dq_force_failures: list
) -> tuple:
    """
    Handle schema change validation based on configuration flags.
    
    Determines whether schema changes should result in failures, warnings, or be allowed.
    Supports different strictness levels for schema evolution vs data type changes.
    
    Args:
        new_schema (bool): Whether schema has changed
        fail_on_new_schema (bool): Whether to fail on any schema change
        first_run (bool): Whether this is the initial load
        last_schema_id (str): Previous schema hash
        schema_change_summary (str): Human-readable description of changes
        fail_on_column_data_type_change (bool): Whether to fail specifically on type changes
        column_types_that_changed (list): List of columns with type changes
        schema_type_updates_summary (str): Summary of type changes
        dq_warnings (list): Accumulated warnings list
        dq_force_failures (list): Accumulated failures list
    
    Returns:
        tuple: (updated warnings list, updated failures list)
    """
    # Handle schema change failures
    schema_change_category = "Table Definition Updated"
    if new_schema and fail_on_new_schema and not first_run and last_schema_id:
        dq_schema_force_failure_message = f"Schema has changed. {schema_change_summary}."

        schema_change_failure_log_error = f"Schema change failure: {dq_schema_force_failure_message}"
        log_and_print(schema_change_failure_log_error, "error")

        json_to_append = {"category": schema_change_category, "message": dq_schema_force_failure_message}
        dq_force_failures.append(json_to_append)

    elif new_schema and fail_on_column_data_type_change and last_schema_id and not first_run and column_types_that_changed:
        dq_schema_force_failure_message = f"Column Data Types have changed. {schema_type_updates_summary}."

        column_type_update_failure_log_error = f"Column type update failure: {dq_schema_force_failure_message}"
        log_and_print(column_type_update_failure_log_error, "error")

        json_to_append = {"category": schema_change_category, "message": dq_schema_force_failure_message}
        dq_force_failures.append(json_to_append)

    elif new_schema and not first_run and last_schema_id:
        dq_schema_warn_message = f"Schema has changed. {schema_change_summary}."

        schema_change_warning_log_warn = f"Schema change warning: {dq_schema_warn_message}"
        log_and_print(schema_change_warning_log_warn, "warn")

        json_to_append = {"outcome": "Warning", "category": schema_change_category, "message": dq_schema_warn_message, "quarantined": "No"}
        dq_warnings.append(json_to_append)
    
    return dq_warnings, dq_force_failures

def finalize_processing(
    dq_force_failures: list, 
    source_details: str, 
    dq_warnings: list, 
    new_schema: bool, 
    new_data: DataFrame, 
    target_datastore_medallion_name: str, 
    new_data_schema_hash: str,
    lineage_info: dict
) -> DataFrame:
    """
    Finalize data processing with failure handling and column reordering.
    
    Performs final checks and formatting before data is written:
    - Exits notebook if critical data quality failures exist
    - Reorders columns to place system metadata at the end
    - Logs warnings and schema changes
    
    Args:
        dq_force_failures (list): List of critical data quality failures
        source_details (str): Source configuration for error reporting
        dq_warnings (list): List of data quality warnings
        new_schema (bool): Whether schema has changed
        new_data (DataFrame): Processed data
        target_datastore_medallion_name (str): Target layer name
        new_data_schema_hash (str): Current schema hash
        lineage_info (dict): Lineage tracking information for logging
    
    Returns:
        DataFrame: Processed DataFrame with schema tracking column added for Bronze/Silver layers
    
    Raises:
        DataQualityFailureError: If dq_force_failures is non-empty
    """
    # Raise exception if critical data quality failures detected.
    # The calling notebook (NB_Batch_Processing) catches DataQualityFailureError,
    # logs the failure via Log_Data_Movement, and re-raises to fail the notebook.
    if dq_force_failures:
        raise DataQualityFailureError(
            message="One or more data quality checks triggered notebook failure. Please review logging record for details.",
            dq_warnings=dq_warnings,
            dq_failures=dq_force_failures,
            source_details=source_details,
            lineage_info=lineage_info
        )

    # Add schema tracking column for Bronze and Silver layers
    if target_datastore_medallion_name in ('bronze','silver'):
        added_schema_tracking_log_info = 'Added schema tracking column, delta__schema_id.'
        log_and_print(added_schema_tracking_log_info)
        
        new_data = new_data.withColumn('delta__schema_id', f.lit(new_data_schema_hash))
    
    return new_data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _trim_string_data(
    df: DataFrame,
    trim_data_in_string_columns: str
) -> DataFrame:
    """
    Trim leading and trailing whitespace from string columns.
    
    Args:
        df (DataFrame): Input DataFrame
        trim_data_in_string_columns (str): Comma-separated list of columns to trim, or "*" for all string columns
    
    Returns:
        DataFrame: DataFrame with trimmed string values
    """
    if not trim_data_in_string_columns:
        return df
    
    if trim_data_in_string_columns.strip() == "*":
        # Trim all string columns
        trimming_whitespace_log_info = "Trimming leading and trailing whitespace for every value in every string column."
        log_and_print(trimming_whitespace_log_info)

        df = df.select([
                f.trim(f.col(column)).alias(column) if dtype == "string" else f.col(column)
                for column, dtype in df.dtypes
            ])
    else:
        # Trim specific columns
        columns_to_trim = [col.strip().lower() for col in trim_data_in_string_columns.split(",")]
        
        # Only string columns can be trimmed - identify which will actually be processed
        string_cols = [column for column, dtype in df.dtypes if dtype == "string" and column.lower() in columns_to_trim]
        
        # Log which columns are actually being trimmed
        if string_cols:
            trimming_whitespace_log_info = f"Trimming leading and trailing whitespace for string columns: {', '.join(string_cols)}"
            log_and_print(trimming_whitespace_log_info)
        
        # Warn about non-string columns that were requested but skipped
        skipped_cols = [col for col in columns_to_trim if col not in [c.lower() for c in string_cols]]
        if skipped_cols:
            skipped_cols_log_warn = f"Skipping non-string columns for trim (only string columns support this): {', '.join(skipped_cols)}"
            log_and_print(skipped_cols_log_warn, "warn")
        
        df = df.select([
                f.trim(f.col(column)).alias(column) if column.lower() in columns_to_trim and dtype == "string" else f.col(column)
                for column, dtype in df.dtypes
            ])
    
    return df


def _replace_blank_with_null(
    df: DataFrame,
    replace_blank_with_null_in_string_columns: str
) -> DataFrame:
    """
    Replace blank strings with NULL values in specified columns.
    
    Args:
        df (DataFrame): Input DataFrame
        replace_blank_with_null_in_string_columns (str): Comma-separated list of columns, or "*" for all string columns
    
    Returns:
        DataFrame: DataFrame with blank strings replaced by NULL
    """
    if not replace_blank_with_null_in_string_columns:
        return df
    
    if replace_blank_with_null_in_string_columns.strip() == "*":
        # Replace blanks in all string columns
        replacing_blank_strings_log_info = "Replacing blank values in all string columns with null."
        log_and_print(replacing_blank_strings_log_info)
        
        df = df.replace("", None)
    else:
        # Replace blanks in specific columns
        columns_to_replace = [col.strip().lower() for col in replace_blank_with_null_in_string_columns.split(",")]
        
        # Build replacement dict for specific columns - only string columns can have blank values replaced
        string_cols = [column for column, dtype in df.dtypes if dtype == "string" and column.lower() in columns_to_replace]
        
        # Log which columns are actually being processed
        if string_cols:
            replacing_blank_strings_log_info = f"Replacing blank values with null in string columns: {', '.join(string_cols)}"
            log_and_print(replacing_blank_strings_log_info)
        
        # Warn about non-string columns that were requested but skipped
        skipped_cols = [col for col in columns_to_replace if col not in [c.lower() for c in string_cols]]
        if skipped_cols:
            skipped_cols_log_warn = f"Skipping non-string columns for blank replacement (only string columns support this): {', '.join(skipped_cols)}"
            log_and_print(skipped_cols_log_warn, "warn")
        
        for col in string_cols:
            df = df.withColumn(col, f.when(f.col(col) == "", None).otherwise(f.col(col)))
    
    return df


def data_cleansing(
    df: DataFrame, 
    trim_data_in_string_columns: str,
    replace_blank_with_null_in_string_columns: str
) -> DataFrame:
    """
    Apply comprehensive data cleansing operations based on configuration.

    This function orchestrates multiple cleansing strategies to ensure data quality:
    1. String trimming - Removes leading/trailing whitespace from specified columns
    2. Blank to null conversion - Standardizes empty values in specified columns

    The cleansing operations are configurable and layer-aware, with different
    defaults for Bronze, Silver, and Gold layers to match typical use cases.
    Explicit type coercion is intentionally out of scope here; use the
    change_data_types transformation when a cast is required.

    Args:
        df (DataFrame): Input DataFrame to cleanse
        trim_data_in_string_columns (str): Comma-separated list of columns to trim whitespace, or "*" for all string columns
        replace_blank_with_null_in_string_columns (str): Comma-separated list of columns to replace blanks with null, or "*" for all string columns

    Returns:
        DataFrame: Cleansed DataFrame with applied transformations
    """
    # Step 1: Trim whitespace from specified string columns
    df = _trim_string_data(
        df=df,
        trim_data_in_string_columns=trim_data_in_string_columns
    )

    # Step 2: Replace blank strings with NULL values in specified columns
    df = _replace_blank_with_null(
        df=df,
        replace_blank_with_null_in_string_columns=replace_blank_with_null_in_string_columns
    )
        
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Transformation Functions
# 
# This section contains functions for applying metadata-driven and custom transformations to your data, including column renaming, derived columns, filtering, and entity resolution. These transformations are orchestrated based on configuration for maximum flexibility.

# CELL ********************

def _validate_columns_exist(
    df: DataFrame, 
    columns_to_check: List[str], 
    operation_name: str
) -> None:
    """Validate that specified columns exist in DataFrame before performing operations.
    
    Args:
        df: DataFrame to check
        columns_to_check: List of column names to validate
        operation_name: Name of the operation for error messaging
        
    Raises:
        Exception: If any columns are missing from the DataFrame
    """
    # spark is not case sensitive so ensure failure doesn't happen on case mismatch
    existing_columns = [col.lower() for col in df.columns]
    missing_columns = [col for col in columns_to_check if col.lower() not in existing_columns]
    
    if missing_columns:
        missing_columns_error = f"Error: The following columns do not exist in DataFrame for {operation_name}: {missing_columns}. Available columns: {existing_columns}"
        log_and_print(missing_columns_error, "error")
        raise Exception(missing_columns_error)

def _rename_columns(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Rename columns for schema alignment or business naming conventions."""
    existing_column_name = data_transformation_step['existing_column_name']
    new_column_name = data_transformation_step['new_column_name']

    existing_columns = [column.strip() for column in existing_column_name.split(',')]
    new_columns = [column.strip() for column in new_column_name.split(',')]
    
    # Validate that all columns to be renamed exist
    _validate_columns_exist(
        df = df, 
        columns_to_check = existing_columns, 
        operation_name = "rename_columns"
    )
    
    column_renames = list(zip(existing_columns, new_columns))
    for column_rename in column_renames:
        existing_column = column_rename[0]
        new_column = column_rename[1]

        renaming_column_log_info = f"Renaming column, {existing_column}, to {new_column}."
        log_and_print(renaming_column_log_info)

        df = df.withColumnRenamed(existing_column, new_column)
    
    return df

def _create_derived_column(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Create new columns using SQL expressions for calculated fields."""
    column_name = data_transformation_step['column_name']
    derived_column_logic = data_transformation_step['expression']

    creating_derived_column_log_info = f"Creating derived column, {column_name}, using logic, {derived_column_logic}."
    log_and_print(creating_derived_column_log_info)
    
    df = df.withColumn(column_name, f.expr(derived_column_logic))
    return df

def _remove_columns(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Remove columns that are not needed in target schema."""
    column_name = data_transformation_step['column_name'] 
    columns = [column.strip() for column in column_name.split(',')]
    
    # Validate that all columns to be removed exist
    _validate_columns_exist(
        df = df, 
        columns_to_check = columns, 
        operation_name = "remove_columns"
    )
    
    for column in columns:
        dropping_column_log_info = f"Dropping column, {column}."
        log_and_print(dropping_column_log_info)

        df = df.drop(column)
    
    return df

def _select_columns(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Select columns that are needed in target schema."""
    column_name = data_transformation_step['column_name'] 
    columns = [column.strip() for column in column_name.split(',')]

    retain_metadata_columns = data_transformation_step.get('retain_metadata_columns', "true").strip().lower() == "true"

    if retain_metadata_columns:
        # Only add metadata columns that aren't already in the user's selection (case-insensitive check)
        existing_columns_lower = [col.lower() for col in columns]
        metadata_columns = [col for col in df.columns if (col.startswith('scd_') or col.startswith('delta__')) and col.lower() not in existing_columns_lower]
        columns = columns + metadata_columns

    # Validate that all columns to be selected exist
    _validate_columns_exist(
        df = df, 
        columns_to_check = columns, 
        operation_name = "select_columns"
    )

    selecting_column_log_info = f"Selecting {columns}."
    log_and_print(selecting_column_log_info)

    df = df.select(*columns)
    
    return df

def _filter_data(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Apply row-level filters to subset data based on business rules."""
    filter_logic = data_transformation_step['filter_logic']

    filtering_data_log_info = f"Filtering data using filter logic, {filter_logic}."
    log_and_print(filtering_data_log_info)

    df = df.filter(filter_logic)
    return df


def _sort_data(
    df: DataFrame,
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Sort DataFrame by one or more columns with configurable sort directions.
    
    This transformation allows ordering data by one or more columns, with 
    independent control over ascending/descending direction for each column.
    Useful for ensuring consistent ordering for downstream processing,
    generating sequence numbers, or preparing data for specific output formats.
    
    Args:
        df (DataFrame): Input DataFrame to sort
        data_transformation_step (dict): Configuration containing:
            - column_name (required): Comma-separated column names to sort by
            - sort_direction (optional): Comma-separated sort directions ('asc'/'desc')
              matching each column. Default is 'asc' for all columns if not specified.
              If fewer directions than columns, remaining columns use 'asc'.
    
    Returns:
        DataFrame: Sorted DataFrame
    
    Attributes:
        - column_name (required): One or more column names to sort by, comma-separated.
          Example: "order_date, customer_id"
        - sort_direction (optional): Sort direction for each column ('asc' or 'desc').
          Comma-separated to match columns. Default: 'asc' for all.
          Example: "desc, asc" (sort order_date descending, customer_id ascending)
    
    Example:
        Configuration:
            column_name: "order_date, customer_id"
            sort_direction: "desc, asc"
        
        Result: Data sorted by order_date descending, then customer_id ascending
    
    Notes:
        - NULL values are sorted according to Spark's default behavior (nulls first for asc)
        - For deterministic results, include enough columns to ensure unique ordering
        - Sort is preserved for subsequent operations in the transformation chain
    """
    # Parse column names
    column_name = data_transformation_step['column_name']
    columns = [col.strip() for col in column_name.split(',')]
    
    # Validate columns exist
    _validate_columns_exist(
        df=df,
        columns_to_check=columns,
        operation_name="sort_data"
    )
    
    # Parse sort directions (default to 'asc' for all columns if not specified)
    sort_direction = data_transformation_step.get('sort_direction', 'asc')
    directions = [d.strip().lower() for d in sort_direction.split(',')]
    
    # Extend directions list if fewer than columns (default remaining to 'asc')
    while len(directions) < len(columns):
        directions.append('asc')
    
    # Validate sort directions
    for direction in directions:
        if direction not in ('asc', 'desc'):
            raise Exception(f"Invalid sort_direction '{direction}'. Must be 'asc' or 'desc'.")
    
    # Build sort specifications
    sort_specs = []
    sort_details = []
    for col, direction in zip(columns, directions):
        if direction == 'desc':
            sort_specs.append(f.desc(col))
        else:
            sort_specs.append(f.asc(col))
        sort_details.append(f"{col} ({direction})")
    
    sort_log_info = f"Sorting data by: {', '.join(sort_details)}."
    log_and_print(sort_log_info)
    
    df = df.orderBy(*sort_specs)
    return df


def _explode_array(
    df: DataFrame,
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Explode array column into multiple rows.
    
    Takes an array column and creates one row for each element in the array,
    duplicating other column values. Useful for normalizing nested data.
    
    Args:
        df (DataFrame): Input DataFrame
        data_transformation_step (dict): Configuration containing:
            - column_name: Name of the array column to explode
            - output_column_name: Optional name for the exploded column (default: same as input)
            - preserve_nulls: If 'true', include rows where array is null/empty (uses explode_outer)
    
    Returns:
        DataFrame: DataFrame with array exploded into rows
    
    Example:
        Input: id=1, tags=['a','b','c']
        Output: id=1, tag='a' | id=1, tag='b' | id=1, tag='c'
    """
    column_name = data_transformation_step['column_name'].strip()
    output_column_name = data_transformation_step.get('output_column_name', column_name).strip()
    preserve_nulls = data_transformation_step.get('preserve_nulls', 'false').strip().lower() == 'true'
    
    _validate_columns_exist(df=df, columns_to_check=[column_name], operation_name="explode_array")
    
    log_and_print(f"Exploding array column '{column_name}' to '{output_column_name}' (preserve_nulls: {preserve_nulls})")
    
    if preserve_nulls:
        df = df.withColumn(output_column_name, f.explode_outer(f.col(column_name)))
    else:
        df = df.withColumn(output_column_name, f.explode(f.col(column_name)))
    
    # Drop original column if output name is different
    if output_column_name != column_name:
        df = df.drop(column_name)
    
    return df


def _flatten_struct(
    df: DataFrame,
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Flatten nested struct columns into individual columns.
    
    Takes a struct column and creates separate columns for each field within
    the struct. Useful for denormalizing nested JSON/struct data.
    
    Args:
        df (DataFrame): Input DataFrame
        data_transformation_step (dict): Configuration containing:
            - column_name: Name of the struct column to flatten
            - prefix: Optional prefix for flattened column names (default: struct column name + '_')
            - fields_to_include: Optional comma-separated list of specific fields to extract (default: all)
            - drop_original: If 'true', drop the original struct column (default: true)
    
    Returns:
        DataFrame: DataFrame with struct fields as individual columns
    
    Example:
        Input: address = {street: '123 Main', city: 'NYC'}
        Output: address_street = '123 Main', address_city = 'NYC'
    """
    column_name = data_transformation_step['column_name'].strip()
    prefix = data_transformation_step.get('prefix', f"{column_name}_").strip()
    fields_to_include = data_transformation_step.get('fields_to_include', '').strip()
    drop_original = data_transformation_step.get('drop_original', 'true').strip().lower() == 'true'
    
    _validate_columns_exist(df=df, columns_to_check=[column_name], operation_name="flatten_struct")
    
    log_and_print(f"Flattening struct column '{column_name}' with prefix '{prefix}'")
    
    # Get struct fields from schema
    struct_field = df.schema[column_name]
    if not hasattr(struct_field.dataType, 'fields'):
        raise ValueError(f"Column '{column_name}' is not a struct type. Use explode_array for array types.")
    
    struct_fields = struct_field.dataType.fields
    
    # Filter fields if specified
    if fields_to_include:
        fields_list = [field.strip() for field in fields_to_include.split(',')]
        struct_fields = [field for field in struct_fields if field.name in fields_list]
    
    # Extract each field
    for field in struct_fields:
        new_col_name = f"{prefix}{field.name}"
        df = df.withColumn(new_col_name, f.col(f"{column_name}.{field.name}"))
        log_and_print(f"  Extracted field '{field.name}' to column '{new_col_name}'")
    
    # Drop original struct column if requested
    if drop_original:
        df = df.drop(column_name)
    
    return df


def _split_column(
    df: DataFrame,
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Split a string column into multiple columns based on a delimiter.
    
    Args:
        df (DataFrame): Input DataFrame
        data_transformation_step (dict): Configuration containing:
            - column_name: Name of the string column to split
            - delimiter: Character(s) to split on (default: ',')
            - output_column_names: Comma-separated names for output columns
            - max_splits: Maximum number of splits to perform (default: -1 for all)
            - drop_original: If 'true', drop the original column (default: false)
    
    Returns:
        DataFrame: DataFrame with split columns added
    
    Example:
        Input: full_name = 'John,Doe,Jr'
        Config: delimiter=',', output_column_names='first_name,last_name,suffix'
        Output: first_name='John', last_name='Doe', suffix='Jr'
    """
    column_name = data_transformation_step['column_name'].strip()
    delimiter = data_transformation_step.get('delimiter', ',')
    output_column_names = data_transformation_step['output_column_names'].strip()
    max_splits = int(data_transformation_step.get('max_splits', -1))
    drop_original = data_transformation_step.get('drop_original', 'false').strip().lower() == 'true'
    
    _validate_columns_exist(df=df, columns_to_check=[column_name], operation_name="split_column")
    
    output_cols = [col.strip() for col in output_column_names.split(',')]
    
    log_and_print(f"Splitting column '{column_name}' by '{delimiter}' into columns: {output_cols}" + 
                  (f" (max_splits: {max_splits})" if max_splits > 0 else ""))
    
    # Split the column into array
    # Spark's split() limit param = max number of resulting strings (not number of splits)
    # So max_splits=2 means we want at most 3 parts, pass limit=max_splits+1
    if max_splits > 0:
        df = df.withColumn("_split_temp", f.split(f.col(column_name), delimiter, max_splits + 1))
    else:
        df = df.withColumn("_split_temp", f.split(f.col(column_name), delimiter))
    
    # Extract each element into named columns
    for idx, col_name in enumerate(output_cols):
        df = df.withColumn(col_name, f.col("_split_temp").getItem(idx))
    
    # Clean up temp column
    df = df.drop("_split_temp")
    
    # Drop original if requested
    if drop_original:
        df = df.drop(column_name)
    
    return df


def _concat_columns(
    df: DataFrame,
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Concatenate multiple columns into a single column.
    
    Args:
        df (DataFrame): Input DataFrame
        data_transformation_step (dict): Configuration containing:
            - column_name: Comma-separated list of columns to concatenate
            - output_column_name: Name for the concatenated output column
            - separator: String to use between values (default: '')
            - null_handling: How to handle nulls - 'skip', 'empty', 'keep' (default: 'skip')
            - drop_original: If 'true', drop the original columns (default: false)
    
    Returns:
        DataFrame: DataFrame with concatenated column added
    
    Example:
        Input: first_name='John', last_name='Doe'
        Config: separator=' ', output_column_name='full_name'
        Output: full_name='John Doe'
    """
    column_names = data_transformation_step['column_name'].strip()
    output_column_name = data_transformation_step['output_column_name'].strip()
    separator = data_transformation_step.get('separator', '')
    null_handling = data_transformation_step.get('null_handling', 'skip').strip().lower()
    drop_original = data_transformation_step.get('drop_original', 'false').strip().lower() == 'true'
    
    columns = [col.strip() for col in column_names.split(',')]
    
    _validate_columns_exist(df=df, columns_to_check=columns, operation_name="concat_columns")
    
    log_and_print(f"Concatenating columns {columns} into '{output_column_name}' with separator '{separator}'")
    
    if null_handling == 'skip':
        # Use concat_ws which skips nulls
        df = df.withColumn(output_column_name, f.concat_ws(separator, *[f.col(c) for c in columns]))
    elif null_handling == 'empty':
        # Replace nulls with empty string then concat
        cols_coalesced = [f.coalesce(f.col(c).cast("string"), f.lit("")) for c in columns]
        if separator:
            df = df.withColumn(output_column_name, f.concat_ws(separator, *cols_coalesced))
        else:
            df = df.withColumn(output_column_name, f.concat(*cols_coalesced))
    else:  # 'keep' - if any null, result is null
        if separator:
            # Build concat with separator manually
            exprs = []
            for i, c in enumerate(columns):
                if i > 0:
                    exprs.append(f.lit(separator))
                exprs.append(f.col(c).cast("string"))
            df = df.withColumn(output_column_name, f.concat(*exprs))
        else:
            df = df.withColumn(output_column_name, f.concat(*[f.col(c).cast("string") for c in columns]))
    
    # Drop original columns if requested
    if drop_original:
        for col in columns:
            df = df.drop(col)
    
    return df


def _conditional_column(
    df: DataFrame,
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Create a column with conditional logic (CASE WHEN equivalent).
    
    Args:
        df (DataFrame): Input DataFrame
        data_transformation_step (dict): Configuration containing:
            - column_name: Name of the output column to create
            - conditions: Comma-separated list of Spark SQL conditions
            - values: Comma-separated list of values (must match conditions count)
            - default_value: Value to use when no conditions match (default: null)
            - values_delimiter: Delimiter for parsing (default: ',')
    
    Returns:
        DataFrame: DataFrame with conditional column added
    
    Example config:
        column_name: 'grade'
        conditions: 'score >= 90, score >= 80, score >= 70'
        values: 'A, B, C'
        values_delimiter: ','  # default
        default_value: 'F'
    """
    column_name = data_transformation_step['column_name'].strip()
    conditions_str = data_transformation_step['conditions']
    values_str = data_transformation_step['values']
    default_value = data_transformation_step.get('default_value')
    values_delimiter = data_transformation_step.get('values_delimiter', ',')
    
    log_and_print(f"Creating conditional column '{column_name}'")
    
    # Parse conditions and values as parallel lists
    conditions_list = [c.strip() for c in conditions_str.split(values_delimiter) if c.strip()]
    values_list = [v.strip() for v in values_str.split(values_delimiter) if v.strip()]
    
    if not conditions_list:
        raise ValueError("conditional_column requires at least one condition")
    
    if len(conditions_list) != len(values_list):
        raise ValueError(f"conditional_column: number of conditions ({len(conditions_list)}) must match number of values ({len(values_list)})")
    
    # Build the CASE WHEN expression
    case_expr = None
    for when_clause, then_value in zip(conditions_list, values_list):
        log_and_print(f"  WHEN {when_clause} THEN {then_value}")
        
        if case_expr is None:
            case_expr = f.when(f.expr(when_clause), f.lit(then_value))
        else:
            case_expr = case_expr.when(f.expr(when_clause), f.lit(then_value))
    
    # Add default value
    if default_value is not None:
        log_and_print(f"  ELSE {default_value}")
        case_expr = case_expr.otherwise(f.lit(default_value))
    else:
        case_expr = case_expr.otherwise(f.lit(None))
    
    df = df.withColumn(column_name, case_expr)
    
    return df


def _string_functions(
    df: DataFrame,
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Apply string transformation functions to columns.
    
    Args:
        df (DataFrame): Input DataFrame
        data_transformation_step (dict): Configuration containing:
            - column_name: Column(s) to transform. Use '*' for all string columns,
              or comma-separated column names for specific columns.
            - function: String function to apply. Supported functions:
                - upper: Convert to uppercase
                - lower: Convert to lowercase
                - trim: Remove leading/trailing whitespace
                - ltrim: Remove leading whitespace
                - rtrim: Remove trailing whitespace
                - reverse: Reverse string
                - length: Get string length
                - initcap: Capitalize first letter of each word
                - soundex: Get soundex code
            - output_column_name: Name for the output column (suffix when using '*')
    
    Returns:
        DataFrame: DataFrame with string transformations applied
    
    Note:
        For advanced string operations (substring, padding, replace, regex),
        use the derived_column transformation with Spark SQL expressions.
    """
    column_names_raw = data_transformation_step['column_name'].strip()
    function_name = data_transformation_step['function'].strip().lower()
    output_column_name = data_transformation_step['output_column_name'].strip()
    
    # Handle * for all string columns
    if column_names_raw == '*':
        columns = [field.name for field in df.schema.fields if field.dataType.simpleString() == 'string']
        if not columns:
            log_and_print("No string columns found for string_functions with '*'", "warn")
            return df
        log_and_print(f"Expanding '*' to string columns: {columns}")
    else:
        columns = [col.strip() for col in column_names_raw.split(',')]
        _validate_columns_exist(df=df, columns_to_check=columns, operation_name="string_functions")
    
    log_and_print(f"Applying string function '{function_name}' to columns: {columns}")
    
    # Supported functions (no extra params needed)
    supported_functions = ['upper', 'lower', 'trim', 'ltrim', 'rtrim', 'reverse', 'length', 'initcap', 'soundex']
    
    if function_name not in supported_functions:
        raise ValueError(f"Unknown string function '{function_name}'. Supported: {', '.join(supported_functions)}. "
                        f"For advanced operations, use derived_column with Spark SQL expressions.")
    
    for col_name in columns:
        # Determine output column name
        if len(columns) == 1:
            out_col = output_column_name
        else:
            out_col = f"{output_column_name}_{col_name}"
        
        # Apply the appropriate string function
        if function_name == 'upper':
            df = df.withColumn(out_col, f.upper(f.col(col_name)))
        elif function_name == 'lower':
            df = df.withColumn(out_col, f.lower(f.col(col_name)))
        elif function_name == 'trim':
            df = df.withColumn(out_col, f.trim(f.col(col_name)))
        elif function_name == 'ltrim':
            df = df.withColumn(out_col, f.ltrim(f.col(col_name)))
        elif function_name == 'rtrim':
            df = df.withColumn(out_col, f.rtrim(f.col(col_name)))
        elif function_name == 'reverse':
            df = df.withColumn(out_col, f.reverse(f.col(col_name)))
        elif function_name == 'length':
            df = df.withColumn(out_col, f.length(f.col(col_name)))
        elif function_name == 'initcap':
            df = df.withColumn(out_col, f.initcap(f.col(col_name)))
        elif function_name == 'soundex':
            df = df.withColumn(out_col, f.soundex(f.col(col_name)))
        
        log_and_print(f"  Applied '{function_name}' to '{col_name}' -> '{out_col}'")
    
    return df


def _normalize_text(
    df: DataFrame,
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Normalize text data for consistency and matching.
    
    Applies multiple text normalization operations in sequence to standardize
    text data for better matching and analysis.
    
    Args:
        df (DataFrame): Input DataFrame
        data_transformation_step (dict): Configuration containing:
            - column_name: Column(s) to normalize. Use '*' for all string columns,
              or comma-separated column names for specific columns.
            - operations: Comma-separated list of operations to apply in order:
                - lowercase: Convert to lowercase
                - uppercase: Convert to uppercase
                - trim: Remove leading/trailing whitespace
                - collapse_whitespace: Replace multiple spaces with single space
                - remove_punctuation: Remove punctuation characters
                - remove_digits: Remove numeric digits
                - remove_special_chars: Remove non-alphanumeric characters (keeps spaces)
                - ascii_only: Remove non-ASCII characters
                - strip_accents: Remove accent marks (é→e, ñ→n)
            - output_column_name: Optional output column name (default: overwrites input)
    
    Returns:
        DataFrame: DataFrame with normalized text
    
    Example:
        Input: '  John   DOE, Jr.  '
        operations: 'trim,lowercase,collapse_whitespace,remove_punctuation'
        Output: 'john doe jr'
    """
    column_names_raw = data_transformation_step['column_name'].strip()
    operations_str = data_transformation_step.get('operations', 'trim,lowercase,collapse_whitespace').strip()
    output_column_name = data_transformation_step.get('output_column_name', '').strip()
    
    # Handle * for all string columns
    if column_names_raw == '*':
        columns = [field.name for field in df.schema.fields if field.dataType.simpleString() == 'string']
        if not columns:
            log_and_print("No string columns found for normalize_text with '*'", "warn")
            return df
        log_and_print(f"Expanding '*' to string columns: {columns}")
    else:
        columns = [col.strip() for col in column_names_raw.split(',')]
        _validate_columns_exist(df=df, columns_to_check=columns, operation_name="normalize_text")
    
    operations = [op.strip().lower() for op in operations_str.split(',')]
    
    log_and_print(f"Normalizing text in columns {columns} with operations: {operations}")
    
    for col_name in columns:
        # Determine output column
        if output_column_name and len(columns) == 1:
            out_col = output_column_name
        else:
            out_col = col_name
        
        # Start with the original column
        result_col = f.col(col_name)
        
        for op in operations:
            if op == 'lowercase':
                result_col = f.lower(result_col)
            elif op == 'uppercase':
                result_col = f.upper(result_col)
            elif op == 'trim':
                result_col = f.trim(result_col)
            elif op == 'collapse_whitespace':
                result_col = f.regexp_replace(result_col, r'\s+', ' ')
            elif op == 'remove_punctuation':
                result_col = f.regexp_replace(result_col, r'[^\w\s]', '')
            elif op == 'remove_digits':
                result_col = f.regexp_replace(result_col, r'\d', '')
            elif op == 'remove_special_chars':
                result_col = f.regexp_replace(result_col, r'[^a-zA-Z0-9\s]', '')
            elif op == 'ascii_only':
                result_col = f.regexp_replace(result_col, r'[^\x00-\x7F]', '')
            elif op == 'strip_accents':
                # Common accent replacements
                result_col = f.regexp_replace(result_col, r'[àáâãäå]', 'a')
                result_col = f.regexp_replace(result_col, r'[èéêë]', 'e')
                result_col = f.regexp_replace(result_col, r'[ìíîï]', 'i')
                result_col = f.regexp_replace(result_col, r'[òóôõö]', 'o')
                result_col = f.regexp_replace(result_col, r'[ùúûü]', 'u')
                result_col = f.regexp_replace(result_col, r'[ýÿ]', 'y')
                result_col = f.regexp_replace(result_col, r'[ñ]', 'n')
                result_col = f.regexp_replace(result_col, r'[ç]', 'c')
                result_col = f.regexp_replace(result_col, r'[ÀÁÂÃÄÅ]', 'A')
                result_col = f.regexp_replace(result_col, r'[ÈÉÊË]', 'E')
                result_col = f.regexp_replace(result_col, r'[ÌÍÎÏ]', 'I')
                result_col = f.regexp_replace(result_col, r'[ÒÓÔÕÖ]', 'O')
                result_col = f.regexp_replace(result_col, r'[ÙÚÛÜ]', 'U')
                result_col = f.regexp_replace(result_col, r'[Ñ]', 'N')
                result_col = f.regexp_replace(result_col, r'[Ç]', 'C')
            else:
                supported_ops = ['lowercase', 'uppercase', 'trim', 'collapse_whitespace', 'remove_punctuation', 
                                'remove_digits', 'remove_special_chars', 'ascii_only', 'strip_accents']
                raise ValueError(f"Unknown normalization operation '{op}'. Supported: {', '.join(supported_ops)}")
        
        df = df.withColumn(out_col, result_col)
        log_and_print(f"  Normalized '{col_name}' -> '{out_col}'")
    
    return df


def _create_surrogate_key(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any],
    target_table_name: str, 
    primary_keys: List[str], 
    first_run: bool, 
    column_to_mark_source_data_deletion: str, 
    merge_type: str
) -> DataFrame:
    """Create surrogate key in target table."""
    creating_surrogate_key_target_log_info = "Creating surrogate key in target table."
    log_and_print(creating_surrogate_key_target_log_info)

    # Extract surrogate key column name from configuration
    dimension_table_key_column_name = data_transformation_step.get('column_name', '').strip()
    if not dimension_table_key_column_name:
        raise Exception("create_surrogate_key requires 'column_name' parameter to specify the surrogate key column name")

    surrogate_key_type = data_transformation_step['type'].strip().lower()
    if surrogate_key_type == "auto_increment":
        df = surr_keys_dim(
            df = df
            , target_table_name = target_table_name
            , primary_keys = primary_keys
            , dimension_table_key_column_name = dimension_table_key_column_name
            , first_run = first_run
        )
    else:
        raise Exception(f"Surrogate key type '{surrogate_key_type}' is not supported. Supported types are: 'auto_increment'")

    # Insert null row with -1 surrogate key for unknown dimension members
    if first_run or merge_type == 'overwrite':
        appending_null_row_log_info = "Appending null row with surrogate key of -1 to dimension table."
        log_and_print(appending_null_row_log_info)

        df = insert_null_row(
            new_data = df,
            column_to_mark_source_data_deletion = column_to_mark_source_data_deletion,
            dimension_table_key_column_name = dimension_table_key_column_name
        ) 
    
    return df

def _execute_custom_transformation_function(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any], 
    all_metadata: Dict[str, Any]
) -> DataFrame:
    """Execute custom transformation functions."""
    # Custom function configurations
    notebooks_to_run = data_transformation_step.get('notebooks_to_run',[])
    if notebooks_to_run:
        notebooks_to_run = notebooks_to_run.split(",")
        notebooks_to_run = [file_name.strip() for file_name in notebooks_to_run]

    custom_transformation_functions = data_transformation_step['functions_to_execute']
    custom_transformation_functions = custom_transformation_functions.split(",")
    custom_transformation_functions = [function_name.strip() for function_name in custom_transformation_functions]
    function_scope = data_transformation_step.get('function_scope', 'local')

    for notebook in notebooks_to_run:
        instantiate_notebook(
            notebook_name=notebook,
            function_scope=function_scope,
        )

    # Execute each custom function in order
    for function_name in custom_transformation_functions:
        running_function_log_info = f"Running function: {function_name}"
        log_and_print(running_function_log_info)

        # set advanced config to the key value pairs specific to the transformation
        all_metadata['function_config'] = data_transformation_step
        
        # Prepare standard parameters for custom functions
        function_kwargs = {
            "new_data": df,
            "metadata": all_metadata,
            "spark": spark
        }  

        # Call the function with standard parameters
        df = globals()[function_name](**function_kwargs)
    
    return df

def _validate_transformation_results(
    df: DataFrame, 
    first_run: bool,
    lineage_info: dict
) -> None:
    """Validate that transformations haven't removed all data.
    
    If the DataFrame is empty, this function raises NoDataFoundError so Batch
    Processing can log a Processed status with zero records and exit gracefully.
    First runs emit a warning through exit_gracefully_no_data.
    
    Args:
        df: DataFrame to validate
        first_run: Whether this is the first run
        lineage_info: Lineage tracking information for logging

    Raises:
        NoDataFoundError: If transformations return no rows
    """
    if df.isEmpty():
        exit_gracefully_no_data(
            source_details = "",
            watermark_value = "",
            lineage_info = lineage_info,
            first_run = first_run,
            context_message = "No data returned after applying data transformations."
        )

def _parse_drop_duplicates_columns(
    data_transformation_step: Dict[str, Any]
) -> List[str]:
    """Parse and validate the column_name parameter for drop_duplicates.
    
    Args:
        data_transformation_step: Configuration dictionary containing 'column_name'
        
    Returns:
        List of column names, or None if '*' (all columns)
        
    Raises:
        Exception: If column_name is not provided
    """
    column_name_value = data_transformation_step.get('column_name', '').strip()
    if not column_name_value:
        raise Exception("'column_name' is required for drop_duplicates. Use '*' for all columns or specify column names.")
    
    if column_name_value == '*':
        return None  # None means all columns in dropDuplicates()
    
    return [field.strip() for field in column_name_value.split(",")]


def _parse_order_specifications(
    data_transformation_step: Dict[str, Any]
) -> tuple:
    """Parse and validate order_by and order_direction parameters.
    
    Args:
        data_transformation_step: Configuration dictionary containing 'order_by' and 'order_direction'
        
    Returns:
        Tuple of (order_cols list, order_directions list)
        
    Raises:
        Exception: If order directions count doesn't match order columns count
        Exception: If invalid order direction is provided
    """
    order_by_value = data_transformation_step.get('order_by', '').strip()
    order_direction_value = data_transformation_step.get('order_direction', 'desc').strip().lower()
    
    order_cols = [col.strip() for col in order_by_value.lower().split(",")]
    order_directions = [direction.strip().lower() for direction in order_direction_value.split(',') if direction.strip()]
    
    # If single direction provided, apply to all columns
    if len(order_directions) == 1 and len(order_cols) > 1:
        order_directions = order_directions * len(order_cols)
    elif len(order_directions) != len(order_cols) and len(order_cols) > 0:
        raise Exception(
            f"Number of order directions ({len(order_directions)}) must be 1 or match number of order_by columns ({len(order_cols)})"
        )
    
    # Validate order directions
    for direction in order_directions:
        if direction not in ['asc', 'desc']:
            raise Exception(f"Invalid order direction '{direction}'. Valid options are: 'asc', 'desc'")
    
    return order_cols, order_directions


def _drop_exact_duplicates(
    df: DataFrame,
    subset_fields: List[str]
) -> DataFrame:
    """Drop exact duplicate rows based on specified columns.
    
    Args:
        df: Input DataFrame
        subset_fields: List of columns to check for duplicates, or None for all columns
        
    Returns:
        DataFrame with exact duplicates removed
    """
    if subset_fields:
        log_message = f"Dropping exact duplicates using columns: {subset_fields}. If multiple rows have the exact same values in these columns, only one will be kept."
        log_and_print(log_message)
        return df.dropDuplicates(subset_fields)
    else:
        log_message = "Dropping exact duplicates across all columns. If multiple rows have the exact same values, only one will be kept."
        log_and_print(log_message)
        return df.dropDuplicates()


def _drop_duplicates_with_ordering(
    df: DataFrame,
    subset_fields: List[str],
    order_cols: List[str],
    order_directions: List[str]
) -> DataFrame:
    """Drop duplicates keeping one row per partition based on ordering.
    
    Uses window function with row_number() to keep exactly one row per
    partition (defined by subset_fields) based on the specified ordering.
    
    Args:
        df: Input DataFrame
        subset_fields: List of columns to partition by, or None for all columns
        order_cols: List of columns to order by
        order_directions: List of sort directions ('asc' or 'desc') matching order_cols
        
    Returns:
        DataFrame with duplicates removed based on ordering
    """
    # Determine partition columns
    if subset_fields:
        partition_cols = subset_fields
        partition_log = f"columns: {subset_fields}"
    else:
        partition_cols = df.columns
        partition_log = "all columns"
    
    order_details = list(zip(order_cols, order_directions))
    log_message = f"Dropping duplicates on {partition_log}. Keeping row based on ordering: {order_details}."
    log_and_print(log_message)
    
    # Build order specifications
    order_specs = []
    for col_name, direction in zip(order_cols, order_directions):
        order_specs.append(f.desc(col_name) if direction == 'desc' else f.col(col_name))

    # Use window function to number rows and keep only the first
    window = Window.partitionBy(partition_cols).orderBy(*order_specs)
    return (df.withColumn('row', f.row_number().over(window))
              .filter(f.col('row') == 1)
              .drop('row'))


def _drop_duplicates(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Drop duplicate rows using exact match or ordered deduplication.
    
    Orchestrates the duplicate removal process by delegating to specialized
    helper functions based on the configuration.
    
    Attributes:
    - column_name (required): Columns to deduplicate on. Use '*' for all columns, or comma-separated column names.
    - order_by (optional): If specified, keeps one row per column_name combination based on ordering.
    - order_direction (optional): 'asc' or 'desc' (default: 'desc'). Can be comma-separated to match order_by columns.
    
    Mode is determined by presence of 'order_by':
    - If 'order_by' is NOT specified: Exact duplicate removal on column_name columns
    - If 'order_by' IS specified: Ordered deduplication - keeps one row per column_name combination based on order_by
    """
    # Parse column_name (required)
    subset_fields = _parse_drop_duplicates_columns(data_transformation_step)
    
    # Check if order_by is present to determine mode
    order_by_value = data_transformation_step.get('order_by', '').strip()
    
    if not order_by_value:
        # Exact duplicate removal mode
        return _drop_exact_duplicates(df, subset_fields)
    else:
        # Ordered deduplication mode
        order_cols, order_directions = _parse_order_specifications(data_transformation_step)
        return _drop_duplicates_with_ordering(df, subset_fields, order_cols, order_directions)

def _change_data_types(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Change data types for one or more columns to a single specified PySpark type."""
    
    # Extract configuration parameters
    columns = data_transformation_step['column_name']
    new_type = data_transformation_step['new_type'].strip().lower()
    
    # Parse column names (handle comma-separated)
    column_list = [col.strip() for col in columns.split(',')]
    
    # Validate columns exist
    _validate_columns_exist(
        df = df, 
        columns_to_check = column_list, 
        operation_name = "change_data_types"
    )
    
    # Apply data type changes to all specified columns
    for column_name in column_list:
        changing_data_type_log_info = f"Changing data type of column '{column_name}' to '{new_type}'."
        log_and_print(changing_data_type_log_info)
        
        if new_type in ['string', 'int', 'long', 'float', 'double', 'boolean', 'binary'] or 'decimal' in new_type:
            df = df.withColumn(column_name, f.col(column_name).cast(new_type))
        elif new_type == 'date':
            df = df.withColumn(column_name, f.to_date(f.col(column_name)))
        elif new_type == 'timestamp':
            df = df.withColumn(column_name, f.to_timestamp(f.col(column_name)))
        else:
            raise Exception(f"Invalid data type '{new_type}'. Valid types are: string, int, long, float, double, decimal, boolean, date, timestamp, binary")
    
    return df

def _replace_values(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Replace multiple different values across one or more columns with a specific value."""
    
    # Extract configuration parameters
    columns = data_transformation_step['column_name']
    values_to_replace = data_transformation_step['values_to_replace']
    replacement_value = data_transformation_step['replacement_value']
    # Optional delimiter parameter for parsing values (defaults to comma for backward compatibility)
    values_delimiter = data_transformation_step.get('values_delimiter', ',')
    
    # Parse column names (handle comma-separated)
    column_list = [col.strip() for col in columns.split(',')]
    
    # Parse values to replace (handle comma-separated)
    values_list = [value.strip() for value in values_to_replace.split(values_delimiter) if value.strip()]
    
    # Validate columns exist
    _validate_columns_exist(
        df = df, 
        columns_to_check = column_list, 
        operation_name = "replace_values"
    )
    
    # Validate that values to replace are provided
    if not values_list:
        raise Exception("'values_to_replace' must be provided and cannot be empty")
    
    # Get dtypes once for all columns
    dtypes_dict = dict(df.dtypes)
    
    # Apply value replacement to all specified columns
    for column_name in column_list:
        col_type = dtypes_dict[column_name]
        is_string_column = col_type in ['string']
        
        replacing_values_log_info = f"Replacing values {values_list} with '{replacement_value}' in column '{column_name}'."
        log_and_print(replacing_values_log_info)
        
        if is_string_column:
            # For string columns: use PySpark isin() function for safe comparison
            df = df.withColumn(
                column_name, 
                f.when(f.col(column_name).isin(values_list), f.lit(replacement_value))
                 .otherwise(f.col(column_name))
            )
        else:
            # For numeric columns: convert values and use PySpark isin() function
            converted_values = []
            for value in values_list:
                try:
                    if col_type in ['int', 'long']:
                        converted_values.append(int(value))
                    elif col_type in ['double', 'float']:
                        converted_values.append(float(value))
                    elif col_type.startswith('decimal'):
                        converted_values.append(Decimal(value))
                    else:
                        raise Exception(f"Data type '{col_type}' is not supported for replace_values action.")
                except ValueError:
                    raise Exception(f"Cannot convert value '{value}' to {col_type} type for column '{column_name}'.")
            
            if converted_values:
                # Convert replacement value to appropriate type for numeric columns
                try:
                    if col_type in ['int', 'long']:
                        converted_replacement = int(replacement_value)
                    elif col_type in ['double', 'float']:
                        converted_replacement = float(replacement_value)
                    elif col_type.startswith('decimal'):
                        converted_replacement = Decimal(replacement_value)
                    else:
                        converted_replacement = replacement_value
                    
                    # Use PySpark isin() function for safe numeric comparison
                    df = df.withColumn(
                        column_name, 
                        f.when(f.col(column_name).isin(converted_values), f.lit(converted_replacement))
                         .otherwise(f.col(column_name))
                    )
                except ValueError:
                    raise Exception(f"Cannot convert replacement value '{replacement_value}' to {col_type} type for column '{column_name}'.")
    
    return df

def _mask_sensitive_data(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Mask sensitive data using PySpark mask function."""
    
    # Extract configuration parameters
    column_name = data_transformation_step['column_name']
    upper_char = data_transformation_step.get('upper_char', 'X')
    lower_char = data_transformation_step.get('lower_char', 'X')
    digit_char = data_transformation_step.get('digit_char', 'X')
    other_char = data_transformation_step.get('other_char', None)
    
    # Parse column names (handle comma-separated)
    columns = [col.strip() for col in column_name.split(',')]
    
    # Validate columns exist
    _validate_columns_exist(
        df = df, 
        columns_to_check = columns, 
        operation_name = "mask_sensitive_data"
    )
    
    # Apply masking to all specified columns
    for column_name in columns:
        masking_log_info = f"Masking sensitive data in column '{column_name}' using mask function."
        log_and_print(masking_log_info)
        
        # Build mask function call with parameters
        if other_char is not None:
            masked_column = f.mask(
                f.col(column_name),
                f.lit(upper_char),
                f.lit(lower_char),
                f.lit(digit_char),
                f.lit(other_char)
            )
        else:
            masked_column = f.mask(
                f.col(column_name),
                f.lit(upper_char),
                f.lit(lower_char),
                f.lit(digit_char)
            )

        df = df.withColumn(column_name, masked_column)
    
    return df

def _add_row_hash(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Add hash column for change detection and data lineage with configurable algorithm.
    
    Supports two hashing algorithms via 'hash_algorithm' parameter:
    
    1. xxhash64 (default) - Fast non-cryptographic hash
       - Performance: Excellent (fastest option)
       - Collision risk: Low for typical datasets, increases with scale
       - Birthday paradox: ~50% collision chance at ~4.3 billion records (2^32)
       - At 1 billion records: ~11% collision probability
       - At 100 million records: ~0.11% collision probability
       - Best for: Standard data engineering, change detection, most use cases
    
    2. sha256 - Cryptographic hash (collision-free)
       - Performance: Good (slower than xxhash64 but still efficient)
       - Collision risk: Virtually zero (2^256 possible outputs)
       - Cryptographically secure with no known collision attacks
       - Best for: Large datasets (>100M records), compliance requirements, 
                   critical applications where collisions are unacceptable
    
    Configuration Parameters:
        - column_name: Columns to hash (comma-separated or '*' for all)
        - output_column_name: Name of the hash column (default: 'row_hash')
        - hash_algorithm: 'xxhash64' (default) or 'sha256'
    
    Examples:
        Fast hashing (default):
        {'column_name': 'id,name,date', 'output_column_name': 'row_hash'}
        
        Collision-free hashing:
        {'column_name': '*', 'output_column_name': 'row_hash', 'hash_algorithm': 'sha256'}
    """
    
    # Extract configuration parameters
    column_name = data_transformation_step.get('column_name', '*')
    output_column_name = data_transformation_step.get('output_column_name', 'row_hash')
    hash_algorithm = data_transformation_step.get('hash_algorithm', 'xxhash64').strip().lower()
    
    # Validate hash_algorithm parameter
    valid_algorithms = ['xxhash64', 'sha256']
    if hash_algorithm not in valid_algorithms:
        raise Exception(f"Invalid hash_algorithm '{hash_algorithm}'. Valid options are: {', '.join(valid_algorithms)}")
    
    # Parse column names for hashing
    if column_name == '*':
        columns_to_hash = df.columns
    else:
        columns_to_hash = [col.strip() for col in column_name.split(',')]
        # Validate columns exist
        _validate_columns_exist(
            df = df, 
            columns_to_check = columns_to_hash, 
            operation_name = "add_row_hash"
        )
    
    adding_hash_log_info = f"Adding {hash_algorithm} hash column '{output_column_name}' based on columns: {columns_to_hash}."
    log_and_print(adding_hash_log_info)
    
    # Apply hashing based on selected algorithm
    if hash_algorithm == 'xxhash64':
        # Fast non-cryptographic hash (default)
        column_refs = [f.col(col_name) for col_name in columns_to_hash]
        df = df.withColumn(output_column_name, f.xxhash64(*column_refs))
    
    elif hash_algorithm == 'sha256':
        # Collision-free cryptographic hash
        # SHA-256 requires concatenating columns into a single string first
        # Convert all columns to string and concatenate with separator
        concat_expr = f.concat_ws('|', *[f.coalesce(f.col(col_name).cast('string'), f.lit('NULL')) for col_name in columns_to_hash])
        df = df.withColumn(output_column_name, f.sha2(concat_expr, 256))
    
    return df

def _pivot_data(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Pivot data to convert rows to columns with configurable aggregation.

    Supports multiple value columns with corresponding aggregations.
    Value columns and aggregations must be comma-separated and match in count.
    Output column names follow the default Spark pivot naming convention
    (e.g., `Jan_sum(sales)`), which downstream metadata consumers should
    account for if they need deterministic aliases.
    """

    pivot_config = _prepare_pivot_configuration(
        df=df,
        data_transformation_step=data_transformation_step
    )

    agg_exprs = _build_pivot_aggregation_expressions(
        value_columns=pivot_config['value_columns'],
        aggregations=pivot_config['aggregations']
    )

    pivoting_log_info = (
        f"Pivoting data: grouping by {pivot_config['group_columns']}, pivot column '{pivot_config['pivot_column']}', "
        f"value columns {pivot_config['value_columns']}, aggregations {pivot_config['aggregations']}."
    )
    log_and_print(pivoting_log_info)

    result_df = df.groupBy(*pivot_config['group_columns']).pivot(pivot_config['pivot_column']).agg(*agg_exprs)

    return result_df


def _prepare_pivot_configuration(
    df: DataFrame,
    data_transformation_step: Dict[str, Any]
) -> Dict[str, Any]:
    """Validate and normalize pivot configuration inputs."""

    group_columns = data_transformation_step['group_columns']
    pivot_column = data_transformation_step['pivot_column']
    value_column = data_transformation_step['value_column']
    aggregation = data_transformation_step['aggregation']
    group_col_list = [col.strip() for col in group_columns.split(',')]
    value_col_list = [col.strip() for col in value_column.split(',')]
    agg_list = [agg.strip().lower() for agg in aggregation.split(',')]

    if len(value_col_list) != len(agg_list):
        raise Exception(
            f"Pivot configuration error: Number of value columns ({len(value_col_list)}) must match number of aggregations ({len(agg_list)}). "
            f"You provided value_column='{value_column}' with {len(value_col_list)} column(s) and aggregation='{aggregation}' with {len(agg_list)} aggregation(s). "
            f"Each value column needs a corresponding aggregation function. "
            f"Example: value_column='sales,quantity', aggregation='sum,avg' (2 columns, 2 aggregations)."
        )

    # Validate columns exist before attempting to pivot
    _validate_columns_exist(
        df=df,
        columns_to_check=group_col_list + [pivot_column] + value_col_list,
        operation_name="pivot_data"
    )

    return {
        'group_columns': group_col_list,
        'pivot_column': pivot_column,
        'value_columns': value_col_list,
        'aggregations': agg_list
    }


def _build_pivot_aggregation_expressions(
    value_columns: List[str],
    aggregations: List[str]
) -> List[Any]:
    """Build aggregation expressions for pivot logic."""

    agg_functions = {
        'sum': f.sum,
        'avg': f.avg,
        'min': f.min,
        'max': f.max,
        'count': f.count,
        'first': f.first,
        'last': f.last
    }

    invalid_aggs = [agg for agg in aggregations if agg not in agg_functions]
    if invalid_aggs:
        raise Exception(f"Invalid aggregation(s) '{', '.join(invalid_aggs)}' for pivot_data. Valid options are: {', '.join(agg_functions.keys())}")

    agg_exprs: List[Any] = []

    for value_col, agg in zip(value_columns, aggregations):
        agg_func = agg_functions[agg]
        agg_exprs.append(agg_func(value_col))

    return agg_exprs

def _unpivot_data(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Unpivot data to convert columns to rows (melt operation)."""
    
    # Extract configuration parameters
    id_columns = data_transformation_step['id_columns']
    value_columns = data_transformation_step['value_columns'] 
    variable_name = data_transformation_step.get('variable_name', 'variable')
    value_name = data_transformation_step.get('value_name', 'value')
    
    # Parse column lists
    id_col_list = [col.strip() for col in id_columns.split(',')]
    value_col_list = [col.strip() for col in value_columns.split(',')]
    
    # Validate columns exist
    all_columns_to_check = id_col_list + value_col_list
    _validate_columns_exist(
        df = df, 
        columns_to_check = all_columns_to_check, 
        operation_name = "unpivot_data"
    )
    
    unpivoting_log_info = f"Unpivoting data: ID columns {id_col_list}, value columns {value_col_list} -> '{variable_name}', '{value_name}'."
    log_and_print(unpivoting_log_info)
    
    # Create unpivot using stack function
    stack_expr = f"stack({len(value_col_list)}, " + ", ".join([f"'{col}', {col}" for col in value_col_list]) + f") as ({variable_name}, {value_name})"
    
    # Select ID columns and apply stack
    select_cols = id_col_list + [f.expr(stack_expr)]
    result_df = df.select(*select_cols)
    
    return result_df

def _drop_null_rows(
    df: DataFrame, 
    columns: List[str], 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Drop rows containing null values."""
    # Get logic parameter (default to 'any' for backward compatibility)
    logic = data_transformation_step.get('logic', 'any').strip().lower()
    
    if logic not in ['any', 'all']:
        raise Exception(f"Invalid logic '{logic}' for drop_rows action. Valid options are: 'any', 'all'")
    
    dropping_null_rows_log_info = f"Dropping rows where {logic} of columns {columns} contain null values."
    log_and_print(dropping_null_rows_log_info)
    
    filter_condition = None
    for column in columns:
        not_null_condition = f.col(column).isNotNull()
        if filter_condition is None:
            filter_condition = not_null_condition
        elif logic == 'any':
            # Drop rows where ANY specified column is null, so keep only rows where ALL are non-null.
            filter_condition = filter_condition & not_null_condition
        else:
            # Drop rows where ALL specified columns are null, so keep rows where ANY are non-null.
            filter_condition = filter_condition | not_null_condition

    if filter_condition is not None:
        df = df.filter(filter_condition)
    
    return df

def _replace_nulls_with_default(
    df: DataFrame, 
    columns: List[str], 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Replace null values with type-appropriate default values."""
    default_value = data_transformation_step['default_value'].strip()
    fill_values = {}
    
    # Get dtypes once for all columns with case insensitive mapping
    dtypes_dict = {col.lower(): dtype for col, dtype in df.dtypes}
    
    for column in columns:
        replacing_nulls_log_info = f"Replacing nulls in column '{column}' with default value '{default_value}' converted to appropriate data type."
        log_and_print(replacing_nulls_log_info)
        
        # Get column data type and convert default_value accordingly
        col_type = dtypes_dict[column.lower()]
        try:
            if col_type in ['int', 'long']:
                converted_value = int(default_value)
            elif col_type in ['double', 'float']:
                converted_value = float(default_value)
            elif col_type.startswith('decimal'):
                converted_value = Decimal(str(default_value))
            elif col_type in ['string']:
                converted_value = str(default_value)
            elif col_type in ['boolean']:
                converted_value = str(default_value).lower() == 'true'
            elif col_type in ['date']:
                converted_value = str(default_value)  # Keep as string for date casting
            elif col_type in ['timestamp']:
                converted_value = str(default_value)  # Keep as string for timestamp casting
            else:
                raise Exception(f"Data type '{col_type}' is not supported for replace_with_default action")
        except ValueError as e:
            raise Exception(f"Cannot convert default value '{default_value}' to {col_type} type for column '{column}': {str(e)}")
        except Exception as e:
            # Re-raise any other exceptions (including the custom ones from unsupported data types)
            raise

        fill_values[column] = converted_value
    
    # Single fillna operation for better performance
    df = df.fillna(fill_values)
    return df

def _convert_values_to_null(
    df: DataFrame, 
    columns: List[str], 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Convert specific values to null."""
    values_to_convert = data_transformation_step.get('values_to_convert', '')
    convert_empty_space = data_transformation_step.get('convert_empty_space', "false").strip().lower() == "true"
    # Optional delimiter parameter for parsing values (defaults to comma for backward compatibility)
    values_delimiter = data_transformation_step.get('values_delimiter', ',')
    
    # Parse comma-separated values to convert (if provided)
    values_list = [value.strip() for value in values_to_convert.split(values_delimiter) if value.strip()]
    
    converting_values_log_info = f"Converting values {values_list} to null in columns {columns}. Convert empty space: {convert_empty_space}. Note: String columns will be trimmed before comparison."
    log_and_print(converting_values_log_info)
    
    # Validate that at least one conversion option is specified
    if not values_list and not convert_empty_space:
        raise Exception("Either 'values_to_convert' must be provided or 'convert_empty_space' must be true")
    
    # Get dtypes once for all columns
    dtypes_dict = dict(df.dtypes)
    
    for column in columns:
        col_type = dtypes_dict[column]
        is_string_column = col_type in ['string']
            
        # Build conditions for the column type
        if is_string_column:
            # Determine values to check based on convert_empty_space setting
            check_values = values_list + [""] if convert_empty_space else values_list
            
            # For string columns: use PySpark trim and isin for safe comparison
            if check_values:
                df = df.withColumn(
                    column, 
                    f.when(f.trim(f.col(column)).isin(check_values), f.lit(None))
                     .otherwise(f.col(column))
                )
        else:
            # For numeric columns: convert values and use PySpark isin function
            converted_values = []
            for value in values_list:
                try:
                    if col_type in ['int', 'long']:
                        converted_values.append(int(value))
                    elif col_type in ['double', 'float']:
                        converted_values.append(float(value))
                    elif col_type.startswith('decimal'):
                        converted_values.append(Decimal(value))
                    else:
                        raise Exception(f"Data type '{col_type}' is not supported for convert_values_to_null action.")
                except ValueError as e:
                    raise Exception(f"Cannot convert default value '{value}' to {col_type} type for column '{column}': {str(e)}")
                except Exception as e:
                    # Re-raise any other exceptions (including the custom ones from unsupported data types)
                    raise
            
            # Use PySpark isin() function for safe numeric comparison
            if converted_values:
                df = df.withColumn(
                    column, 
                    f.when(f.col(column).isin(converted_values), f.lit(None))
                     .otherwise(f.col(column))
                )
    
    return df

def _join_data(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any],
    all_metadata: Dict[str, Any]
) -> DataFrame:
    """Join with another table or dataset using SQL-like join conditions.
    
    Join conditions should reference tables using 'a' for left table and 'b' for right table.
    Examples:
    - Simple equality: "a.customer_id = b.customer_id"
    - Multiple conditions: "a.customer_id = b.customer_id AND a.region = b.region"
    - Range joins: "a.order_date >= b.start_date AND a.order_date <= b.end_date"
    - Complex logic: "a.customer_id = b.customer_id AND (a.amount > b.threshold OR a.tier = 'Premium')"
    
    Column Selection:
    - left_columns: Comma-separated list of columns to select from left table (default: all columns with 'a.' prefix)
    - right_columns: Comma-separated list of columns to select from right table (default: all columns with 'b.' prefix)
    - Use '*' to select all columns from a table
    - Columns are automatically prefixed with table alias (a. or b.) in the result
    
    Note: For semi and anti joins, only left table columns are returned (right table is used only for filtering).
          The right_columns parameter is ignored for these join types.
    """
    
    # Extract configuration parameters
    join_type = data_transformation_step.get('join_type', 'inner').strip().lower()
    right_table_name = data_transformation_step['right_table_name']
    broadcast_hint = data_transformation_step.get('broadcast_hint', 'false').strip().lower() == 'true'
    join_condition = data_transformation_step['join_condition']  # Required parameter
    left_columns = data_transformation_step.get('left_columns', '*').strip()
    right_columns = data_transformation_step.get('right_columns', '*').strip()
    
    # Validate join condition is provided
    if not join_condition or join_condition.strip() == '':
        raise Exception("'join_condition' must be provided and cannot be empty. Use 'a' for left table and 'b' for right table in your condition.")
    
    # Validate join type
    valid_join_types = ['inner', 'left', 'outer', 'left_outer', 'full_outer', 'semi', 'anti']
    if join_type not in valid_join_types:
        raise Exception(f"Invalid join type '{join_type}'. Valid options are: {valid_join_types}")
    
    # Determine workspace name from metadata
    right_table_datastore_name = right_table_name.split('.')[0].strip()
    workspace_name = _get_datastore_config(all_metadata['datastore_config'], right_table_datastore_name, 'Workspace_Name')
    right_full_table_name = f"`{workspace_name}`.{right_table_name}"
    
    joining_log_info = f"Joining with table {right_full_table_name} using {join_type} join with condition: {join_condition}. Broadcast hint: {broadcast_hint}. Left columns: {left_columns}, Right columns: {right_columns}."
    log_and_print(joining_log_info)
    
    # Read the right table
    right_df = spark.read.table(right_full_table_name)
    
    # Apply broadcast hint if specified
    if broadcast_hint:
        right_df = f.broadcast(right_df)
        join_log_broadcast = "Applied broadcast hint to right table for performance optimization."
        log_and_print(join_log_broadcast)
    
    # Alias DataFrames for join condition referencing
    # User writes conditions like: "a.customer_id = b.customer_id AND a.date >= b.start_date"
    left_aliased = df.alias("a")
    right_aliased = right_df.alias("b")
    
    # Perform the join with aliased DataFrames
    joined_df = left_aliased.join(right_aliased, f.expr(join_condition), join_type)
    
    # For semi and anti joins, right table columns are not available in the result
    # These join types only use the right table for filtering
    is_semi_or_anti_join = join_type in ['semi', 'anti']
    
    # Build column selection list
    select_columns = []
    
    # Process left table columns
    if left_columns == '*':
        # Select all columns from left table with 'a.' prefix
        select_columns.extend([f.col(f"a.{col}") for col in df.columns])
    else:
        # Parse comma-separated list and validate
        left_col_list = [col.strip() for col in left_columns.split(',') if col.strip()]
        _validate_columns_exist(df, left_col_list, "join_data (left_columns)")
        select_columns.extend([f.col(f"a.{col}") for col in left_col_list])
    
    # Process right table columns (only for non-semi/anti joins)
    if not is_semi_or_anti_join:
        if right_columns == '*':
            # Select all columns from right table with 'b.' prefix
            select_columns.extend([f.col(f"b.{col}") for col in right_df.columns])
        else:
            # Parse comma-separated list and validate
            right_col_list = [col.strip() for col in right_columns.split(',') if col.strip()]
            _validate_columns_exist(right_df, right_col_list, "join_data (right_columns)")
            select_columns.extend([f.col(f"b.{col}") for col in right_col_list])
    elif right_columns != '*':
        # Warn if user specified right columns for semi/anti join
        warning_msg = f"Note: right_columns parameter ignored for {join_type} join. Semi and anti joins only return left table columns."
        log_and_print(warning_msg)
    
    # Apply column selection to joined DataFrame
    joined_df = joined_df.select(*select_columns)
    
    return joined_df

_WINDOW_FUNCTIONS = ['row_number', 'rank', 'dense_rank', 'first_value', 'last_value', 'sum', 'avg', 'count', 'min', 'max']
_WINDOW_AGGREGATION_FUNCTIONS = ['sum', 'avg', 'count', 'min', 'max']
_WINDOW_VALUE_FUNCTIONS = ['first_value', 'last_value']
_VALID_AGGREGATION_FUNCTIONS = ['sum', 'avg', 'count', 'min', 'max', 'first', 'last', 'count_distinct']

_DATETIME_OPERATIONS_REQUIRING_COLUMN = {
    'year', 'month', 'day', 'dayofmonth', 'dayofweek', 'dayofyear',
    'hour', 'minute', 'second', 'quarter', 'weekofyear',
    'add_days', 'subtract_days', 'add_months', 'date_diff',
    'format_date', 'to_timestamp'
}

_VALID_DATETIME_OPERATIONS = [
    'year', 'month', 'day', 'dayofmonth', 'dayofweek', 'dayofyear',
    'hour', 'minute', 'second', 'quarter', 'weekofyear', 'add_days',
    'subtract_days', 'add_months', 'date_diff', 'format_date',
    'to_timestamp', 'current_date', 'current_timestamp'
]


def _parse_window_function_config(
    data_transformation_step: Dict[str, Any]
) -> Dict[str, Any]:
    """Parse and normalize configuration for window functions."""

    column_name = data_transformation_step.get('column_name', '').strip()
    output_column_name = data_transformation_step.get('output_column_name', '').strip()
    if not output_column_name:
        raise Exception("'output_column_name' must be provided for add_window_function transformations")

    window_function = data_transformation_step['window_function'].strip().lower()
    partition_by = data_transformation_step.get('partition_by', '')
    order_by = data_transformation_step.get('order_by', '')
    order_direction_value = data_transformation_step.get('order_direction', 'asc').strip().lower()

    partition_cols = [col.strip() for col in partition_by.split(',') if col.strip()] if partition_by else []
    order_cols = [col.strip() for col in order_by.split(',') if col.strip()] if order_by else []
    order_directions = [direction.strip().lower() for direction in order_direction_value.split(',') if direction.strip()]

    if len(order_directions) == 1 and len(order_cols) > 1:
        order_directions = order_directions * len(order_cols)

    return {
        'column_name': column_name,
        'output_column_name': output_column_name,
        'window_function': window_function,
        'partition_cols': partition_cols,
        'order_cols': order_cols,
        'order_directions': order_directions
    }


def _validate_window_function_config(
    df: DataFrame,
    config: Dict[str, Any]
) -> None:
    """Validate window function configuration and ensure required columns exist."""

    window_function = config['window_function']
    if window_function not in _WINDOW_FUNCTIONS:
        raise Exception(f"Invalid window function '{window_function}'. Valid options are: {_WINDOW_FUNCTIONS}")

    if window_function in _WINDOW_AGGREGATION_FUNCTIONS + _WINDOW_VALUE_FUNCTIONS:
        if not config['column_name']:
            raise Exception(f"'column_name' must be provided for window function '{window_function}'")

    order_cols = config['order_cols']
    order_directions = config['order_directions']
    if len(order_directions) == 1 and len(order_cols) > 1:
        config['order_directions'] = order_directions * len(order_cols)
        order_directions = config['order_directions']
    elif len(order_directions) != len(order_cols) and len(order_cols) > 0:
        raise Exception(
            f"Number of order directions ({len(order_directions)}) must be 1 or match number of order_by columns ({len(order_cols)})"
        )

    for direction in order_directions:
        if direction not in ['asc', 'desc']:
            raise Exception(f"Invalid order direction '{direction}'. Valid options are: 'asc', 'desc'")

    columns_to_check = config['partition_cols'] + config['order_cols']
    if config['column_name']:
        columns_to_check.append(config['column_name'])
    if columns_to_check:
        _validate_columns_exist(
            df=df,
            columns_to_check=columns_to_check,
            operation_name="add_window_function"
        )


def _log_window_function_configuration(config: Dict[str, Any]) -> None:
    """Generate consistent logging for window function transformations."""

    source_col_info = f"source column: '{config['column_name']}'" if config['column_name'] else "no source column (ranking function)"
    order_details = list(zip(config['order_cols'], config['order_directions'])) if config['order_cols'] else []
    window_log_info = (
        f"Adding window function '{config['window_function']}' as column '{config['output_column_name']}' "
        f"({source_col_info}). Partition by: {config['partition_cols']}, Order by: {order_details}."
    )
    log_and_print(window_log_info)


def _build_window_spec(
    partition_cols: List[str],
    order_cols: List[str],
    order_directions: List[str]
) -> WindowSpec:
    """Create a WindowSpec based on partition and order configuration."""

    window_spec = Window.partitionBy(*partition_cols) if partition_cols else Window.partitionBy()

    if order_cols:
        order_specs = []
        for col_name, direction in zip(order_cols, order_directions):
            order_specs.append(f.desc(col_name) if direction == 'desc' else f.col(col_name))
        window_spec = window_spec.orderBy(*order_specs)

    return window_spec


def _apply_window_function(
    df: DataFrame,
    config: Dict[str, Any],
    window_spec: WindowSpec
) -> DataFrame:
    """Apply the requested window function to the DataFrame."""

    output_column_name = config['output_column_name']
    column_name = config['column_name']
    window_function = config['window_function']
    order_cols = config['order_cols']

    if window_function == 'row_number':
        return df.withColumn(output_column_name, f.row_number().over(window_spec))
    if window_function == 'rank':
        return df.withColumn(output_column_name, f.rank().over(window_spec))
    if window_function == 'dense_rank':
        return df.withColumn(output_column_name, f.dense_rank().over(window_spec))
    if window_function == 'first_value':
        return df.withColumn(output_column_name, f.first(column_name).over(window_spec))
    if window_function == 'last_value':
        return df.withColumn(output_column_name, f.last(column_name).over(window_spec))
    if window_function in _WINDOW_AGGREGATION_FUNCTIONS:
        agg_col = column_name if column_name else (order_cols[0] if order_cols else '*')
        if window_function == 'sum':
            return df.withColumn(output_column_name, f.sum(agg_col).over(window_spec))
        if window_function == 'avg':
            return df.withColumn(output_column_name, f.avg(agg_col).over(window_spec))
        if window_function == 'count':
            return df.withColumn(output_column_name, f.count(agg_col).over(window_spec))
        if window_function == 'min':
            return df.withColumn(output_column_name, f.min(agg_col).over(window_spec))
        if window_function == 'max':
            return df.withColumn(output_column_name, f.max(agg_col).over(window_spec))

    raise Exception(f"Invalid window function '{window_function}'. Valid options are: {_WINDOW_FUNCTIONS}")


def _add_window_function(
    df: DataFrame,
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Orchestrate window function calculations while preserving public API."""

    config = _parse_window_function_config(data_transformation_step=data_transformation_step)
    _validate_window_function_config(df=df, config=config)
    _log_window_function_configuration(config=config)
    window_spec = _build_window_spec(
        partition_cols=config['partition_cols'],
        order_cols=config['order_cols'],
        order_directions=config['order_directions']
    )
    return _apply_window_function(df=df, config=config, window_spec=window_spec)


def _parse_aggregate_config(
    data_transformation_step: Dict[str, Any]
) -> Dict[str, Any]:
    """Parse and normalize aggregate configuration inputs."""

    if 'column_name' not in data_transformation_step or not data_transformation_step['column_name'].strip():
        raise Exception("'column_name' must be provided for aggregate_data transformations")
    if 'aggregation' not in data_transformation_step or not data_transformation_step['aggregation'].strip():
        raise Exception("'aggregation' must be provided for aggregate_data transformations")
    if 'output_column_name' not in data_transformation_step or not data_transformation_step['output_column_name'].strip():
        raise Exception("'output_column_name' must be provided for aggregate_data transformations")

    group_by_columns = data_transformation_step.get('group_by_columns', '')
    group_cols = [col.strip() for col in group_by_columns.split(',') if col.strip()] if group_by_columns else []
    agg_columns = [col.strip() for col in data_transformation_step['column_name'].split(',') if col.strip()]
    agg_functions = [func.strip().lower() for func in data_transformation_step['aggregation'].split(',') if func.strip()]
    output_column_names = [col.strip() for col in data_transformation_step['output_column_name'].split(',') if col.strip()]

    return {
        'group_cols': group_cols,
        'agg_columns': agg_columns,
        'agg_functions': agg_functions,
        'output_column_names': output_column_names
    }


def _validate_aggregate_config(
    df: DataFrame,
    config: Dict[str, Any]
) -> None:
    """Validate aggregate configuration values and column existence."""

    agg_columns = config['agg_columns']
    agg_functions = config['agg_functions']
    output_column_names = config['output_column_names']
    group_cols = config['group_cols']

    if len(agg_columns) != len(agg_functions):
        raise Exception(
            f"Number of columns ({len(agg_columns)}) must match number of aggregation functions ({len(agg_functions)})"
        )
    if len(output_column_names) != len(agg_columns):
        raise Exception(
            f"Number of output column names ({len(output_column_names)}) must match number of columns to aggregate ({len(agg_columns)})"
        )

    invalid_functions = [func for func in agg_functions if func not in _VALID_AGGREGATION_FUNCTIONS]
    if invalid_functions:
        raise Exception(
            f"Invalid aggregation function(s) {invalid_functions}. Valid options are: {_VALID_AGGREGATION_FUNCTIONS}"
        )

    columns_to_check = group_cols + agg_columns
    if columns_to_check:
        _validate_columns_exist(
            df=df,
            columns_to_check=columns_to_check,
            operation_name="aggregate_data"
        )


def _log_aggregate_config(config: Dict[str, Any]) -> None:
    """Log aggregate configuration for observability."""

    agg_pairs = list(zip(config['agg_columns'], config['agg_functions']))
    aggregating_log_info = (
        f"Aggregating data. Group by: {config['group_cols']}, Column-Function pairs: {agg_pairs}, "
        f"Output names: {config['output_column_names']}."
    )
    log_and_print(aggregating_log_info)


def _build_grouped_dataframe(
    df: DataFrame,
    group_cols: List[str]
):
    """Group DataFrame by provided columns or aggregate entire dataset."""

    if group_cols:
        return df.groupBy(*group_cols)
    return df.groupBy()


def _build_aggregate_expressions(
    agg_columns: List[str],
    agg_functions: List[str],
    output_column_names: List[str]
) -> List[Any]:
    """Create Spark aggregation expressions."""

    agg_expressions: List[Any] = []
    for column_name, agg_func, output_col in zip(agg_columns, agg_functions, output_column_names):
        if agg_func == 'sum':
            agg_expressions.append(f.sum(column_name).alias(output_col))
        elif agg_func == 'avg':
            agg_expressions.append(f.avg(column_name).alias(output_col))
        elif agg_func == 'count':
            agg_expressions.append(f.count(column_name).alias(output_col))
        elif agg_func == 'min':
            agg_expressions.append(f.min(column_name).alias(output_col))
        elif agg_func == 'max':
            agg_expressions.append(f.max(column_name).alias(output_col))
        elif agg_func == 'first':
            agg_expressions.append(f.first(column_name).alias(output_col))
        elif agg_func == 'last':
            agg_expressions.append(f.last(column_name).alias(output_col))
        elif agg_func == 'count_distinct':
            agg_expressions.append(f.countDistinct(column_name).alias(output_col))
        else:
            raise Exception(
                f"Invalid aggregation function '{agg_func}' for column '{column_name}'. Valid options are: {_VALID_AGGREGATION_FUNCTIONS}"
            )
    return agg_expressions


def _aggregate_data(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Orchestrate aggregate transformations with helper utilities."""

    config = _parse_aggregate_config(data_transformation_step=data_transformation_step)
    _validate_aggregate_config(df=df, config=config)
    _log_aggregate_config(config=config)
    grouped_df = _build_grouped_dataframe(df=df, group_cols=config['group_cols'])
    agg_expressions = _build_aggregate_expressions(
        agg_columns=config['agg_columns'],
        agg_functions=config['agg_functions'],
        output_column_names=config['output_column_names']
    )
    return grouped_df.agg(*agg_expressions)


def _parse_transform_datetime_config(
    data_transformation_step: Dict[str, Any]
) -> Dict[str, Any]:
    """Parse and normalize configuration for datetime transformations."""

    operation_value = data_transformation_step.get('operation', '').strip()
    if not operation_value:
        raise Exception("'operation' must be provided for transform_datetime transformations")
    operation = operation_value.lower()

    if operation not in _VALID_DATETIME_OPERATIONS:
        raise Exception(
            f"Invalid datetime operation '{operation}'. Valid options are: {_VALID_DATETIME_OPERATIONS}"
        )

    output_column_name = data_transformation_step.get('output_column_name', '').strip()
    if not output_column_name:
        raise Exception("'output_column_name' must be provided for transform_datetime transformations")

    column_name = data_transformation_step.get('column_name', '').strip()

    return {
        'operation': operation,
        'column_name': column_name,
        'output_column_name': output_column_name,
        'days': None,
        'months': None,
        'date_format': '',
        'timestamp_format': None,
        'end_date_column': ''
    }


def _validate_transform_datetime_config(
    df: DataFrame,
    data_transformation_step: Dict[str, Any],
    config: Dict[str, Any]
) -> None:
    """Validate inputs and populate derived values for datetime transformations."""

    operation = config['operation']
    column_name = config['column_name']
    columns_to_check: List[str] = []

    if operation in _DATETIME_OPERATIONS_REQUIRING_COLUMN:
        if not column_name:
            raise Exception(f"'column_name' must be provided for transform_datetime operation '{operation}'")
        columns_to_check.append(column_name)
    elif column_name:
        columns_to_check.append(column_name)

    if operation in {'add_days', 'subtract_days'}:
        days_value = data_transformation_step.get('days', '').strip()
        if days_value == '':
            raise Exception(f"'days' must be provided for transform_datetime operation '{operation}'")
        config['days'] = int(days_value)
    elif operation == 'add_months':
        months_value = data_transformation_step.get('months', '').strip()
        if months_value == '':
            raise Exception("'months' must be provided for transform_datetime operation 'add_months'")
        config['months'] = int(months_value)
    elif operation == 'format_date':
        date_format = data_transformation_step.get('date_format', '').strip()
        if not date_format:
            raise Exception("'date_format' must be provided for transform_datetime operation 'format_date'")
        config['date_format'] = date_format
    elif operation == 'to_timestamp':
        timestamp_format = data_transformation_step.get('timestamp_format', None)
        if timestamp_format is not None:
            config['timestamp_format'] = timestamp_format

    if operation == 'date_diff':
        end_date_column = data_transformation_step.get('end_date_column', '').strip()
        if not end_date_column:
            raise Exception("'end_date_column' must be provided for transform_datetime date_diff operations")
        config['end_date_column'] = end_date_column
        columns_to_check.append(end_date_column)

    if columns_to_check:
        _validate_columns_exist(
            df = df,
            columns_to_check = columns_to_check,
            operation_name = "transform_datetime"
        )


def _log_transform_datetime_config(config: Dict[str, Any]) -> None:
    """Log details about the datetime transformation for observability."""

    column_name = config['column_name']
    operation = config['operation']
    output_column_name = config['output_column_name']

    if column_name:
        datetime_transform_log_info = (
            f"Applying datetime transformation '{operation}' to column '{column_name}' -> '{output_column_name}'."
        )
    else:
        datetime_transform_log_info = (
            f"Applying datetime transformation '{operation}' -> '{output_column_name}'."
        )
    log_and_print(datetime_transform_log_info)


def _apply_transform_datetime_operation(
    df: DataFrame,
    config: Dict[str, Any]
) -> DataFrame:
    """Execute the requested datetime transformation using explicit keyword arguments."""

    operation = config.get('operation')
    output_column_name = config.get('output_column_name')
    column_name = config.get('column_name', '')

    if not operation or not output_column_name:
        raise Exception("'operation' and 'output_column_name' must be provided for transform_datetime transformations")

    if operation == 'year':
        return df.withColumn(output_column_name, f.year(f.col(column_name)))
    if operation == 'month':
        return df.withColumn(output_column_name, f.month(f.col(column_name)))
    if operation in {'day', 'dayofmonth'}:
        return df.withColumn(output_column_name, f.dayofmonth(f.col(column_name)))
    if operation == 'dayofweek':
        return df.withColumn(output_column_name, f.dayofweek(f.col(column_name)))
    if operation == 'dayofyear':
        return df.withColumn(output_column_name, f.dayofyear(f.col(column_name)))
    if operation == 'hour':
        return df.withColumn(output_column_name, f.hour(f.col(column_name)))
    if operation == 'minute':
        return df.withColumn(output_column_name, f.minute(f.col(column_name)))
    if operation == 'second':
        return df.withColumn(output_column_name, f.second(f.col(column_name)))
    if operation == 'quarter':
        return df.withColumn(output_column_name, f.quarter(f.col(column_name)))
    if operation == 'weekofyear':
        return df.withColumn(output_column_name, f.weekofyear(f.col(column_name)))
    if operation == 'add_days':
        days_to_add = config['days']
        return df.withColumn(output_column_name, f.date_add(f.col(column_name), days_to_add))
    if operation == 'subtract_days':
        days_to_subtract = config['days']
        return df.withColumn(output_column_name, f.date_sub(f.col(column_name), days_to_subtract))
    if operation == 'add_months':
        months_to_add = config['months']
        return df.withColumn(output_column_name, f.add_months(f.col(column_name), months_to_add))
    if operation == 'date_diff':
        end_date_column = config['end_date_column']
        return df.withColumn(output_column_name, f.datediff(f.col(end_date_column), f.col(column_name)))
    if operation == 'format_date':
        date_format = config['date_format']
        return df.withColumn(output_column_name, f.date_format(f.col(column_name), date_format))
    if operation == 'to_timestamp':
        timestamp_format = config['timestamp_format']
        if timestamp_format:
            return df.withColumn(output_column_name, f.to_timestamp(f.col(column_name), timestamp_format))
        return df.withColumn(output_column_name, f.to_timestamp(f.col(column_name)))
    if operation == 'current_date':
        return df.withColumn(output_column_name, f.current_date())
    if operation == 'current_timestamp':
        return df.withColumn(output_column_name, f.current_timestamp())

    raise Exception(
        f"Invalid datetime operation '{operation}'. Valid options are: {_VALID_DATETIME_OPERATIONS}"
    )

def _transform_datetime(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Apply date/time transformations and extractions."""
    config = _parse_transform_datetime_config(data_transformation_step)
    _validate_transform_datetime_config(
        df=df,
        data_transformation_step=data_transformation_step,
        config=config
    )
    _log_transform_datetime_config(config=config)
    return _apply_transform_datetime_operation(df=df, config=config)

def _union_data(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any], 
    all_metadata: Dict[str, Any]
) -> DataFrame:
    """Union with other datasets vertically."""
    
    # Extract configuration parameters
    union_type = data_transformation_step.get('union_type', 'by_name').strip().lower()
    # Comma-separated list of datastore.schema.table_name
    union_tables = data_transformation_step['union_tables']  
    allow_missing_columns = data_transformation_step.get('allow_missing_columns', 'true').strip().lower() == 'true'
    deduplicate = data_transformation_step.get('deduplicate', 'false').strip().lower() == 'true'
    
    # Validate union type
    valid_union_types = ['by_name', 'by_position']
    if union_type not in valid_union_types:
        raise Exception(f"Invalid union type '{union_type}'. Valid options are: {valid_union_types}")
    
    # Parse union tables
    table_list = [table.strip() for table in union_tables.split(',')]
    
    union_log_info = f"Union operation: type='{union_type}', tables={table_list}, allow_missing_columns={allow_missing_columns}, deduplicate={deduplicate}."
    log_and_print(union_log_info)

    if union_type == "by_position" and allow_missing_columns:
        union_warn_log_info = f'If `union_type` = `by_position`, the `allow_missing_columns` parameter is ignored. The function will always fail on schema mismatch.'
        log_and_print(union_warn_log_info, "warn")
    
    # Start with the original DataFrame
    result_df = df
    
    # Union with each specified table
    for table_name in table_list:
        datastore_name = table_name.split('.')[0].strip()
        workspace_name = _get_datastore_config(all_metadata['datastore_config'], datastore_name, 'Workspace_Name')
        full_table_name = f"`{workspace_name}`.{table_name}"
        
        union_table_log = f"Reading and union with table: {full_table_name}"
        log_and_print(union_table_log)
        
        # Read the table to union
        union_df = spark.read.table(full_table_name)
        
        # Perform union based on type
        if union_type == 'by_name':
            result_df = result_df.unionByName(union_df, allowMissingColumns = allow_missing_columns)
        else:  # by_position
            result_df = result_df.unionAll(union_df)
    
    # Apply deduplication if requested
    if deduplicate:
        dedup_log = "Applying deduplication to union result."
        log_and_print(dedup_log)
        result_df = result_df.dropDuplicates()
    
    return result_df

def _apply_null_handling(
    df: DataFrame, 
    data_transformation_step: Dict[str, Any]
) -> DataFrame:
    """Handle null values by filling, dropping, or replacing."""
    
    column_name = data_transformation_step['column_name']
    action = data_transformation_step['action'].strip().lower()
    
    # Handle multiple columns if comma-separated
    columns = [col.strip() for col in column_name.split(',')]
    
    # Validate columns exist
    _validate_columns_exist(
        df = df, 
        columns_to_check = columns, 
        operation_name = "apply_null_handling"
    )
    
    # Validate action type
    valid_actions = ['drop_rows', 'replace_with_default', 'convert_values_to_null']

    if action == 'drop_rows':
        df = _drop_null_rows(df, columns, data_transformation_step)
    elif action == 'replace_with_default':
        df = _replace_nulls_with_default(df, columns, data_transformation_step)
    elif action == 'convert_values_to_null':
        df = _convert_values_to_null(df, columns, data_transformation_step)
    else:
        raise Exception(f"Invalid null handling action '{action}'. Valid options are: {valid_actions}")
    
    return df

def transformation_functions(
    df: DataFrame,
    data_transformation_steps: List[Dict[str, Any]],
    first_run: bool,
    target_table_name: str,
    primary_keys: List[str],
    dimension_config: dict,
    watermark_config: dict,
    merge_type: str,
    all_metadata: Dict[str, Any],
    lineage_info: dict
) -> DataFrame:
    """
    Apply metadata-driven transformations to DataFrames based on configuration.

    This function serves as a transformation orchestrator that executes various
    data transformation operations in sequence. It supports both standard
    transformations (column operations, filtering) and advanced features like
    entity resolution for matching records across datasets.

    The function processes transformations in the order they appear in the
    metadata, ensuring predictable and repeatable results.

    Args:

    Returns:
        DataFrame: Transformed DataFrame with all operations applied

    Transformation Categories:
        1. columns_to_rename: Rename columns to match target schema
        2. derived_column: Create new columns using Spark SQL expressions
        3. remove_columns: Drop unnecessary columns
        4. select_columns: Select only specified columns (with optional metadata retention)
        5. filter_data: Apply row-level filters to subset data
        6. sort_data: Sort data by one or more columns with configurable direction
        7. create_surrogate_key: Generate surrogate keys for dimension tables
        8. attach_dimension_surrogate_key: Insert surrogate keys from reference tables
        9. drop_duplicates: Remove duplicate rows using exact or primary key logic
        10. change_data_types: Change data types for one or more columns
        11. apply_null_handling: Handle null values (fill/drop/replace/convert values to null)
        12. replace_values: Replace multiple different values across one or more columns with a specific value
        13. mask_sensitive_data: Mask sensitive data for PII protection and anonymization
        14. add_row_hash: Add hash column for change detection and data lineage
        15. pivot_data: Pivot data to convert rows to columns
        16. unpivot_data: Unpivot data to convert columns to rows (melt operation)
        17. join_data: Join with another table or dataset using various join types
        18. add_window_function: Add window function calculations (rank, row_number, lag, lead, etc.)
        19. aggregate_data: Group by and aggregate data with multiple aggregation functions
        20. transform_datetime: Apply date/time transformations and extractions
        21. union_data: Union with other datasets vertically
        22. entity_resolution: Match and merge records from multiple sources
        23. custom_transformation_function: Execute user-defined transformation functions
        24. explode_array: Explode array column into multiple rows
        25. flatten_struct: Flatten nested struct columns into individual columns
        26. split_column: Split a string column into multiple columns by delimiter
        27. concat_columns: Concatenate multiple columns into a single column
        28. conditional_column: Create column with conditional logic (CASE WHEN)
        29. string_functions: Apply string transformation functions (upper, lower, trim, etc.)
        30. normalize_text: Normalize text data for consistency and matching

    Raises:
        Exception: If an unknown transformation category is specified
        Exception: If filter removes all data on first run (data quality check)

    Notes:
        - Transformations are applied sequentially in metadata order
        - Filter operations that remove all data trigger different behaviors
          based on whether it's a first run (error) or incremental (graceful exit)
        - Entity resolution is triggered by presence of specific config keys
        - All column validation is case-insensitive to handle Spark behavior
    """
    # Dispatch table: maps category → transformation function.
    # Most functions share a common signature: (df, data_transformation_step).
    # Functions needing extra args or different param names are handled below.
    _transform_dispatch = {
        "columns_to_rename":              _rename_columns,
        "derived_column":                 _create_derived_column,
        "remove_columns":                 _remove_columns,
        "select_columns":                 _select_columns,
        "filter_data":                    _filter_data,
        "sort_data":                      _sort_data,
        "create_surrogate_key":           _create_surrogate_key,
        "attach_dimension_surrogate_key": _attach_dimension_surrogate_key,
        "entity_resolution":              entity_resolution,
        "drop_duplicates":                _drop_duplicates,
        "change_data_types":              _change_data_types,
        "apply_null_handling":            _apply_null_handling,
        "replace_values":                 _replace_values,
        "mask_sensitive_data":            _mask_sensitive_data,
        "add_row_hash":                   _add_row_hash,
        "pivot_data":                     _pivot_data,
        "unpivot_data":                   _unpivot_data,
        "join_data":                      _join_data,
        "add_window_function":            _add_window_function,
        "aggregate_data":                 _aggregate_data,
        "transform_datetime":             _transform_datetime,
        "union_data":                     _union_data,
        "custom_transformation_function": _execute_custom_transformation_function,
        "explode_array":                  _explode_array,
        "flatten_struct":                 _flatten_struct,
        "split_column":                   _split_column,
        "concat_columns":                 _concat_columns,
        "conditional_column":             _conditional_column,
        "string_functions":               _string_functions,
        "normalize_text":                 _normalize_text,
    }

    # Extra keyword args required by specific categories (captured from outer scope)
    _transform_extra_args = {
        "create_surrogate_key": {
            "target_table_name": target_table_name,
            "primary_keys": primary_keys,
            "first_run": first_run,
            "column_to_mark_source_data_deletion": watermark_config['column_to_mark_source_data_deletion'],
            "merge_type": merge_type,
        },
        "attach_dimension_surrogate_key": {
            "all_metadata": all_metadata,
            "dimension_table_key_column_name": dimension_config['dimension_table_key_column_name'],
        },
        "join_data":                      {"all_metadata": all_metadata},
        "union_data":                     {"all_metadata": all_metadata},
        "custom_transformation_function": {"all_metadata": all_metadata},
    }

    # Parameter name overrides for functions with non-standard signatures.
    # Maps {standard_param: actual_param} so the dispatch can rename kwargs.
    _transform_param_overrides = {
        "entity_resolution": {
            "df": "primary_dataset_df",
            "data_transformation_step": "data_transformation_config",
        },
        "attach_dimension_surrogate_key": {
            "data_transformation_step": "surrogate_key_logic",
        },
    }

    # Process each transformation step from metadata
    for data_transformation_step in data_transformation_steps:
        category = data_transformation_step["Category"]

        fn = _transform_dispatch.get(category)
        if fn is None:
            raise Exception(f"No logic currently exists to run the {category} data transformation.")

        # Build kwargs — standard params, then rename if needed, then merge extras
        kwargs = {"df": df, "data_transformation_step": data_transformation_step}
        for old_name, new_name in _transform_param_overrides.get(category, {}).items():
            kwargs[new_name] = kwargs.pop(old_name)
        kwargs.update(_transform_extra_args.get(category, {}))

        df = fn(**kwargs)

    # Validate that transformations haven't removed all data
    # Exits gracefully if empty (subsequent runs) or raises exception (first run)
    _validate_transformation_results(
        df=df,
        first_run=first_run,
        lineage_info=lineage_info
    )
    
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Merge and Delta Table Management Functions
# 
# This section contains advanced merge patterns and Delta table management utilities that handle various data integration scenarios. These functions are critical for maintaining data consistency and implementing different merge strategies based on business requirements.
# 
# Key capabilities include:
# - **Merge Condition Generation**: Dynamic SQL condition building for complex joins
# - **Delta Table Creation**: DDL generation with liquid clustering and external location support
# - **Merge Strategies**: Multiple merge patterns including standard upserts, SCD2, selective overwrites, and deletion tracking
# - **Performance Optimization**: Leverages Delta Lake features for efficient data operations

# CELL ********************

def get_merge_condition(merge_keys: list, right_alias: str, left_alias: str):
    """
    Generate a null-safe merge condition string for DataFrame merge operations.

    This function constructs SQL join conditions that properly handle NULL values
    using Spark's null-safe equality operator (<=>). This is crucial for ensuring
    that NULL values in join keys are handled correctly during merge operations.

    Args:
        merge_keys (list): List of column names to use as join keys
        right_alias (str): Alias for the source/right DataFrame in the merge
        left_alias (str): Alias for the target/left DataFrame in the merge

    Returns:
        str: SQL merge condition string with null-safe comparisons

    Example:
        >>> get_merge_condition(['id', 'date'], 'source', 'target')
        'target.`id` <=> source.`id` AND target.`date` <=> source.`date`'

    Notes:
        - Uses backticks around column names to handle special characters
        - The <=> operator returns true when both operands are NULL
        - Essential for SCD2 implementations where NULL end dates are common
    """
    merge_condition = ""

    if not merge_keys:
        raise Exception('To enable data merging into the table, ensure that Primary Keys are specified in the Orchestration metadata table. Alternatively, update the merge_type in the primary configuration to options such as Overwrite or Append if merging is not required.')
    for pk in merge_keys:
        if merge_condition != "":
            merge_condition += " AND "
        # <=> is null safe comparison operator - handles NULL = NULL as true
        merge_condition += f"{left_alias}.`{pk}` <=> {right_alias}.`{pk}`"
    return merge_condition

def _prepare_merge_config(
    data_to_merge: DataFrame,
    primary_keys: list,
    source_alias: str,
    target_alias: str
) -> tuple[str, dict, dict]:
    """
    Prepare merge condition and update columns for Delta table merge operations.
    
    This function generates the merge condition and update column mapping needed for
    Delta Lake merge operations, automatically excluding system columns that should
    not be updated (delta__created_datetime and Change Data Feed columns).
    
    Args:
        data_to_merge (DataFrame): Source DataFrame to merge
        primary_keys (list): List of primary key column names for merge condition
        source_alias (str): Alias for source DataFrame in merge condition
        target_alias (str): Alias for target DataFrame in merge condition
    
    Returns:
        tuple[str, dict, dict]: A tuple containing:
            - merge_condition (str): SQL condition string for merge operation
            - update_columns (dict): Dictionary mapping target columns to source column references
            - insert_columns (dict): Dictionary mapping column names for inserts (excludes CDF helpers)
    
    Excluded Columns:
        - delta__created_datetime: Preserved from initial insert
        - Primary key columns: Used for matching, not updating
        - _change_type: Change Data Feed system column
        - _commit_version: Change Data Feed system column
        - _commit_timestamp: Change Data Feed system column
    
    Example:
        >>> merge_condition, update_columns, insert_columns = _prepare_merge_config(
        ...     data_to_merge=df,
        ...     primary_keys=['customer_id'],
        ...     source_alias='source',
        ...     target_alias='target'
        ... )
    """
    # Change Data Feed system columns that should be excluded from updates
    cdf_system_columns = {'_change_type', '_commit_version', '_commit_timestamp'}
    
    merge_condition = get_merge_condition(
        merge_keys=primary_keys,
        right_alias=source_alias,
        left_alias=target_alias
    )
    
    insert_columns = {
        column: f"{source_alias}.`{column}`"
        for column in data_to_merge.columns
        if column not in cdf_system_columns
    }

    update_columns = {
        column: f"{source_alias}.`{column}`"
        for column in data_to_merge.columns
        if column != "delta__created_datetime" 
           and column not in primary_keys
           and column not in cdf_system_columns
    }
    
    return merge_condition, update_columns, insert_columns

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _data_type_to_ddl(data_type) -> str:
    """
    Convert a Spark DataType to a DDL-safe string with proper backtick-escaping.

    The built-in simpleString() does not escape field names inside complex types,
    causing DDL failures when field names contain special characters (e.g., $date,
    $numberLong). This function recursively handles StructType, ArrayType, and
    MapType to produce valid DDL syntax.

    Args:
        data_type: A Spark DataType instance

    Returns:
        str: DDL-safe type string with backtick-escaped field names in structs

    Examples:
        >>> _data_type_to_ddl(StringType())
        'string'
        >>> _data_type_to_ddl(StructType([StructField("$date", StringType())]))
        'STRUCT<`$date`: string>'
    """
    if isinstance(data_type, StructType):
        fields = ", ".join(
            f"`{field.name}`: {_data_type_to_ddl(field.dataType)}"
            for field in data_type.fields
        )
        return f"STRUCT<{fields}>"
    elif isinstance(data_type, ArrayType):
        return f"ARRAY<{_data_type_to_ddl(data_type.elementType)}>"
    elif isinstance(data_type, MapType):
        return f"MAP<{_data_type_to_ddl(data_type.keyType)}, {_data_type_to_ddl(data_type.valueType)}>"
    else:
        return data_type.simpleString()


def build_table_properties(
    spark_config,
    layer_config,
    stats_properties: dict | None = None
) -> dict:
    """Build Delta TBLPROPERTIES dict for CREATE TABLE.

    Centralises all per-table Delta properties so they are set at table
    creation time rather than via session-level ``spark.conf.set`` calls.
    This is critical for high-concurrency mode where multiple notebooks
    share a single SparkSession. Existing tables are not reconciled in the
    notebook write path because metadata round-trips such as SHOW
    TBLPROPERTIES were too expensive at scale.

    Args:
        spark_config: SparkConfig dataclass (from parse_spark_configuration).
        layer_config: LayerSparkConfig dataclass (from apply_spark_configurations).
        stats_properties: Optional Delta stats table properties from
            build_statistics_columns_config().

    Returns:
        dict: ``{property_name: value}`` ready for ``create_delta_table``.
    """
    props: dict = {}

    # --- per-table settings (vary across concurrent notebooks) ---
    # Emit explicit boolean values so desired-state semantics are unambiguous.
    props['delta.enableChangeDataFeed'] = str(spark_config.enable_change_data_feed).lower()
    props['delta.autoOptimize.optimizeWrite'] = str(layer_config.optimize_write_enabled).lower()

    if layer_config.checkpoint_interval:
        props['delta.checkpointInterval'] = layer_config.checkpoint_interval

    if stats_properties:
        props.update(stats_properties)

    props['delta.parquet.vorder.enabled'] = str(layer_config.v_order_enabled).lower()

    # --- constant settings (same value everywhere, but still safer as table props) ---
    props['delta.enableDeletionVectors'] = 'true'
    props['delta.columnMapping.mode'] = 'name'
    props['delta.autoOptimize.autoCompact'] = 'true'

    return props


def create_delta_table(
    df: DataFrame,
    liquid_clustering_columns: list,
    target_table_name: str,
    output_external_location: str,
    lakehouse_table_output: bool,
    target_table_exists: bool,
    table_properties: dict = None
):
    """
    Dynamically create a Delta table with optimal configuration based on input DataFrame schema.

    This function generates and executes DDL statements to create Delta tables with:
    - Schema inference from the input DataFrame
    - Liquid clustering for optimized query performance
    - Optional external storage location support
    - Column-level comments and constraints preservation

    Args:
        df (DataFrame): Source DataFrame whose schema will be used for table creation
        liquid_clustering_columns (list): Columns to use for liquid clustering optimization
        target_table_name (str): Fully qualified table name (schema.table)
        output_external_location (str): Optional ADLS Gen2 path for external table storage
        lakehouse_table_output (bool): target is lakehouse table
        target_table_exists (bool): whether table already exists
        table_properties (dict): Delta TBLPROPERTIES key-value pairs from build_table_properties()

    Implementation Details:
        1. Creates temporary view to extract schema metadata
        2. Iterates through columns to build DDL with proper data types
        3. Preserves column descriptions and nullable constraints
        4. Applies liquid clustering if specified
        5. Supports external Delta tables for cross-platform scenarios

    Notes:
        - Temporary view is automatically cleaned up after use
        - Supports all Spark SQL data types including nested structs with special characters
        - External tables enable OneLake shortcut creation
    """

    if not lakehouse_table_output or target_table_exists:
        return

    log_and_print("Creating table.")

    column_details = [{
        "name": field.name,
        "type": _data_type_to_ddl(field.dataType),
        "nullable": field.nullable
    } for field in df.schema.fields]


    # Begin constructing CREATE TABLE statement
    spark_ddl = f"""CREATE TABLE {target_table_name} (
        """
    
    # Build column definitions with proper formatting
    cdf_system_columns = {"_change_type", "_commit_version", "_commit_timestamp"}
    column_written = False
    for column_detail in column_details:
        col_name = column_detail['name']
        data_type = column_detail['type']
        nullable = column_detail['nullable']

        # Skip Change Data Feed helper columns that should never exist in target schemas
        if col_name in cdf_system_columns:
            continue

        # Add NOT NULL constraint if column is non-nullable
        if not nullable:
            nullable_text = " NOT NULL"
        else:
            nullable_text = ""

        # Format column definition with proper comma placement
        if not column_written:
            column_definition = f"`{col_name}` {data_type} {nullable_text} \n"
            column_written = True
        else:
            column_definition = f",`{col_name}` {data_type} {nullable_text} \n"

        spark_ddl += column_definition
    
    # Add liquid clustering for query optimization
    if liquid_clustering_columns:
        liquid_clustering_columns_str = ', '.join(liquid_clustering_columns)
        clusterBy = f"CLUSTER BY ({liquid_clustering_columns_str})"
    else:
        clusterBy = ""

    # Add external location for ADLS Gen2 storage
    if output_external_location:
        external_table_path = f"LOCATION '{output_external_location}'"
    else:
        external_table_path = ""

    # Build TBLPROPERTIES clause from dict
    if table_properties:
        props_sql = ", ".join(f"'{k}' = '{v}'" for k, v in table_properties.items())
        tbl_properties = f"TBLPROPERTIES ({props_sql})"
    else:
        tbl_properties = ""

    # Complete DDL with table properties
    spark_ddl += f""")
        USING delta
        {tbl_properties}
        {clusterBy}
        {external_table_path}
    """

    # Execute the CREATE TABLE statement
    log_and_print(f"Executing DDL:\n{spark_ddl}")
    spark.sql(spark_ddl)

# Data types that Delta does not support for data skipping statistics
_DATA_SKIPPING_UNSUPPORTED_TYPES = (BooleanType, ArrayType, MapType, StructType, BinaryType, NullType)

def _filter_unsupported_stats_columns(
    df: DataFrame,
    columns: List[str]
) -> List[str]:
    """
    Filter out columns with data types unsupported by Delta data skipping.
    
    Delta Lake's data skipping feature does not support statistics collection
    on BooleanType, ArrayType, MapType, StructType, BinaryType, or NullType columns.
    Attempting to configure statistics on these types raises an IllegalArgumentException.
    
    Args:
        df (DataFrame): Source DataFrame to check column types against
        columns (List[str]): Column names to filter
    
    Returns:
        List[str]: Columns with supported types for data skipping
    """
    schema_lookup = {field.name.lower(): field.dataType for field in df.schema.fields}
    
    eligible = [col for col in columns if not isinstance(schema_lookup.get(col.lower()), _DATA_SKIPPING_UNSUPPORTED_TYPES)]
    skipped = [col for col in columns if isinstance(schema_lookup.get(col.lower()), _DATA_SKIPPING_UNSUPPORTED_TYPES)]
    
    if skipped:
        skipped_details = [(col, str(schema_lookup.get(col.lower()))) for col in skipped]
        log_and_print(f"Excluded {len(skipped)} column(s) with unsupported types for data skipping: {skipped_details}")
    
    return eligible

def build_statistics_columns_config(
    df: DataFrame,
    compute_statistics_on_columns: str,
    compute_statistics_on_first_n_columns: str,
    lakehouse_table_output: bool
) -> dict:
    """
    Configure Delta table column statistics for query optimization.
    
    This function enables column-level statistics collection on Delta tables to improve
    query performance through better query planning and optimization. Statistics help
    the Spark catalyst optimizer make informed decisions about join ordering, filter
    pushdown, and partition pruning.
    
    This function computes the relevant Delta table properties and returns them as a
    dictionary. The caller passes this value to create_delta_table() so new tables
    get the desired TBLPROPERTIES without relying on session-level spark.conf.set
    calls, which are unsafe under high-concurrency mode where multiple notebooks
    share a single SparkSession.
    
    Args:
        df (DataFrame): Source DataFrame to analyze for column statistics
        compute_statistics_on_columns (str): Comma-separated list of specific columns to collect stats on
        compute_statistics_on_first_n_columns (str): Number of first columns to collect stats on (alternative to specific columns)
        lakehouse_table_output (bool): target is lakehouse table

    Configuration Logic:
        1. If compute_statistics_on_columns is specified: Persist those exact columns in
           delta.dataSkippingStatsColumns (validates existence)
        2. If compute_statistics_on_first_n_columns is specified: Persist that count in
           delta.dataSkippingNumIndexedCols
        3. If neither specified: Return an empty dict so the accelerator does not persist
           either managed stats property and the table stays on Fabric's built-in
           automated statistics behavior
    
    Explicit Column Filtering:
        - Explicit stats columns are validated against the DataFrame schema
        - Columns with unsupported types are automatically excluded
        - Change Data Feed helper columns are always excluded
    
    Error Handling:
        - Raises ValueError if specified columns don't exist in the DataFrame
        - Provides detailed error message with available columns for debugging
        - Ensures data integrity by failing fast on invalid configurations
    
    Notes:
        - Column statistics improve query performance for filtering and joining
        - Recommended for frequently queried columns like keys, dates, and status fields
        - Statistics collection has minimal overhead during write operations
        - Cannot combine both column list and first N columns approaches
        - Columns with unsupported types (Boolean, Array, Map, Struct, Binary, Null) are
          automatically excluded to prevent IllegalArgumentException
        - When neither stats option is configured, the accelerator intentionally avoids
            persisting delta.dataSkippingStatsColumns or delta.dataSkippingNumIndexedCols
            so most tables rely on Fabric's default automated stats behavior
        - Existing tables are not reconciled in the notebook write path. Change
            stats properties with an explicit table recreate or ALTER TABLE flow
            outside the per-table processing notebook.

    Raises:
        ValueError: When compute_statistics_on_columns contains non-existent columns
    """

    if not lakehouse_table_output:
        return {}

    # Rule 88 is now checked upstream in parse_performance_configuration() in HF3
    # where it catches the conflict during config parsing, before any defaults are applied.
    
    if not compute_statistics_on_columns and not compute_statistics_on_first_n_columns:
        log_and_print(
            "No explicit statistics columns configured. Leaving Delta stats properties unmanaged and relying on Fabric automated statistics defaults."
        )
        return {}

    stats_columns: list = []
    
    if compute_statistics_on_columns:
        # Use specific columns provided by user
        stats_columns = [col.strip() for col in compute_statistics_on_columns.split(',')]

        log_and_print(f"Configuring statistics for columns: {stats_columns}.")
        
        # Validate all specified columns exist in DataFrame - fail fast if any are missing (case-insensitive)
        existing_columns_lower = [col.lower() for col in df.columns]
        missing_columns = [col for col in stats_columns if col.lower() not in existing_columns_lower]
        if missing_columns:
            error_msg = f"Cannot configure statistics for non-existent columns: {missing_columns}. Available columns: {df.columns}"
            raise ValueError(error_msg)
        
        # Filter out unsupported types even for explicitly specified columns
        stats_columns = _filter_unsupported_stats_columns(df, stats_columns)

    # Exclude Change Data Feed system columns - these exist in the DataFrame when CDF
    # is enabled on the source table but are never part of the target table schema.
    # Every other write-path function (create_delta_table, _prepare_merge_config,
    # _handle_first_run) already strips them; statistics must do the same.
    cdf_system_columns = {"_change_type", "_commit_version", "_commit_timestamp"}
    if compute_statistics_on_columns:
        stats_columns = [col for col in stats_columns if col not in cdf_system_columns]

        if not stats_columns:
            log_and_print("No columns eligible for data skipping statistics after filtering unsupported types. Skipping statistics configuration.")
            return {}

        # Build the comma-separated, backtick-quoted column list
        stats_config_with_backticks = [f"`{col}`" for col in stats_columns]
        stats_config_comma_separated = ",".join(stats_config_with_backticks)
        log_and_print(f"Data skipping statistics columns: {stats_config_comma_separated}")
        return {'delta.dataSkippingStatsColumns': stats_config_comma_separated}

    n_columns = int(compute_statistics_on_first_n_columns)
    log_and_print(f"Configuring statistics for first {n_columns} indexed columns via delta.dataSkippingNumIndexedCols.")
    return {'delta.dataSkippingNumIndexedCols': str(n_columns)}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


def _apply_datetime_substitutions(path: str) -> str:
    """
    Apply datetime placeholder substitutions to a file path.
    
    Imports datetime locally to avoid conflicts with custom functions that may
    have modified the datetime module in the global namespace.
    
    Supported placeholders:
        %Y - Four-digit year (e.g., 2026)
        %m - Two-digit month (01-12)
        %d - Two-digit day of month (01-31)
        %B - Full month name (e.g., January)
        %b - Abbreviated month name (e.g., Jan)
        %H - Two-digit hour in 24-hour format (00-23)
        %M - Two-digit minute (00-59)
        %S - Two-digit second (00-59)
        %f - Microseconds (000000-999999)
        %j - Day of year (001-366)
        %W - Week number of year (00-53)
        %A - Full weekday name (e.g., Monday)
        %a - Abbreviated weekday name (e.g., Mon)
    
    Args:
        path: File path potentially containing datetime placeholders
        
    Returns:
        Path with all datetime placeholders replaced with current values
    """
    from datetime import datetime as dt_module
    dt_now = dt_module.now()
    
    # Define all supported datetime placeholders
    datetime_placeholders = [
        '%Y',  # Four-digit year
        '%m',  # Two-digit month
        '%d',  # Two-digit day
        '%B',  # Full month name
        '%b',  # Abbreviated month name
        '%H',  # Hour (24-hour)
        '%M',  # Minute
        '%S',  # Second
        '%f',  # Microseconds
        '%j',  # Day of year
        '%W',  # Week number
        '%A',  # Full weekday name
        '%a',  # Abbreviated weekday name
    ]
    
    result_path = path
    for placeholder in datetime_placeholders:
        if placeholder in result_path:
            result_path = result_path.replace(placeholder, dt_now.strftime(placeholder))
    
    return result_path


def _write_spark_file_with_rename(
    data_to_merge: DataFrame,
    output_path: str,
    file_format: str,
    delimiter: str = ','
) -> None:
    """
    Write DataFrame using Spark to avoid OOM errors, then rename to desired filename.
    
    Spark writes files to directories with part-xxxx naming which is not consumer-friendly.
    This function writes to a temp directory, then uses notebookutils to rename the file
    to the exact path specified by the user.
    
    Args:
        data_to_merge: Spark DataFrame to write
        output_path: Desired output file path (abfss://...)
        file_format: File format ('csv', 'txt', 'parquet', 'json', 'jsonl', or 'ndjson')
        delimiter: Field delimiter for CSV/TXT files (default: ',')
    """
    import uuid
    
    # Generate unique temp directory path adjacent to target file
    path_without_filename = output_path.rsplit('/', 1)[0]
    temp_dir_name = f"_temp_spark_output_{uuid.uuid4().hex[:8]}"
    temp_dir_path = f"{path_without_filename}/{temp_dir_name}"
    
    write_spark_file_log_info = f"Writing {file_format} file using Spark to avoid memory issues. Temp path: {temp_dir_path}"
    log_and_print(write_spark_file_log_info)
    
    try:
        # Write using Spark with coalesce(1) to produce single file
        # This avoids pandas OOM while producing a single output file
        if file_format in ('csv', 'txt'):
            data_to_merge.coalesce(1).write.mode("overwrite").option("header", "true").option("delimiter", delimiter).csv(temp_dir_path)
        elif file_format == 'parquet':
            data_to_merge.coalesce(1).write.mode("overwrite").parquet(temp_dir_path)
        elif file_format in ('json', 'jsonl', 'ndjson'):
            # JSON Lines format - one JSON object per line (Spark's native JSON output)
            # Note: All JSON formats produce the same output; .json extension is kept for 
            # compatibility but consumers should expect JSON Lines, not a JSON array
            data_to_merge.coalesce(1).write.mode("overwrite").json(temp_dir_path)
        
        # Find the actual output file (part-00000-xxx.format)
        files = notebookutils.fs.ls(temp_dir_path)
        data_file = None
        for file_info in files:
            file_name = file_info.name
            if file_name.startswith("part-") and not file_name.endswith(".crc"):
                data_file = file_name
                break
        
        if data_file is None:
            raise Exception(f"Could not find output data file in temp directory: {temp_dir_path}")
        
        temp_file_path = f"{temp_dir_path}/{data_file}"
        
        # Move/rename to the desired output path
        rename_log_info = f"Renaming temp file to final destination: {output_path}"
        log_and_print(rename_log_info)
        notebookutils.fs.mv(temp_file_path, output_path, overwrite=True)
        
    finally:
        # Clean up temp directory (non-critical - main write already succeeded if we got here)
        try:
            notebookutils.fs.rm(temp_dir_path, recurse=True)
        except Exception as cleanup_error:
            # Log but don't raise - cleanup failure shouldn't mask a successful write operation
            cleanup_warning_msg = f"Warning: Could not clean up temp directory {temp_dir_path}: {cleanup_error}. The file was written successfully but orphaned temp files may need manual cleanup."
            log_and_print(cleanup_warning_msg, level="warn")


def _write_excel_file(
    data_to_merge: DataFrame, 
    output_path: str, 
    sheet_name: str
) -> None:
    """
    Write DataFrame to Excel file using pandas.
    
    Excel requires pandas as there is no Spark Excel writer in Fabric runtime.
    Includes row count validation to prevent exceeding Excel limits or causing
    out-of-memory errors during Spark-to-pandas conversion.
    
    Limits:
        - Hard limit: 1,048,576 rows (Excel maximum)
        - Warning at: 500,000 rows (pandas conversion may cause OOM)
    
    Args:
        data_to_merge: Spark DataFrame to write
        output_path: Desired output file path (abfss://...)
        sheet_name: Name of the Excel sheet to write to
        
    Raises:
        Exception: If row count exceeds Excel's maximum limit
    """
    row_count = data_to_merge.count()
    
    # Excel has a hard limit of ~1M rows, and pandas conversion becomes risky above ~500K
    excel_row_limit = 1048576  # Excel max rows
    pandas_safe_limit = 500000  # Recommended limit for pandas conversion
    
    if row_count > excel_row_limit:
        error_msg = f"Error: Dataset has {row_count:,} rows which exceeds Excel's maximum of {excel_row_limit:,} rows. Use CSV or Parquet format instead."
        log_and_print(error_msg, level="ERROR")
        raise Exception(error_msg)
    
    if row_count > pandas_safe_limit:
        warning_msg = f"Warning: Dataset has {row_count:,} rows. Large Excel files may cause memory issues. Consider using CSV or Parquet format for datasets over {pandas_safe_limit:,} rows."
        log_and_print(warning_msg, level="warn")
    
    # Arrow optimization is enabled by default in Fabric Runtime 1.3+ (Spark 3.5)
    # which provides more efficient Spark to Pandas conversion
    excel_log_info = f"Writing Excel file: {output_path} (sheet: {sheet_name}, rows: {row_count:,})"
    log_and_print(excel_log_info)
    
    data_to_merge_pandas = data_to_merge.toPandas()
    data_to_merge_pandas.to_excel(output_path, sheet_name=sheet_name, index=False)


def _write_output_file(
    data_to_merge: DataFrame,
    target_abfss_path: str,
    target_config: dict
) -> None:
    """
    Route file output to format-specific handlers with datetime substitution.
    
    This is the main dispatcher for file output operations. It applies datetime
    placeholders to the output path, determines the file format from the extension,
    and routes to the appropriate format-specific handler.
    
    For large datasets, Spark-based handlers (CSV, TXT, Parquet, JSON, JSONL) avoid
    out-of-memory errors. Excel uses pandas as there is no Spark alternative.
    
    Dynamic datetime placeholders supported in file path:
        %Y, %m, %d, %B, %b, %H, %M, %S, %f, %j, %W, %A, %a
        
    Example paths:
        - reports_%Y-%m-%d.csv -> reports_2026-01-22.csv
        - data_%Y%m%d_%H%M%S.parquet -> data_20260122_143025.parquet
        - monthly/%B_%Y/export.xlsx -> monthly/January_2026/export.xlsx
        - events_%Y%m%d.jsonl -> events_20260122.jsonl
        
    Supported formats:
        - csv: Comma-separated values with header row
        - txt: Text file (same as CSV format)
        - parquet: Columnar binary format (efficient for analytics)
        - json: JSON Lines format (one JSON object per line)
        - jsonl: JSON Lines format (explicit extension)
        - ndjson: Newline-delimited JSON (alias for JSON Lines)
        - xls/xlsx: Excel workbook (requires pandas, limited to ~1M rows)
    
    Args:
        data_to_merge: Spark DataFrame to write
        target_abfss_path: Target file path with optional datetime placeholders
        target_config: Configuration dict containing format-specific options
                       (e.g., target_excel_sheet_name for Excel files,
                        target_output_delimiter for CSV/TXT files)
    
    Raises:
        Exception: If file extension is not supported
    """
    # Apply datetime substitutions (imports datetime locally to avoid conflicts)
    output_path = _apply_datetime_substitutions(target_abfss_path)

    # Determine file format from extension
    target_file_extension = output_path.split('.')[-1].lower()
    
    # Get custom delimiter if configured (empty string means use format default)
    custom_delimiter = target_config['target_output_delimiter']
    
    writing_file_log_info = f"Writing output file: {output_path} (format: {target_file_extension})"
    log_and_print(writing_file_log_info)
    
    # Route to appropriate handler
    if target_file_extension == 'csv':
        delimiter = custom_delimiter if custom_delimiter else ','
        _write_spark_file_with_rename(data_to_merge, output_path, 'csv', delimiter)
        
    elif target_file_extension == 'txt':
        delimiter = custom_delimiter if custom_delimiter else '\t'
        _write_spark_file_with_rename(data_to_merge, output_path, 'txt', delimiter)
        
    elif target_file_extension == 'parquet':
        _write_spark_file_with_rename(data_to_merge, output_path, 'parquet')
        
    elif target_file_extension in ('json', 'jsonl', 'ndjson'):
        _write_spark_file_with_rename(data_to_merge, output_path, 'json')
        
    elif target_file_extension in ('xls', 'xlsx'):
        sheet_name = target_config.get('target_excel_sheet_name', 'Sheet1')
        _write_excel_file(data_to_merge, output_path, sheet_name)
        
    else:
        allowed_extensions = "'csv', 'txt', 'parquet', 'json', 'jsonl', 'ndjson', 'xls', 'xlsx'"
        raise Exception(
            f"Invalid file extension '{target_file_extension}'. "
            f"Allowed extensions are {allowed_extensions}."
        )


def _write_warehouse_spark_connector(
    data_to_merge: DataFrame,
    target_datastore_workspace_id: str,
    target_table_name: str,
    warehouse_write_mode: str
) -> None:
    """Write data to Fabric Warehouse table using spark connector."""
    writing_warehouse_table_log_info = f"Writing data to Fabric Warehouse table using {warehouse_write_mode} logic."
    log_and_print(writing_warehouse_table_log_info)

    target_table_name_without_ws = target_table_name.split('.', 1)[1]

    data_to_merge.write.mode(warehouse_write_mode).option(Constants.WorkspaceId, target_datastore_workspace_id) \
                   .synapsesql(target_table_name_without_ws)

def _handle_first_run(
    data_to_merge: DataFrame,
    target_abfss_path: str
) -> None:
    """Handle first run write operation."""
    first_run_log_info = "This is the first run. Appending data to table."
    log_and_print(first_run_log_info)

    # Drop Change Data Feed helper columns so they are never written to user tables
    cdf_system_columns = [col for col in ("_change_type", "_commit_version", "_commit_timestamp") if col in data_to_merge.columns]
    if cdf_system_columns:
        log_and_print(f"Dropping Change Data Feed helper columns on first run before insert: {cdf_system_columns}.")
        cleaned_data = data_to_merge.drop(*cdf_system_columns)
    else:
        cleaned_data = data_to_merge

    cleaned_data.write.mode("append").save(target_abfss_path)

def _handle_append(
    data_to_merge: DataFrame,
    target_abfss_path: str
) -> None:
    """Handle simple append operation."""
    appending_data_log_info = "Appending data to existing table"
    log_and_print(appending_data_log_info)

    data_to_merge.write.mode("append").save(target_abfss_path)

def _handle_overwrite(
    data_to_merge: DataFrame,
    target_abfss_path: str,
    data_skipping_stats_columns: str = ""
) -> None:
    """Handle full table replacement with schema evolution."""
    overwriting_data_log_info = "Overwriting data in existing table"
    log_and_print(overwriting_data_log_info)

    try:
        data_to_merge.write.mode("overwrite").option("overwriteSchema", "true").save(target_abfss_path)
    except Exception as error:
        error_text = str(error)
        if "DELTA_COLUMN_NOT_FOUND_IN_SCHEMA" not in error_text:
            raise

        table_identifier = f"delta.`{target_abfss_path}`"
        current_props = {
            row[0]: row[1]
            for row in spark.sql(f"SHOW TBLPROPERTIES {table_identifier}").collect()
        }
        if 'delta.dataSkippingStatsColumns' not in current_props:
            raise

        log_and_print(
            "Overwrite failed due to stale delta.dataSkippingStatsColumns; clearing property and retrying once.",
            "warn"
        )
        spark.sql(
            f"ALTER TABLE {table_identifier} UNSET TBLPROPERTIES IF EXISTS ('delta.dataSkippingStatsColumns')"
        )
        data_to_merge.write.mode("overwrite").option("overwriteSchema", "true").save(target_abfss_path)

        if data_skipping_stats_columns:
            log_and_print(
                f"Reapplying valid delta.dataSkippingStatsColumns after overwrite retry: {data_skipping_stats_columns}."
            )
            spark.sql(
                f"ALTER TABLE {table_identifier} SET TBLPROPERTIES ('delta.dataSkippingStatsColumns' = '{data_skipping_stats_columns}')"
            )

def _load_target_delta_table(target_abfss_path: str) -> DeltaTable:
    """Load the target Delta table for merge-based write operations."""
    return DeltaTable.forPath(spark, target_abfss_path)

def _handle_merge(
    target_table: DeltaTable,
    data_to_merge: DataFrame,
    primary_keys: list,
    target_alias: str,
    source_alias: str
) -> None:
    """Handle standard upsert operation."""
    merging_data_log_info = "Merging data to existing table"
    log_and_print(merging_data_log_info)
    
    merge_condition, update_columns, insert_columns = _prepare_merge_config(
        data_to_merge=data_to_merge,
        primary_keys=primary_keys,
        source_alias=source_alias,
        target_alias=target_alias
    )
    
    target_table.alias(target_alias).merge(source = data_to_merge.alias(source_alias)
                    ,condition = merge_condition
                ).whenMatchedUpdate(set = update_columns
                ).whenNotMatchedInsert(values = insert_columns
                ).execute()

def _get_dataframe_column_data_type(
    dataframe: DataFrame,
    column_name: str
):
    """Return a Spark data type for a column, matching case-insensitively as a fallback."""
    for field in dataframe.schema.fields:
        if field.name == column_name:
            return field.dataType

    column_name_lower = column_name.lower()
    for field in dataframe.schema.fields:
        if field.name.lower() == column_name_lower:
            return field.dataType

    return None


def _coerce_delete_marker_value(
    delete_rows_with_value: Any,
    column_data_type: Any
) -> Any:
    """Coerce configured delete marker values to match the Spark column type."""
    if delete_rows_with_value is None:
        return None

    raw_value = delete_rows_with_value.strip() if isinstance(delete_rows_with_value, str) else delete_rows_with_value

    try:
        if isinstance(column_data_type, (ByteType, ShortType, IntegerType, LongType)):
            return int(raw_value)
        if isinstance(column_data_type, (FloatType, DoubleType)):
            return float(raw_value)
        if isinstance(column_data_type, DecimalType):
            return Decimal(str(raw_value))
        if isinstance(column_data_type, BooleanType):
            if isinstance(raw_value, bool):
                return raw_value
            normalized_value = str(raw_value).strip().lower()
            if normalized_value in ("true", "1", "yes", "y"):
                return True
            if normalized_value in ("false", "0", "no", "n"):
                return False
            raise ValueError(f"Cannot interpret '{delete_rows_with_value}' as a boolean delete marker.")
        if isinstance(column_data_type, DateType) and not isinstance(column_data_type, (TimestampType, TimestampNTZType)):
            if isinstance(raw_value, datetime):
                return raw_value.date()
            if isinstance(raw_value, date):
                return raw_value
            return date.fromisoformat(str(raw_value))
        if isinstance(column_data_type, (TimestampType, TimestampNTZType)):
            if isinstance(raw_value, datetime):
                return raw_value
            if isinstance(raw_value, date):
                return datetime.combine(raw_value, datetime.min.time())
            return datetime.fromisoformat(str(raw_value).replace("Z", "+00:00"))
    except Exception as error:
        raise ValueError(
            f"delete_rows_with_value '{delete_rows_with_value}' cannot be cast to "
            f"{column_data_type} for the source deletion marker column."
        ) from error

    return raw_value


def _delete_marker_literal_for_dataframe(
    delete_rows_with_value: Any,
    dataframe: DataFrame,
    column_name: str
):
    """Build a typed Python literal for f.lit based on a DataFrame column."""
    column_data_type = _get_dataframe_column_data_type(dataframe, column_name)
    return _coerce_delete_marker_value(delete_rows_with_value, column_data_type)


def _delete_marker_sql_literal_for_dataframe(
    delete_rows_with_value: Any,
    dataframe: DataFrame,
    column_name: str
) -> str:
    """Build a typed SQL literal for delete marker comparisons."""
    column_data_type = _get_dataframe_column_data_type(dataframe, column_name)
    coerced_value = _coerce_delete_marker_value(delete_rows_with_value, column_data_type)

    if coerced_value is None:
        return "NULL"
    if isinstance(column_data_type, BooleanType):
        return "true" if coerced_value else "false"
    if isinstance(column_data_type, (ByteType, ShortType, IntegerType, LongType, FloatType, DoubleType, DecimalType)):
        return str(coerced_value)
    if isinstance(column_data_type, DateType) and not isinstance(column_data_type, (TimestampType, TimestampNTZType)):
        return f"DATE '{coerced_value.isoformat()}'"
    if isinstance(column_data_type, TimestampNTZType):
        return f"TIMESTAMP_NTZ '{coerced_value.strftime('%Y-%m-%d %H:%M:%S.%f')}'"
    if isinstance(column_data_type, TimestampType):
        return f"TIMESTAMP '{coerced_value.strftime('%Y-%m-%d %H:%M:%S.%f')}'"

    return _escape_sql_string_value(str(coerced_value))

def _handle_merge_and_delete(
    target_table: DeltaTable,
    data_to_merge: DataFrame,
    primary_keys: list,
    target_alias: str,
    source_alias: str,
    delete_rows_with_value: str,
    column_to_mark_source_data_deletion: str
) -> None:
    """Handle merge with deletion of marked records."""
    deleting_marked_data_log_info = f"Deleting data marked as deleted in source. All rows with a value of {delete_rows_with_value} in column, {column_to_mark_source_data_deletion}, are deleted."
    log_and_print(deleting_marked_data_log_info)
            
    merge_condition, update_columns, insert_columns = _prepare_merge_config(
        data_to_merge=data_to_merge,
        primary_keys=primary_keys,
        source_alias=source_alias,
        target_alias=target_alias
    )

    delete_marker_sql_literal = _delete_marker_sql_literal_for_dataframe(
        delete_rows_with_value = delete_rows_with_value,
        dataframe = data_to_merge,
        column_name = column_to_mark_source_data_deletion
    )

    target_table.alias(target_alias).merge(source = data_to_merge.alias(source_alias)
                , condition = merge_condition
            ).whenMatchedUpdate(
                    condition = f"{source_alias}.`{column_to_mark_source_data_deletion}` != {delete_marker_sql_literal} OR {source_alias}.`{column_to_mark_source_data_deletion}` IS NULL"
                    ,set = update_columns
            ).whenMatchedDelete(
                condition = f"{source_alias}.`{column_to_mark_source_data_deletion}` = {delete_marker_sql_literal}"
            ).whenNotMatchedInsert(values = insert_columns).execute()

def _handle_merge_mark_unmatched_deleted(
    target_table: DeltaTable,
    data_to_merge: DataFrame,
    primary_keys: list,
    target_alias: str,
    source_alias: str,
    delete_rows_with_value: str,
    column_to_mark_source_data_deletion: str
) -> None:
    """Handle merge with deletion tracking for unmatched target records."""
    merging_mark_unmatched_log_info = "Merging data to existing table and marking target records not found in source dataset as deleted."
    log_and_print(merging_mark_unmatched_log_info)
    
    merge_condition, update_columns, insert_columns = _prepare_merge_config(
        data_to_merge=data_to_merge,
        primary_keys=primary_keys,
        source_alias=source_alias,
        target_alias=target_alias
    )

    target_delete_marker_value = _delete_marker_literal_for_dataframe(
        delete_rows_with_value = delete_rows_with_value,
        dataframe = target_table.toDF(),
        column_name = column_to_mark_source_data_deletion
    )

    data_to_merge = data_to_merge.withColumn(column_to_mark_source_data_deletion, f.lit(None))

    target_table.alias(target_alias) \
                .merge(source = data_to_merge.alias(source_alias)
                    ,condition = merge_condition
                ).whenMatchedUpdate(set = update_columns) \
                .whenNotMatchedInsert(values = insert_columns) \
                .whenNotMatchedBySourceUpdate(set={column_to_mark_source_data_deletion: f.lit(target_delete_marker_value)}) \
                .execute()

def _handle_merge_mark_all_deleted(
    target_table: DeltaTable,
    data_to_merge: DataFrame,
    primary_keys: list,
    target_alias: str,
    source_alias: str,
    delete_rows_with_value: str,
    column_to_mark_source_data_deletion: str
) -> None:
    """Handle merge marking all matched records as deleted (soft delete)."""
    merging_mark_all_deleted_log_info = "Merging data to existing table and marking all matched records as deleted."
    log_and_print(merging_mark_all_deleted_log_info)
    
    delete_marker_value = _delete_marker_literal_for_dataframe(
        delete_rows_with_value = delete_rows_with_value,
        dataframe = data_to_merge,
        column_name = column_to_mark_source_data_deletion
    )
    
    data_to_merge = data_to_merge.withColumn(column_to_mark_source_data_deletion, f.lit(delete_marker_value))

    merge_condition, update_columns, insert_columns = _prepare_merge_config(
        data_to_merge=data_to_merge,
        primary_keys=primary_keys,
        source_alias=source_alias,
        target_alias=target_alias
    )

    target_table.alias(target_alias) \
                .merge(source = data_to_merge.alias(source_alias)
                    ,condition = merge_condition
                ).whenMatchedUpdate(set = update_columns) \
                .whenNotMatchedInsert(values = insert_columns) \
                .execute()

REPLACE_WHERE_STRING_DISTINCT_LIMIT = 5000


def _escape_sql_string_value(raw_value: str) -> str:
    """Build a SQL expression that evaluates to the given string value.

    Values without apostrophes use simple SQL string literals.
    Values WITH apostrophes use ``concat()`` + ``chr(39)`` to construct the
    value safely, avoiding replaceWhere expression parsing issues where
    the SQL-standard ``''`` escape may be misinterpreted.
    """
    if "'" not in raw_value:
        return f"'{raw_value}'"

    # Split on apostrophes and reconstruct using concat + chr(39)
    parts = raw_value.split("'")
    concat_args = []
    for i, part in enumerate(parts):
        if i > 0:
            concat_args.append("chr(39)")
        if part:
            concat_args.append(f"'{part}'")

    if len(concat_args) == 1:
        return concat_args[0]
    return f"concat({', '.join(concat_args)})"


def _format_temporal_replace_where_literal(value: Any, replace_dtype: str) -> str:
    """Format date/timestamp replaceWhere bounds as explicit Spark SQL literals."""
    if 'timestamp' in replace_dtype:
        if isinstance(value, date) and not isinstance(value, datetime):
            value = datetime.combine(value, datetime.min.time())
        literal_keyword = "TIMESTAMP_NTZ" if 'timestamp_ntz' in replace_dtype else "TIMESTAMP"
        return f"{literal_keyword} '{value.strftime('%Y-%m-%d %H:%M:%S.%f')}'"

    if isinstance(value, datetime):
        value = value.date()
    return f"DATE '{value.isoformat()}'"


def _handle_replace_where(
    data_to_merge: DataFrame,
    target_abfss_path: str,
    replace_where_column: str
) -> None:
    """Handle selective partition overwrite based on column values."""
    merging_replace_where_log_info = "Merging data to existing table using replaceWhere Spark logic."
    log_and_print(merging_replace_where_log_info)

    # Get column data type
    matching_dtypes = [
        dtype for name, dtype in data_to_merge.dtypes
        if name == replace_where_column
    ]
    if not matching_dtypes:
        raise Exception(
            f"Column '{replace_where_column}' does not exist in source DataFrame for replaceWhere. "
            f"Available columns: {[name for name, _ in data_to_merge.dtypes]}"
        )

    replace_dtype = matching_dtypes[0].lower()

    # Build replace condition based on data type
    if 'timestamp' in replace_dtype or 'date' in replace_dtype:
        # Handle date/timestamp columns
        bounds = data_to_merge.agg(
            f.min(f.col(replace_where_column)).alias("min_v"),
            f.max(f.col(replace_where_column)).alias("max_v")
        ).first()
        min_v, max_v = bounds.min_v, bounds.max_v
        if min_v is None or max_v is None:
            raise Exception(
                f"Cannot build replaceWhere expression for column '{replace_where_column}' because all values are NULL."
            )
        min_literal = _format_temporal_replace_where_literal(min_v, replace_dtype)
        max_literal = _format_temporal_replace_where_literal(max_v, replace_dtype)
        replace_where_expression = (
            f"`{replace_where_column}` >= {min_literal} AND `{replace_where_column}` <= {max_literal}"
        )
        
    elif any(t in replace_dtype for t in ('int', 'bigint', 'double', 'float', 'decimal')):
        # Handle numeric columns
        bounds = data_to_merge.agg(
            f.min(f.col(replace_where_column)).alias("min_v"),
            f.max(f.col(replace_where_column)).alias("max_v")
        ).first()
        if bounds.min_v is None or bounds.max_v is None:
            raise Exception(
                f"Cannot build replaceWhere expression for column '{replace_where_column}' because all values are NULL."
            )
        replace_where_expression = (
            f"`{replace_where_column}` >= {bounds.min_v} AND `{replace_where_column}` <= {bounds.max_v}"
        )
        
    else:
        # Handle string and other columns with driver-safety guardrails.
        # We only collect up to limit+1 values to prevent very large driver payloads.
        limited_rows = (
            data_to_merge.select(f.col(replace_where_column).alias("replace_value"))
            .filter(f.col(replace_where_column).isNotNull())
            .distinct()
            .limit(REPLACE_WHERE_STRING_DISTINCT_LIMIT + 1)
            .collect()
        )

        if not limited_rows:
            raise Exception(
                f"Cannot build replaceWhere expression for column '{replace_where_column}' because all values are NULL."
            )

        if len(limited_rows) > REPLACE_WHERE_STRING_DISTINCT_LIMIT:
            raise Exception(
                f"replaceWhere on string column '{replace_where_column}' exceeds safe limit of "
                f"{REPLACE_WHERE_STRING_DISTINCT_LIMIT} distinct values. "
                "Use a numeric/date replaceWhere key or a different merge strategy."
            )

        escaped_values = [
            _escape_sql_string_value(str(row["replace_value"]))
            for row in limited_rows
        ]

        values_str = ", ".join(escaped_values)
        replace_where_expression = f"`{replace_where_column}` IN ({values_str})"

    data_to_merge.write \
       .mode("overwrite") \
       .option("replaceWhere", replace_where_expression) \
       .save(target_abfss_path)

def get_records_written_for_table(target_abfss_path: str):
    """
    This function gets the number of records written for the last write operation in a delta table.

    Args:
        target_abfss_path (str): abfss path for table

    Returns:
        records_written (int): Records written

    """
    history_df = spark.sql(f"DESCRIBE HISTORY delta.`{target_abfss_path}`") \
                     .filter("operation in ('MERGE', 'UPDATE', 'WRITE', 'CREATE OR REPLACE TABLE AS SELECT', " +
                            "'CREATE TABLE AS SELECT', 'REPLACE TABLE AS SELECT', 'COPY INTO')")

    history_rows = history_df.orderBy(f.desc("timestamp")).select("operationMetrics").limit(1).collect()
    
    if not history_rows:
        # No matching write operations found in history - this can happen if:
        # 1. Table was just created but no data written yet
        # 2. Custom function performed a non-standard write operation
        # 3. Delta history hasn't been updated yet (rare timing issue)
        log_and_print(
            f"⚠️ Warning: No write operations found in Delta history for {target_abfss_path}. "
            "Returning 0 records. If data was written, verify the write operation completed successfully.",
            "warn"
        )
        return 0
    
    operation_metrics = history_rows[0][0]
    
    if operation_metrics is None:
        log_and_print(
            f"⚠️ Warning: Operation metrics not available for {target_abfss_path}. Returning 0 records.",
            "warn"
        )
        return 0
        
    records_written = int(operation_metrics.get('numOutputRows', 0))

    return records_written

def _get_records_processed(
    merge_type: str,
    target_abfss_path: str,
    data_to_merge: DataFrame
) -> int:
    """Get count of records processed."""
    if merge_type not in ("output_file", "warehouse_spark_connector"):   
        records_processed = get_records_written_for_table(
            target_abfss_path = target_abfss_path
        )
    else:
        records_processed = data_to_merge.count()
    
    return records_processed

def _quote_sql_identifier(identifier: str) -> str:
    """Quote a Spark SQL identifier for display-only expressions."""
    return f"`{str(identifier).replace('`', '``')}`"


def _format_batch_filter_value_for_display(value: Any) -> str:
    """Format a batch value for human-readable logging."""
    if value is None:
        return "NULL"
    if isinstance(value, datetime):
        return f"TIMESTAMP '{value.strftime('%Y-%m-%d %H:%M:%S.%f')}'"
    if isinstance(value, date):
        return f"DATE '{value.isoformat()}'"
    if isinstance(value, str):
        return _escape_sql_string_value(value)
    return str(value)


def _describe_filter_expression_for_batch(
    ingestion_row: object,
    columns: list
) -> str:
    """Build a display-only description of a batch filter."""
    filter_conditions = []
    for column_name in columns:
        column_value = getattr(ingestion_row, column_name)
        quoted_column_name = _quote_sql_identifier(column_name)
        if column_value is None:
            filter_conditions.append(f"{quoted_column_name} IS NULL")
        else:
            filter_conditions.append(
                f"{quoted_column_name} = {_format_batch_filter_value_for_display(column_value)}"
            )

    return " AND ".join(filter_conditions)


# Extracted function to build dynamic filter expressions
def _build_filter_expression_for_batch(
    ingestion_row: object,
    columns: list
):
    """Build a Spark Column predicate for the specified columns and row values."""
    filter_expression = None
    for column_name in columns:
        column_value = getattr(ingestion_row, column_name)
        quoted_column_name = _quote_sql_identifier(column_name)
        if column_value is None:
            condition = f.col(quoted_column_name).isNull()
        else:
            condition = f.col(quoted_column_name) == f.lit(column_value)

        filter_expression = condition if filter_expression is None else filter_expression & condition

    return filter_expression if filter_expression is not None else f.lit(True)

def merge_data(
    data_to_merge: DataFrame,
    first_run: bool,
    target_config: dict,
    watermark_config: str,
    primary_keys: list,
    dimension_config: dict,
    data_skipping_stats_columns: str = ""
) -> tuple[bool, int]:
    """
    Execute data merge operations with multiple strategies for different business scenarios.

    This function is the central hub for all data write operations, supporting various
    merge patterns from simple appends to complex SCD2 implementations. It intelligently
    routes to the appropriate merge strategy based on configuration.

    Args:

    Returns:
        tuple[bool, int]: Updated first_run flag and records processed count

    Merge Types:
        - output_file: Write to file formats (CSV, Parquet, Excel)
        - append: Simple append to existing table
        - overwrite: Full table replacement
        - overwrite_with_all_paths: Overwrite with multi-file support
        - merge: Upsert based on primary keys
        - merge_mark_unmatched_deleted: Upsert + mark missing as deleted
        - merge_mark_all_deleted: Mark all matched records as deleted
        - replace_where: Selective partition overwrite

    Performance Considerations:
        - Uses Delta Lake merge for efficient updates
        - Leverages partition pruning for replace_where
        - Supports batch processing for large datasets
        - Maintains statistics through operation metrics
    """
    merge_type = watermark_config['merge_type']
    target_abfss_path = target_config['target_abfss_path']
    delete_rows_with_value = watermark_config['delete_rows_with_value']
    column_to_mark_source_data_deletion = watermark_config['column_to_mark_source_data_deletion']

    # Rule 86 is now checked upstream in parse_target_configuration() in HF3
    # where it catches the conflict during config parsing, before merge defaults are applied.

    # Log target path for debugging and tracking
    target_path_log_info = f"Writing data to path: {target_abfss_path}"
    log_and_print(target_path_log_info)
    
    target_alias = "target_df"
    source_alias = "source_df"    

    if merge_type == "output_file":
        _write_output_file(
            data_to_merge = data_to_merge,
            target_abfss_path = target_abfss_path,
            target_config = target_config
        )

    elif merge_type == "warehouse_spark_connector":
        _write_warehouse_spark_connector(
            data_to_merge = data_to_merge,
            target_datastore_workspace_id = target_config['target_datastore_workspace_id'],
            target_table_name = target_config['target_table_name'],
            warehouse_write_mode = target_config['warehouse_write_mode']
        )
                       
    elif first_run:
        _handle_first_run(
            data_to_merge = data_to_merge,
            target_abfss_path = target_abfss_path
        )
        first_run = False

    elif merge_type == "scd2":
        scd_dimensions(
            new_data = data_to_merge,
            target_abfss_path = target_abfss_path,
            primary_keys = primary_keys,
            source_timestamp_column_name = dimension_config['source_timestamp_column_name'],
            column_to_mark_source_data_deletion = column_to_mark_source_data_deletion,
            delete_rows_with_value = delete_rows_with_value,
            dimension_table_key_column_name = dimension_config['dimension_table_key_column_name'],
            watermark_column_name = watermark_config['watermark_column_name']
        )

    elif merge_type == "append":
        _handle_append(
            data_to_merge = data_to_merge,
            target_abfss_path = target_abfss_path
        )

    elif merge_type == "overwrite":
        _handle_overwrite(
            data_to_merge = data_to_merge,
            target_abfss_path = target_abfss_path,
            data_skipping_stats_columns = data_skipping_stats_columns
        )
        
    elif merge_type == "merge":
        target_table = _load_target_delta_table(target_abfss_path)
        _handle_merge(
            target_table = target_table,
            data_to_merge = data_to_merge,
            primary_keys = primary_keys,
            target_alias = target_alias,
            source_alias = source_alias
        )

    elif merge_type == "merge_and_delete":
        target_table = _load_target_delta_table(target_abfss_path)
        _handle_merge_and_delete(
            target_table = target_table,
            data_to_merge = data_to_merge,
            primary_keys = primary_keys,
            target_alias = target_alias,
            source_alias = source_alias,
            delete_rows_with_value = delete_rows_with_value,
            column_to_mark_source_data_deletion = column_to_mark_source_data_deletion
        )

    elif merge_type == "merge_mark_unmatched_deleted":
        target_table = _load_target_delta_table(target_abfss_path)
        _handle_merge_mark_unmatched_deleted(
            target_table = target_table,
            data_to_merge = data_to_merge,
            primary_keys = primary_keys,
            target_alias = target_alias,
            source_alias = source_alias,
            delete_rows_with_value = delete_rows_with_value,
            column_to_mark_source_data_deletion = column_to_mark_source_data_deletion
        )

    elif merge_type == "merge_mark_all_deleted":
        target_table = _load_target_delta_table(target_abfss_path)
        _handle_merge_mark_all_deleted(
            target_table = target_table,
            data_to_merge = data_to_merge,
            primary_keys = primary_keys,
            target_alias = target_alias,
            source_alias = source_alias,
            delete_rows_with_value = delete_rows_with_value,
            column_to_mark_source_data_deletion = column_to_mark_source_data_deletion
        )

    elif merge_type == "replace_where":
        _handle_replace_where(
            data_to_merge = data_to_merge,
            target_abfss_path = target_abfss_path,
            replace_where_column = target_config['replace_where_column']
        )
            
    else:
        raise Exception(f"No logic currently exists to execute the {merge_type} merge type.")

    records_processed = _get_records_processed(
        merge_type = merge_type,
        target_abfss_path = target_abfss_path,
        data_to_merge = data_to_merge
    )

    return first_run, records_processed

def get_unique_batch_metadata(df, batch_columns: list):
    """
    Extract unique batch values from DataFrame for batch processing.
    
    This function identifies all unique combinations of values in the specified
    batch columns, which are used to partition the data for sequential processing.
    This is helpful when recreating tables from prior data loads or when processing
    large datasets in smaller, manageable chunks.
    
    Args:
        df: Spark DataFrame containing the data to process
        batch_columns (list): List of column names to use for batching
    
    Returns:
        list: List of Row objects containing unique batch value combinations,
              sorted in ascending order by the batch columns
    
    Example:
        >>> batch_metadata = get_unique_batch_metadata(
        ...     df = new_data,
        ...     batch_columns = ['delta__raw_folderpath', 'source_file_name']
        ... )
        >>> print(f"Processing {len(batch_metadata)} batches")
    """
    ingestion_metadata = df.dropDuplicates(batch_columns) \
                          .select(batch_columns) \
                          .sort(*[f.asc(col) for col in batch_columns]) \
                          .collect()
    
    return ingestion_metadata


def process_single_batch(
    df: DataFrame,
    batch_row: Row,
    merge_in_batches_with_columns: list,
    first_run: bool,
    target_config: dict,
    watermark_config: dict,
    primary_keys: list,
    dimension_config: dict,
    data_skipping_stats_columns: str = ""
) -> tuple:
    """
    Process a single batch of data from the source DataFrame.
    
    This function filters the source data for a specific batch, logs the operation,
    and merges the filtered data to the target location. Each batch is processed
    sequentially to maintain data lineage and enable recovery from specific batches.
    
    Args:
        df: Spark DataFrame containing the data to process
        batch_row: Row object containing the batch column values
        merge_in_batches_with_columns (list): List of column names used for batching
        first_run (bool): Whether this is the first load for the table
        target_config (dict): Target configuration dictionary
        watermark_config (dict): Watermark configuration dictionary
        primary_keys (list): List of primary key column names
        dimension_config (dict): Dimension configuration dictionary

    Returns:
        tuple: (updated_first_run, records_processed)
            - updated_first_run (bool): Updated first_run flag after merge
            - records_processed (int): Number of records processed in this batch
    
    """
    # Build filter expression for current batch
    filter_expression = _build_filter_expression_for_batch(
        ingestion_row = batch_row,
        columns = merge_in_batches_with_columns
    )
    filter_description = _describe_filter_expression_for_batch(
        ingestion_row = batch_row,
        columns = merge_in_batches_with_columns
    )
    
    log_and_print(f"Writing batch with filter: {filter_description}")
    
    # Filter data for current batch
    data_to_merge = df.filter(filter_expression)
    
    # Merge current batch to target
    updated_first_run, records_processed = merge_data(
        data_to_merge = data_to_merge,
        first_run = first_run,
        target_config = target_config,
        watermark_config = watermark_config,
        primary_keys = primary_keys,
        dimension_config = dimension_config,
        data_skipping_stats_columns = data_skipping_stats_columns
    )
    
    return updated_first_run, records_processed

def write_data_orchestrator(
    df: DataFrame,
    first_run: bool,
    target_config: dict,
    watermark_config: dict,
    primary_keys: list,
    dimension_config: dict,
    stats_properties: dict
) -> int:
    """
    Process all data in a single merge operation.
    
    This is the standard processing mode where all source data is merged to the
    target in one operation. This provides optimal performance for most use cases.
    
    Args:
    
    Returns:
        int: Number of records processed

    """
    merge_in_batches_with_columns = watermark_config['merge_in_batches_with_columns']
    data_skipping_stats_columns = stats_properties.get('delta.dataSkippingStatsColumns', '')

    if merge_in_batches_with_columns:
        log_and_print(f"Writing data in batches, grouped by distinct values in columns: {merge_in_batches_with_columns}.")
        
        # Get unique batch combinations
        batch_metadata = get_unique_batch_metadata(
            df = df,
            batch_columns = merge_in_batches_with_columns
        )
        
        total_records_processed = 0
        current_first_run = first_run
        
        # Process each batch sequentially
        for batch_row in batch_metadata:
            current_first_run, batch_records = process_single_batch(
                df = df,
                merge_in_batches_with_columns = merge_in_batches_with_columns,
                batch_row = batch_row,
                first_run = current_first_run,
                target_config = target_config,
                watermark_config = watermark_config,
                primary_keys = primary_keys,
                dimension_config = dimension_config,
                data_skipping_stats_columns = data_skipping_stats_columns
            )
            total_records_processed += batch_records
    else:
        first_run, total_records_processed = merge_data(
            data_to_merge = df,
            first_run = first_run,
            target_config = target_config,
            watermark_config = watermark_config,
            primary_keys = primary_keys,
            dimension_config = dimension_config,
            data_skipping_stats_columns = data_skipping_stats_columns
        )
    
    return total_records_processed

def write_quarantined_data(
    quarantined_df,
    target_quarantined_abfss_path: str
) -> int:
    """
    Write quarantined data to the quarantine table and return count of records.
    
    This function handles the complete quarantine workflow: checking for data,
    writing to the quarantine table (append mode), and retrieving the count
    of records written from table history.
    
    Args:
        quarantined_df: Spark DataFrame containing quarantined records
        target_quarantined_abfss_path (str): abfss path for quarantined table
    
    Returns:
        int: Number of records written to quarantine table (0 if no data)
    
    Example:
        >>> quarantined_count = write_quarantine_data(
        ...     quarantined_df = failed_quality_checks,
        ...     target_quarantined_abfss_path = ''
        ... )
        >>> print(f"Quarantined {quarantined_count} records")
    
    Note:
        - Always uses append mode to preserve historical quarantine records
        - Quarantine tables store records that failed validation including:
          * Primary key duplicates
          * Data quality rule violations
          * Schema validation failures
          * Reference data mismatches
    """
    if not quarantined_df.isEmpty():
        log_and_print("Writing quarantined data to table.")
        
        # Create or append to quarantine table
        quarantined_df.write.mode("append") \
                      .save(target_quarantined_abfss_path)
        
        # Get quarantine metrics from table history
        quarantined_records = get_records_written_for_table(
            target_abfss_path = target_quarantined_abfss_path
        )
    else:
        quarantined_records = 0
    
    return quarantined_records

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Slowly Changing Dimensions (SCD) Type 2 Implementation
# 
# This section implements Type 2 Slowly Changing Dimensions (SCD2), a critical pattern for tracking historical changes in dimension data. SCD2 preserves the complete history of changes by creating new records for each change while maintaining the old records with validity date ranges.
# 
# Key features:
# - **Historical Tracking**: Maintains full audit trail of all dimension changes
# - **Temporal Queries**: Enables point-in-time analysis with start/end dates
# - **Performance Optimization**: Uses hash-based change detection for efficiency
# - **Data Integrity**: Ensures no gaps or overlaps in temporal coverage

# CELL ********************

def _scd2_prepare_existing_data(target_abfss_path: str) -> DataFrame:
    """Load existing dimension data and filter for active records."""
    existing_df = spark.read.format("delta").load(target_abfss_path)
    existing_df_active = existing_df.filter(f.col("scd_active") == 1).alias('existing_df_active')
    return existing_df_active


def _prepare_comparison_columns(
    new_data: DataFrame, 
    existing_df_active: DataFrame, 
    dimension_table_key_column_name: str, 
    column_to_mark_source_data_deletion: str,
    source_timestamp_column_name: str,
    watermark_column_name: str
) -> Tuple[List[str], List[str]]:
    """Identify columns for comparison (exclude system columns, scd2 timestamp, and watermark column)."""
    
    # Build set of columns to exclude from comparison
    columns_to_exclude = {
        dimension_table_key_column_name,
        source_timestamp_column_name,
        watermark_column_name
    }
    
    # Filter new data columns (exclude specified columns from comparison)
    new_data_non_delta_columns = sorted([
        col
        for col in new_data.columns
        if (col not in columns_to_exclude) and 
           ("DELTA__" not in col.upper() or col.upper() == column_to_mark_source_data_deletion.upper()) and 
           ("SCD_" not in col.upper())
    ])

    # Filter existing table columns (exclude specified columns from comparison)
    existing_table_non_delta_columns = sorted([
        col
        for col in existing_df_active.columns
        if (col not in columns_to_exclude) and 
           ("DELTA__" not in col.upper() or col.upper() == column_to_mark_source_data_deletion.upper()) and 
           ("SCD" not in col.upper())
    ])
    
    return new_data_non_delta_columns, existing_table_non_delta_columns


def _calculate_hash_comparison(
    new_data: DataFrame, 
    existing_df_active: DataFrame, 
    primary_keys: List[str], 
    new_data_non_delta_columns: List[str], 
    existing_table_non_delta_columns: List[str]
) -> Tuple[DataFrame, DataFrame]:
    """Calculate hash values for efficient comparison."""
    
    # Calculate hash for new data
    new_data_hash = new_data.select(
        *primary_keys, 
        f.hash(*new_data_non_delta_columns).alias("new_hash")
    )
    
    # Calculate hash for existing data
    existing_df_hash = existing_df_active.select(
        *primary_keys, 
        f.hash(*existing_table_non_delta_columns).alias("existing_hash")
    )

    # Join and compare hashes
    hash_comparison = new_data_hash.join(
        existing_df_hash, primary_keys, how="left_outer"
    ).select(new_data_hash["*"], existing_df_hash["existing_hash"])
    
    # Identify changed records
    changed_dimension_hash = hash_comparison.filter(
        f.col("new_hash") != f.col("existing_hash")
    )
    
    # Identify new records
    new_dimension_hash = hash_comparison.filter(f.col("existing_hash").isNull())
    
    return changed_dimension_hash, new_dimension_hash


def _get_changed_dimensions_values(
    new_data: DataFrame, 
    existing_df_active: DataFrame, 
    changed_dimension_hash: DataFrame, 
    primary_keys: List[str]
) -> DataFrame:
    """Get full data for changed records."""
    
    # Get distinct primary keys for changed records
    changed_dimension_hashes_distinct_pks = changed_dimension_hash.select(
        *primary_keys
    ).distinct()
    
    # Join to get full new data for changed records
    changed_dimensions_values = new_data.join(
        changed_dimension_hashes_distinct_pks, primary_keys, how="inner"
    ).select(new_data["*"])
    
    # Add existing start date
    changed_dimensions_values = changed_dimensions_values.join(
        existing_df_active, primary_keys, how="left_outer"
    ).select(changed_dimensions_values["*"], "scd_start_date")
    
    log_and_print(f"Identified {changed_dimension_hash.count()} changed dimension records.")
    
    return changed_dimensions_values


def _get_new_dimensions_values(
    new_data: DataFrame, 
    new_dimension_hash: DataFrame, 
    primary_keys: List[str]
) -> DataFrame:
    """Get full data for new records."""
    
    # Get distinct primary keys for new records
    new_dimension_hashes_distinct_pks = new_dimension_hash.select(
        *primary_keys
    ).distinct()
    
    # Join to get full new data for new records
    new_dimensions_values = new_data.join(
        new_dimension_hashes_distinct_pks, primary_keys, how="inner"
    ).select(new_data["*"])
    
    log_and_print(f"Identified {new_dimension_hash.count()} new dimension records.")
    
    return new_dimensions_values

def _process_deleted_records(
    target_table: DeltaTable, 
    new_dimensions_values: DataFrame, 
    changed_dimensions_values: DataFrame, 
    column_to_mark_source_data_deletion: Optional[str], 
    delete_rows_with_value: str, 
    source_timestamp_column_name: str, 
    primary_keys: List[str]
) -> None:
    """Process deleted records."""
    
    # Create primary key join condition
    primary_key_join = " AND ".join(
        [f"current.`{col}` <=> new.`{col}`" for col in primary_keys]
    )
    
    if column_to_mark_source_data_deletion:
        # Filter new deleted records
        new_deleted_values = new_dimensions_values.where(
            f"`{column_to_mark_source_data_deletion}` = {delete_rows_with_value}"
        )

        new_deleted_values = new_deleted_values.withColumn(
            "scd_start_date", f.col(source_timestamp_column_name)
        )
        
        # Log deletion operation
        marking_records_inactive_log_info = (
            f"Marking all records as inactive where "
            f"`{column_to_mark_source_data_deletion}` = {delete_rows_with_value}"
        )
        log_and_print(marking_records_inactive_log_info)
        
        # Filter changed deleted records
        deleted_dimension_values = changed_dimensions_values.where(
            f"`{column_to_mark_source_data_deletion}` = {delete_rows_with_value}"
        )
        
        # Combine all deleted records
        deleted_dimension_values = new_deleted_values.unionByName(
            deleted_dimension_values, allowMissingColumns=True
        )
        
        # Mark as inactive with end date
        deleted_dimension_values = deleted_dimension_values.withColumn(
            "scd_end_date", f.col(source_timestamp_column_name)
        ).withColumn(
            "scd_active", f.lit(0)
        )
        
        log_and_print("Prepared deleted dimension records for merge.")

        # Create join condition for active records
        primary_key_join_deleted = primary_key_join + " AND current.scd_active = 1"

        # Execute merge for deleted records
        target_table.alias("current").merge(
            deleted_dimension_values.alias("new"), primary_key_join_deleted
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

def _process_modified_records(
    changed_dimensions_values: DataFrame, 
    existing_df_active: DataFrame, 
    column_to_mark_source_data_deletion: Optional[str], 
    delete_rows_with_value: str, 
    source_timestamp_column_name: str, 
    primary_keys: List[str]
) -> DataFrame:
    """Process non-deleted changed records."""
    
    # Filter out deleted records from changed records
    if column_to_mark_source_data_deletion:
        modified_dimension_values = changed_dimensions_values.where(
            f"{column_to_mark_source_data_deletion} != {delete_rows_with_value} OR "
            f"{column_to_mark_source_data_deletion} IS NULL"
        )
    else:
        modified_dimension_values = changed_dimensions_values

    # Join with existing data to get current record details
    modified_dimension_values = (
        modified_dimension_values.alias("A")
        .join(
            existing_df_active.alias("B")
            .drop(source_timestamp_column_name)
            .drop("scd_end_date"),
            primary_keys,
            how="inner",
        )
        .select(f.col("B.*"), f.col(f"A.{source_timestamp_column_name}"))
    )
    
    # Mark existing records as inactive
    new_dimensions_values_set_inactive = modified_dimension_values.withColumn(
        "scd_end_date", f.col(source_timestamp_column_name)
    ).withColumn(
        "scd_active", f.lit(0)
    )
    
    log_and_print("Prepared modified dimension records to mark as inactive.")
    
    return new_dimensions_values_set_inactive

def _create_new_versions(
    changed_dimensions_values: DataFrame, 
    column_to_mark_source_data_deletion: Optional[str], 
    delete_rows_with_value: str, 
    source_timestamp_column_name: str
) -> DataFrame:
    """Insert new versions for modified records."""
    
    # Filter out deleted records from changed records
    if column_to_mark_source_data_deletion:
        modified_dimension_values = changed_dimensions_values.where(
            f"{column_to_mark_source_data_deletion} != {delete_rows_with_value} OR "
            f"{column_to_mark_source_data_deletion} IS NULL"
        )
    else:
        modified_dimension_values = changed_dimensions_values
    
    # Create new active versions with current timestamp
    new_dimensions_values_insert = modified_dimension_values.withColumn(
        "scd_start_date", f.col(source_timestamp_column_name)
    ).withColumn(
        "scd_end_date", f.lit(None)
    ).withColumn(
        "scd_active", f.lit(1)
    )
    
    log_and_print("Prepared new versions for modified dimension records.")
    
    return new_dimensions_values_insert

def _prepare_new_records(
    new_dimensions_values: DataFrame, 
    column_to_mark_source_data_deletion: Optional[str], 
    delete_rows_with_value: str, 
    source_timestamp_column_name: str
) -> DataFrame:
    """Insert completely new records."""
    
    # Filter out deleted records from new records
    if column_to_mark_source_data_deletion:
        new_dimensions_values = new_dimensions_values.where(
            f"{column_to_mark_source_data_deletion} != {delete_rows_with_value} OR "
            f"{column_to_mark_source_data_deletion} IS NULL"
        )

    # Set SCD columns for new records
    new_dimensions_values = new_dimensions_values.withColumn(
        "scd_start_date", f.col(source_timestamp_column_name)
    ).withColumn(
        "scd_end_date", f.lit(None)
    ).withColumn(
        "scd_active", f.lit(1)
    )
    
    log_and_print("Prepared completely new dimension records for insert.")
    
    return new_dimensions_values

def _execute_final_merge(
    target_table: DeltaTable, 
    new_dimensions_values_set_inactive: DataFrame, 
    new_dimensions_values_insert: DataFrame, 
    new_dimensions_values: DataFrame, 
    primary_keys: List[str]
) -> None:
    """Combine all updates and execute final merge."""
    
    # Combine all dimension updates
    all_updated_dimension_values = new_dimensions_values_set_inactive.unionByName(
        new_dimensions_values_insert, allowMissingColumns=True
    ).unionByName(
        new_dimensions_values, allowMissingColumns=True
    )
    
    log_and_print("Combined all dimension updates for final merge.")
    
    # Create join conditions
    primary_key_join = " AND ".join(
        [f"current.`{col}` <=> new.`{col}`" for col in primary_keys]
    )
    primary_key_join_updates = (
        primary_key_join + " AND current.scd_active = 1 AND new.scd_active = 0"
    )

    # Execute final merge operation
    target_table.alias("current").merge(
        all_updated_dimension_values.alias("new"), primary_key_join_updates
    ).whenMatchedUpdate(
        set={"scd_active": "new.scd_active", "scd_end_date": "new.scd_end_date"}
    ).whenNotMatchedInsertAll().execute()


def scd_dimensions(
    new_data: DataFrame,
    target_abfss_path: str,
    primary_keys: List[str],
    source_timestamp_column_name: str,
    column_to_mark_source_data_deletion: Optional[str],
    delete_rows_with_value: str,
    dimension_table_key_column_name: str,
    watermark_column_name: str
) -> None:
    """
    Implement Type 2 Slowly Changing Dimensions with comprehensive change tracking.

    This function manages the complete SCD2 lifecycle including:
    - Detection of new, changed, and deleted dimension members
    - Creation of new versions for changed records
    - Closure of previous versions with appropriate end dates
    - Preservation of surrogate keys across versions

    The implementation uses hash-based comparison for efficient change detection
    and maintains referential integrity for fact table relationships.

    Args:
        new_data (DataFrame): Incoming dimension data with potential changes
        target_abfss_path (str): ABFSS Path of target table
        primary_keys (List[str]): Natural/business key columns
        source_timestamp_column_name (str): Timestamp column for SCD2 versioning (scd_start_date/scd_end_date)
        column_to_mark_source_data_deletion (Optional[str]): Column indicating source deletions
        delete_rows_with_value (str): value that indicates a record should be deleted
        dimension_table_key_column_name (str): Surrogate key column name
        watermark_column_name (str): Watermark column to exclude from hash comparison (can be empty string if not configured)

    Processing Steps:
        1. Load existing active dimension records
        2. Calculate hash values for change detection
        3. Identify new, changed, and deleted records
        4. Close out changed/deleted records (set end date)
        5. Insert new versions for changed records
        6. Insert completely new records

    Performance Optimizations:
        - Hash-based comparison reduces column-by-column checks
        - Processes only active records for comparison
        - Bulk operations for all record types
        - Minimizes table scans with targeted filters

    Data Integrity:
        - Ensures no gaps in temporal coverage
        - Maintains consistent surrogate keys
        - Handles edge cases (deletes, resurrections)
    """

    # Log the SCD2 operation
    merging_scd2_dimensions_log_info = "Merging dimension data to existing table with slowly changing type 2 format."
    log_and_print(merging_scd2_dimensions_log_info)

    # Initialize target table
    target_table = _load_target_delta_table(target_abfss_path)
    
    # Step 1: Load existing active dimension records
    existing_df_active = _scd2_prepare_existing_data(
        target_abfss_path=target_abfss_path
    )

    # Format delete value if needed
    if column_to_mark_source_data_deletion:
        delete_rows_with_value = _delete_marker_sql_literal_for_dataframe(
            delete_rows_with_value = delete_rows_with_value,
            dataframe = new_data,
            column_name = column_to_mark_source_data_deletion
        )

    # Step 2: Prepare columns for comparison
    new_data_non_delta_columns, existing_table_non_delta_columns = _prepare_comparison_columns(
        new_data=new_data,
        existing_df_active=existing_df_active,
        dimension_table_key_column_name=dimension_table_key_column_name,
        column_to_mark_source_data_deletion=column_to_mark_source_data_deletion,
        source_timestamp_column_name=source_timestamp_column_name,
        watermark_column_name=watermark_column_name
    )
    
    # Step 3: Calculate hash comparison to identify changes
    changed_dimension_hash, new_dimension_hash = _calculate_hash_comparison(
        new_data=new_data,
        existing_df_active=existing_df_active,
        primary_keys=primary_keys,
        new_data_non_delta_columns=new_data_non_delta_columns,
        existing_table_non_delta_columns=existing_table_non_delta_columns
    )
    
    # Step 4: Get full data for changed records
    changed_dimensions_values = _get_changed_dimensions_values(
        new_data=new_data,
        existing_df_active=existing_df_active,
        changed_dimension_hash=changed_dimension_hash,
        primary_keys=primary_keys
    )
    
    # Step 5: Get full data for new records
    new_dimensions_values = _get_new_dimensions_values(
        new_data=new_data,
        new_dimension_hash=new_dimension_hash,
        primary_keys=primary_keys
    )

    # Step 6: Process deleted records
    _process_deleted_records(
        target_table=target_table,
        new_dimensions_values=new_dimensions_values,
        changed_dimensions_values=changed_dimensions_values,
        column_to_mark_source_data_deletion=column_to_mark_source_data_deletion,
        delete_rows_with_value=delete_rows_with_value,
        source_timestamp_column_name=source_timestamp_column_name,
        primary_keys=primary_keys
    )

    # Step 7: Process modified records (mark existing as inactive)
    new_dimensions_values_set_inactive = _process_modified_records(
        changed_dimensions_values=changed_dimensions_values,
        existing_df_active=existing_df_active,
        column_to_mark_source_data_deletion=column_to_mark_source_data_deletion,
        delete_rows_with_value=delete_rows_with_value,
        source_timestamp_column_name=source_timestamp_column_name,
        primary_keys=primary_keys
    )

    # Step 8: Create new versions for changed records
    new_dimensions_values_insert = _create_new_versions(
        changed_dimensions_values=changed_dimensions_values,
        column_to_mark_source_data_deletion=column_to_mark_source_data_deletion,
        delete_rows_with_value=delete_rows_with_value,
        source_timestamp_column_name=source_timestamp_column_name
    )

    # Step 9: Prepare completely new records
    new_dimensions_values = _prepare_new_records(
        new_dimensions_values=new_dimensions_values,
        column_to_mark_source_data_deletion=column_to_mark_source_data_deletion,
        delete_rows_with_value=delete_rows_with_value,
        source_timestamp_column_name=source_timestamp_column_name
    )

    # Step 10: Execute final merge operation
    _execute_final_merge(
        target_table=target_table,
        new_dimensions_values_set_inactive=new_dimensions_values_set_inactive,
        new_dimensions_values_insert=new_dimensions_values_insert,
        new_dimensions_values=new_dimensions_values,
        primary_keys=primary_keys
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5b. Schema Contract Validation for Delta Tables
# 
# This section provides functions for validating Delta table schemas against expected schema contracts.
# When a schema is defined in `source_details.schema`, the framework validates that the source table
# matches the expected columns and types before processing continues.
# 
# Key capabilities:
# - **Schema Contract Enforcement**: Ensures source tables have required columns with correct types
# - **Forward Compatibility**: Extra columns in source are allowed by default
# - **Clear Error Messages**: Provides detailed information about schema mismatches

# CELL ********************

def _parse_expected_schema(expected_schema_str: str) -> dict:
    """
    Parse an expected schema string into a dictionary of column names and types.
    
    Handles complex types with commas inside parentheses like DECIMAL(10,2).
    
    Args:
        expected_schema_str: Schema definition string like 'name STRING, age INT, amount DECIMAL(10,2)'
    
    Returns:
        dict: Dictionary mapping column names (lowercase) to data types (lowercase)
        
    Example:
        >>> _parse_expected_schema('name STRING, age INT')
        {'name': 'string', 'age': 'int'}
        >>> _parse_expected_schema('id INT, amount DECIMAL(10,2), name STRING')
        {'id': 'int', 'amount': 'decimal(10,2)', 'name': 'string'}
    """
    if not expected_schema_str:
        return {}
    
    schema_parts = []
    current_part = []
    parenthesis_depth = 0
    angle_depth = 0

    for character in expected_schema_str:
        if character == ',' and parenthesis_depth == 0 and angle_depth == 0:
            schema_parts.append(''.join(current_part).strip())
            current_part = []
            continue

        current_part.append(character)
        if character == '(':
            parenthesis_depth += 1
        elif character == ')':
            parenthesis_depth = max(parenthesis_depth - 1, 0)
        elif character == '<':
            angle_depth += 1
        elif character == '>':
            angle_depth = max(angle_depth - 1, 0)

    if current_part:
        schema_parts.append(''.join(current_part).strip())

    parsed_schema = {}
    for schema_part in schema_parts:
        tokens = schema_part.split(None, 1)
        if len(tokens) == 2:
            column_name, type_definition = tokens
            parsed_schema[column_name.lower()] = type_definition.lower().replace(' ', '')

    return parsed_schema


def _normalize_spark_type(spark_type: str) -> str:
    """
    Normalize Spark data type names to standard lowercase forms.
    
    Handles variations like 'StringType()', 'string', 'STRING' -> 'string'
    
    Args:
        spark_type: Spark data type as string
        
    Returns:
        str: Normalized type name in lowercase
    """
    if hasattr(spark_type, "simpleString"):
        type_str = spark_type.simpleString().lower()
    else:
        type_str = str(spark_type).strip().lower().replace(' ', '')
    
    # Remove 'type()' suffix if present (e.g., 'StringType()' -> 'string')
    if re.fullmatch(r'[a-z]+type\(\)', type_str):
        type_str = type_str.replace('type()', '')
    
    # Handle common type aliases
    type_mappings = {
        'bigint': 'long',
        'int': 'integer',
        'short': 'smallint',
        'byte': 'tinyint',
        'real': 'float',
        'text': 'string',
        'bool': 'boolean',
    }
    
    return type_mappings.get(type_str, type_str)


def validate_table_schema_contract(
    df: "DataFrame",
    expected_schema: str,
    source_description: str
) -> None:
    """
    Validate that a DataFrame's schema matches the expected schema contract.
    
    This function enforces schema contracts for Delta table sources. It verifies that:
    1. All expected columns exist in the source
    2. Column data types match the expected types
    
    Extra columns in the source are allowed (forward compatible).
    
    Args:
        df: The DataFrame to validate
        expected_schema: Expected schema string like 'name STRING, age INT, city STRING'
        source_description: Description of the source for error messages
        
    Raises:
        Exception: If schema validation fails with detailed mismatch information
        
    Example:
        >>> validate_table_schema_contract(
        ...     df=spark.sql("SELECT * FROM bronze.dbo.customers"),
        ...     expected_schema='customer_id INT, name STRING, email STRING',
        ...     source_description='bronze.dbo.customers'
        ... )
    """
    if not expected_schema:
        return  # No schema contract defined, skip validation
    
    # Parse expected schema
    expected_cols = _parse_expected_schema(expected_schema)
    
    if not expected_cols:
        return  # Empty schema, skip validation
    
    # Get actual schema from DataFrame
    actual_schema = {field.name.lower(): _normalize_spark_type(field.dataType)
                     for field in df.schema.fields}
    
    # Track validation errors
    missing_columns = []
    type_mismatches = []
    
    for expected_col, expected_type in expected_cols.items():
        normalized_expected_type = _normalize_spark_type(expected_type)
        
        if expected_col not in actual_schema:
            missing_columns.append(expected_col)
        elif actual_schema[expected_col] != normalized_expected_type:
            type_mismatches.append({
                'column': expected_col,
                'expected': normalized_expected_type,
                'actual': actual_schema[expected_col]
            })
    
    # Build error message if validation failed
    if missing_columns or type_mismatches:
        error_parts = [f"Schema contract validation failed for source: {source_description}"]
        
        if missing_columns:
            error_parts.append(f"\nMissing columns: {', '.join(missing_columns)}")
        
        if type_mismatches:
            mismatch_details = [f"{m['column']} (expected: {m['expected']}, actual: {m['actual']})" 
                               for m in type_mismatches]
            error_parts.append(f"\nType mismatches: {'; '.join(mismatch_details)}")
        
        error_parts.append(f"\nExpected schema: {expected_schema}")
        error_parts.append(f"\nActual columns: {', '.join(sorted(actual_schema.keys()))}")
        
        raise Exception(''.join(error_parts))
    
    log_and_print(f"Schema contract validated successfully for {source_description}. "
                  f"Verified {len(expected_cols)} expected columns.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Ingestion from Fabric Table Sources
# 
# This section provides functions for ingesting data from existing Delta tables within the Fabric environment. These functions support incremental loading patterns through watermark-based filtering and handle various data types and query complexities.
# 
# Key capabilities:
# - **Incremental Data Loading**: Efficiently processes only new or changed records using watermark columns
# - **Flexibility**: Handles complex ingestion functions including joins, CTEs, and aggregations
# - **Type-Safe Watermarking**: Supports both datetime and numeric watermark columns with proper filtering
# - **Empty Result Handling**: Gracefully exits when no new data is available

# CELL ********************

def route_to_ingestion_method(
    source_config: dict,
    watermark_config: dict,
    custom_paths: dict,
    file_config: dict,
    all_metadata: dict,
    folder_path_from_trigger: str,
    first_run: bool,
    lineage_info: dict
) -> tuple:
    """
    Route to appropriate ingestion method based on source configuration.
    
    This function determines whether to ingest data from files or from a Fabric table
    and calls the appropriate ingestion function accordingly.
    
    Args:
        source_config (dict): Source configuration settings
        watermark_config (dict): Watermark tracking configuration
        custom_paths (dict): Custom path configurations
        file_config (dict): Configs for file data
        all_metadata (dict): Complete metadata dictionary
        folder_path_from_trigger (str, optional): Folder path provided by trigger
        first_run (bool, optional): Flag indicating first run
        lineage_info (dict): Lineage tracking information for logging
    
    Returns:
        tuple: (new_data, new_watermark_value, source_details)
            - new_data (DataFrame): Ingested data
            - new_watermark_value: New watermark value after ingestion
            - source_details (str): Description of the data source
    
    Notes:
        - File ingestion uses ingest_raw_files() for Bronze_Files or shortcuts
        - Table ingestion uses ingest_from_fabric_table() for Fabric tables
        - Source details format differs between methods
    """
    if source_config['custom_source_function']:
        # Ingest from external source via custom source function (returns DataFrame directly)
        new_data, new_watermark_value, source_details = _ingest_with_custom_source_function(
            custom_source_function = source_config['custom_source_function'],
            custom_source_function_notebook = source_config['custom_source_function_notebook'],
            all_metadata = all_metadata,
            watermark_value = watermark_config['watermark_value']
        )

        # Check if any data was returned
        if new_data.isEmpty():
            exit_gracefully_no_data(
                source_details = source_details,
                watermark_value = new_watermark_value,
                lineage_info = lineage_info,
                first_run = first_run,
                context_message = "No new data returned from custom source function."
            )
    elif source_config['using_source_folder_path']:
        # Ingest from files (Bronze_Files or shortcuts)
        source_details = f"{custom_paths['source_files_lakehouse_path']}/{source_config['source_path']}" 
        new_data, new_watermark_value = ingest_raw_files(
            source_config = source_config,
            watermark_config = watermark_config,
            file_config = file_config,
            custom_paths = custom_paths,
            all_metadata = all_metadata,
            folder_path_from_trigger = folder_path_from_trigger,
            first_run = first_run,
            lineage_info = lineage_info
        )
    else:
        # Ingest from Fabric table
        new_data, new_watermark_value, source_details = ingest_from_fabric_table(
            source_config = source_config,
            watermark_config = watermark_config,
            all_metadata = all_metadata,
            first_run = first_run,
            lineage_info = lineage_info
        )
    
        # Validate schema contract if expected schema is defined
        expected_schema = source_config.get('expected_schema', '')
        if expected_schema:
            validate_table_schema_contract(
                df = new_data,
                expected_schema = expected_schema,
                source_description = source_details
            )
        # Note: exit_gracefully_no_data() is called inside ingest_from_fabric_table() when no data found
        
    return new_data, new_watermark_value, source_details

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _extract_new_watermark_value(
    watermark_table_name: str, 
    watermark_column_name: str
) -> str:
    """Extract new watermark value from table, supporting multiple watermark columns.
    
    Args:
        watermark_table_name (str): The table name to extract watermark from
        watermark_column_name (str): Single column name or comma-separated column names
        
    Returns:
        str: The maximum watermark value across all specified columns
    """    
    # Handle multiple watermark columns separated by commas
    if ',' in watermark_column_name:
        # Split and clean column names
        watermark_columns = [col.strip() for col in watermark_column_name.split(',')]
        
        # Use greatest function to find overall maximum across all columns.
        # Intentional SELECT *: Spark/Delta builds this lazily, and the aggregate below
        # references only watermark columns, so Catalyst/Delta prunes unused columns.
        overall_max = (
            spark.sql(f"SELECT * FROM {watermark_table_name}")
                 .agg(f.greatest(*[f.max(c) for c in watermark_columns]).alias("overall_max"))
                 .first()["overall_max"]
        )
        return str(overall_max)
    else:
        # Single column - use existing logic
        return str(spark.sql(f"SELECT MAX(`{watermark_column_name}`) as new_watermark_value FROM {watermark_table_name}").first()['new_watermark_value'])

def _construct_watermark_filter(
    watermark_column_data_type: str, 
    watermark_column_name: str, 
    watermark_value: str
) -> str:
    """Construct watermark filter based on data type and value."""
    if not watermark_value:
        # No watermark = initial/full load
        return ""
    
    # Handle multiple watermark columns separated by commas
    if ',' in watermark_column_name:
        watermark_columns = [col.strip() for col in watermark_column_name.split(',')]
        
        if watermark_column_data_type == 'datetime':
            # Create filter for each column and combine with OR
            column_filters = [f"{col} > TIMESTAMP '{watermark_value}'" for col in watermark_columns]
            return f"({' OR '.join(column_filters)})"
        elif watermark_column_data_type == 'numeric':
            # Create filter for each column and combine with OR
            column_filters = [f"{col} > {watermark_value}" for col in watermark_columns]
            return f"({' OR '.join(column_filters)})"
        else:
            # Fail fast on unsupported data types
            raise Exception(f"No logic currently exists for the {watermark_column_data_type} watermark column type. " +
                        "Supported types are: 'datetime', 'numeric'")
    else:
        # Single column - use existing logic
        if watermark_column_data_type == 'datetime':
            # datetime comparison using TIMESTAMP literal
            return f"{watermark_column_name} > TIMESTAMP '{watermark_value}'"
        elif watermark_column_data_type == 'numeric':
            # numeric comparison without quotes
            return f"{watermark_column_name} > {watermark_value}"
        else:
            # Fail fast on unsupported data types
            raise Exception(f"No logic currently exists for the {watermark_column_data_type} watermark column type. " +
                        "Supported types are: 'datetime', 'numeric'")

def _extract_and_validate_watermark(
    watermark_table_name: str, 
    watermark_column_name: str,
    table_id=None
) -> str:
    """Extract and validate new watermark value from table."""
    retrieving_watermark_log_info = f"Retrieving watermark value for next run from table, {watermark_table_name}."
    log_and_print(retrieving_watermark_log_info)

    # Schema-only validation; accessing .columns does not scan table rows.
    columns = spark.sql(f"SELECT * FROM {watermark_table_name}").columns
    
    # Check if all watermark columns exist in the source table (case-insensitive, matching Spark behavior)
    watermark_columns = [col.strip() for col in watermark_column_name.split(',')]
    columns_lower = [c.lower() for c in columns]
    missing_columns = [col for col in watermark_columns if col.lower() not in columns_lower]
    if missing_columns:
        raise Exception(
            f"The following watermark columns do not exist in the source table: {missing_columns}. "
            f"Available columns are: {columns}. To resolve, update your metadata by inputting valid "
            f"watermark column names in watermark_details.column_name with watermark_details.strategy='column', "
            f"or set watermark_details.strategy='none' for full-refresh processing. "
            f"{_agent_failure_routing_hint('watermark column missing', table_id)}"
        )
    
    new_watermark_value = _extract_new_watermark_value(
        watermark_table_name = watermark_table_name, 
        watermark_column_name = watermark_column_name
    )

    new_watermark_value_log_info = f"Watermark value for next run: {new_watermark_value}."
    log_and_print(new_watermark_value_log_info)
    
    return new_watermark_value

def _ingest_with_custom_table_function(
    custom_table_ingestion_function: str, 
    custom_table_ingestion_function_notebook: str, 
    all_metadata: dict, 
    watermark_table_name: str,
    watermark_column_name: str,
    watermark_column_data_type: str, 
    watermark_value: str
):
    """Handle ingestion using custom table/SQL function."""
    table_id = (all_metadata.get('orchestration_metadata') or {}).get('Table_ID')

    # Extract watermark value from the watermark table for next run
    if watermark_value and watermark_table_name:
        new_watermark_value = _extract_and_validate_watermark(
            watermark_table_name = watermark_table_name, 
            watermark_column_name = watermark_column_name,
            table_id = table_id
        )

        watermark_filter = _construct_watermark_filter(
            watermark_column_data_type = watermark_column_data_type, 
            watermark_column_name = watermark_column_name, 
            watermark_value = watermark_value
        )
    elif watermark_value and not watermark_table_name:
        # No watermark_details.table_name configured but watermark_value is non-empty.
        # Skip watermark extraction — treat as a full load to avoid querying with an empty table name.
        log_and_print(
            f"Warning: watermark_value '{watermark_value}' is set but watermark_table_name is empty. "
            f"Skipping watermark extraction and filter construction — treating as full load. "
            f"Set watermark_details.strategy='none' for full-refresh processing, or provide "
            f"watermark_details.table_name with watermark_details.strategy='column' when the custom table function needs a framework-built watermark filter."
        )
        new_watermark_value = ""
        watermark_filter = ""
        watermark_value = ""  # Clear so custom function receives empty value
    else:
        new_watermark_value = ""
        watermark_filter = ""

    ingesting_custom_table_ingestion_function_log_info = f"Ingesting data with custom table function, {custom_table_ingestion_function}."
    log_and_print(ingesting_custom_table_ingestion_function_log_info)

    source_details = f"Custom table function, {custom_table_ingestion_function}"

    if custom_table_ingestion_function_notebook:
        source_details += f", in {custom_table_ingestion_function_notebook}"
        instantiate_notebook(
            notebook_name=custom_table_ingestion_function_notebook,
            function_scope=(all_metadata.get('primary_config') or {}).get(
                'source_details_custom_table_ingestion_function_scope',
                'local',
            ),
        )

    all_metadata['watermark_column_name'] = watermark_column_name
    all_metadata['watermark_value'] = watermark_value
    all_metadata['watermark_filter'] = watermark_filter

    function_kwargs = {
        "metadata": all_metadata,
        "spark": spark
    }

    new_data = globals()[custom_table_ingestion_function](**function_kwargs)

    return new_data, new_watermark_value, source_details


def _assert_delta_change_data_feed_enabled(
    extracted_table_name: str,
    table_version: int | None = None
) -> None:
    """Validate a Delta source table is currently configured to emit Change Data Feed."""
    table_detail = DeltaTable.forName(spark, extracted_table_name).detail().select("properties").first()
    properties = table_detail["properties"] if table_detail else {}
    normalized_properties = {
        str(key).lower(): str(value).lower()
        for key, value in (properties or {}).items()
    }
    cdf_enabled = normalized_properties.get("delta.enablechangedatafeed") == "true"

    if not cdf_enabled:
        version_text = f" at current version {table_version}" if table_version is not None else ""
        raise ValueError(
            "This table is configured for Change Data Feed ingestion, but the source Delta table "
            f"'{extracted_table_name}' does not currently have delta.enableChangeDataFeed=true{version_text}. "
            "The initial CDF load uses a full table snapshot and then stores a Delta version watermark; "
            "without CDF enabled on the source first, the next incremental run can fail with "
            "DELTA_MISSING_CHANGE_DATA because that saved version has no recorded change feed. "
            "Add target_details.enable_change_data_feed='true' to the source table metadata, deploy it, "
            "then reset/reinitialize this table's CDF watermark or run a full reload once."
        )


def _read_full_table_with_cdf_columns(
    extracted_table_name: str
) -> tuple:
    """Read full table for initial load and add CDF metadata columns for downstream compatibility.
    
    Args:
        extracted_table_name: Name of the Delta table to read from
    
    Returns:
        tuple: (new_data DataFrame, new_watermark_value, source_details string)
    """
    log_and_print(f"Reading initial full table snapshot (no CDF): {extracted_table_name}")

    # Get table version BEFORE reading to ensure consistency
    table_version = DeltaTable.forName(spark, extracted_table_name).history(1).select("version").first()["version"]

    # Fast metadata-only guardrail: prevent writing a CDF watermark for a table that cannot serve CDF later.
    _assert_delta_change_data_feed_enabled(extracted_table_name, table_version)
    
    # Read full table
    new_data = spark.read.format("delta").table(extracted_table_name)
    
    # Add CDF metadata columns for downstream compatibility
    new_data = new_data \
        .withColumn("_commit_version", f.lit(table_version)) \
        .withColumn("_commit_timestamp", f.current_timestamp()) \
        .withColumn("_change_type", f.lit("insert"))
    
    # New watermark is current version + 1
    new_watermark_value = str(table_version + 1)
    log_and_print(f"New watermark (version): {new_watermark_value} (initial load from version: {table_version})")

    source_details = f"Full table read from {extracted_table_name} (initial load, version: {table_version})"
    
    return new_data, new_watermark_value, source_details


def _ingest_with_custom_source_function(
    custom_source_function: str,
    custom_source_function_notebook: str,
    all_metadata: dict,
    watermark_value: str
):
    """Handle ingestion using a custom external source function.

    Unlike custom_table_ingestion_function which reads from internal Fabric tables
    and receives framework-injected watermark filters, custom_source_function is
    designed for external sources (APIs, services, non-Fabric systems) where the
    function manages its own data retrieval and watermark tracking.

    Args:
        custom_source_function (str): Name of the Python function to call.
        custom_source_function_notebook (str): Name of the notebook containing the function.
        all_metadata (dict): Complete metadata dictionary passed to the function.
        watermark_value (str): Previous watermark value for incremental processing.

    Returns:
        tuple: (new_data DataFrame, new_watermark_value str, source_details str)
    """
    log_and_print(f"Ingesting data with custom source function: {custom_source_function}")

    source_details = f"Custom source function, {custom_source_function}"

    if custom_source_function_notebook:
        source_details += f", in {custom_source_function_notebook}"
        instantiate_notebook(
            notebook_name=custom_source_function_notebook,
            function_scope=(all_metadata.get('primary_config') or {}).get(
                'source_details_custom_source_function_scope',
                'local',
            ),
        )

    # Pass watermark and metadata to the function — no framework-injected filter
    all_metadata['watermark_value'] = watermark_value

    function_kwargs = {
        "metadata": all_metadata,
        "spark": spark
    }

    result = globals()[custom_source_function](**function_kwargs)

    # The function may return a plain DataFrame or a tuple of (DataFrame, next_watermark_value)
    if isinstance(result, tuple):
        new_data = result[0]
        new_watermark_value = str(result[1]) if len(result) > 1 and result[1] is not None else ""
    else:
        new_data = result
        new_watermark_value = ""

    if new_watermark_value:
        log_and_print(f"Custom source function returned next watermark: {new_watermark_value}")

    return new_data, new_watermark_value, source_details


def _read_cdf_incremental(
    extracted_table_name: str,
    watermark_value: str
) -> tuple:
    """Read incremental changes from Delta table using Change Data Feed.
    
    Args:
        extracted_table_name: Name of the Delta table to read from
        watermark_value: Previous watermark value (Delta version number) for incremental loading
    
    Returns:
        tuple: (new_data DataFrame, new_watermark_value, source_details string)
    """
    log_and_print(f"Ingesting incremental data using Change Data Feed: {extracted_table_name}")
    log_and_print(f"Reading CDF changes from version: {watermark_value}")
    
    # Check if the requested version exists in the table history
    current_version = DeltaTable.forName(spark, extracted_table_name).history(1).select("version").first()["version"]
    requested_version = int(watermark_value)
    
    if requested_version > current_version:
        # Requested version doesn't exist yet - no new changes
        log_and_print(f"No new changes: requested version {requested_version} > current version {current_version}")
        log_and_print("Returning empty DataFrame - no data to process")
        
        # Create empty DataFrame with CDF schema for consistency
        base_df = spark.read.format("delta").table(extracted_table_name).limit(0)
        new_data = base_df \
            .withColumn("_commit_version", f.lit(None).cast("long")) \
            .withColumn("_commit_timestamp", f.lit(None).cast("timestamp")) \
            .withColumn("_change_type", f.lit(None).cast("string"))
        
        # Keep same watermark since no new data
        new_watermark_value = str(requested_version)
        log_and_print(f"No new watermark - keeping version: {new_watermark_value}")
        
        source_details = f"Change Data Feed from {extracted_table_name} (no changes: version {requested_version} not yet available)"
    else:
        # Version exists - read CDF changes
        try:
            new_data = spark.read.format("delta") \
                .option("readChangeFeed", "true") \
                .option("startingVersion", watermark_value) \
                .table(extracted_table_name)
        except Exception as exc:
            error_text = str(exc)
            if "DELTA_MISSING_CHANGE_DATA" in error_text:
                raise ValueError(
                    "Change Data Feed is not available for the requested Delta version range. "
                    f"Table: {extracted_table_name}; requested startingVersion: {watermark_value}; "
                    f"current table version: {current_version}. This usually means the downstream "
                    "table already stored a CDF watermark, but the source table did not have "
                    "delta.enableChangeDataFeed=true when that version was created. Add "
                    "target_details.enable_change_data_feed='true' to the source table metadata, "
                    "deploy it, then reset/reinitialize this table's CDF watermark or run a full reload once. "
                    "CDF is not retroactive for Delta versions created before it was enabled. "
                    f"Original Delta error: {error_text}"
                ) from exc
            raise
        
        # Filter to only relevant change types (exclude update_preimage)
        log_and_print("Filtering CDF data to include only insert, update_postimage, and delete change types")
        new_data = new_data.filter(
            f.col("_change_type").isin("insert", "update_postimage", "delete")
        )
        
        # Set watermark based on whether data exists after filtering
        if new_data.isEmpty():
            # Data was filtered out completely - keep current version
            new_watermark_value = str(requested_version)
            log_and_print(f"No data after filtering - keeping watermark at version: {new_watermark_value}")
        else:
            # Data exists - advance watermark to current table version + 1
            new_watermark_value = str(current_version + 1)
            log_and_print(f"New watermark (version): {new_watermark_value} (current table version: {current_version})")
        
        source_details = f"Change Data Feed from {extracted_table_name} (startingVersion: {watermark_value})"
    
    return new_data, new_watermark_value, source_details


def _ingest_from_table_with_cdf(
    extracted_table_name: str,
    watermark_value: str,
    first_run: bool
) -> tuple:
    """Handle ingestion from Fabric tables using Change Data Feed.
    
    Routes to appropriate ingestion method based on whether this is an initial or incremental load.
    For incremental loads, it reads CDF changes and filters to relevant change types.
    For initial loads, it reads the full table and adds CDF metadata columns for consistency.
    
    Important Notes:
        - Watermark tracks the Delta table version number (integer)
        - New watermark is set to current table version + 1 to avoid re-reading same version
        - Automatically filters out 'update_preimage' change type (only 'insert', 
          'update_postimage', and 'delete' are returned)
        - If a single record has multiple updates/deletes between watermark checks,
          CDF will return multiple rows for that primary key
        - Recommended: Add drop_duplicates transformation on primary keys with 
          order_by='_commit_version' to keep only the most recent change
    
    Args:
        extracted_table_name: Name of the Delta table to read from
        watermark_value: Previous watermark value (Delta version number as string) for incremental loading
        first_run: Whether this is the first run (full load vs incremental)
    
    Returns:
        tuple: (new_data DataFrame, new_watermark_value, source_details string)
    
    Example Configuration for Deduplication:
        ```sql
        INSERT INTO Advanced_Configuration VALUES
        (106, 'data_transformation_steps', 'drop_duplicates', 1, 'order_by', '_commit_version');
        ```
    """
    # Check if watermark is a default/uninitialized value (treat as first run)
    normalized_watermark_value = "" if watermark_value is None else str(watermark_value).strip()
    is_default_watermark = normalized_watermark_value in ('', '1900-01-01 00:00:00', '-1')
    
    # Route based on first_run flag or default watermark values
    if first_run or is_default_watermark:
        # Initial load: Read full table with CDF columns
        new_data, new_watermark_value, source_details = _read_full_table_with_cdf_columns(
            extracted_table_name=extracted_table_name
        )
    else:
        # Incremental load: Read CDF changes
        new_data, new_watermark_value, source_details = _read_cdf_incremental(
            extracted_table_name=extracted_table_name,
            watermark_value=watermark_value
        )
    
    return new_data, new_watermark_value, source_details


def _ingest_from_table_with_watermark(
    source_query: str,
    extracted_table_name: str,
    watermark_column_data_type: str,
    watermark_column_name: str,
    watermark_value: str,
    watermark_table_name: str,
    first_run: bool,
    table_id=None
) -> tuple:
    """Handle ingestion from Fabric tables using traditional watermark filtering.
    
    Args:
        source_query: SQL query to execute
        extracted_table_name: Name of the table being queried
        watermark_column_data_type: Data type of watermark column ('datetime' or 'numeric')
        watermark_column_name: Name of watermark column(s)
        watermark_value: Previous watermark value for incremental loading
        watermark_table_name: Table name to extract watermark from
        first_run: Whether this is the first run (full load vs incremental)
    
    Returns:
        tuple: (new_data DataFrame, new_watermark_value, source_details string)
    """
    ingesting_from_table_log_info = f"Ingesting data from table, {extracted_table_name}."
    log_and_print(ingesting_from_table_log_info)
    
    # Construct watermark filter based on data type and value
    if watermark_value:
        new_watermark_value = _extract_and_validate_watermark(
            watermark_table_name = watermark_table_name, 
            watermark_column_name = watermark_column_name,
            table_id = table_id
        )

        watermark_filter = _construct_watermark_filter(
            watermark_column_data_type = watermark_column_data_type, 
            watermark_column_name = watermark_column_name, 
            watermark_value = watermark_value
        )
    else:
        new_watermark_value = ""
        watermark_filter = ""

    # Execute the source query
    executing_source_query_log_info = f"Executing source query: {source_query}"
    log_and_print(executing_source_query_log_info)

    source_details = source_query
    new_data = spark.sql(source_query)

    # Apply watermark filtering if applicable
    if not first_run and watermark_filter:
        applying_watermark_filter_log_info = f"Applying watermark filter: {watermark_filter}"
        log_and_print(applying_watermark_filter_log_info)

        new_data = new_data.filter(watermark_filter)
        source_details += f" WHERE {watermark_filter}"
    
    return new_data, new_watermark_value, source_details


def _ingest_from_table(
    source_query: str, 
    watermark_column_data_type: str, 
    watermark_column_name: str, 
    watermark_value: str, 
    watermark_table_name: str,
    watermark_strategy: str,
    first_run: bool,
    table_id=None
) -> tuple:
    """Orchestrate ingestion from Fabric tables with watermark filtering or Change Data Feed.
    
    This function routes to the appropriate ingestion method based on configuration:
    - Change Data Feed (CDF): Uses Delta Lake's built-in change tracking with _commit_timestamp
    - Traditional Watermark: Uses business columns to track changes
    
    Args:
        source_query: SQL query to execute
        watermark_column_data_type: Data type of watermark column ('datetime' or 'numeric')
        watermark_column_name: Name of watermark column(s) - only used for traditional watermarking
        watermark_value: Previous watermark value for incremental loading
        watermark_table_name: Table name to extract watermark from
        watermark_strategy: Watermark behavior for this table (`column`, `change_data_feed`, `none`, etc.)
    
    Returns:
        tuple: (new_data DataFrame, new_watermark_value, source_details string)
    """
    extracted_table_name = source_query.replace("SELECT * FROM ", "")

    # Route to appropriate ingestion method
    if watermark_strategy == "change_data_feed":
        return _ingest_from_table_with_cdf(
            extracted_table_name = extracted_table_name,
            watermark_value = watermark_value,
            first_run = first_run
        )
    else:
        return _ingest_from_table_with_watermark(
            source_query = source_query,
            extracted_table_name = extracted_table_name,
            watermark_column_data_type = watermark_column_data_type,
            watermark_column_name = watermark_column_name,
            watermark_value = watermark_value,
            watermark_table_name = watermark_table_name,
            first_run = first_run,
            table_id = table_id
        )

def convert_datetime_to_string(value):
    """Convert datetime object to string format if needed."""
    from datetime import datetime
    
    if type(value) == datetime:
        log_and_print("Converting watermark datetime value from datetime to string.")
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")
    
    return value

def ingest_from_fabric_table(
    source_config: dict,
    watermark_config: dict,
    all_metadata: dict,
    first_run: bool,
    lineage_info: dict
) -> DataFrame:
    """
    Ingest data incrementally from Fabric Delta tables using watermark-based filtering or Change Data Feed.

    This function executes SQL queries against Fabric tables and applies either:
    1. Traditional watermark filtering - Uses business columns to track changes
    2. Change Data Feed (CDF) - Uses Delta Lake's built-in change tracking with _commit_timestamp

    CDF provides more reliable incremental loading by capturing all changes (inserts, updates, deletes)
    at the transaction level, eliminating the need for business watermark columns.

    The function automatically exits the notebook if no new data is found,
    preventing unnecessary processing and maintaining pipeline efficiency.

    Args:
        source_config (dict): Source configuration with query and table details
        watermark_config (dict): Watermark settings including strategy
        custom_paths (dict): Path configurations for custom functions
        all_metadata (dict): Complete metadata dictionary
        first_run (bool): Flag indicating first run
        lineage_info (dict): Lineage tracking information for logging

    Returns:
        tuple: (new_data DataFrame, new_watermark_value string, source_details string)

    Raises:
        Exception: If an unsupported watermark column data type is specified (traditional mode).

    Performance Considerations:
        - CDF mode: Efficient change tracking at transaction level, no column scans needed
        - Traditional mode: Watermark filtering applied after query execution
        - For large datasets with traditional mode, consider adding watermark predicates to source_query
        - Indexes on watermark columns significantly improve traditional mode performance

    Notes:
        - CDF mode requires Delta tables with Change Data Feed enabled
        - CDF tracks changes using _commit_timestamp for better observability in logs
        - Traditional mode: Empty watermark values trigger full table scans (initial loads)
        - The function preserves the original query's column order and naming
        - Notebook exits gracefully if no new data is found
    """
    watermark_value = watermark_config['watermark_value']

    # Ingest using custom table function if provided in metadata
    if source_config['custom_table_ingestion_function']:
        new_data, new_watermark_value, source_details = _ingest_with_custom_table_function(
            custom_table_ingestion_function = source_config['custom_table_ingestion_function'],
            custom_table_ingestion_function_notebook = source_config['custom_table_ingestion_function_notebook'],
            all_metadata = all_metadata,
            watermark_table_name = source_config['watermark_table_name'],
            watermark_column_name = watermark_config['watermark_column_name'],
            watermark_column_data_type = watermark_config['watermark_column_data_type'],
            watermark_value = watermark_value
        )
    # otherwise, ingest all data from table
    else:
        new_data, new_watermark_value, source_details = _ingest_from_table(
            source_query = source_config['source_query'],
            watermark_column_data_type = watermark_config['watermark_column_data_type'],
            watermark_column_name = watermark_config['watermark_column_name'],
            watermark_value = watermark_value,
            watermark_table_name = source_config['watermark_table_name'],
            watermark_strategy = watermark_config['strategy'],
            first_run = first_run,
            table_id = source_config.get('table_id')
        )

    # Check if any data was returned after filtering
    if new_data.isEmpty():
        exit_gracefully_no_data(
            source_details = source_details,
            watermark_value = new_watermark_value,
            lineage_info = lineage_info,
            first_run = first_run,
            context_message = "No new data returned from Fabric table after watermark filtering."
        )

    # Convert datetime objects to string format for logging
    new_watermark_value = convert_datetime_to_string(value = new_watermark_value)

    return new_data, new_watermark_value, source_details

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Initiate Logging Function

# CELL ********************

_run_log_buffer: list = []
"""Module-level buffer that accumulates structured log entries during a run.
Each entry is a dict with keys: seq, ts, level, step_name, step_number, message.
Written to Activity_Run_Logs (structured)."""

_current_step_name: Optional[str] = None
"""The currently active processing step name (e.g., 'Data Ingestion')."""

_current_step_number: Optional[int] = None
"""The currently active processing step number (e.g., 6)."""


def set_current_step(step_number: int, step_name: str) -> None:
    """Set the active processing step. All subsequent log_and_print() calls
    will be tagged with this step until it is changed or cleared."""
    global _current_step_name, _current_step_number
    _current_step_name = step_name
    _current_step_number = step_number


def clear_current_step() -> None:
    """Clear the active step context."""
    global _current_step_name, _current_step_number
    _current_step_name = None
    _current_step_number = None


def get_run_log_messages() -> Optional[str]:
    """Return all accumulated log messages joined by newlines, or None if empty."""
    if not _run_log_buffer:
        return None
    return "\n".join(
        f"{entry['ts']} [{entry['level']}] {entry['message']}"
        for entry in _run_log_buffer
    )


def get_run_log_entries() -> list:
    """Return the structured log entries list for persistence to Activity_Run_Logs."""
    return _run_log_buffer


def clear_run_log_buffer() -> None:
    """Reset the log buffer and step context. Call at the start of each notebook run."""
    _run_log_buffer.clear()
    clear_current_step()


def log_and_print(
    message: str, 
    level: str = "info"
):
    """
    Reusable function to log and print messages with different severity levels.
    Appends a structured entry to _run_log_buffer for
    Activity_Run_Logs persistence.
    
    Args:
        message (str): The message to log and print
        level (str): The logging level - "info", "warn", or "error"
    """
    # https://learn.microsoft.com/en-us/fabric/data-engineering/azure-fabric-diagnostic-emitters-log-analytics#write-custom-application-logs
    logger = sc._jvm.org.apache.log4j.LogManager.getLogger("spark_application_logger")

    valid_levels = ["info", "warn", "error"]
    if level.lower() not in valid_levels:
        raise ValueError(f"Invalid log level '{level}'. Valid options are: {', '.join(valid_levels)}")
    
    level_upper = level.upper()
    if level.lower() == "warn":
        level_upper = "WARNING"
    elif level.lower() == "error":
        level_upper = "ERROR"
    
    formatted_message = f"[{level_upper}] {message}"

    # Append structured entry to buffer
    ts = datetime.now(timezone.utc)
    _run_log_buffer.append({
        "seq": len(_run_log_buffer) + 1,
        "ts": ts.strftime('%Y-%m-%dT%H:%M:%S'),
        "level": level_upper,
        "step_name": _current_step_name,
        "step_number": _current_step_number,
        "message": message[:4000],
    })
    
    if level.lower() == "info":
        logger.info(message)
    elif level.lower() == "warn":
        logger.warn(message)
    elif level.lower() == "error":
        logger.error(message)

    print(formatted_message)
    # Add a newline for better readability in console output
    print()

def exit_gracefully_no_data(
    source_details: str,
    watermark_value: Union[str, datetime, Any],
    lineage_info: dict,
    first_run: bool,
    context_message: str = ""
):
    """
    Reusable function to handle graceful exit when no data is found.
    
    This function handles the common pattern of exiting a notebook when no new data
    is available. First runs emit a warning; subsequent runs emit an informational
    message. Both cases exit through NoDataFoundError so the caller can log a
    Processed status with zero records.
    
    Args:
        source_details (str): Source path or description for error/exit messages
        watermark_value (Union[str, datetime, Any]): Current watermark value. Accepts string,
            datetime objects, or any type - will be converted to string for exit payload.
        lineage_info (dict): Lineage tracking info for exit payload
        first_run (bool): Whether this is the first run (controls warning vs info logging)
        context_message (str): Optional additional context for log messages
    
    Side Effects:
        Raises NoDataFoundError so the calling notebook can log a Processed/0-record
        event and stop execution gracefully.
    """
    if first_run:
        no_new_data_log_info = (
            "No data was found during initial load; processing will complete "
            f"with 0 records. Context: {context_message} Source: {source_details}"
        )
        log_and_print(no_new_data_log_info, "warn")
    else:
        no_new_data_log_info = f"No new data found. {context_message}" if context_message else "No new data found since last data load."
        log_and_print(no_new_data_log_info)

    raise NoDataFoundError(
        message=no_new_data_log_info,
        watermark_value=watermark_value,
        source_details=source_details,
        lineage_info=lineage_info
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
