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

# # NB_Helper_Functions_2 - File Ingestion and DDL Generation Utilities
# 
# ## Overview
# This notebook contains specialized helper functions for the Fabric Data Platform Accelerator, focusing on:
# - **File Ingestion**: Reading various file formats from Bronze_Files with schema inference and evolution support
# - **DDL Generation**: Creating Delta table DDL from source database schemas (DB2, SQL Server, PostgreSQL, MySQL)
# - **Custom Ingestion**: Framework for user-defined ingestion functions
# 
# ### Key Features
# - Automatic schema detection and type mapping from database sources
# - Support for incremental file processing with watermark tracking
# - Flexible file format handling (CSV, JSON, Parquet, Excel, XML)
# - Schema evolution management with configurable failure modes
# 
# ### Integration Points
# - Used by `NB_Batch_Processing` for Bronze layer data ingestion
# - Integrates with metadata warehouse for DDL extraction
# - Supports custom functions stored in `metadata_lakehouse`

# MARKDOWN ********************

# ## 1. Dynamic DDL Processing Orchestrator
# 
# The following function serves as the central orchestrator for converting source database schemas to Delta Lake DDL. It intelligently routes to the appropriate DDL generator based on the source system type, ensuring accurate data type mapping and constraint preservation.
# 
# ### Supported Source Systems
# - **Oracle**: Full support for NUMBER, VARCHAR2, DATE, TIMESTAMP types
# - **SQL Server / Azure SQL**: Handles all common data types including MONEY, UNIQUEIDENTIFIER
# - **DB2**: Supports DECIMAL, FLOAT, and specialized DB2 types
# - **PostgreSQL**: Includes support for ARRAY, JSON, and PostgreSQL-specific types
# - **MySQL**: Handles MySQL-specific types and precision specifications
# 
# ### Key Capabilities
# - Preserves source column comments and table descriptions
# - Maintains NOT NULL constraints based on configuration
# - Applies liquid clustering for query optimization
# - Supports external Delta tables on ADLS Gen2

# CELL ********************

def _updated_column_definition(
    df: DataFrame, 
    column_definitions: list, 
):
    """Filter DDL to only include columns that were actually ingested."""
    updated_ddl = []

    df_columns = df.columns

    for column_definition in column_definitions:
        col_name = column_definition[0]
        if col_name not in df_columns and not col_name.startswith('delta__'):
            column_not_ingested_log_warn = f"Column, `{col_name}`, was not ingested from the source database."
            log_and_print(column_not_ingested_log_warn, "warn")
            continue
        
        updated_ddl.append(column_definition)

    if not updated_ddl:
        raise Exception("None of the columns in the source database were ingested.")

    return updated_ddl


def _safe_get_comment(row: dict, key: str) -> str:
    """
    Safely get a comment field from DDL metadata, handling JSON null values.
    
    When metadata comes from JSON (e.g., pipeline lookup activities), null values
    become Python None. This helper handles both missing keys and None values,
    and removes single quotes since Spark's COMMENT clause parser cannot handle them.
    
    Args:
        row (dict): Dictionary containing column/table metadata
        key (str): Key to look up (e.g., 'COLUMN_COMMENTS', 'TABLE_COMMENTS')
    
    Returns:
        str: The comment value (with apostrophes removed), or empty string if missing/null
    
    Example:
        >>> row = {'COLUMN_COMMENTS': None}  # From JSON null
        >>> _safe_get_comment(row, 'COLUMN_COMMENTS')
        ''
        >>> row = {'COLUMN_COMMENTS': "It's a test"}
        >>> _safe_get_comment(row, 'COLUMN_COMMENTS')
        "Its a test"
    """
    value = row.get(key) or ""
    return value.replace("'", "")

def _safe_int_or_none(value, default=None):
    """Convert DDL numeric metadata to int while treating JSON null/blank as a default."""
    if value is None or value == "":
        return default
    return int(float(value))

def _apply_common_type_casts(
    df: DataFrame, 
    column_definitions: list
):
    """Apply common type-casting rules used by all source DDL converters.

    Args:
        df: Source Spark DataFrame.
        column_definitions: Iterable of (column_name, spark_type, ...) tuples.

    Returns:
        DataFrame: DataFrame with supported source types cast to target Spark types.
    """
    for column_definition in column_definitions:
        col_name = column_definition[0]
        spark_data_type = column_definition[1]
        if spark_data_type in ('BOOLEAN', 'TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'FLOAT', 'DOUBLE'):
            log_and_print(f"Converting `{col_name}` to `{spark_data_type}` to match source database ddl.")
            df = df.withColumn(col_name, f.col(col_name).cast(spark_data_type))
        elif spark_data_type == 'DATE':
            log_and_print(f"Converting `{col_name}` to `{spark_data_type}` to match source database ddl.")
            df = df.withColumn(col_name, f.to_date(f.to_timestamp(f.col(col_name), 'yyyy-MM-dd HH:mm:ss.SSSSSSS')))
        elif spark_data_type == 'TIMESTAMP':
            log_and_print(f"Converting `{col_name}` to `{spark_data_type}` to match source database ddl.")
            df = df.withColumn(col_name, f.to_timestamp(f.col(col_name)))
        elif spark_data_type == 'BINARY':
            log_and_print(f"Converting `{col_name}` to `{spark_data_type}` to match source database ddl.")
            df = df.withColumn(col_name, f.col(col_name).cast('BINARY'))
        elif spark_data_type.startswith('DECIMAL'):
            log_and_print(f"Converting `{col_name}` to `{spark_data_type}` to match source database ddl.")
            df = df.withColumn(col_name, f.col(col_name).cast(spark_data_type))
    
    return df

def apply_source_type_casts(
    df: DataFrame,
    column_definitions: list
):
    """
    Apply source-driven type casting to DataFrame columns.
    
    This function aligns ingested DataFrame column types with source metadata before
    downstream table creation/writes. Table creation is handled in create_delta_table().
    
    Args:
        df (DataFrame): Source DataFrame with data to be written to the table.
                       Column names should already be standardized to match DDL.
        
        column_definitions (list): List of column definition tuples from DDL generators.
                      Each tuple contains: [col_name, spark_type, constraints]
                      Example: [["Customer_Name", "STRING", "COMMENT 'Customer name' NOT NULL"]]
    
    Returns:
        DataFrame: DataFrame with proper type casting applied
    
    Raises:
        Exception: If no columns from column_definitions exist in the DataFrame
    
    Processing Steps:
        1. Filter column definitions to only include columns present in DataFrame
        2. Apply type casting to DataFrame columns based on filtered definitions
        3. Return type-casted DataFrame
    
    Example:
        >>> column_defs = [
        ...     ["Customer_Name", "STRING", "COMMENT 'Name' NOT NULL"],
        ...     ["Customer_Id", "INT", "COMMENT 'ID'"]
        ... ]
        >>> df = apply_source_type_casts(df, column_defs)
    
    Notes:
        - Automatically skips columns not present in DataFrame (with warning logs)
        - Preserves delta__ system columns even if not in column_definitions
        - Type casting follows Spark SQL semantics with implicit conversions
    """

    if not column_definitions:
        return df

    # Filter to only columns that were actually ingested from source
    # This handles cases where DDL metadata includes columns not in the data
    updated_column_definitions = _updated_column_definition(
        df = df, 
        column_definitions = column_definitions 
    )

    # Apply type casting to DataFrame columns to match DDL types
    # Ensures data types align with table schema (e.g., strings to dates, numbers to decimals)
    df = _apply_common_type_casts(
        df = df,
        column_definitions = updated_column_definitions
    )

    return df

def build_source_column_definitions(
    table_ddl: list,
    enforce_not_null: bool,
    source_category: str,
    advanced_processing_config: dict
):
    """
    Orchestrate the conversion of source database DDL to Spark Delta table DDL.
    
    This function serves as a router that delegates to source-specific DDL generators
    based on the detected source system type. It ensures consistent DDL generation
    across different database platforms while preserving source-specific nuances.

    Args:
        table_ddl (list): Source DDL metadata as python list containing column definitions
        enforce_not_null (bool): Whether to enforce NOT NULL constraints
        source_category (str): Source system type (oracle, sql_server, etc.)
        advanced_processing_config (dict): Dictionary containing processing configuration with column name cleansing options

    Returns:
        list: column definitions

    Raises:
        Exception: If source system type is not supported

    """
    if not table_ddl:
        return []

    log_and_print(f"Generating source-aligned column definitions for `{source_category}` table.")
    
    # Database-specific DDL generators registry
    DDL_GENERATORS = {
        'oracle': create_oracle_table_ddl,
        'azure_sql': create_sql_server_table_ddl,
        'sql_server': create_sql_server_table_ddl,
        'db2': create_db2_table_ddl,
        'postgre_sql': create_postgre_sql_table_ddl,
        'my_sql': create_my_sql_table_ddl
    }
    
    # Get the appropriate DDL generator
    ddl_generator = DDL_GENERATORS.get(source_category)
    if not ddl_generator:
        raise Exception(f"Add logic to support creating DDL for the source, {source_category}.")

    # Generate DDL using the appropriate generator
    column_definitions = ddl_generator(
        table_ddl=table_ddl,
        enforce_not_null=enforce_not_null,
        advanced_processing_config=advanced_processing_config
    )
    
    return column_definitions

def create_oracle_table_ddl(
    table_ddl: dict,
    enforce_not_null: bool,
    advanced_processing_config: dict
):
    """
    Convert Oracle database DDL to Spark Delta table DDL with precise type mapping.
    
    This function handles Oracle-specific data type conversions, preserving precision
    and scale for numeric types while maintaining column constraints and metadata.
    Special attention is given to Oracle's NUMBER type variations and timestamp handling.

    Args:
        table_ddl (list): Source DDL metadata as python list containing column definitions
        enforce_not_null (bool): Whether to enforce NOT NULL constraints
        advanced_processing_config (dict): Dictionary containing processing configuration with column name cleansing options

    Returns:
        list: column definitions

    Oracle Type Mappings:
        - VARCHAR2/CHAR/CLOB → STRING
        - NUMBER (no precision) → DOUBLE
        - NUMBER(p,s) → DECIMAL(p,s)
        - DATE → TIMESTAMP (Oracle DATE includes time-of-day)
        - TIMESTAMP → TIMESTAMP (microsecond precision)
        - INTEGER → INT

    Special Handling:
        - Primary key indicators added to column comments
        - Table-level comments preserved from source
        - Column name standardization applied based on configuration
        - Automatic type casting for numeric precision preservation
    """
    # Comprehensive Oracle to Spark type mapping
    oracle_to_spark_type_mapping = {
        # Character Types
        'VARCHAR2': 'STRING',              # Variable-length character string
        'VARCHAR': 'STRING',               # ANSI SQL VARCHAR
        'CHAR': 'STRING',                  # Fixed-length character string
        'NCHAR': 'STRING',                 # Fixed-length national character set
        'NVARCHAR2': 'STRING',             # Variable-length national character set
        'CLOB': 'STRING',                  # Character large object
        'NCLOB': 'STRING',                 # National character large object
        'LONG': 'STRING',                  # Legacy large character (deprecated)
        
        # Numeric Types (excluding NUMBER which needs special handling)
        'INTEGER': 'INT',                  # ANSI integer
        'INT': 'INT',                      # Alias for INTEGER
        'SMALLINT': 'SMALLINT',            # Small integer
        'FLOAT': 'DOUBLE',                 # Floating point
        'BINARY_FLOAT': 'FLOAT',           # 32-bit floating point
        'BINARY_DOUBLE': 'DOUBLE',         # 64-bit floating point
        'REAL': 'FLOAT',                   # Single precision float
        
        # Date/Time Types
        'DATE': 'TIMESTAMP',               # Oracle DATE stores date + time-of-day
        'TIMESTAMP': 'TIMESTAMP',          # Timestamp without timezone
        'TIMESTAMP WITH TIME ZONE': 'TIMESTAMP',      # Timestamp with timezone
        'TIMESTAMP WITH LOCAL TIME ZONE': 'TIMESTAMP', # Timestamp with local timezone
        'INTERVAL YEAR TO MONTH': 'STRING',           # Year-month interval
        'INTERVAL DAY TO SECOND': 'STRING',           # Day-time interval
        
        # Binary Types
        'RAW': 'BINARY',                   # Variable-length binary
        'LONG RAW': 'BINARY',              # Large binary (deprecated)
        'BLOB': 'BINARY',                  # Binary large object
        'BFILE': 'STRING',                 # External binary file reference
        
        # Other Types
        'ROWID': 'STRING',                 # Physical row identifier
        'UROWID': 'STRING',                # Universal row identifier
        'XMLTYPE': 'STRING',               # XML data type
        'ANYDATA': 'STRING',               # Generic data type
        'SDO_GEOMETRY': 'STRING',          # Spatial geometry type
        'JSON': 'STRING',                  # JSON data type (12c+)
        
        # User-Defined Types - mapped to STRING by default
        'OBJECT': 'STRING',                # Object type
        'VARRAY': 'STRING',                # Variable array
        'NESTED TABLE': 'STRING'           # Nested table type
    }

    column_definitions = []

    col_names = [column['COLUMN_NAME'] for column in table_ddl]
    
    col_name_mapping = dict(column_name_cleansing(
        columns=col_names,
        advanced_processing_config=advanced_processing_config
    ))
    
    # Process each column definition
    for row in table_ddl:
        col_name = col_name_mapping[row['COLUMN_NAME']]
        data_type = row['DATA_TYPE'].upper()
        precision = row.get('DATA_PRECISION')
        scale = row.get('DATA_SCALE')
    
        spark_data_type = oracle_to_spark_type_mapping.get(data_type, 'STRING')
        
        # Handle Oracle NUMBER type with special logic
        # Note: JSON deserialization can turn integers into floats (e.g., 9 → 9.0),
        # so we cast precision/scale to int to avoid invalid DDL like DECIMAL(9.0,0.0)
        if data_type == 'NUMBER':
            precision_int = _safe_int_or_none(precision)
            scale_int = _safe_int_or_none(scale, 0)
            if precision_int is None:
                spark_data_type = 'DOUBLE'
            elif precision_int > 38 or scale_int < 0 or scale_int > precision_int:
                spark_data_type = 'STRING'  # Fallback for types Spark DECIMAL cannot represent
            else:
                spark_data_type = f'DECIMAL({precision_int},{scale_int})'
        elif spark_data_type == 'DECIMAL':
            precision_int = _safe_int_or_none(precision)
            scale_int = _safe_int_or_none(scale, 0)
            if precision_int is None:
                pass
            elif precision_int > 38 or scale_int < 0 or scale_int > precision_int:
                spark_data_type = 'STRING'  # Fallback for very large decimals
            else:
                spark_data_type = f"DECIMAL({precision_int},{scale_int})"
        
        # Determine NULL constraint
        nullable_text = ""
        if enforce_not_null and row['NULLABLE'] == 'N':
            nullable_text = " NOT NULL"
        
        # PostgreSQL folds unquoted aliases like AS COLUMN_NAME to lowercase keys.
        # The current PL_02 PostgreSQL metadata query does not return column comments.
        column_comments = _safe_get_comment(row, "COLUMN_COMMENTS")
        
        # Format column definition
        column_definitions.append([col_name, spark_data_type, f"COMMENT '{column_comments}' {nullable_text}"])

    return column_definitions

def create_db2_table_ddl(
    table_ddl: dict,
    enforce_not_null: bool,
    advanced_processing_config: dict
):
    """
    Generate Delta table DDL from IBM DB2 source table schema.
    
    This function transforms DB2 column definitions into Spark SQL DDL, handling DB2's
    specialized data types and maintaining schema fidelity. It supports advanced Delta
    features like liquid clustering and external storage locations.
    
    Args:
        table_ddl (list): Source DDL metadata as python list containing column definitions
        enforce_not_null (bool): Whether to enforce NOT NULL constraints
        advanced_processing_config (dict): Dictionary containing processing configuration with column name cleansing options
    
    Returns:
        list: column definitions
    
    DB2 to Spark Type Mappings:
        - SMALLINT → SMALLINT
        - INTEGER → INT
        - BIGINT → BIGINT
        - DECIMAL/NUMERIC → DECIMAL(precision, scale)
        - REAL → FLOAT
        - DOUBLE/FLOAT → DOUBLE
        - CHAR/VARCHAR → STRING
        - DATE → DATE
        - TIME → STRING (no direct TIME type in Spark)
        - TIMESTAMP → TIMESTAMP
        - CLOB/DBCLOB → STRING
        - BLOB → BINARY
        - GRAPHIC/VARGRAPHIC → STRING (Unicode support)
    """
    # DB2 to Spark SQL type mapping dictionary
    db2_to_spark_type_mapping = {
        'SMALLINT': 'SMALLINT',         # 16-bit integer
        'INTEGER': 'INT',                # 32-bit integer
        'INT': 'INT',                    # Alias for INTEGER
        'BIGINT': 'BIGINT',              # 64-bit integer
        'DECIMAL': 'DECIMAL',            # Fixed-point numeric
        'NUMERIC': 'DECIMAL',            # Alias for DECIMAL
        'DEC': 'DECIMAL',                # Alias for DECIMAL
        'REAL': 'FLOAT',                 # Single precision float
        'DOUBLE': 'DOUBLE',              # Double precision float
        'FLOAT': 'DOUBLE',               # Alias for DOUBLE
        'CHAR': 'STRING',                # Fixed-length character
        'VARCHAR': 'STRING',             # Variable-length character
        'LONG VARCHAR': 'STRING',        # Long variable character
        'CLOB': 'STRING',                # Character large object
        'GRAPHIC': 'STRING',             # Fixed-length graphic string
        'VARGRAPHIC': 'STRING',          # Variable-length graphic string
        'DBCLOB': 'STRING',              # Double-byte character large object
        'BLOB': 'BINARY',                # Binary large object
        'DATE': 'DATE',                  # Date without time
        'TIME': 'STRING',                # Time without date (no Spark equivalent)
        'TIMESTAMP': 'TIMESTAMP',        # Date and time with fractional seconds
        'BINARY': 'BINARY',              # Fixed-length binary
        'VARBINARY': 'BINARY',           # Variable-length binary
        'FOR BIT DATA': 'BINARY'         # Binary string modifier
    }

    column_definitions = []

    col_names = [column['COLUMN_NAME'] for column in table_ddl]
    
    col_name_mapping = dict(column_name_cleansing(
        columns=col_names,
        advanced_processing_config=advanced_processing_config
    ))

    # Process each column definition
    for row in table_ddl:
        col_name = col_name_mapping[row['COLUMN_NAME']]
        
        # Extract base data type (remove any modifiers or parameters)
        data_type = row['DATA_TYPE'].upper().split('(')[0].strip()
        
        # Handle special case for VARCHAR/CHAR FOR BIT DATA
        if 'FOR BIT DATA' in row['DATA_TYPE'].upper():
            spark_data_type = 'BINARY'
        else:
            spark_data_type = db2_to_spark_type_mapping.get(data_type, 'STRING')
        
        # Add precision and scale for DECIMAL types
        # Cast to int to guard against JSON float deserialization (e.g., 9.0 → 9)
        if spark_data_type == 'DECIMAL':
            precision = _safe_int_or_none(row.get('DATA_PRECISION'))
            if precision is not None:
                scale = _safe_int_or_none(row.get('DATA_SCALE'), 0)
                spark_data_type = f"DECIMAL({precision},{scale})"
        
        # Determine NULL constraint based on configuration
        nullable_text = ""
        if enforce_not_null and row.get('NULLABLE') == 'NO':
            nullable_text = " NOT NULL"
        
        column_comments = _safe_get_comment(row, "COLUMN_COMMENTS")
        
        # Format column definition
        column_definitions.append([col_name, spark_data_type, f"COMMENT '{column_comments}' {nullable_text}"])

    return column_definitions

def create_sql_server_table_ddl(
    table_ddl: dict,
    enforce_not_null: bool,
    advanced_processing_config: dict
):
    """
    Generate Delta table DDL from Microsoft SQL Server source table schema.
    
    This function transforms SQL Server column definitions into Spark SQL DDL, handling
    SQL Server's extensive type system including CLR types, spatial types, and deprecated
    types. It maintains precision and scale for numeric types and handles special SQL
    Server features.
    
    Args:
        table_ddl (list): Source DDL metadata as python list containing column definitions
        enforce_not_null (bool): Whether to enforce NOT NULL constraints
        advanced_processing_config (dict): Configuration dict containing column name cleansing options
    
    Returns:
        list: column definitions
    """
    # Comprehensive SQL Server to Spark type mapping
    sql_server_to_spark_type_mapping = {
        # Exact Numeric Types
        'bit': 'BOOLEAN',                # 0 or 1, commonly used for flags
        'tinyint': 'TINYINT',            # 0 to 255
        'smallint': 'SMALLINT',          # -32,768 to 32,767
        'int': 'INT',                    # -2^31 to 2^31-1
        'bigint': 'BIGINT',              # -2^63 to 2^63-1
        'decimal': 'DECIMAL',            # Fixed precision and scale
        'numeric': 'DECIMAL',            # Synonym for decimal
        'money': 'DECIMAL(19,4)',        # Currency with 4 decimal places
        'smallmoney': 'DECIMAL(10,4)',   # Smaller currency type
        
        # Approximate Numeric Types
        'float': 'DOUBLE',               # Double precision floating point
        'real': 'FLOAT',                 # Single precision floating point
        
        # Character String Types
        'char': 'STRING',                # Fixed-length non-Unicode
        'varchar': 'STRING',             # Variable-length non-Unicode
        'text': 'STRING',                # Deprecated large text storage
        
        # Unicode Character String Types
        'nchar': 'STRING',               # Fixed-length Unicode
        'nvarchar': 'STRING',            # Variable-length Unicode
        'ntext': 'STRING',               # Deprecated large Unicode text
        
        # Binary String Types
        'binary': 'BINARY',              # Fixed-length binary
        'varbinary': 'BINARY',           # Variable-length binary
        'image': 'BINARY',               # Deprecated large binary storage
        
        # Date and Time Types
        'date': 'DATE',                  # Date only (no time)
        'time': 'STRING',                # Time only (no direct Spark equivalent)
        'datetime': 'TIMESTAMP',         # Legacy datetime (3.33ms precision)
        'datetime2': 'TIMESTAMP',        # High precision datetime (100ns)
        'smalldatetime': 'TIMESTAMP',    # Lower precision datetime (1 minute)
        'datetimeoffset': 'STRING',      # Datetime with timezone (no Spark equivalent)
        
        # Other Data Types
        'uniqueidentifier': 'STRING',    # GUID/UUID storage
        'xml': 'STRING',                 # XML document storage
        'sql_variant': 'STRING',         # Multi-type storage (rare)
        
        # Spatial Types (SQL Server 2008+)
        'geometry': 'STRING',            # Planar spatial data
        'geography': 'STRING',           # Geographic spatial data
        
        # Hierarchical Type
        'hierarchyid': 'STRING',         # Tree structure storage
        
        # Timestamp/Rowversion
        'timestamp': 'BINARY',           # Row version indicator (not time-related)
        'rowversion': 'BINARY'           # Synonym for timestamp
    }

    column_definitions = []

    col_names = [column['COLUMN_NAME'] for column in table_ddl]
    
    col_name_mapping = dict(column_name_cleansing(
        columns=col_names,
        advanced_processing_config=advanced_processing_config
    ))

    # Process each column definition
    for row in table_ddl:
        col_name = col_name_mapping[row['COLUMN_NAME']]
        data_type = row['DATA_TYPE'].lower()
        
        # Map SQL Server type to Spark type
        spark_data_type = sql_server_to_spark_type_mapping.get(data_type, 'STRING')
        
        # Handle DECIMAL types with precision and scale
        # Cast to int to guard against JSON float deserialization (e.g., 9.0 → 9)
        if spark_data_type == 'DECIMAL':
            precision = _safe_int_or_none(row.get('DATA_PRECISION'))
            if precision is not None:
                scale = _safe_int_or_none(row.get('DATA_SCALE'), 0)
                spark_data_type = f"DECIMAL({precision},{scale})"
        
        # SQL Server sys.columns.is_nullable is a bit: 0 = NOT NULL, 1 = nullable.
        # The current PL_02 SQL Server query aliases that bit as NULLABLE.
        # If this metadata ever changes to string values, update this check.
        # Determine NULL constraint
        nullable_text = ""
        if enforce_not_null and not row.get('NULLABLE'):
            nullable_text = " NOT NULL"
        
        column_comments = _safe_get_comment(row, "COLUMN_COMMENTS")
        
        # Format column definition
        column_definitions.append([col_name, spark_data_type, f"COMMENT '{column_comments}' {nullable_text}"])

    return column_definitions

def create_postgre_sql_table_ddl(
    table_ddl: dict,
    enforce_not_null: bool,
    advanced_processing_config: dict
):
    """
    Generate Delta table DDL from PostgreSQL source table schema.
    
    This function transforms PostgreSQL column definitions into Spark SQL DDL, handling
    PostgreSQL's rich type system including arrays, JSON types, geometric types, and
    user-defined types. It ensures accurate type mapping while preserving data integrity.
    
    Args:
        table_ddl (list): Source DDL metadata as python list containing column definitions
        enforce_not_null (bool): Whether to enforce NOT NULL constraints
        advanced_processing_config (dict): Configuration dict containing column name cleansing options
    
    Returns:
        list: column definitions
    """
    # Comprehensive PostgreSQL to Spark type mapping
    postgre_sql_to_spark_type_mapping = {
        # Numeric Types
        'smallint': 'SMALLINT',                    # 2 bytes, -32768 to +32767
        'integer': 'INT',                          # 4 bytes, typical choice
        'int': 'INT',                              # Alias for integer
        'int2': 'SMALLINT',                        # Alias for smallint
        'int4': 'INT',                             # Alias for integer
        'int8': 'BIGINT',                          # Alias for bigint
        'bigint': 'BIGINT',                        # 8 bytes, large range
        'decimal': 'DECIMAL',                      # User-specified precision
        'numeric': 'DECIMAL',                      # Alias for decimal
        'real': 'FLOAT',                           # 4 bytes, 6 decimal digits precision
        'double precision': 'DOUBLE',              # 8 bytes, 15 decimal digits precision
        'float4': 'FLOAT',                         # Alias for real
        'float8': 'DOUBLE',                        # Alias for double precision
        'money': 'DECIMAL(19,4)',                  # Currency type
        
        # Serial Types (auto-incrementing)
        'serial': 'INT',                           # Auto-incrementing integer
        'smallserial': 'SMALLINT',                 # Auto-incrementing smallint
        'bigserial': 'BIGINT',                     # Auto-incrementing bigint
        'serial2': 'SMALLINT',                     # Alias for smallserial
        'serial4': 'INT',                          # Alias for serial
        'serial8': 'BIGINT',                       # Alias for bigserial
        
        # Character Types
        'character varying': 'STRING',             # Variable-length with limit
        'varchar': 'STRING',                       # Alias for character varying
        'character': 'STRING',                     # Fixed-length, blank padded
        'char': 'STRING',                          # Alias for character
        'text': 'STRING',                          # Variable unlimited length
        'bpchar': 'STRING',                        # Internal name for char
        
        # Binary Data Types
        'bytea': 'BINARY',                         # Binary data ("byte array")
        
        # Date/Time Types
        'timestamp': 'TIMESTAMP',                  # Without time zone
        'timestamp without time zone': 'TIMESTAMP', # Explicit form
        'timestamp with time zone': 'TIMESTAMP',   # With time zone
        'timestamptz': 'TIMESTAMP',                # Alias for timestamp with time zone
        'date': 'DATE',                            # Calendar date
        'time': 'STRING',                          # Time without date
        'time without time zone': 'STRING',        # Explicit form
        'time with time zone': 'STRING',           # Time with time zone
        'timetz': 'STRING',                        # Alias for time with time zone
        'interval': 'STRING',                      # Time interval
        
        # Boolean Type
        'boolean': 'BOOLEAN',                      # true/false
        'bool': 'BOOLEAN',                         # Alias for boolean
        
        # Geometric Types
        'point': 'STRING',                         # Geometric point (x,y)
        'line': 'STRING',                          # Infinite line
        'lseg': 'STRING',                          # Line segment
        'box': 'STRING',                           # Rectangular box
        'path': 'STRING',                          # Geometric path
        'polygon': 'STRING',                       # Closed geometric path
        'circle': 'STRING',                        # Circle
        
        # Network Address Types
        'cidr': 'STRING',                          # IPv4/IPv6 network
        'inet': 'STRING',                          # IPv4/IPv6 host and network
        'macaddr': 'STRING',                       # MAC address
        'macaddr8': 'STRING',                      # MAC address (EUI-64 format)
        
        # Text Search Types
        'tsvector': 'STRING',                      # Text search document
        'tsquery': 'STRING',                       # Text search query
        
        # UUID Type
        'uuid': 'STRING',                          # Universally unique identifier
        
        # XML Type
        'xml': 'STRING',                           # XML data
        
        # JSON Types
        'json': 'STRING',                          # JSON text
        'jsonb': 'STRING',                         # JSON binary (more efficient)
        
        # Range Types
        'int4range': 'STRING',                     # Range of integer
        'int8range': 'STRING',                     # Range of bigint
        'numrange': 'STRING',                      # Range of numeric
        'tsrange': 'STRING',                       # Range of timestamp
        'tstzrange': 'STRING',                     # Range of timestamptz
        'daterange': 'STRING',                     # Range of date
        
        # Bit String Types
        'bit': 'STRING',                           # Fixed-length bit string
        'bit varying': 'STRING',                   # Variable-length bit string
        'varbit': 'STRING',                        # Alias for bit varying
        
        # Other Types
        'pg_lsn': 'STRING',                        # PostgreSQL Log Sequence Number
        'pg_snapshot': 'STRING',                   # Transaction snapshot
        'regclass': 'STRING',                      # OID alias types
        'regproc': 'STRING',
        'regtype': 'STRING',
        'void': 'STRING'                           # Function returns nothing
    }

    column_definitions = []

    col_names = [column['column_name'] for column in table_ddl]
    
    col_name_mapping = dict(column_name_cleansing(
        columns=col_names,
        advanced_processing_config=advanced_processing_config
    ))

    # Process each column definition
    for row in table_ddl:
        col_name = col_name_mapping[row['column_name']]
        data_type = row['data_type'].lower()

        # Handle array types (PostgreSQL specific)
        if data_type.endswith('[]') or data_type.startswith('array'):
            spark_data_type = 'STRING'  # Arrays mapped to STRING for parsing
        else:
            spark_data_type = postgre_sql_to_spark_type_mapping.get(data_type, 'STRING')
        
        # Handle DECIMAL/NUMERIC types with precision and scale
        # Cast to int to guard against JSON float deserialization (e.g., 9.0 → 9)
        if spark_data_type == 'DECIMAL':
            precision = _safe_int_or_none(row.get('data_precision'))
            scale = _safe_int_or_none(row.get('data_scale'), 0)
            # PostgreSQL supports up to 1000 digits, Spark supports up to 38
            if precision is None:
                pass
            elif precision > 38:
                spark_data_type = 'STRING'  # Fallback for very large decimals
            else:
                spark_data_type = f"DECIMAL({precision},{scale})"
        
        # Determine NULL constraint
        nullable_text = ""
        if enforce_not_null and row['nullable'] == 'NO':
            nullable_text = " NOT NULL"
        
        column_comments = _safe_get_comment(row, "COLUMN_COMMENTS")
        
        # Format column definition
        column_definitions.append([col_name, spark_data_type, f"COMMENT '{column_comments}' {nullable_text}"])

    return column_definitions

def create_my_sql_table_ddl(
    table_ddl: dict,
    enforce_not_null: bool,
    advanced_processing_config: dict
):
    """
    Generate Delta table DDL from MySQL source table schema.
    
    This function transforms MySQL column definitions into Spark SQL DDL, handling MySQL's
    data types including integer display widths, unsigned types, and various string types
    with different character sets and collations. It ensures proper type mapping while
    maintaining data integrity.
    
    Args:
        table_ddl (list): Source DDL metadata as python list containing column definitions
        enforce_not_null (bool): Whether to enforce NOT NULL constraints
        advanced_processing_config (dict): Configuration dict containing column name cleansing options
    
    MySQL Type Mappings:
        - TINYINT → TINYINT; TINYINT UNSIGNED → SMALLINT
        - SMALLINT → SMALLINT; SMALLINT UNSIGNED → INT
        - MEDIUMINT → INT
        - INT/INTEGER → INT; INT/INTEGER UNSIGNED → BIGINT
        - BIGINT → BIGINT; BIGINT UNSIGNED → DECIMAL(20,0)
        - FLOAT → FLOAT
        - DOUBLE/REAL → DOUBLE
        - DECIMAL/NUMERIC → DECIMAL(p,s)
        - CHAR/VARCHAR/TEXT variants → STRING
        - DATE → DATE
        - DATETIME/TIMESTAMP → TIMESTAMP
        - TIME → STRING (no direct Spark TIME type)
        - YEAR → INT
        - JSON → STRING
        - ENUM/SET → STRING
        - BINARY/VARBINARY/BLOB variants → BINARY
        - BIT → BOOLEAN
    """
    # Comprehensive MySQL to Spark type mapping dictionary
    my_sql_to_spark_type_mapping = {
        # Integer Types
        'TINYINT': 'TINYINT',            # 8-bit signed integer
        'SMALLINT': 'SMALLINT',          # 16-bit integer (-32768 to 32767)
        'MEDIUMINT': 'INT',              # 24-bit integer (no Spark equivalent, upcast to INT)
        'INT': 'INT',                    # 32-bit integer
        'INTEGER': 'INT',               # Alias for INT
        'BIGINT': 'BIGINT',              # 64-bit integer (-2^63 to 2^63-1)

        # Floating-Point Types
        'FLOAT': 'FLOAT',                # Single precision floating point
        'DOUBLE': 'DOUBLE',              # Double precision floating point
        'REAL': 'DOUBLE',                # Alias for DOUBLE

        # Fixed-Point Types
        'DECIMAL': 'DECIMAL',            # Fixed-point numeric (requires precision handling)
        'NUMERIC': 'DECIMAL',            # Alias for DECIMAL
        'MONEY': 'DECIMAL',              # Currency type

        # Character String Types
        'CHAR': 'STRING',                # Fixed-length character string
        'VARCHAR': 'STRING',             # Variable-length character string
        'TINYTEXT': 'STRING',            # Up to 255 bytes
        'TEXT': 'STRING',                # Up to 65,535 bytes
        'MEDIUMTEXT': 'STRING',          # Up to 16,777,215 bytes
        'LONGTEXT': 'STRING',            # Up to 4,294,967,295 bytes

        # Binary Types
        'BINARY': 'BINARY',              # Fixed-length binary string
        'VARBINARY': 'BINARY',           # Variable-length binary string
        'TINYBLOB': 'BINARY',            # Up to 255 bytes
        'BLOB': 'BINARY',                # Up to 65,535 bytes
        'MEDIUMBLOB': 'BINARY',          # Up to 16,777,215 bytes
        'LONGBLOB': 'BINARY',            # Up to 4,294,967,295 bytes

        # Date and Time Types
        'DATE': 'DATE',                  # Calendar date
        'DATETIME': 'TIMESTAMP',         # Date and time
        'TIMESTAMP': 'TIMESTAMP',        # Date and time with auto-update
        'TIME': 'STRING',                # Time without date (no Spark equivalent)
        'YEAR': 'INT',                   # Year value (1901–2155)

        # Other Types
        'BIT': 'BOOLEAN',                # Bit-field, commonly used as boolean flag
        'ENUM': 'STRING',                # Enumeration of string values
        'SET': 'STRING',                 # Set of string values
        'JSON': 'STRING',                # JSON document
        'UUID': 'STRING',                # UUID value
        'XML': 'STRING',                 # XML document
        'GEOMETRY': 'STRING',            # Spatial geometry type
        'POINT': 'STRING',               # Spatial point type
        'LINESTRING': 'STRING',          # Spatial linestring type
        'POLYGON': 'STRING',             # Spatial polygon type
    }

    my_sql_unsigned_integer_type_mapping = {
        'TINYINT': 'SMALLINT',
        'SMALLINT': 'INT',
        'MEDIUMINT': 'INT',
        'INT': 'BIGINT',
        'INTEGER': 'BIGINT',
        'BIGINT': 'DECIMAL(20,0)',
    }

    column_definitions = []

    col_names = [column['Field'] for column in table_ddl]
    
    col_name_mapping = dict(column_name_cleansing(
        columns=col_names,
        advanced_processing_config=advanced_processing_config
    ))

    # Process each column definition
    for column in table_ddl:
        primary_key = column.get("Key")
        col_name = col_name_mapping[column.get("Field")]
        
        # Normalize Type by stripping modifiers (UNSIGNED, ZEROFILL) BEFORE splitting
        # on '(' so that precision_scale stays clean (e.g., "decimal(10,0) unsigned"
        # becomes "decimal(10,0)" before extraction, avoiding float("0 unsigned") errors)
        raw_type = column.get("Type").upper().strip()
        is_unsigned = ' UNSIGNED' in raw_type
        Type = raw_type.replace(' UNSIGNED', '').replace(' ZEROFILL', '').strip()
        if '(' in Type:
            data_type = Type.replace(')', '').split('(')[0].strip()
            precision_scale = Type.replace(')', '').split('(')[1].strip()
        else:
            data_type = Type
            precision_scale = ""

        nullable = column["Null"]
        nullable_text = " NOT NULL" if nullable == "NO" and enforce_not_null else ""

        # MySQL DESCRIBE returns Field/Type/Null/Key/Default/Extra; it does not include comments.
        # If the metadata query changes to SHOW FULL COLUMNS, normalize Comment to COLUMN_COMMENTS.
        column_comments = _safe_get_comment(column, "COLUMN_COMMENTS")

        # Add primary key indicator to comment
        if column_comments and primary_key:
            column_comments = f"PRIMARY KEY; {column_comments}"
        elif primary_key:
            column_comments = "PRIMARY KEY"

        # Look up Spark type from dictionary
        spark_data_type = my_sql_to_spark_type_mapping.get(data_type)
        if not spark_data_type:
            raise Exception(f"Please add My SQL to Spark translation logic for {data_type} in the create_my_sql_table_ddl function")

        if is_unsigned:
            spark_data_type = my_sql_unsigned_integer_type_mapping.get(data_type, spark_data_type)

        # Handle DECIMAL types with precision and scale
        # Cast to int to guard against JSON float deserialization (e.g., 9.0 → 9)
        if spark_data_type == 'DECIMAL':
            if not precision_scale:
                # MySQL defaults DECIMAL without precision to DECIMAL(10,0)
                spark_data_type = 'DECIMAL(10,0)'
            else:
                precision_scale = ','.join(str(int(float(p.strip()))) for p in precision_scale.split(','))
                spark_data_type = f'DECIMAL({precision_scale})'
        
        # Format column definition
        column_definitions.append([col_name, spark_data_type, f"COMMENT '{column_comments}' {nullable_text}"])

    return column_definitions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Bronze Files Ingestion Framework
# 
# The following section contains a comprehensive suite of functions for ingesting data from the Bronze Files layer. This framework provides:
# 
# ### Key Capabilities
# - **Flexible Path Resolution**: Support for base paths, wildcard patterns, and trigger-based paths
# - **Incremental Processing**: Watermark-based change detection for efficient data loading
# - **Schema Evolution**: Configurable schema enforcement with drift detection
# - **Format Agnostic**: Native support for CSV, JSON, Parquet, Excel, and custom formats
# - **Error Handling**: Sophisticated error quarantine and recovery mechanisms
# 
# ### Architecture
# 1. **Path Discovery**: Intelligent file discovery using Hadoop FileSystem API
# 2. **Watermark Management**: Timestamp-based incremental processing
# 3. **Schema Validation**: Optional schema enforcement with configurable failure modes
# 4. **Custom Ingestion**: Extensible framework for proprietary file formats
# 
# ### Integration Points
# - Seamlessly integrates with Bronze layer storage patterns
# - Supports both ADLS Gen2 shortcuts and native Fabric storage
# - Maintains lineage through `delta__raw_folderpath` tracking


# CELL ********************

def build_exit_context(
    first_run: bool,
    lineage_info: dict,
    watermark_value: str,
    source_details: str
) -> dict:
    """
    Build a standardized exit context dictionary for file ingestion functions.
    
    This helper ensures consistent exit context structure across all file ingestion
    call sites, reducing code duplication and maintenance burden.
    
    Args:
        first_run (bool): Whether this is the first run of the pipeline.
        lineage_info (dict): Dictionary containing lineage tracking information.
        watermark_value (str): Current watermark value for incremental processing.
        source_details (str): Source path or identifier for error messaging.
    
    Returns:
        dict: Standardized exit context dictionary with keys:
              - first_run: Pipeline run indicator
              - lineage_info: Lineage tracking data
              - watermark_value: Incremental processing marker
              - source_details: Source identifier for diagnostics
    """
    return {
        'first_run': first_run,
        'lineage_info': lineage_info,
        'watermark_value': watermark_value,
        'source_details': source_details
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_files_with_modified_date(wildcard_folder_path: str) -> DataFrame:
    """
    Retrieve file paths and modification timestamps using Spark API.
    
    This function leverages the underlying Hadoop FileSystem to efficiently discover files
    matching wildcard patterns, returning both the file path and its last modification
    timestamp. It's designed to work with large-scale distributed file systems.
    
    Args:
        wildcard_folder_path (str): Path pattern supporting Hadoop wildcards:
                                 - * matches any sequence of characters
                                 - ? matches single character
                                 - [abc] matches any character in set
                                 - {a,b} matches any of the strings
    
    Returns:
       Spark Dataframe: 
                    - file_path (str): Absolute path to the file
                    - datetime: Python datetime of last modification
    """

    # Read file metadata (not the file payload) from all files matching wildcard_folder_path.
    # The "binaryFile" data source exposes columns: path, modificationTime, length, content.
    # Because we only select path and modificationTime (renamed mod_time_utc) and DO NOT
    # reference the 'content' column, Spark can prune it and skip actually loading
    # the binary bytes of each file. Thus this gathers metadata, not file contents.
    try:
        files_with_date = (spark.read.format("binaryFile")
            # optional: search nested folders that don't follow partition naming
            .option("recursiveFileLookup", "true")
            .load(wildcard_folder_path)  # glob is applied here
            .select("path", f.col("modificationTime").alias("mod_time_utc")))
    except Exception as e:
        if 'PATH_NOT_FOUND' in str(e):
            no_files_error_log_info = f"No files exists in path, {wildcard_folder_path}. Please update configuration to point to a file path with data."
            log_and_print(no_files_error_log_info, "error")
        raise
            
    return files_with_date

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def ingest_raw_files(
    source_config: dict,
    watermark_config: dict,
    file_config: dict,
    custom_paths: dict,
    all_metadata: dict,
    folder_path_from_trigger: str,
    first_run: bool,
    lineage_info: dict
) -> tuple:
    """
    Primary orchestrator for Bronze Files layer data ingestion.
    
    This function serves as the main entry point for ingesting raw files from the Bronze
    layer, supporting multiple ingestion patterns and providing a unified interface for
    various data loading scenarios. It intelligently routes to the appropriate ingestion
    strategy based on the provided parameters.
    
    Args:

    Returns:
        tuple[DataFrame, list]: Tuple containing:
                              - new_data: Spark DataFrame with ingested data
                              - folder_time_stamps: List of processed folder/file timestamps
    
    Raises:
        Exception: If no valid ingestion path is provided or if ingestion fails
    
    Processing Flow:
        1. Validates input parameters and determines ingestion strategy
        2. Routes to appropriate handler based on path type
        3. Discovers files based on watermark and patterns
        4. Reads and validates data according to configuration
        5. Returns unified DataFrame with processing metadata
    
    Performance Notes:
        - Watermark-based processing minimizes data scanning
        - Schema caching reduces metadata operations
        - Parallel file reading for improved throughput
        - Memory-efficient processing for large file sets
    """
    folder_time_stamps = []
    watermark_value = watermark_config['watermark_value']

    if source_config['staging_folder_path']:
        # Time-partitioned folder structure ingestion
        new_data, folder_time_stamps = ingest_raw_files_with_base_path(
            source_config = source_config,
            watermark_config = watermark_config,
            custom_paths = custom_paths,
            file_config = file_config,
            all_metadata = all_metadata,
            first_run = first_run,
            lineage_info = lineage_info
        )
    elif source_config['wildcard_folder_path']:
        # Pattern-based file discovery ingestion
        new_data, folder_time_stamps = ingest_raw_files_with_wildcard_path(
            source_config = source_config,
            watermark_config = watermark_config,
            custom_paths = custom_paths,
            file_config = file_config,
            all_metadata = all_metadata,
            first_run = first_run,
            lineage_info = lineage_info
        )
    elif folder_path_from_trigger:
        # Event-driven targeted ingestion
        lakehouse_folder_path = f"{custom_paths['source_files_lakehouse_path']}/{folder_path_from_trigger}"
        new_data = read_all_files_in_paths(
            new_paths = [lakehouse_folder_path],
            file_extension = source_config['file_extension'],
            all_metadata = all_metadata,
            custom_file_ingestion_function_notebook = source_config['custom_file_ingestion_function_notebook'],
            custom_file_ingestion_function = source_config['custom_file_ingestion_function'],
            file_config = file_config,
            file_staging_path = custom_paths['file_staging_path'],
            exit_context = build_exit_context(
                first_run = first_run,
                lineage_info = lineage_info,
                watermark_value = watermark_value,
                source_details = lakehouse_folder_path
            )
        )
    else:
        raise Exception(f"All folderpaths are empty. Please recheck logic that caused ingestion to be routed to the function: `ingest_raw_files`.")

    if folder_time_stamps:
        new_watermark_value = max(folder_time_stamps)
    else:
        new_watermark_value = ""
        
    # Convert datetime objects to string format for logging
    new_watermark_value = convert_datetime_to_string(value = new_watermark_value)

    return new_data, new_watermark_value

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _extract_schema_and_options(kwargs_dict: Dict[str, Any]) -> tuple:
    """Extract schema from kwargs dict, returning (schema, remaining_options).
    
    Note: This function is used by CSV, JSON, Excel, and XML readers.
    Parquet files use post-read validation via validate_table_schema_contract instead.
    """
    kwargs_copy = kwargs_dict.copy()
    schema = kwargs_copy.pop('schema', None)
    
    if schema is not None:
        log_and_print(
            "⚠️ WARNING: Schema enforcement during file ingestion can cause SILENT DATA LOSS. "
            "When Spark cannot cast a value to the expected type (e.g., 'ABC' to INT), "
            "the value becomes NULL without error. This behavior differs from Delta table "
            "schema validation which fails explicitly on mismatches. "
            "Consider: (1) Ingesting to Bronze WITHOUT schema (CSV defaults to strings, "
            "JSON infers types natively), then (2) Using data_quality checks with "
            "validate_condition in Bronze→Silver to explicitly validate and cast data types "
            "with proper error handling.",
            "warn"
        )
    
    return schema, kwargs_copy


def _handle_custom_file_ingestion(
    custom_file_ingestion_function_notebook: str,
    custom_file_ingestion_function: str,
    new_paths: List[str],
    all_metadata: Dict
) -> DataFrame:
    """Handle custom file format ingestion."""
    if custom_file_ingestion_function_notebook:
        instantiate_notebook(
            notebook_name=custom_file_ingestion_function_notebook,
            function_scope=(all_metadata.get('primary_config') or {}).get(
                'source_details_custom_file_ingestion_function_scope',
                'local',
            ),
        )

    if custom_file_ingestion_function:
        function_name = custom_file_ingestion_function
        executing_custom_file_ingestion_function_log_info = f"Executing custom file ingestion function: {function_name}"
        log_and_print(executing_custom_file_ingestion_function_log_info)

        function_kwargs = {"file_paths": new_paths, "all_metadata": all_metadata, "spark": spark}
        new_data = globals()[function_name](**function_kwargs)
        return new_data
    
    return None

def _read_csv_batch(new_paths: List[str], file_config: Dict[str, Any]) -> DataFrame:
    """Read multiple CSV/TSV/TXT files in a single Spark action."""
    schema, extra_options = _extract_schema_and_options(file_config['schema_kwargs'])
    options = {
        'sep': file_config['delimiter'],
        'header': file_config['file_has_header_row'],
        'encoding': file_config['encoding'],
        'multiLine': file_config['multiline']
    } | extra_options
    reader = spark.read
    if schema:
        reader = reader.schema(schema)
    return reader.options(**options).csv(new_paths)


def _read_json_batch(new_paths: List[str], file_config: Dict[str, Any]) -> DataFrame:
    """Read multiple JSON files in a single Spark action."""
    schema, extra_options = _extract_schema_and_options(file_config['schema_kwargs'])
    options = {
        'multiLine': file_config['multiline'],
        'encoding': file_config['encoding']
    } | extra_options
    reader = spark.read
    if schema:
        reader = reader.schema(schema)
    return reader.options(**options).json(new_paths)


def _xml_row_tag_from_xpath(xml_xpath: str) -> str:
    """Translate a simple XML row selector into Spark's required rowTag."""
    normalized_xpath = str(xml_xpath or "").strip()
    if not normalized_xpath:
        raise ValueError(
            "Native Spark XML ingestion requires source_details.xml_xpath with a row element, "
            "for example './/row' or '/orders/order'."
        )

    unsupported_tokens = ("[", "]", "@", "*", "(", ")", "|")
    if any(token in normalized_xpath for token in unsupported_tokens):
        raise ValueError(
            f"XML XPath '{normalized_xpath}' cannot be translated to Spark rowTag. "
            "Use a simple element path without predicates, attributes, wildcards, or functions."
        )

    path_segments = [
        segment.strip()
        for segment in normalized_xpath.split("/")
        if segment.strip() not in {"", "."}
    ]
    if not path_segments or path_segments[-1] == "..":
        raise ValueError(
            f"XML XPath '{normalized_xpath}' does not identify a row element for Spark rowTag."
        )

    # Spark's XML reader matches a namespace-ignored row by its local element name.
    return path_segments[-1].split(":", 1)[-1]


def _xml_schema_with_string_leaves(data_type: Any) -> Any:
    """Preserve inferred XML structure while representing every leaf as a string."""
    from pyspark.sql.types import ArrayType, MapType, StringType, StructField, StructType

    if isinstance(data_type, StructType):
        return StructType([
            StructField(
                field.name,
                _xml_schema_with_string_leaves(field.dataType),
                field.nullable,
                field.metadata
            )
            for field in data_type.fields
        ])
    if isinstance(data_type, ArrayType):
        return ArrayType(
            _xml_schema_with_string_leaves(data_type.elementType),
            data_type.containsNull
        )
    if isinstance(data_type, MapType):
        return MapType(
            StringType(),
            _xml_schema_with_string_leaves(data_type.valueType),
            data_type.valueContainsNull
        )
    return StringType()


def _read_xml_paths(new_paths: str | List[str], file_config: Dict[str, Any]) -> DataFrame:
    """Read one or more XML paths with Spark's native XML data source."""
    schema, extra_options = _extract_schema_and_options(file_config['schema_kwargs'])
    options = {
        'rowTag': _xml_row_tag_from_xpath(file_config.get('xml_xpath')),
        'encoding': file_config['encoding'],
        'attributePrefix': '',
        'ignoreNamespace': str(bool(file_config.get('xml_namespaces'))).lower()
    } | extra_options

    if not schema:
        inferred_schema = spark.read.options(
            **(options | {'inferSchema': 'true'})
        ).xml(new_paths).schema
        schema = _xml_schema_with_string_leaves(inferred_schema)

    return spark.read.schema(schema).options(**options).xml(new_paths)


def _read_xml_batch(new_paths: List[str], file_config: Dict[str, Any]) -> DataFrame:
    """Read multiple XML files in one native Spark scan."""
    return _read_xml_paths(new_paths, file_config)


def _read_parquet_batch(new_paths: List[str], file_config: Dict[str, Any]) -> DataFrame:
    """Read multiple Parquet files in a single Spark action.
    
    Note: Parquet files are self-describing with embedded schema metadata.
    Schema enforcement is intentionally NOT applied during read because:
    1. Parquet already contains its own schema
    2. Applying a different schema would cause silent type casting (data loss risk)
    3. Use mergeSchema for schema evolution across files instead
    
    Instead, we validate schema AFTER reading using validate_table_schema_contract,
    which explicitly fails on mismatches rather than silently converting data.
    """
    options = {
        'mergeSchema': str(file_config['allow_missing_columns']).lower()
    }
    df = spark.read.options(**options).parquet(*new_paths)
    
    # Validate schema contract after reading (explicit failure vs silent data loss)
    expected_schema = file_config.get('expected_schema_string')
    if expected_schema:
        validate_table_schema_contract(
            df=df,
            expected_schema=expected_schema,
            source_description=f"Parquet files: {new_paths[0]}{'...' if len(new_paths) > 1 else ''}"
        )
    
    return df


# Registry mapping file extensions to batch reader functions
BATCH_READERS = {
    'csv': _read_csv_batch,
    'tsv': _read_csv_batch,
    'txt': _read_csv_batch,
    'json': _read_json_batch,
    'xml': _read_xml_batch,
    'parquet': _read_parquet_batch
}


def _read_files_in_batch(
    new_paths: List[str],
    file_extension: str,
    file_config: Dict[str, Any]
) -> DataFrame:
    """Read multiple files in a single Spark action while preserving lineage metadata."""    
    # Log batch processing details
    batch_processing_log_info = f"Processing {len(new_paths)} files in batch (Format: {file_extension})"
    log_and_print(batch_processing_log_info)
    
    for new_path in new_paths:
        file_log_info = f"Processing file: {new_path} (Format: {file_extension})"
        log_and_print(file_log_info)

    # Get the appropriate batch reader function
    batch_reader = BATCH_READERS.get(file_extension)
    if not batch_reader:
        raise Exception(f"Batch processing not supported for file extension '{file_extension}'.")

    df = batch_reader(new_paths, file_config)
    return df.withColumn('delta__raw_folderpath', f.input_file_name())

def _handle_csv_tsv_txt(
    new_path: str,
    file_config: Dict[str, Any],
    i: int,
    new_data: DataFrame
) -> DataFrame:
    """Handle CSV/TSV/TXT file reading."""
    schema, extra_options = _extract_schema_and_options(file_config['schema_kwargs'])
    options = {
        'sep': file_config['delimiter'], 
        'header': file_config['file_has_header_row'], 
        'encoding': file_config['encoding'],
        'multiLine': file_config['multiline']
    } | extra_options
    reader = spark.read
    if schema:
        reader = reader.schema(schema)
    if i == 0:
        new_data = reader.options(**options).csv(new_path) 
        new_data = new_data.withColumn('delta__raw_folderpath', f.lit(new_path))
    else: 
        df = reader.options(**options).csv(new_path) 
        df = df.withColumn('delta__raw_folderpath', f.lit(new_path))
        new_data = new_data.unionByName(df, allowMissingColumns = file_config['allow_missing_columns'])
    
    return new_data

def _handle_parquet(
    new_path: str,
    file_config: Dict[str, Any],
    i: int,
    new_data: DataFrame
) -> DataFrame:
    """Handle Parquet file reading.
    
    Note: Parquet files are self-describing with embedded schema metadata.
    Schema enforcement is intentionally NOT applied during read because:
    1. Parquet already contains its own schema
    2. Applying a different schema would cause silent type casting (data loss risk)
    3. Use mergeSchema for schema evolution across files instead
    
    Instead, we validate schema AFTER reading using validate_table_schema_contract,
    which explicitly fails on mismatches rather than silently converting data.
    """
    options = {
        'mergeSchema': str(file_config['allow_missing_columns']).lower()
    }
    
    # Read the parquet file
    df = spark.read.options(**options).parquet(new_path)
    
    # Validate schema contract after reading (explicit failure vs silent data loss)
    expected_schema = file_config.get('expected_schema_string')
    if expected_schema:
        validate_table_schema_contract(
            df=df,
            expected_schema=expected_schema,
            source_description=f"Parquet file: {new_path}"
        )
    
    # Add lineage column
    df = df.withColumn('delta__raw_folderpath', f.lit(new_path))
    
    # Merge with existing data or return as first result
    if i == 0:
        new_data = df
    else:
        new_data = new_data.unionByName(df, allowMissingColumns = file_config['allow_missing_columns'])
    
    return new_data

def _handle_json(
    new_path: str,
    file_config: Dict[str, Any],
    i: int,
    new_data: DataFrame
) -> DataFrame:
    """Handle JSON file reading."""
    schema, extra_options = _extract_schema_and_options(file_config['schema_kwargs'])
    options = {
        'multiLine': file_config['multiline'],
        'encoding': file_config['encoding']
    } | extra_options
    reader = spark.read
    if schema:
        reader = reader.schema(schema)
    if i == 0:
        new_data = reader.options(**options).json(new_path)
        new_data = new_data.withColumn('delta__raw_folderpath', f.lit(new_path))
    else: 
        df = reader.options(**options).json(new_path)
        df = df.withColumn('delta__raw_folderpath', f.lit(new_path))
        new_data = new_data.unionByName(df, allowMissingColumns = file_config['allow_missing_columns'])
    
    return new_data


def _validate_excel_sheet_config(file_config: Dict[str, Any]) -> str:
    """Ensure sheet configuration is provided and normalized."""
    sheet_config = file_config.get('sheet_name')
    if sheet_config is None:
        raise Exception(
            "Excel ingestion requires `sheet_name`. Provide a sheet name, comma-separated list, or '*' for all sheets."
        )

    normalized_value = str(sheet_config).strip()
    if not normalized_value:
        raise Exception("`sheet_name` must contain at least one sheet name or the '*' wildcard.")

    return normalized_value


def _build_excel_pandas_kwargs(file_config: Dict[str, Any]) -> Dict[str, Any]:
    """Construct keyword arguments for pandas Excel reader."""
    return {
        'dtype': str,
        'header': file_config['pandas_header_config']
    }


def _parse_requested_sheets(sheet_value: str) -> List[str]:
    """Turn comma-separated sheet names into a cleaned list."""
    requested_sheets = [sheet.strip() for sheet in sheet_value.split(',') if sheet.strip()]
    if not requested_sheets:
        raise Exception("`sheet_name` must contain at least one sheet name or the '*' wildcard.")
    return requested_sheets


def _resolve_requested_excel_sheet_names(excel_file: pd.ExcelFile, normalized_value: str, new_path: str) -> List[str]:
    """Resolve which sheets should be read from the workbook."""
    if normalized_value == '*':
        requested_sheets = list(excel_file.sheet_names)
        log_and_print(f"Reading all sheets from Excel file `{new_path}`.")
        return requested_sheets

    requested_sheets = _parse_requested_sheets(normalized_value)
    log_and_print(f"Reading sheets {requested_sheets} from Excel file `{new_path}`.")
    return requested_sheets


def _prepare_excel_sheet_frame(sheet_frame: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
    """Normalize a single Excel sheet frame before converting it to Spark."""
    prepared_frame = sheet_frame.copy()
    if 'sheet_name' in prepared_frame.columns:
        log_and_print(
            f"Column `sheet_name` already exists in `{sheet_name}`. Overwriting to store source sheet metadata.",
            "warn"
        )

    prepared_frame['sheet_name'] = str(sheet_name)

    # Replace pandas NaN/NaT markers with Python None values so Spark does not
    # upcast string/object columns unexpectedly during conversion.
    return prepared_frame.astype(object).where(pd.notna(prepared_frame), None)


def _create_empty_excel_spark_dataframe(column_names: List[str]) -> DataFrame:
    """Create an empty Spark DataFrame for an empty Excel sheet while preserving columns."""
    if not column_names:
        raise Exception(
            "Excel sheet is empty and produced no columns. "
            "Provide a header row or configure `sheet_name` to point to a populated sheet."
        )

    schema_string = ', '.join(f"`{column_name}` STRING" for column_name in column_names)
    return spark.createDataFrame([], schema_string)


def _excel_sheet_has_no_user_columns(sheet_frame: pd.DataFrame) -> bool:
    """Detect blank Excel sheets that pandas represents with placeholder-only columns."""
    column_names = list(sheet_frame.columns)
    if not column_names:
        return True

    return all(
        isinstance(column_name, int)
        or (isinstance(column_name, str) and column_name.startswith('Unnamed:'))
        for column_name in column_names
    )


def _apply_excel_schema_if_configured(df: DataFrame, schema: Any) -> DataFrame:
    """Apply configured schema casts to Excel data while preserving helper columns."""
    if schema is None:
        return df

    schema_field_names = [field.name for field in schema.fields]
    projected_columns = []

    for field in schema.fields:
        if field.name in df.columns:
            projected_columns.append(
                f.expr(f"try_cast(`{field.name}` as {field.dataType.simpleString()})").alias(field.name)
            )
        else:
            projected_columns.append(f.lit(None).cast(field.dataType).alias(field.name))

    projected_columns.extend(
        f.col(column_name)
        for column_name in df.columns
        if column_name not in schema_field_names
    )

    return df.select(*projected_columns)


def _union_ingested_frames(existing_df: DataFrame, next_df: DataFrame, allow_missing_columns: bool) -> DataFrame:
    """Union ingested DataFrames while handling the initial empty state."""
    if existing_df is None:
        return next_df

    return existing_df.unionByName(next_df, allowMissingColumns = allow_missing_columns)


def _handle_excel(
    new_path: str,
    file_config: Dict[str, Any],
    file_staging_path: str,
    i: int,
    new_data: DataFrame
) -> DataFrame:
    """Handle Excel file reading sheet-by-sheet to avoid CSV staging and large driver spikes."""
    del file_staging_path, i

    normalized_value = _validate_excel_sheet_config(file_config)
    pandas_kwargs = _build_excel_pandas_kwargs(file_config)
    schema, _ = _extract_schema_and_options(file_config['schema_kwargs'])

    excel_data = None

    with pd.ExcelFile(new_path) as excel_file:
        requested_sheets = _resolve_requested_excel_sheet_names(
            excel_file = excel_file,
            normalized_value = normalized_value,
            new_path = new_path
        )

        for sheet_name in requested_sheets:
            pandas_sheet = excel_file.parse(sheet_name = sheet_name, **pandas_kwargs)
            prepared_sheet = _prepare_excel_sheet_frame(pandas_sheet, sheet_name)

            if prepared_sheet.empty:
                if _excel_sheet_has_no_user_columns(pandas_sheet):
                    raise Exception(
                        "Excel sheet is empty and produced no columns. "
                        "Provide a header row or configure `sheet_name` to point to a populated sheet."
                    )
                sheet_df = _create_empty_excel_spark_dataframe(prepared_sheet.columns.tolist())
            else:
                sheet_df = spark.createDataFrame(prepared_sheet)

            sheet_df = _apply_excel_schema_if_configured(sheet_df, schema)
            sheet_df = sheet_df.withColumn('delta__raw_folderpath', f.lit(new_path))
            excel_data = _union_ingested_frames(
                existing_df = excel_data,
                next_df = sheet_df,
                allow_missing_columns = file_config['allow_missing_columns']
            )

            del pandas_sheet
            del prepared_sheet

    if excel_data is None:
        raise Exception(f"No data found in the requested Excel sheets for file `{new_path}`.")

    new_data = _union_ingested_frames(
        existing_df = new_data,
        next_df = excel_data,
        allow_missing_columns = file_config['allow_missing_columns']
    )

    for column, datatype in new_data.dtypes:
        if datatype == "null":
            new_data = new_data.withColumn(column, f.col(column).cast("string"))

    return new_data


def _handle_xml(
    new_path: str,
    file_config: Dict[str, Any],
    file_staging_path: str,
    i: int,
    new_data: DataFrame
) -> DataFrame:
    """Read one XML file natively when sequential mode is explicitly requested."""
    del file_staging_path, i

    xml_data = _read_xml_paths(new_path, file_config)
    xml_data = xml_data.withColumn('delta__raw_folderpath', f.lit(new_path))
    new_data = _union_ingested_frames(
        existing_df=new_data,
        next_df=xml_data,
        allow_missing_columns=file_config['allow_missing_columns']
    )

    for column, datatype in new_data.dtypes:
        if datatype == "null":
            new_data = new_data.withColumn(column, f.col(column).cast("string"))

    return new_data

def _process_file_by_extension(
    new_path: str,
    file_extension: str,
    file_config: Dict[str, Any],
    file_staging_path: str,
    i: int,
    new_data: DataFrame
) -> DataFrame:
    """Route file processing based on extension."""
    if file_extension in ('csv', 'tsv', 'txt'):
        return _handle_csv_tsv_txt(
            new_path = new_path,
            file_config = file_config,
            i = i,
            new_data = new_data
        )
    elif file_extension == 'parquet':
        return _handle_parquet(
            new_path = new_path,
            file_config = file_config,
            i = i,
            new_data = new_data
        )
    elif file_extension == 'json':
        return _handle_json(
            new_path = new_path,
            file_config = file_config,
            i = i,
            new_data = new_data
        )
    elif file_extension == 'xml':
        return _handle_xml(
            new_path = new_path,
            file_config = file_config,
            file_staging_path = file_staging_path,
            i = i,
            new_data = new_data
        )
    elif file_extension in ('xls','xlsx'):
        return _handle_excel(
            new_path = new_path,
            file_config = file_config,
            file_staging_path = file_staging_path,
            i = i,
            new_data = new_data
        )
    else:
        raise Exception(f"Unsupported file extension '{file_extension}'. Please implement a handler or use a custom ingestion function.")

def _resolve_file_extension(file_extension: str, new_paths: List[str]) -> str:
    """Resolve file extension from parameter or first file path."""
    resolved = file_extension.lower() if file_extension else None
    if not resolved and new_paths:
        resolved = new_paths[0].split('.')[-1].lower()
    return resolved

def _should_use_batch_mode(file_extension: str, file_config: Dict[str, Any]) -> bool:
    """
    Determine if files should be read in batch mode.
    
    Batch mode allows Spark to read multiple files in a single operation, which is more
    efficient for formats that support native distributed reading. However, certain file
    types require sequential processing due to their complex structure and parsing requirements.
    
    Args:
        file_extension (str): The file extension to check (e.g., 'csv', 'xml', 'xlsx')
        file_config (Dict[str, Any]): Configuration dict containing process_one_file_at_a_time flag
    
    Returns:
        bool: True if files should be read in batch mode, False if sequential processing required
    
    Batch-Capable Formats:
        - CSV/TSV/TXT: Simple delimited text with native Spark readers
        - JSON: Native Spark reader with efficient schema inference
        - XML: Native Spark 4.1 XML reader with rowTag-based parsing
        - Parquet: Native columnar format with optimal parallel reading
    
    Sequential-Only Formats:
        - Excel (XLS/XLSX): Binary/compressed format requiring pandas (pd.read_excel) with
          sheet-specific reading. Each sheet is converted directly into Spark in sequence
          to control peak driver memory, and sheet configurations may vary per file.
    
    Note:
        Even batch-capable formats can be forced into sequential mode by setting
        file_config["process_one_file_at_a_time"] = True, which is useful for:
        - Files with inconsistent schemas
        - Memory-constrained environments
        - Detailed per-file error handling
    """
    batch_capable_extensions = {"csv", "tsv", "txt", "json", "xml", "parquet"}
    return (
        file_extension 
        and file_extension in batch_capable_extensions 
        and not file_config["process_one_file_at_a_time"]
    )

def _read_files_sequentially(
    new_paths: List[str],
    file_extension: str,
    file_config: Dict[str, Any],
    file_staging_path: str
) -> DataFrame:
    """Process files one at a time with schema merging."""
    new_data = None
    resolved_extension = file_extension
    
    for i, new_path in enumerate(new_paths):
        # Auto-detect extension if needed
        current_extension = resolved_extension
        if not current_extension:
            current_extension = new_path.split('.')[-1].lower()
            resolved_extension = current_extension

        processing_file_log_info = f"Processing file: {new_path} (Format: {current_extension})"
        log_and_print(processing_file_log_info)

        new_data = _process_file_by_extension(
            new_path = new_path,
            file_extension = current_extension,
            file_config = file_config,
            file_staging_path = file_staging_path,
            i = i,
            new_data = new_data
        )

    return new_data

def read_all_files_in_paths(
    new_paths: List[str],
    file_extension: str,
    all_metadata: Dict[str, Any],
    custom_file_ingestion_function: str,
    custom_file_ingestion_function_notebook: str,
    file_config: Dict[str, Any],
    file_staging_path: str,
    exit_context: Dict[str, Any]
) -> DataFrame:
    """
    Core file reading engine with format detection and schema management.
    
    This function provides the unified file reading logic for all ingestion patterns,
    supporting multiple file formats with intelligent schema handling and error recovery.
    It implements a sophisticated merge strategy for combining multiple files while
    managing schema evolution and data quality.
    
    Args:
        new_paths (list[str]): Absolute paths to files requiring ingestion.
                             Files are processed in order with schema reconciliation.
        
        file_extension (str): Target file format for reading.
                           Supported: csv, tsv, txt, json, parquet, xls, xlsx
                           Auto-detected from first file if not specified.
        
        primary_config (dict): Comprehensive configuration including:
            File Format Settings:
            - source_details_delimiter: Column separator for CSV (default: ",")
            - source_details_file_has_header_row: CSV header presence (default: true)
            - source_details_sheet_name: Excel worksheet name
            - source_details_multiline: Multi-line record support (default: false)
            
            Schema Management:
            - source_details_schema: Spark schema definition (StructType)
            - source_details_on_bad_records: Error handling strategy
                * "fail": Abort on first error (FAILFAST mode)
                * "drop": Silently drop malformed records
                * "quarantine": Isolate bad records in delta__corrupt_record
            
            Processing Mode:
            - process_one_file_at_a_time: Set to true to force sequential processing
              for CSV, JSON, and Parquet sources (default: false).
              When false, these formats are read in batch mode for better performance.
              XML and Excel files are always processed sequentially regardless of this setting.
    
        custom_file_ingestion_function (str): Python function name for custom file formats.
        
        custom_file_ingestion_function_notebook (str): name of notebook without .ipynb to execute as part of ingestion
    
    Returns:
        DataFrame: Consolidated DataFrame containing:
                  - All columns from source files (with type alignment)
                  - delta__raw_folderpath: Source file tracking column
                  - delta__corrupt_record: Quarantined records (if applicable)
    
    Raises:
        Exception: For unsupported file formats or processing errors
        AttributeError: If custom function is not found in specified path
    
    File Format Specifications:
        CSV/TSV/TXT:
        - Configurable delimiters and header detection
        - Multi-line record support for embedded newlines
        - Schema inference or enforcement
        
        JSON:
        - Single-line (JSON Lines) or multi-line support
        - Nested structure preservation
        - Schema validation with type coercion
        
        Parquet:
        - Native schema evolution support
        - Efficient columnar reading
        - Predicate pushdown optimization

        XML:
        - Native Spark 4.1 XML parsing with rowTag derived from source_details.xml_xpath
        - XML attributes preserved as unprefixed columns
        - Nested elements preserved as structs
        
        Excel (XLS/XLSX):
        - Sheet-specific reading
        - Header row configuration
        - String-based initial reading for type safety
        - Direct sheet-to-Spark conversion to reduce peak driver memory
        
        Custom Formats:
        - User-defined parsing logic
        - Access to full file path and configuration
        - Must return Spark DataFrame
    
    Schema Evolution Strategy:
        1. First file establishes base schema
        2. Subsequent files merged using unionByName
        3. New columns added with null values for previous records
        4. Type conflicts resolved based on precedence rules
        5. Optional strict mode prevents schema changes
    
    Error Handling:
        - FAILFAST: Immediate termination on data quality issues
        - DROPMALFORMED: Continue processing, log dropped records
        - PERMISSIVE: Quarantine bad records for analysis
    
    Performance Optimizations:
        - Lazy evaluation for schema discovery
        - Incremental sheet processing for Excel workbooks
        - Reuse of parsed schemas across files
        - Minimal data movement during union operations
    """

    # Handle custom file ingestion
    custom_result = _handle_custom_file_ingestion(
        custom_file_ingestion_function_notebook = custom_file_ingestion_function_notebook,
        custom_file_ingestion_function = custom_file_ingestion_function,
        new_paths = new_paths,
        all_metadata = all_metadata
    )
    if custom_result is not None:
        return custom_result
    
    # Resolve file extension
    resolved_extension = _resolve_file_extension(file_extension, new_paths)

    # Check if batch mode should be used
    if _should_use_batch_mode(resolved_extension, file_config):
        new_data = _read_files_in_batch(
            new_paths = new_paths,
            file_extension = resolved_extension,
            file_config = file_config
        )
    else:
        # Process files sequentially
        new_data = _read_files_sequentially(
            new_paths = new_paths,
            file_extension = resolved_extension,
            file_config = file_config,
            file_staging_path = file_staging_path
        )

    # Check if files contained any data
    if new_data.isEmpty():
        exit_gracefully_no_data(
            source_details = exit_context['source_details'],
            watermark_value = exit_context['watermark_value'],
            lineage_info = exit_context['lineage_info'],
            first_run = exit_context['first_run'],
            context_message = "Files were found but contained no data."
        )

    return new_data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def ingest_raw_files_with_base_path(
    source_config: dict,
    watermark_config: dict,
    custom_paths: dict,
    file_config: dict,
    all_metadata: dict,
    first_run: bool,
    lineage_info: dict
) -> tuple:
    """
    Ingest files from time-partitioned folder structures with watermark-based filtering.
    
    This function implements an efficient ingestion pattern for data organized in
    timestamp-based folders, commonly used in batch processing architectures. It
    discovers new folders since the last watermark, processes all files within those
    folders, and maintains folder-level processing state.
    
    Args:

    Returns:
        tuple[DataFrame, list]: Tuple containing:
            - new_data: Consolidated DataFrame from all processed files
            - folder_time_stamps: List of processed folder names (for watermark update)
    
    Processing Logic:
        1. List all folders under staging_folder_path
        2. Filter out archived folders (prefixed with "Archived_")
        3. Skip empty folders without deleting source-owned paths
        4. Select folders newer than watermark_value
        5. Collect all files from selected folders
        6. Delegate to read_all_files_in_paths for actual ingestion
        7. Return data with folder tracking information
    
    Folder Naming Convention:
        - Folders must have sortable names (typically timestamps)
        - Common patterns: YYYYMMDD, YYYYMMDD_HHMMSS, epoch_timestamp
        - Archived folders use prefix: "Archived_YYYYMMDD"
    
    Error Handling:
        - Empty folders are skipped without deletion
        - Missing folders are gracefully handled
        - File-level errors follow primary_config settings
    
    Performance Optimization:
        - Folder-level filtering reduces file system operations
        - Batch processing of multiple files per folder
        - Watermark ensures incremental processing
    
    Exit Conditions:
        If no new folders are found after the watermark, the function exits early
        with a status report, avoiding unnecessary processing.
    """
    watermark_value = watermark_config['watermark_value']
    staging_folder_load_mode = source_config['staging_folder_load_mode']

    # Construct full lakehouse path
    staging_folder_path_lakehouse = f"{custom_paths['source_files_lakehouse_path']}/{source_config['staging_folder_path']}"

    # Discover all folders in the base path
    folder_paths = notebookutils.fs.ls(staging_folder_path_lakehouse)

    # Filter out archived folders
    folder_paths = [folder_path for folder_path in folder_paths if "Archived_" not in folder_path.name]

    # === Incremental Folder Discovery ===
    
    eligible_folders = []
    
    for folder_path in folder_paths:
        files_in_path = notebookutils.fs.ls(folder_path.path)

        # Empty source folders may be created before files arrive; skip them without deletion.
        if len(files_in_path) == 0:
            continue

        # Check if folder is newer than watermark
        if not watermark_value or int(folder_path.name) > int(watermark_value):
            eligible_folders.append((folder_path, files_in_path))

    if staging_folder_load_mode == 'latest_snapshot' and eligible_folders:
        latest_folder = max(eligible_folders, key=lambda item: int(item[0].name))
        eligible_folders = [latest_folder]
        log_and_print(f"staging_folder_load_mode='latest_snapshot': selected latest staged folder '{latest_folder[0].name}'.")

    new_paths = []
    folder_time_stamps = []
    for folder_path, files_in_path in eligible_folders:
        # Collect all file paths from this folder
        file_paths_in_folder = [file.path for file in files_in_path]
        new_paths = new_paths + file_paths_in_folder
        folder_time_stamps.append(folder_path.name)

    # === Early Exit Optimization ===
    
    if len(new_paths) == 0:
        # No files found - exit gracefully (or raise on first run)
        exit_gracefully_no_data(
            source_details=staging_folder_path_lakehouse,
            watermark_value=watermark_value,
            lineage_info=lineage_info,
            first_run=first_run,
            context_message=f"No new data exists in staging path since last data load."
        )
    
    # === Delegate File Processing ===
    new_data = read_all_files_in_paths(
                        new_paths = new_paths,
                        file_extension = source_config['file_extension'],
                        all_metadata = all_metadata,
                        custom_file_ingestion_function_notebook = source_config['custom_file_ingestion_function_notebook'],
                        custom_file_ingestion_function = source_config['custom_file_ingestion_function'],
                        file_config = file_config,
                        file_staging_path = custom_paths['file_staging_path'],
                        exit_context = build_exit_context(
                            first_run = first_run,
                            lineage_info = lineage_info,
                            watermark_value = watermark_value,
                            source_details = staging_folder_path_lakehouse
                        )
                    )

    return new_data, folder_time_stamps

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def ingest_raw_files_with_wildcard_path(
    source_config: dict,
    watermark_config: dict,
    custom_paths: dict,
    file_config: dict,
    all_metadata: dict,
    first_run: bool,
    lineage_info: dict
) -> tuple:
    """
    Ingest files using pattern matching with modification time-based filtering.
    
    This function provides flexible file discovery using Hadoop-style wildcards,
    combined with modification timestamp filtering for incremental processing.
    It's ideal for scenarios where files are distributed across multiple directories
    or follow specific naming patterns.
    
    Args:


    Returns:
        tuple[DataFrame, list]: Tuple containing:
            - new_data: Merged DataFrame from all matching files
            - file_time_stamps: List of file modification timestamps
    
    Key Features:
        - Efficient pattern-based file discovery
        - Modification time filtering for incremental loads
        - Support for complex directory structures
        - Handles thousands of files efficiently
    
    Use Cases:
        1. Cross-partition file discovery
           Pattern: "transactions/*/region_*/sales_*.parquet"
        
        2. Date-based file patterns
           Pattern: "logs/2024/*/app_log_*.json"
        
        3. Multi-source consolidation
           Pattern: "raw/{source1,source2,source3}/data_*.csv"
    
    Performance Considerations:
        - Server-side glob expansion minimizes network calls
        - Timestamp filtering reduces data volume early
        - Batch processing of discovered files
        - Parallel file reading when possible
    
    Early Exit:
        If no files match the pattern or all files are older than the watermark,
        the function exits with a detailed status report.
    
    Error Handling:
        - Invalid patterns raise exceptions early
        - Missing files are logged but don't stop processing
        - File-level errors follow primary_config settings
    """
    watermark_value = watermark_config['watermark_value']

    # Build fully qualified wildcard pattern
    lakehouse_wildcard_folder_path = f"{custom_paths['source_files_lakehouse_path']}/{source_config['wildcard_folder_path']}"

    # Discover all matching files (returns DataFrame with path + mod_time_utc)
    all_files = get_files_with_modified_date(lakehouse_wildcard_folder_path)

    # Filter incrementally using watermark if provided
    if watermark_value:
        new_files = all_files.filter(
            f"mod_time_utc > to_timestamp('{str(watermark_value)}', 'yyyy-MM-dd HH:mm:ss.SSSSSSS')"
        )
    else:
        new_files = all_files

    # Order by modification time for deterministic processing sequence
    new_files_in_order = new_files.orderBy("mod_time_utc")
        
    # Collect paths and timestamps into driver memory (acceptable when file count is moderate)
    new_paths = new_files_in_order.select("path").rdd.flatMap(lambda x: x).collect()
    file_time_stamps = new_files_in_order.select("mod_time_utc").rdd.flatMap(lambda x: x).collect()

    # No files found - exit gracefully (or raise on first run)
    if len(new_paths) == 0:
        exit_gracefully_no_data(
            source_details=lakehouse_wildcard_folder_path,
            watermark_value=watermark_value,
            lineage_info=lineage_info,
            first_run=first_run,
            context_message=f"No new files match wildcard pattern since last data load."
        )
    
    # Read and merge all discovered files with schema / custom function handling
    new_data = read_all_files_in_paths(
        new_paths = new_paths,
        file_extension = source_config['file_extension'],
        all_metadata = all_metadata,
        custom_file_ingestion_function_notebook = source_config['custom_file_ingestion_function_notebook'],
        custom_file_ingestion_function = source_config['custom_file_ingestion_function'],
        file_config = file_config,
        file_staging_path = custom_paths['file_staging_path'],
        exit_context = build_exit_context(
            first_run = first_run,
            lineage_info = lineage_info,
            watermark_value = watermark_value,
            source_details = lakehouse_wildcard_folder_path
        )
    )

    # Return DataFrame plus list of timestamps (used for watermark advancement)
    return new_data, file_time_stamps

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
