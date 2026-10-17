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

# # NB_Helper_Functions_3 - Configuration and Advanced Processing Utilities
# 
# ## Overview
# This notebook serves as the third module in the helper functions suite, focusing on configuration parsing, parameter management, and advanced data processing operations. It bridges the gap between metadata-driven configuration and runtime execution, ensuring seamless integration across the data platform.
# 
# ### Key Responsibilities
# - **Parameter Parsing**: Transforms JSON metadata into actionable Python variables
# - **Spark Configuration**: Optimizes Spark settings based on target layer requirements
# - **Advanced Processing**: Implements SCD2 operations, schema management, and entity resolution
# - **Validation Functions**: Ensures data integrity through schema comparison and metadata updates
# 
# ### Integration Context
# This notebook is typically executed after `NB_Helper_Functions_1` and `NB_Helper_Functions_2`, inheriting their context while adding specialized processing capabilities for complex data engineering scenarios.
# 
# ---
# 
# ## 1. Configuration Parameter Parsing
# 
# The following section parses all input parameters from the orchestration pipeline and transforms them into variables used throughout the data processing workflow.


# CELL ********************

# ===========================================================================================
# CONFIGURATION PARSING HELPER FUNCTIONS
# ===========================================================================================
# These atomic functions parse and validate configuration inputs, making them independently
# testable while maintaining backward compatibility with existing code.
# ===========================================================================================

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Tuple

STAGING_FOLDER_LOAD_MODES = ('all_unprocessed_folders', 'latest_snapshot')
WATERMARK_STRATEGIES = ('none', 'column', 'change_data_feed', 'file_modified_time', 'custom')

# ===========================================================================================
# CONFIGURATION DATACLASSES
# ===========================================================================================
# Typed return objects for all parse_* functions. These replace plain dicts to provide:
#   - IDE autocomplete and typo detection at construction time
#   - mypy / pyright type checking
#   - Self-documenting field names with defaults
#
# _ConfigBase provides __getitem__ and get() so existing dict-style access
# (e.g., config['key'] or config.get('key', default)) continues to work unchanged.
# ===========================================================================================

class _ConfigBase:
    """Mixin providing dict-style access for backward compatibility with dict callers."""
    def __getitem__(self, key):
        if hasattr(self, key):
            return getattr(self, key)
        raise KeyError(key)

    def __setitem__(self, key, value):
        if hasattr(self, key):
            setattr(self, key, value)
        else:
            raise KeyError(key)

    def get(self, key, default=None):
        return getattr(self, key, default)

    def __contains__(self, key):
        return hasattr(self, key)

    def keys(self):
        """Return dataclass field names, matching dict.keys() behaviour."""
        return [f.name for f in self.__dataclass_fields__.values()]

    def values(self):
        """Return dataclass field values, matching dict.values() behaviour."""
        return [getattr(self, f.name) for f in self.__dataclass_fields__.values()]

    def items(self):
        """Return (field_name, value) pairs, matching dict.items() behaviour."""
        return [(f.name, getattr(self, f.name)) for f in self.__dataclass_fields__.values()]

    def __iter__(self):
        """Iterate over field names, matching dict.__iter__() behaviour."""
        return iter(self.keys())

    def __len__(self):
        """Return number of fields, matching dict.__len__() behaviour."""
        return len(self.__dataclass_fields__)


@dataclass
class ParsedMetadata(_ConfigBase):
    """Return type for parse_json_metadata()."""
    orchestration_metadata: dict = field(default_factory=dict)
    primary_config: dict = field(default_factory=dict)
    advanced_config: list = field(default_factory=list)
    table_ddl: list = field(default_factory=list)
    latest_schema_details: dict = field(default_factory=dict)


@dataclass
class SchemaTrackingInfo(_ConfigBase):
    """Return type for extract_schema_tracking_info()."""
    last_schema_id: Optional[str] = None
    last_schema_details: list = field(default_factory=list)


@dataclass
class SourceConfig(_ConfigBase):
    """Return type for parse_source_configuration()."""
    table_id: Optional[int] = None
    input_delta_table_external_location: str = ''
    staging_folder_path: Optional[str] = None
    staging_folder_load_mode: str = ''
    wildcard_folder_path: Optional[str] = None
    source_path: Optional[str] = None
    using_source_folder_path: bool = False
    custom_table_ingestion_function: str = ''
    custom_table_ingestion_function_notebook: str = ''
    custom_file_ingestion_function: str = ''
    custom_file_ingestion_function_notebook: str = ''
    custom_source_function: str = ''
    custom_source_function_notebook: str = ''
    watermark_table_name: str = ''
    source_datastore_name: str = ''
    source_workspace_name: Optional[str] = None
    source_query: str = ''
    connection_id: str = ''
    source_category: str = ''
    file_extension: str = ''
    expected_schema: str = ''


@dataclass
class MedallionLayerDefaults(_ConfigBase):
    """Return type for determine_medallion_layer_defaults()."""
    default_watermark_column_name: str = ''
    default_merge_type: str = ''


@dataclass
class TargetConfig(_ConfigBase):
    """Return type for parse_target_configuration()."""
    target_workspace_name: str = ''
    target_datastore_id: str = ''
    target_datastore_workspace_id: str = ''
    target_table_name: str = ''
    target_folder_path: Optional[str] = None
    target_abfss_path: str = ''
    default_merge_type: str = ''
    output_external_location: str = ''
    enforce_not_null: bool = False
    schema_name_for_path: Optional[str] = None
    table_name_for_path: Optional[str] = None
    target_quarantined_abfss_path: Optional[str] = None
    lakehouse_table_output: bool = False
    replace_where_column: str = ''
    target_datastore_name: str = ''
    target_datastore_medallion_name: str = ''
    warehouse_write_mode: str = ''
    target_excel_sheet_name: str = 'Sheet1'
    target_output_delimiter: str = ''
    default_watermark_column_name: str = ''


@dataclass
class FileIngestionPathsConfig(_ConfigBase):
    """Return type for parse_file_ingestion_paths()."""
    source_files_datastore_id: str = ''
    source_files_datastore_workspace_id: str = ''
    source_files_lakehouse_path: str = ''
    file_staging_path: str = ''
    clean_up_temporary_path: bool = False


@dataclass
class LineageInfo(_ConfigBase):
    """Return type for extract_lineage_information()."""
    source_medallion_layer: str = ''
    source_type: str = ''
    target_medallion_layer: str = ''
    target_type: str = ''


@dataclass
class WatermarkConfig(_ConfigBase):
    """Return type for parse_watermark_configuration()."""
    column_to_mark_source_data_deletion: str = ''
    delete_rows_with_value: str = ''
    merge_type: str = ''
    default_merge_type: str = ''
    merge_in_batches_with_columns: list = field(default_factory=list)
    strategy: str = 'none'
    watermark_column_data_type: str = ''
    watermark_column_name: str = ''
    use_watermark_column: bool = False
    watermark_value: str = ''


@dataclass
class AdvancedProcessingConfig(_ConfigBase):
    """Return type for parse_advanced_processing_configuration()."""
    liquid_clustering_columns: list = field(default_factory=list)
    fail_on_new_schema: bool = False
    fail_on_column_data_type_change: bool = False
    if_duplicate_primary_keys: str = 'fail'
    trim_column_names: bool = False
    apply_case_column_names: str = ''
    replace_non_alphanumeric_chars_with_underscore_in_column_names: bool = False
    regex_find_in_column_names: str = ''
    regex_replace_in_column_names: str = ''
    exact_find_in_column_names: str = ''
    exact_replace_in_column_names: str = ''
    trim_data_in_string_columns: str = ''
    replace_blank_with_null_in_string_columns: str = ''


@dataclass
class PrimaryKeyConfig(_ConfigBase):
    """Return type for parse_primary_key_configuration()."""
    primary_keys: list = field(default_factory=list)


@dataclass
class AdvancedStepsConfig(_ConfigBase):
    """Return type for parse_advanced_configuration_steps()."""
    data_quality_steps: list = field(default_factory=list)
    data_transformation_steps: list = field(default_factory=list)
    post_write_reconciliation_steps: list = field(default_factory=list)


@dataclass
class DimensionConfig(_ConfigBase):
    """Return type for parse_dimension_table_configuration()."""
    dimension_table_key_column_name: str = ''
    source_timestamp_column_name: str = 'delta__modified_datetime'
    fact_table_data_load: list = field(default_factory=list)


@dataclass
class WarehouseConfig(_ConfigBase):
    """Return type for parse_warehouse_configuration()."""
    warehouse_configured: bool = False
    warehouse_endpoint: Optional[str] = None


@dataclass
class FileConfig(_ConfigBase):
    """Return type for extract_file_configuration()."""
    delimiter: str = ','
    file_has_header_row: bool = True
    sheet_name: str = ''
    pandas_header_config: Optional[int] = 0
    multiline: bool = False
    encoding: str = 'utf-8'
    allow_missing_columns: bool = True
    parquet_schema_kwargs: dict = field(default_factory=dict)
    schema_kwargs: dict = field(default_factory=dict)
    xml_xpath: Optional[str] = None
    xml_namespaces: Optional[dict] = None
    process_one_file_at_a_time: bool = False
    expected_schema_string: Optional[str] = None


@dataclass
class PerformanceConfig(_ConfigBase):
    """Return type for parse_performance_configuration()."""
    compute_statistics_on_columns: str = ''
    compute_statistics_on_first_n_columns: str = ''
    column_stats_configured: bool = False
    use_spark_config_for_lakehouse: str = ''


@dataclass
class SparkConfig(_ConfigBase):
    """Return type for parse_spark_configuration()."""
    enable_change_data_feed: bool = False
    spark_timestamp_rebase_mode_write: str = 'CORRECTED'
    spark_timestamp_rebase_mode_read: str = 'CORRECTED'


@dataclass
class LayerSparkConfig(_ConfigBase):
    """Return type for parse_*_layer_spark_configuration()."""
    optimize_write_enabled: bool = False
    v_order_enabled: bool = False
    checkpoint_interval: Optional[str] = None


@dataclass
class CustomStagingParams(_ConfigBase):
    """Return type for parse_custom_staging_parameters()."""
    metadata: dict = field(default_factory=dict)
    primary_config: dict = field(default_factory=dict)
    custom_staging_function: str = ''
    custom_staging_function_notebook: str = ''


def _validate_conflicting_source_configs(primary_config: dict, table_id) -> None:
    """
    Validate that source_details configs don't silently conflict.
    
    These checks match Rules 80, 84, 85 from validate_metadata_sql.py.
    They act as a runtime safety net so conflicting metadata fails loudly
    instead of silently ignoring user-provided values.
    
    Raises:
        ValueError: When conflicting source configs are detected.
    """
    staging_folder_path = primary_config.get("source_details_staging_folder_path", "")
    wildcard_folder_path = primary_config.get("source_details_wildcard_folder_path", "")
    custom_table_fn = primary_config.get("source_details_custom_table_ingestion_function", "")
    custom_file_fn = primary_config.get("source_details_custom_file_ingestion_function", "")
    custom_source_fn = primary_config.get("source_details_custom_source_function", "")
    table_name = primary_config.get("source_details_table_name", "")

    # Rule 80: custom ingestion function + table_name
    if table_name and (custom_table_fn or custom_file_fn or custom_source_fn):
        conflicting = [f for f in ['custom_table_ingestion_function', 'custom_file_ingestion_function', 'custom_source_function']
                       if primary_config.get(f'source_details_{f}', '')]
        error_msg = (
            f"Table_ID {table_id}: Conflicting source_details — 'table_name' cannot be used with "
            f"'{', '.join(conflicting)}'. Custom ingestion functions bypass table_name entirely "
            f"(it is never read). Remove 'table_name' or the custom function. "
            f"Run validate_metadata_sql.py to catch this before deployment."
        )
        log_and_print(error_msg)
        raise ValueError(error_msg)

    # Rule 84: staging_folder_path + wildcard_folder_path
    if staging_folder_path and wildcard_folder_path:
        error_msg = (
            f"Table_ID {table_id}: Conflicting source_details — both 'staging_folder_path' and "
            f"'wildcard_folder_path' are set. staging_folder_path takes precedence and "
            f"wildcard_folder_path is silently ignored. Set only one. "
            f"Run validate_metadata_sql.py to catch this before deployment."
        )
        log_and_print(error_msg)
        raise ValueError(error_msg)

    # Rule 85: staging_folder_path + custom_table_ingestion_function
    if staging_folder_path and custom_table_fn:
        error_msg = (
            f"Table_ID {table_id}: Conflicting source_details — both 'staging_folder_path' and "
            f"'custom_table_ingestion_function' are set. Staging routes to file ingestion, so "
            f"the custom table function is never invoked. Remove one. "
            f"(Note: custom_file_ingestion_function IS compatible with staging.) "
            f"Run validate_metadata_sql.py to catch this before deployment."
        )
        log_and_print(error_msg)
        raise ValueError(error_msg)

    # custom_source_function conflicts with staging/file paths (it returns a DataFrame, not files)
    if custom_source_fn and (staging_folder_path or wildcard_folder_path):
        error_msg = (
            f"Table_ID {table_id}: Conflicting source_details — 'custom_source_function' cannot be used with "
            f"'staging_folder_path' or 'wildcard_folder_path'. custom_source_function returns a DataFrame "
            f"directly — it does not read from or write to file paths. Use custom_staging_function for "
            f"file-based staging, or remove the folder path. "
            f"Run validate_metadata_sql.py to catch this before deployment."
        )
        log_and_print(error_msg)
        raise ValueError(error_msg)

    # custom_source_function conflicts with custom_table_ingestion_function (only one source function per Table_ID)
    if custom_source_fn and custom_table_fn:
        error_msg = (
            f"Table_ID {table_id}: Conflicting source_details — both 'custom_source_function' and "
            f"'custom_table_ingestion_function' are set. Only one source function can be active per Table_ID. "
            f"Use custom_source_function for external sources or custom_table_ingestion_function for "
            f"internal Fabric table reads. "
            f"Run validate_metadata_sql.py to catch this before deployment."
        )
        log_and_print(error_msg)
        raise ValueError(error_msg)


def _resolve_staging_folder_load_mode(primary_config: dict, staging_folder_path: str) -> str:
    """Resolve how a staged folder base path should be read during batch processing."""
    explicit_mode = primary_config.get("source_details_staging_folder_load_mode", "")
    explicit_mode = (explicit_mode or "").strip().lower()

    if explicit_mode and explicit_mode not in STAGING_FOLDER_LOAD_MODES:
        valid_modes = ', '.join(STAGING_FOLDER_LOAD_MODES)
        raise ValueError(
            f"Invalid source_details_staging_folder_load_mode='{explicit_mode}'. "
            f"Valid values are: {valid_modes}."
        )

    if not staging_folder_path:
        return ""

    if explicit_mode:
        return explicit_mode

    source_cursor_strategy = _resolve_watermark_strategy(primary_config)
    if source_cursor_strategy in ("column", "custom", "file_modified_time"):
        return "all_unprocessed_folders"

    return "latest_snapshot"


def _resolve_watermark_strategy(primary_config: dict) -> str:
    explicit_strategy = (primary_config.get("watermark_details_strategy", "") or "").strip().lower()
    if explicit_strategy and explicit_strategy not in WATERMARK_STRATEGIES:
        valid_values = ", ".join(WATERMARK_STRATEGIES)
        raise ValueError(
            f"Invalid watermark_details.strategy='{explicit_strategy}'. Valid values are: {valid_values}."
        )

    if explicit_strategy:
        return explicit_strategy

    legacy_field_names = {
        "watermark_details_use_change_data_feed": "watermark_details.use_change_data_feed",
        "watermark_details_use_watermark_column": "watermark_details.use_watermark_column",
    }
    legacy_fields = [
        field_name
        for field_name in legacy_field_names
        if (primary_config.get(field_name, "") or "").strip()
    ]
    if legacy_fields:
        legacy_metadata_fields = [legacy_field_names[field_name] for field_name in legacy_fields]
        raise ValueError(
            "Legacy watermark flags are no longer supported: "
            f"{', '.join(legacy_metadata_fields)}. Use watermark_details.strategy instead."
        )

    watermark_column_name = (primary_config.get("watermark_details_column_name", "") or "").strip()
    if watermark_column_name:
        raise ValueError(
            "watermark_details.column_name requires watermark_details.strategy='column'. "
            "Do not rely on implicit column watermarking."
        )

    raise ValueError(
        "Missing required watermark_details.strategy. Set watermark_details.strategy='none' "
        "explicitly when no watermarking is used."
    )


def _validate_conflicting_watermark_configs(primary_config: dict, table_id=None) -> None:
    """
    Validate that watermark/merge configs don't silently conflict.
    
    These checks match Rules 81, 82, 83, 87 from validate_metadata_sql.py.
    
    Raises:
        ValueError: When conflicting watermark/merge configs are detected.
    """
    table_id_label = f"Table_ID {table_id}" if table_id is not None else "Current table"
    strategy = _resolve_watermark_strategy(primary_config)
    use_cdf = strategy == "change_data_feed"
    user_delete_col = primary_config.get("target_details_column_to_mark_source_data_deletion", "").strip()
    user_delete_val = primary_config.get("target_details_delete_rows_with_value", "").strip()
    user_watermark_col = primary_config.get("watermark_details_column_name", "").strip()
    explicit_merge_type = primary_config.get("target_details_merge_type", "").strip().lower()

    # Rule 81: CDF overrides user-set soft delete columns
    if use_cdf and user_delete_col:
        error_msg = (
            f"{table_id_label}: Conflicting config — watermark strategy='change_data_feed' AND "
            f"column_to_mark_source_data_deletion='{user_delete_col}'. CDF overwrites this to "
            f"'_change_type'. Remove column_to_mark_source_data_deletion when using CDF. "
            f"Run validate_metadata_sql.py to catch this before deployment."
        )
        log_and_print(error_msg)
        raise ValueError(error_msg)

    # Rule 82: CDF overrides watermark column
    if use_cdf and user_watermark_col:
        error_msg = (
            f"{table_id_label}: Conflicting config — watermark strategy='change_data_feed' AND "
            f"watermark column_name='{user_watermark_col}'. CDF clears the watermark column "
            f"and uses _commit_version internally. Remove 'column_name' from watermark_details. "
            f"Run validate_metadata_sql.py to catch this before deployment."
        )
        log_and_print(error_msg)
        raise ValueError(error_msg)

    # Rule 87: Soft delete config with merge_type that ignores deletion columns
    # Only these merge types actually use soft delete columns in merge_data():
    deletion_aware_types = {'merge_and_delete', 'merge_mark_unmatched_deleted', 'merge_mark_all_deleted', 'scd2'}
    if explicit_merge_type and explicit_merge_type not in deletion_aware_types and (user_delete_col or user_delete_val):
        deletion_aware_list = ', '.join(sorted(deletion_aware_types))
        error_msg = (
            f"{table_id_label}: Conflicting config — soft delete attributes are set "
            f"(column_to_mark_source_data_deletion='{user_delete_col}', "
            f"delete_rows_with_value='{user_delete_val}') but merge_type is explicitly '{explicit_merge_type}'. "
            f"Only these merge types use soft delete columns: [{deletion_aware_list}]. "
            f"The '{explicit_merge_type}' handler completely ignores these columns. "
            f"Change merge_type or remove the soft delete configs. "
            f"Run validate_metadata_sql.py to catch this before deployment."
        )
        log_and_print(error_msg)
        raise ValueError(error_msg)


def _log_config_overrides(scope: str, overrides: list) -> None:
    """Log explicit, user-visible notes when config parsing normalizes or overrides values."""
    if not overrides:
        return

    for override in overrides:
        log_and_print(f"Config override ({scope}): {override}")


# ===========================================================================================
# ERROR MESSAGE SANITIZATION
# ===========================================================================================
# Redacts common secret patterns and truncates before persisting to Data_Pipeline_Logs.
# Full, unredacted errors remain available in Spark driver logs (Log4j / Fabric Monitor).
# ===========================================================================================

import re as _re_module  # local alias avoids shadowing if re is already imported

_SECRET_PATTERNS: list = [
    # Bearer / Basic auth tokens
    (_re_module.compile(r'(Bearer|Basic)\s+[A-Za-z0-9\-._~+/]+=*', _re_module.IGNORECASE), r'\1 [REDACTED]'),
    # ODBC connection-string keys (UID, PWD, Password, Access_Token)
    (_re_module.compile(r'(UID|PWD|Password|Access_Token)\s*=\s*[^;\s]+', _re_module.IGNORECASE), r'\1=[REDACTED]'),
    # Generic key=value secrets (password, pwd, secret, token, key, sig, api_key, apikey, client_secret)
    (_re_module.compile(r'(password|pwd|secret|token|key|sig|api_key|apikey|client_secret)\s*=\s*[^;&\s]+', _re_module.IGNORECASE), r'\1=[REDACTED]'),
    # SAS token query-string parameters
    (_re_module.compile(r'([?&])(sig|sv|se|sp|spr|srt|ss)=([^&\s]+)', _re_module.IGNORECASE), r'\1\2=[REDACTED]'),
    # JWT-like tokens (three dot-separated base64url segments, first starting with eyJ)
    (_re_module.compile(r'eyJ[A-Za-z0-9_-]{10,}\.eyJ[A-Za-z0-9_-]{10,}\.[A-Za-z0-9_-]{10,}'), '[JWT REDACTED]'),
]

_ERROR_MESSAGE_MAX_LENGTH: int = 4000


def _sanitize_error_message(error_text: str, max_length: int = _ERROR_MESSAGE_MAX_LENGTH) -> str:
    """Redact common secret patterns and truncate an error message for safe logging.

    This function is applied before writing error details so
    that connection strings, bearer tokens, SAS signatures, JWTs, and other secrets
    embedded in exception strings are not persisted in the warehouse or printed to
    stdout.  The full, unredacted error remains in the Spark driver logs via the
    standard traceback.

    Patterns redacted:
      - Bearer / Basic auth headers
      - ODBC connection-string keys (UID, PWD, Password, Access_Token)
      - Generic key=value secrets (password, secret, token, key, sig, …)
      - SAS query-string parameters (sig, sv, se, sp, spr, srt, ss)
      - JWT tokens (three dot-separated base64url segments starting with eyJ)

    Args:
        error_text: Raw error string (typically ``str(exception)``).
        max_length: Maximum character length for the returned string.  Defaults to
                    4000.  Messages exceeding this are truncated with a suffix
                    indicating the original length.

    Returns:
        str: Sanitized, possibly truncated error message.

    Examples:
        >>> _sanitize_error_message('Connection failed: password=MyS3cret;host=db')
        'Connection failed: password=[REDACTED];host=db'
        >>> _sanitize_error_message('x' * 5000, max_length=100)
        'xxxx...xx ... [truncated, 5000 chars total]'
    """
    if not error_text:
        return error_text

    sanitized = error_text
    for pattern, replacement in _SECRET_PATTERNS:
        sanitized = pattern.sub(replacement, sanitized)

    if max_length is not None and max_length < 0:
        raise ValueError(f"max_length must be non-negative, got {max_length}")

    if max_length is None:
        return sanitized

    if len(sanitized) <= max_length:
        return sanitized

    truncation_suffix = f" ... [truncated, {len(error_text)} chars total]"

    if max_length == 0:
        return ""

    if max_length <= len(truncation_suffix):
        return truncation_suffix[:max_length]

    return sanitized[:max_length - len(truncation_suffix)] + truncation_suffix


def parse_json_metadata(
    orchestration_metadata_json: str,
    primary_config_json: str,
    advanced_config_json: str,
    table_ddl_json: str,
    latest_schema_details_json: str
) -> ParsedMetadata:
    """
    Parse all JSON metadata inputs into Python dictionaries.
    
    This function serves as the entry point for metadata parsing, converting
    JSON strings from the orchestration pipeline into structured dictionaries
    for downstream processing.
    
    Args:
        orchestration_metadata_json (str): JSON containing table ID and target details
        primary_config_json (str): JSON containing primary configuration settings
        advanced_config_json (str): JSON array of advanced configurations
        table_ddl_json (str): JSON array containing source DDL definitions
        latest_schema_details_json (str): JSON containing previous schema information
    
    Returns:
        ParsedMetadata: Parsed metadata container:
            - orchestration_metadata (dict): Orchestration context
            - primary_config (dict): Primary configuration
            - advanced_config (list): List of advanced config dictionaries
            - table_ddl (list): DDL definitions
            - latest_schema_details (dict): Schema tracking information
    
    Example:
        >>> metadata = parse_json_metadata(
        ...     orchestration_metadata_json='{"Table_ID": 123}',
        ...     primary_config_json='{"source_details_table_name": "dbo.sales"}',
        ...     advanced_config_json='[]',
        ...     table_ddl_json='[]',
        ...     latest_schema_details_json='{"Schema_ID": "abc123"}'
        ... )
        >>> print(metadata.orchestration_metadata['Table_ID'])
        123
    """
    return ParsedMetadata(
        orchestration_metadata=json.loads(orchestration_metadata_json),
        primary_config=json.loads(primary_config_json),
        advanced_config=json.loads(advanced_config_json),
        table_ddl=json.loads(table_ddl_json),
        latest_schema_details=json.loads(latest_schema_details_json)
    )


def clean_advanced_config(advanced_config: list) -> list:
    """
    Remove NoData placeholder rows from advanced configuration.
    
    Args:
        advanced_config (list): Raw advanced configuration from metadata
    
    Returns:
        list: Filtered configuration without NoData rows
    
    Example:
        >>> config = [
        ...     {'Configuration_Category': 'data_quality'},
        ...     {'Configuration_Category': 'NoData'},
        ...     {'Configuration_Category': 'data_transformation_steps'}
        ... ]
        >>> cleaned = clean_advanced_config(config)
        >>> len(cleaned)
        2
    """
    return [row for row in advanced_config if row.get('Configuration_Category') != 'NoData']


def normalize_schema_details(latest_schema_details: dict) -> dict:
    """
    Convert NoData schema placeholder to empty dictionary.
    
    When no schema has been logged for a table, the metadata returns a
    'NoData' placeholder. This function normalizes it to an empty dict
    for consistent downstream handling.
    
    Args:
        latest_schema_details (dict): Raw schema details from metadata
    
    Returns:
        dict: Normalized schema details (empty dict if NoData)
    
    Example:
        >>> schema = {'Schema_ID': 'NoData'}
        >>> normalized = normalize_schema_details(schema)
        >>> normalized
        {}
    """
    if latest_schema_details.get('Schema_ID') == 'NoData':
        return {}

    return latest_schema_details


def _parse_schema_details(raw) -> list:
    """
    Parse schema details from the stored database value into a list of
    [column_name, data_type] pairs.

    Supports two storage formats:
      - JSON (current):  '[["id", "int"], ["name", "string"]]'
      - Python repr (legacy):  "[('id', 'int'), ('name', 'string')]"

    Args:
        raw: Schema details — already a list, a JSON string, a legacy
             Python repr string, or empty/None.

    Returns:
        list: Schema as a list of [column_name, data_type] pairs.

    Raises:
        ValueError: If raw is a non-empty string that cannot be parsed
                    as either JSON or a Python literal.
    """
    if isinstance(raw, list):
        return raw
    if not raw:
        return []

    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        result = ast.literal_eval(raw)
        if not isinstance(result, list):
            raise ValueError(f"Schema details parsed to {type(result).__name__}, expected list")
        return result


def extract_schema_tracking_info(latest_schema_details: dict) -> SchemaTrackingInfo:
    """
    Extract schema ID and parsed schema details for change detection.
    
    Args:
        latest_schema_details (dict): Normalized schema details dictionary
    
    Returns:
        SchemaTrackingInfo: Parsed schema tracking information:
            - last_schema_id: Previous schema identifier
            - last_schema_details: Parsed list of [column, type] pairs
    
    Example:
        >>> schema = {
        ...     'Schema_ID': 'abc123',
        ...     'Schema_Details': '[["id", "int"], ["name", "string"]]'
        ... }
        >>> tracking = extract_schema_tracking_info(schema)
        >>> tracking.last_schema_id
        'abc123'
        >>> len(tracking.last_schema_details)
        2
    """
    return SchemaTrackingInfo(
        last_schema_id=latest_schema_details.get('Schema_ID'),
        last_schema_details=_parse_schema_details(latest_schema_details.get('Schema_Details', '[]'))
    )

# ===========================================================================================
# SOURCE CONFIGURATION EXTRACTION
# ===========================================================================================
# Extract and validate source-related configurations including connection details,
# query specifications, and file paths for various ingestion patterns.

# ===========================================================================================
# DATASTORE CONFIGURATION LOOKUP
# ===========================================================================================
# These functions parse and lookup datastore configuration from the Datastore_Configuration
# table results. The pipeline passes lookup activity results as a string representation
# of an array of dicts.

def _parse_datastore_config(datastore_config: str | list | dict) -> list:
    """
    Parse datastore configuration from pipeline lookup activity results.
    
    The pipeline passes the lookup activity result as JSON, like:
    '[{"Datastore_Name": "bronze", "Datastore_Type": "Lakehouse", ...}]'
    
    Args:
        datastore_config: Either a JSON string from the pipeline, an already-parsed list,
                         or a single datastore dictionary.
    
    Returns:
        list: Parsed list of datastore configuration dictionaries.
    
    Example:
        >>> config_str = '[{"Datastore_Name": "bronze", "Datastore_ID": "abc123"}]'
        >>> parsed = _parse_datastore_config(config_str)
        >>> parsed[0]['Datastore_Name']
        'bronze'
    """
    if isinstance(datastore_config, list):
        return datastore_config

    if isinstance(datastore_config, dict):
        return [datastore_config]
    
    if isinstance(datastore_config, str):
        if not datastore_config.strip():
            return []
        try:
            parsed = json.loads(datastore_config)
            if isinstance(parsed, list):
                return parsed
            if isinstance(parsed, dict):
                return [parsed]
            raise ValueError(
                f"Parsed datastore_config must be a JSON array or object, got {type(parsed)}"
            )
        except json.JSONDecodeError as e:
            raise ValueError(
                f"Failed to parse datastore_config as JSON. "
                f"Expected format: '[{{\"Datastore_Name\": \"bronze\", ...}}]'. Error: {e}"
            )
    
    raise TypeError(f"datastore_config must be str, list, or dict, got {type(datastore_config)}")


def _get_datastore_config(datastore_config: str | list | dict, datastore_name: str, property_name: str) -> str:
    """
    Get a specific property from the datastore configuration for a given datastore.
    
    This function looks up the datastore configuration from the Datastore_Configuration
    table results (passed from pipeline lookup activity) and returns the requested property.
    
    Args:
        datastore_config: Lookup activity result - either a string like 
                         "[{'Datastore_Name': 'bronze', ...}]" or an already-parsed list.
        datastore_name: The name of the datastore to look up (case-insensitive).
        property_name: The property to retrieve. Valid values:
                      - 'Datastore_ID': The GUID of the datastore artifact
                      - 'Workspace_ID': The GUID of the Fabric workspace
                      - 'Workspace_Name': The name of the Fabric workspace
                      - 'Medallion_Layer': The medallion layer (bronze/silver/gold)
                      - 'Endpoint': The SQL endpoint for warehouses
                      - 'Connection_ID': The Fabric Connection ID for authentication
                      - 'Datastore_Type': The type (Lakehouse/Warehouse)
    
    Returns:
        str: The requested property value.
    
    Raises:
        KeyError: If the datastore is not found in the configuration.
        ValueError: If the datastore_config cannot be parsed.
    
    Example:
        >>> config_str = "[{'Datastore_Name': 'bronze', 'Datastore_ID': 'abc123', 'Workspace_Name': 'Dev'}]"
        >>> _get_datastore_config(config_str, 'bronze', 'Datastore_ID')
        'abc123'
        >>> _get_datastore_config(config_str, 'bronze', 'Workspace_Name')
        'Dev'
    """
    parsed_config = _parse_datastore_config(datastore_config)
    
    # Find the datastore by name (case-insensitive)
    datastore_name_lower = datastore_name.strip().lower()
    
    for datastore in parsed_config:
        if datastore.get('Datastore_Name', '').strip().lower() == datastore_name_lower:
            value = datastore.get(property_name, '')
            if value is None:
                value = ''
            return str(value).strip()
    
    # Datastore not found - provide helpful error message
    available_datastores = [d.get('Datastore_Name', 'unknown') for d in parsed_config]
    raise KeyError(
        f"Datastore '{datastore_name}' not found in Datastore_Configuration. "
        f"Available datastores: {available_datastores}. "
        f"Add '{datastore_name}' to the Datastore_Configuration table. "
        f"See docs/FAQ.md for setup instructions."
    )


def get_datastore_entry(datastore_config: str | list | dict, datastore_name: str) -> dict:
    """Return the full datastore configuration row for a given datastore name."""
    parsed_config = _parse_datastore_config(datastore_config)
    datastore_name_lower = datastore_name.strip().lower()

    for datastore in parsed_config:
        if datastore.get('Datastore_Name', '').strip().lower() == datastore_name_lower:
            return dict(datastore)

    available_datastores = [d.get('Datastore_Name', 'unknown') for d in parsed_config]
    raise KeyError(
        f"Datastore '{datastore_name}' not found in Datastore_Configuration. "
        f"Available datastores: {available_datastores}."
    )


# ===========================================================================================
# LAKEHOUSE MOUNTING UTILITIES FOR CUSTOM FUNCTIONS
# ===========================================================================================
# These functions provide local file system access to lakehouse files for custom ingestion
# functions that need to use libraries requiring local paths (e.g., zipfile, PIL, PyPDF2).
#
# Documentation: https://learn.microsoft.com/en-us/fabric/data-engineering/notebook-utilities
#
# WHEN TO USE:
# - Python open() - e.g., open(path, 'r')
# - zipfile.ZipFile() for archives
# - PIL/Pillow for image processing
# - PyPDF2 for PDF parsing
# - Any library using os.path or pathlib internally
#
# WHEN NOT NEEDED (ABFSS works directly):
# - Spark readers: spark.read.csv(), .json(), .parquet(), .text()
# - Pandas: pd.read_csv(), pd.read_parquet(), pd.read_json()
# - notebookutils.fs operations
# ===========================================================================================

def _mount_lakehouse_for_local_access(file_paths: list, table_id: int) -> tuple:
    """
    Mount a lakehouse to enable local file system access for custom file ingestion.
    
    This function extracts the lakehouse root from ABFSS paths and mounts it to a
    unique mount point based on table_id, preventing conflicts when multiple custom
    functions run in the same Spark session.
    
    Args:
        file_paths (list): List of ABFSS file paths to process. At least one path required.
                          Format: abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Files/...
        table_id (int): Unique table identifier used to create mount point name.
    
    Returns:
        tuple: (mount_point, local_mount_path, lakehouse_root)
            - mount_point (str): Mount point name (e.g., "/mount_table_123")
            - local_mount_path (str): Local file system path where lakehouse is mounted
            - lakehouse_root (str): ABFSS root of the lakehouse (for path conversion)
    
    Example:
        >>> file_paths = ["abfss://ws123@onelake.dfs.fabric.microsoft.com/lh456/Files/data/file.csv"]
        >>> mount_point, local_path, lh_root = _mount_lakehouse_for_local_access(file_paths, 100)
        >>> print(mount_point)
        '/mount_table_100'
        >>> # Later: convert paths and process files
        >>> _unmount_lakehouse(mount_point)  # Cleanup when done
    
    Note:
        Always call _unmount_lakehouse() when done to prevent mount point accumulation.
    """
    if not file_paths:
        raise ValueError("file_paths cannot be empty - at least one ABFSS path is required")
    
    # Extract lakehouse root from first file path
    # ABFSS format: abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Files/...
    first_file = file_paths[0]
    if '/Files/' not in first_file and '/Tables/' not in first_file:
        raise ValueError(
            f"Invalid ABFSS path format. Expected '/Files/' or '/Tables/' in path. Got: {first_file}"
        )
    
    # Split to get lakehouse root (everything before /Files/ or /Tables/)
    if '/Files/' in first_file:
        lakehouse_root = first_file.split('/Files/')[0]
    else:
        lakehouse_root = first_file.split('/Tables/')[0]
    
    # Create unique mount point name using table_id
    mount_point = f"/mount_table_{table_id}"
    
    # Check existing mounts to avoid duplicate mounting
    existing_mounts = [m.mountPoint for m in notebookutils.fs.mounts()]
    
    if mount_point not in existing_mounts:
        notebookutils.fs.mount(
            lakehouse_root,
            mount_point,
            {"fileCacheTimeout": 120}  # Cache timeout in seconds
        )
    
    # Get the local file system path for the mount
    local_mount_path = notebookutils.fs.getMountPath(mount_point)
    
    return mount_point, local_mount_path, lakehouse_root


def _unmount_lakehouse(mount_point: str) -> None:
    """
    Unmount a lakehouse mount point to clean up resources.
    
    Call this function after completing file processing to prevent accumulation
    of mount points during long-running Spark sessions.
    
    Args:
        mount_point (str): The mount point name to unmount (e.g., "/mount_table_123")
    
    Example:
        >>> _unmount_lakehouse("/mount_table_100")
    
    Note:
        Safe to call even if the mount point doesn't exist or the unmount fails.
        Failures are logged as warnings (not raised) so cleanup paths are never
        interrupted.
    """
    try:
        notebookutils.fs.unmount(mount_point)
    except Exception as e:
        log_and_print(f"Unmount of '{mount_point}' skipped: {e}", "warn")


def _convert_abfss_paths_to_local(file_paths: list, local_mount_path: str, lakehouse_root: str) -> list:
    """
    Convert a list of ABFSS paths to local mounted file system paths.
    
    Use this function after calling _mount_lakehouse_for_local_access() to convert
    ABFSS paths to local paths that can be used with standard Python file I/O.
    
    Args:
        file_paths (list): List of ABFSS file paths to convert
        local_mount_path (str): Local mount path from _mount_lakehouse_for_local_access()
        lakehouse_root (str): Lakehouse root from _mount_lakehouse_for_local_access()
    
    Returns:
        list: List of local file system paths corresponding to the input ABFSS paths
    
    Example:
        >>> file_paths = ["abfss://ws@onelake.../lh/Files/data/file.csv"]
        >>> mount_point, local_path, lh_root = _mount_lakehouse_for_local_access(file_paths, 100)
        >>> local_files = _convert_abfss_paths_to_local(file_paths, local_path, lh_root)
        >>> # local_files[0] is now like "/synfs/123/mount_table_100/Files/data/file.csv"
        >>> with open(local_files[0], 'r') as f:
        ...     content = f.read()
    """
    local_paths = []
    for abfss_path in file_paths:
        # Extract relative path after lakehouse root
        relative_path = abfss_path.replace(lakehouse_root, '')
        local_paths.append(local_mount_path + relative_path)
    return local_paths


def _mount_abfss_path_for_local_access(abfss_path: str, table_id: int) -> tuple:
    """
    Mount a lakehouse and resolve a single ABFSS path to a local file system path.

    This is a convenience wrapper for custom staging and other write-oriented
    scenarios where authors start with a single target ABFSS path instead of a
    list of source file paths.

    Args:
        abfss_path (str): ABFSS path to mount and resolve locally.
        table_id (int): Unique table identifier used to create mount point name.

    Returns:
        tuple: (mount_point, local_path, local_mount_path, lakehouse_root)
            - mount_point (str): Mount point name (e.g., "/mount_table_123")
            - local_path (str): Local file system path corresponding to abfss_path
            - local_mount_path (str): Root local mount path for the lakehouse
            - lakehouse_root (str): ABFSS root of the mounted lakehouse

    Example:
        >>> target_path = "abfss://ws123@onelake.dfs.fabric.microsoft.com/lh456/Files/data/output/"
        >>> mount_point, local_path, local_mount_path, lakehouse_root = _mount_abfss_path_for_local_access(target_path, 100)
        >>> with open(f"{local_path}result.json", "w", encoding="utf-8") as file_handle:
        ...     _ = file_handle.write("{}")
        >>> _unmount_lakehouse(mount_point)
    """
    mount_point, local_mount_path, lakehouse_root = _mount_lakehouse_for_local_access(
        file_paths = [abfss_path],
        table_id = table_id
    )
    local_path = _convert_abfss_paths_to_local(
        file_paths = [abfss_path],
        local_mount_path = local_mount_path,
        lakehouse_root = lakehouse_root
    )[0]
    return mount_point, local_path, local_mount_path, lakehouse_root


def parse_source_configuration(
    orchestration_metadata: dict,
    primary_config: dict,
    datastore_config: str | list,
    folder_path_from_trigger: str
) -> SourceConfig:
    """
    Extract and validate source-related configurations.
    
    This function parses all source-related settings including table identifiers,
    file paths, custom functions, and query generation logic. It handles multiple
    ingestion patterns: staging folders, wildcard paths, trigger-based paths, and
    direct table queries.
    
    Args:
        orchestration_metadata (dict): Orchestration context with table and target info
        primary_config (dict): Primary configuration dictionary
        datastore_config (str | list): Datastore configuration from Datastore_Configuration table.
                                       Either a string like "[{'Datastore_Name': 'bronze', ...}]" 
                                       or an already-parsed list.
        folder_path_from_trigger (str): Optional folder path from event trigger
    
    Returns:
        SourceConfig: Parsed source configuration containing:
            - table_id (int): Unique table identifier
            - target_datastore_name (str): Target lakehouse name (lowercase)
            - target_datastore_medallion_name (str): Medallion layer name
            - input_delta_table_external_location (str): External Delta table path
            - staging_folder_path (str): Staging folder path if specified
            - staging_folder_load_mode (str): How to select staged child folders for batch reads
            - wildcard_folder_path (str): Wildcard folder path if specified
            - source_path (str): Resolved source path (first non-empty)
            - using_source_folder_path (bool): Whether using folder-based ingestion
            - custom_table_ingestion_function (str): Custom function name for table/SQL sources
            - custom_table_ingestion_function_notebook (str): Custom notebook name for table/SQL sources
            - custom_file_ingestion_function (str): Custom function name for file sources
            - custom_file_ingestion_function_notebook (str): Custom notebook name for file sources
            - source_table_name (str): Source table name (may be modified)
            - watermark_table_name (str): Table for watermark tracking
            - source_datastore_name (str): Source datastore name
            - source_workspace_name (str): Source workspace name (if applicable)
            - source_query (str): Generated SELECT query (empty for file ingestion)
            - connection_id (str): External connection identifier
            - source_category (str): Source type (lowercase)
            - file_extension (str): File format (lowercase)
            - expected_schema (str): Expected schema definition for contract validation
    
    Example:
        >>> config = parse_source_configuration(
        ...     orchestration_metadata={'Table_ID': 123, 'Target_Datastore': 'Silver'},
        ...     primary_config={'source_details_table_name': 'bronze.dbo.sales'},
        ...     datastore_config="[{'Datastore_Name': 'bronze', 'Workspace_Name': 'Dev'}]",
        ...     folder_path_from_trigger=''
        ... )
        >>> config.table_id
        123
    """
    table_id = orchestration_metadata.get('Table_ID')

    # Runtime conflict detection - fail loudly instead of silently ignoring configs
    _validate_conflicting_source_configs(primary_config, table_id)

    input_delta_table_external_location = primary_config.get("source_details_input_delta_table_external_location", '').strip()
    
    # File path configurations
    staging_folder_path = primary_config.get("source_details_staging_folder_path")
    staging_folder_load_mode = _resolve_staging_folder_load_mode(primary_config, staging_folder_path)
    wildcard_folder_path = primary_config.get("source_details_wildcard_folder_path")
    source_path = staging_folder_path or wildcard_folder_path or folder_path_from_trigger
    using_source_folder_path = bool(source_path)
    
    # Custom function configurations - now explicit for table vs file vs external sources
    custom_table_ingestion_function = primary_config.get("source_details_custom_table_ingestion_function", '')
    custom_table_ingestion_function_notebook = primary_config.get("source_details_custom_table_ingestion_function_notebook", '')
    custom_file_ingestion_function = primary_config.get("source_details_custom_file_ingestion_function", '')
    custom_file_ingestion_function_notebook = primary_config.get("source_details_custom_file_ingestion_function_notebook", '')
    custom_source_function = primary_config.get("source_details_custom_source_function", '')
    custom_source_function_notebook = primary_config.get("source_details_custom_source_function_notebook", '')
    
    # Determine source datastore and build query based on ingestion pattern
    overrides = []
    source_workspace_name = None
    if staging_folder_path:
        source_datastore_name = primary_config.get("source_details_staging_lakehouse_name").strip().lower()
        source_query = ""
        # Staging uses folder timestamps, not table watermarks
        watermark_table_name = ""
        if primary_config.get("watermark_details_table_name", "").strip():
            overrides.append(
                "Cleared watermark_details.table_name because source_details.staging_folder_path uses folder timestamps, not table watermark extraction."
            )
        overrides.append(
            "Set source_query='' because source is file/staging ingestion, not table SQL ingestion."
        )
        if not primary_config.get("source_details_staging_folder_load_mode", "").strip():
            if staging_folder_load_mode == "all_unprocessed_folders":
                overrides.append(
                    "Defaulted source_details.staging_folder_load_mode='all_unprocessed_folders' because watermark_details.strategy uses a source cursor."
                )
            else:
                overrides.append(
                    "Defaulted source_details.staging_folder_load_mode='latest_snapshot' because watermark_details.strategy does not use a source cursor."
                )
    elif using_source_folder_path or custom_table_ingestion_function or custom_file_ingestion_function:
        source_datastore_name = primary_config.get("source_details_datastore_name", "bronze").strip().lower()
        source_query = ""
        watermark_table_name = primary_config.get("watermark_details_table_name", "")
        overrides.append(
            "Set source_query='' because source is file/custom ingestion path, not table SQL ingestion."
        )
    elif custom_source_function:
        # Custom external source function: no table query, no folder path
        source_datastore_name = primary_config.get("source_details_datastore_name", "").strip().lower()
        source_query = ""
        watermark_table_name = ""
        overrides.append(
            "Set source_query='' and watermark_table_name='' because source is custom_source_function — "
            "the function manages its own data retrieval and watermark tracking."
        )
    else:
        # Table-based ingestion: parse qualified table name and build query
        source_table_name = primary_config.get("source_details_table_name")
        source_datastore_name = source_table_name.split('.')[0].strip().lower()
        source_workspace_name = _get_datastore_config(datastore_config, source_datastore_name, 'Workspace_Name')
        source_query = f"SELECT * FROM `{source_workspace_name}`.{source_table_name.lower()}"
        watermark_table_name = primary_config.get("watermark_details_table_name", source_table_name)

    _log_config_overrides(scope="parse_source_configuration", overrides=overrides)
    
    # Resolve watermark table to fully-qualified name (shared logic for all patterns)
    if watermark_table_name:
        watermark_datastore_name = watermark_table_name.split('.')[0].strip().lower()
        watermark_workspace_name = _get_datastore_config(datastore_config, watermark_datastore_name, 'Workspace_Name')
        watermark_full_table_name = f"`{watermark_workspace_name}`.{watermark_table_name.lower()}"
    else:
        watermark_full_table_name = ""
    
    # External source configuration
    connection_id = primary_config.get("source_details_connection_id", '').strip()
    source_category = primary_config.get("source_details_source", "").lower()
    file_extension = primary_config.get("source_details_file_extension", "").lower()
    
    # Expected schema for schema contract validation (works for both files and Delta tables)
    expected_schema = primary_config.get("source_details_schema", "").strip()
    
    return SourceConfig(
        table_id=table_id,
        input_delta_table_external_location=input_delta_table_external_location,
        staging_folder_path=staging_folder_path,
        staging_folder_load_mode=staging_folder_load_mode,
        wildcard_folder_path=wildcard_folder_path,
        source_path=source_path,
        using_source_folder_path=using_source_folder_path,
        custom_table_ingestion_function=custom_table_ingestion_function,
        custom_table_ingestion_function_notebook=custom_table_ingestion_function_notebook,
        custom_file_ingestion_function=custom_file_ingestion_function,
        custom_file_ingestion_function_notebook=custom_file_ingestion_function_notebook,
        custom_source_function=custom_source_function,
        custom_source_function_notebook=custom_source_function_notebook,
        watermark_table_name=watermark_full_table_name,
        source_datastore_name=source_datastore_name,
        source_workspace_name=source_workspace_name,
        source_query=source_query,
        connection_id=connection_id,
        source_category=source_category,
        file_extension=file_extension,
        expected_schema=expected_schema
    )

# ===========================================================================================
# LAKEHOUSE-SPECIFIC CONFIGURATION
# ===========================================================================================
# Set layer-specific defaults and extract workspace/lakehouse identifiers for each medallion layer.

SUPPORTED_MEDALLION_LAYERS = ('bronze', 'silver', 'gold')

def determine_medallion_layer_defaults(target_datastore_medallion_name: str) -> MedallionLayerDefaults:
    """
    Set layer-specific defaults based on medallion architecture layer.
    
    Each layer (Bronze, Silver, Gold) has different optimization characteristics
    and default behaviors tailored to its role in the data pipeline.
    
    Args:
        target_datastore_medallion_name (str): Medallion layer name ('bronze', 'silver', 'gold')
    
    Returns:
        MedallionLayerDefaults: Layer-specific defaults containing:
            - default_watermark_column_name (str): Default column for incremental processing
            - default_merge_type (str): Default merge strategy for the layer
    
    Layer Characteristics:
        - Bronze: Raw ingestion, append-only by default, no watermark
        - Silver: Curated data, merge by default, modified_datetime watermark
        - Gold: Analytics-ready, merge by default, modified_datetime watermark
    
    Example:
        >>> defaults = determine_medallion_layer_defaults('silver')
        >>> defaults.default_merge_type
        'merge'
        >>> defaults.default_watermark_column_name
        'delta__modified_datetime'
    """
    if target_datastore_medallion_name == 'gold':
        # Gold layer defaults - optimized for analytics workloads
        result = MedallionLayerDefaults(
            default_watermark_column_name="delta__modified_datetime",
            default_merge_type="merge"
        )
    elif target_datastore_medallion_name == 'silver':
        # Silver layer defaults - balanced for transformation and storage
        result = MedallionLayerDefaults(
            default_watermark_column_name="delta__modified_datetime",
            default_merge_type="merge"
        )
    elif target_datastore_medallion_name == 'bronze':
        # Bronze layer defaults - optimized for fast ingestion
        result = MedallionLayerDefaults(
            default_watermark_column_name="",
            default_merge_type="append"
        )
    else:
        allowed_layers = ", ".join(f"'{layer}'" for layer in SUPPORTED_MEDALLION_LAYERS)
        error_message = (
            f"Unsupported target_datastore_medallion_name '{target_datastore_medallion_name}'. "
            f"Expected one of [{allowed_layers}]."
        )
        log_and_print(error_message)
        raise ValueError(error_message)
    
    medallion_log_info = f"The target_datastore_medallion_name is {target_datastore_medallion_name}. The default merge type is now set to '{result.default_merge_type}' and default watermark column is now set to '{result.default_watermark_column_name}'."
    log_and_print(medallion_log_info)
    
    return result

# ===========================================================================================
# TARGET CONFIGURATION EXTRACTION
# ===========================================================================================
# Define target destination details including lakehouse, table/file paths, and storage options.

def _parse_two_part_table_name(value: str, field_name: str) -> tuple[str, str]:
    """Parse a lakehouse table identifier that must use schema.table format."""
    parts = [part.strip() for part in str(value or '').split('.')]
    if len(parts) != 2 or not all(parts):
        raise ValueError(
            f"{field_name} must use 2-part schema.table format for lakehouse table targets; got '{value}'."
        )
    return parts[0].lower(), parts[1].lower()

def parse_target_configuration(
    orchestration_metadata: dict,
    primary_config: dict,
    datastore_config: str | list
) -> TargetConfig:
    """
    Parse target destination configuration including paths and settings.
    
    This function determines whether the target is a table or file, constructs
    the appropriate ABFSS paths, and extracts related configuration settings.
    
    Args:
        orchestration_metadata (dict): Orchestration context
        primary_config (dict): Primary configuration dictionary
        datastore_config (str | list): Datastore configuration from Datastore_Configuration table.
                                       Either a string like "[{'Datastore_Name': 'bronze', ...}]" 
                                       or an already-parsed list.
    
    Returns:
        TargetConfig: Parsed target configuration containing:
            - target_workspace_name (str): Fabric workspace name
            - target_datastore_id (str): Datastore GUID
            - target_datastore_workspace_id (str): Workspace GUID
            - target_table_name (str): Table name (empty for file targets)
            - target_folder_path (str): Folder path (only for file targets)
            - target_abfss_path (str): Full ABFSS path to target
            - default_merge_type (str): Updated merge type (may be 'output_file')
            - output_external_location (str): External storage path
            - enforce_not_null (bool): Whether to enforce NOT NULL constraints
            - schema_name_for_path (str): Schema name for path construction
            - table_name_for_path (str): Table name for path construction
    
    Example:
        >>> config = parse_target_configuration(
        ...     orchestration_metadata={'Target_Entity': 'dbo.customers', 'Target_Datastore': 'silver'},
        ...     primary_config={},
        ...     datastore_config="[{'Datastore_Name': 'silver', 'Workspace_Name': 'Analytics', 'Medallion_Layer': 'silver', 'Datastore_ID': 'abc', 'Workspace_ID': 'xyz'}]"
        ... )
        >>> config.target_workspace_name
        'Analytics'
    """
    target_datastore_name = orchestration_metadata.get('Target_Datastore').strip().lower()
    
    target_datastore_medallion_name = _get_datastore_config(datastore_config, target_datastore_name, 'Medallion_Layer').lower().strip()

    # Determine layer-specific defaults
    layer_defaults = determine_medallion_layer_defaults(target_datastore_medallion_name)

    default_watermark_column_name = layer_defaults.default_watermark_column_name
    default_merge_type = layer_defaults.default_merge_type
    
    target_workspace_name = _get_datastore_config(datastore_config, target_datastore_name, 'Workspace_Name')
    target_datastore_id = _get_datastore_config(datastore_config, target_datastore_name, 'Datastore_ID')
    target_datastore_workspace_id = _get_datastore_config(datastore_config, target_datastore_name, 'Workspace_ID')
    
    target_entity = str(orchestration_metadata.get('Target_Entity') or '').strip()
    if not target_entity:
        raise ValueError("Target_Entity is required and cannot be empty.")
    
    # Determine if target is a table or file based on path pattern
    overrides = []
    if '/' in target_entity:
        # File-based output
        log_and_print(f"Target_Entity has a / and is assumed to be a folder path. The default merge type is now set to 'output_file'.")
        full_target_table_name = ""
        target_folder_path = target_entity
        target_abfss_path = f"abfss://{target_datastore_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_datastore_id}/Files/{target_folder_path}"
        default_merge_type = "output_file"
        overrides.append(
            "Set default_merge_type='output_file' because Target_Entity is a file/folder path target."
        )
        schema_name_for_path = None
        table_name_for_path = None
        target_quarantined_abfss_path = None
    else:
        # Table-based output
        target_table_name = target_entity
        target_folder_path = None
        schema_name_for_path, table_name_for_path = _parse_two_part_table_name(
            value = target_table_name,
            field_name = "Target_Entity"
        )
        target_abfss_path = f"abfss://{target_datastore_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_datastore_id}/Tables/{schema_name_for_path}/{table_name_for_path}"
        quarantine_table_name = primary_config.get("target_details_quarantine_table_name", f"{target_table_name}_quarantined")
        quarantine_schema_name_for_path, quarantine_table_name_for_path = _parse_two_part_table_name(
            value = quarantine_table_name,
            field_name = "target_details_quarantine_table_name"
        )
        target_quarantined_abfss_path = f"abfss://{target_datastore_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_datastore_id}/Tables/{quarantine_schema_name_for_path}/{quarantine_table_name_for_path}"

        full_target_table_name = f"`{target_workspace_name}`.{target_datastore_name.lower()}.{target_table_name.lower()}"

    # Additional target settings
    output_external_location = primary_config.get("target_details_external_location", '').strip()
    enforce_not_null = primary_config.get("target_details_enforce_not_null", "false").strip().lower() == "true"        
    log_and_print(f"Target ABFSS Path: {target_abfss_path}")

    merge_type = primary_config.get("target_details_merge_type", "").strip().lower()

    lakehouse_table_output = bool(full_target_table_name) and merge_type != 'warehouse_spark_connector'

    replace_where_column = primary_config.get("target_details_replace_where_column", "").strip()

    # Rule 86: replace_where_column set but merge_type is not 'replace_where'
    # Check raw values before defaults are applied — if merge_type is empty (user didn't set it),
    # the layer default will never be 'replace_where', so replace_where_column will be silently ignored.
    if replace_where_column and merge_type != 'replace_where':
        error_msg = (
            f"Conflicting config — 'replace_where_column' is set to '{replace_where_column}' "
            f"but merge_type is '{merge_type or '(not set — will use layer default)'}', not 'replace_where'. "
            f"The replace_where_column is only used by _handle_replace_where() and will be silently ignored. "
            f"Either change merge_type to 'replace_where' or remove replace_where_column. "
            f"Run validate_metadata_sql.py to catch this before deployment."
        )
        raise ValueError(error_msg)
    warehouse_write_mode = primary_config.get('target_details_warehouse_write_mode', '').lower().strip()
    target_excel_sheet_name = primary_config.get('target_details_sheet_name', 'Sheet1').strip()
    target_output_delimiter = primary_config.get('target_details_delimiter', '').strip()

    _log_config_overrides(scope="parse_target_configuration", overrides=overrides)

    return TargetConfig(
        target_workspace_name=target_workspace_name,
        target_datastore_id=target_datastore_id,
        target_datastore_workspace_id=target_datastore_workspace_id,
        target_table_name=full_target_table_name,
        target_folder_path=target_folder_path,
        target_abfss_path=target_abfss_path,
        default_merge_type=default_merge_type,
        output_external_location=output_external_location,
        enforce_not_null=enforce_not_null,
        schema_name_for_path=schema_name_for_path,
        table_name_for_path=table_name_for_path,
        target_quarantined_abfss_path=target_quarantined_abfss_path,
        lakehouse_table_output=lakehouse_table_output,
        replace_where_column=replace_where_column,
        target_datastore_name=target_datastore_name,
        target_datastore_medallion_name=target_datastore_medallion_name,
        warehouse_write_mode=warehouse_write_mode,
        target_excel_sheet_name=target_excel_sheet_name,
        target_output_delimiter=target_output_delimiter,
        default_watermark_column_name=default_watermark_column_name
    )

# ===========================================================================================
# FILE INGESTION PATHS
# ===========================================================================================
# Configure paths for source files and staging areas used during file-based ingestion.

def parse_file_ingestion_paths(
    datastore_config: str | list,
    source_datastore_name: str,
    target_datastore_workspace_id: str,
    target_datastore_id: str,
    table_id: int,
    wildcard_folder_path: str,
    primary_config: dict
) -> FileIngestionPathsConfig:
    """
    Parse and construct paths for file-based ingestion and staging.
    
    This function builds ABFSS paths for source file locations and staging areas.
    It also determines if temporary file cleanup is needed for staged ingestion formats.
    
    Args:
        datastore_config (str | list): Datastore configuration from Datastore_Configuration table.
                                       Either a string like "[{'Datastore_Name': 'bronze', ...}]" 
                                       or an already-parsed list.
        source_datastore_name (str): Source datastore name
        target_datastore_workspace_id (str): Target workspace GUID
        target_datastore_id (str): Target datastore GUID
        table_id (int): Unique table identifier
        wildcard_folder_path (str): Wildcard folder path if specified
        primary_config (dict): Primary configuration (will be mutated with file_staging_path)
    
    Returns:
        FileIngestionPathsConfig: Parsed file ingestion path configuration containing:
            - source_files_datastore_id (str): Source datastore GUID
            - source_files_datastore_workspace_id (str): Source workspace GUID
            - source_files_lakehouse_path (str): ABFSS path to source files
            - file_staging_path (str): ABFSS path for temporary staging
            - clean_up_temporary_path (bool): Whether to cleanup staged temp files
    
    Side Effects:
        Mutates primary_config dict by adding 'file_staging_path' key
    
    Example:
        >>> paths = parse_file_ingestion_paths(
        ...     datastore_config="[{'Datastore_Name': 'bronze', 'Datastore_ID': 'lh-123', 'Workspace_ID': 'ws-456'}]",
        ...     source_datastore_name='bronze',
        ...     target_datastore_workspace_id='ws-456',
        ...     target_datastore_id='lh-789',
        ...     table_id=100,
        ...     wildcard_folder_path='',
        ...     primary_config={}
        ... )
        >>> paths.source_files_datastore_id
        'lh-123'
    """
    # source_datastore_name is intentionally empty for custom_source_function.
    # In that path, no file-based source lakehouse is read, so skip datastore lookup.
    if source_datastore_name:
        source_files_datastore_id = _get_datastore_config(datastore_config, source_datastore_name, 'Datastore_ID')
        source_files_datastore_workspace_id = _get_datastore_config(datastore_config, source_datastore_name, 'Workspace_ID')
        source_files_lakehouse_path = f"abfss://{source_files_datastore_workspace_id}@onelake.dfs.fabric.microsoft.com/{source_files_datastore_id}/Files"
    else:
        source_files_datastore_id = ''
        source_files_datastore_workspace_id = ''
        source_files_lakehouse_path = ''
    
    # Create staging path for temporary file processing
    file_staging_path = f"abfss://{target_datastore_workspace_id}@onelake.dfs.fabric.microsoft.com/{target_datastore_id}/Files/staging_for_file_ingestion/{table_id}"
    
    # Mutate primary_config to add staging path (maintains backward compatibility)
    primary_config['file_staging_path'] = file_staging_path
    
    # Native Spark readers and direct Excel conversion do not create staged files.
    clean_up_temporary_path = False
    
    return FileIngestionPathsConfig(
        source_files_datastore_id=source_files_datastore_id,
        source_files_datastore_workspace_id=source_files_datastore_workspace_id,
        source_files_lakehouse_path=source_files_lakehouse_path,
        file_staging_path=file_staging_path,
        clean_up_temporary_path=clean_up_temporary_path
    )

# ===========================================================================================
# LINEAGE INFORMATION EXTRACTION
# ===========================================================================================
# Derive source and target lineage details (medallion layer and system type) for logging.

def extract_lineage_information(
    source_config: SourceConfig,
    target_config: TargetConfig,
    datastore_config: str | list,
    merge_type: str
) -> LineageInfo:
    """
    Extract lineage information for logging and tracking.
    
    This function derives the source and target medallion layers and system types
    based on the configuration. This information is used for:
    - Data lineage tracking in logs
    - Understanding data flow through medallion architecture
    - Identifying external vs internal data sources
    
    Args:
        source_config (SourceConfig): Source configuration from parse_source_configuration()
        target_config (TargetConfig): Target configuration from parse_target_configuration()
        datastore_config (str | list): Datastore configuration from Datastore_Configuration table.
                                       Either a string like "[{'Datastore_Name': 'bronze', ...}]" 
                                       or an already-parsed list.
        merge_type (str): The merge type being used (affects target type determination)
    
    Returns:
        LineageInfo: Lineage information containing:
            - source_medallion_layer (str): Source medallion layer (Bronze/Silver/Gold/External)
            - source_type (str): Source system type
            - target_medallion_layer (str): Target medallion layer (Bronze/Silver/Gold)
            - target_type (str): Target system type (Fabric Lakehouse or Fabric Warehouse)
    
    Source Type Logic:
        - If custom_source_function is set → Custom Source Function
        - If using_source_folder_path is True → Fabric Lakehouse (Files)
        - Otherwise → Fabric Lakehouse (Tables)
    
    Source Medallion Layer Logic:
        - If custom_source_function is set → External
        - Else lookup from datastore_config based on source_datastore_name
        - source_datastore_name is already set to staging_lakehouse_name for external DB ingestion
    
    Target Type Logic:
        - If merge_type == 'warehouse_spark_connector' → Fabric Warehouse
        - Else → Fabric Lakehouse
    
    Example:
        >>> lineage = extract_lineage_information(
        ...     source_config=SourceConfig(source_datastore_name='bronze', using_source_folder_path=True),
        ...     target_config=TargetConfig(target_datastore_medallion_name='silver'),
        ...     datastore_config="[{'Datastore_Name': 'bronze', 'Medallion_Layer': 'bronze'}]",
        ...     merge_type='merge'
        ... )
        >>> lineage.source_medallion_layer
        'Bronze'
        >>> lineage.source_type
        'Fabric Lakehouse (Files)'
    """
    source_datastore_name = source_config.get('source_datastore_name', '').strip().lower()
    staging_folder_path = source_config.get('staging_folder_path', '')
    using_source_folder_path = source_config.get('using_source_folder_path', False)
    custom_source_function = source_config.get('custom_source_function', '').strip()
    
    # Determine Source Type and Medallion Layer
    # Note: External database sources are handled by pipelines before this notebook runs,
    # so by the time we're here, sources are either Delta tables or Files in a Fabric Lakehouse
    
    if custom_source_function:
        source_type = 'Custom Source Function'
        source_medallion_layer = 'External'
    elif staging_folder_path:
        # External DB staged to lakehouse Files - source_datastore_name is already set to staging_lakehouse_name
        source_type = 'Fabric Lakehouse (Files)'
        source_medallion_layer = _get_datastore_config(datastore_config, source_datastore_name, 'Medallion_Layer').strip().title()
    elif using_source_folder_path:
        # File-based ingestion from lakehouse Files section - source_datastore_name is set to datastore_name
        source_type = 'Fabric Lakehouse (Files)'
        source_medallion_layer = _get_datastore_config(datastore_config, source_datastore_name, 'Medallion_Layer').strip().title()
    else:
        # Delta table source (Fabric Lakehouse Tables section)
        source_type = 'Fabric Lakehouse (Tables)'
        source_medallion_layer = _get_datastore_config(datastore_config, source_datastore_name, 'Medallion_Layer').strip().title()

    # Determine Target Medallion Layer (already parsed in target_config)
    target_medallion_layer = target_config.get('target_datastore_medallion_name', '').strip().title()
    
    # Determine Target Type
    if merge_type == 'warehouse_spark_connector':
        target_type = 'Fabric Warehouse'
    else:
        target_type = 'Fabric Lakehouse'
    
    lineage_info = LineageInfo(
        source_medallion_layer=source_medallion_layer,
        source_type=source_type,
        target_medallion_layer=target_medallion_layer,
        target_type=target_type
    )
    
    log_and_print(f"Lineage Information - Source: {source_medallion_layer} ({source_type}) → Target: {target_medallion_layer} ({target_type})")
    
    return lineage_info


def extract_custom_staging_lineage_information(
    staging_lakehouse_name: str,
    datastore_config: str | list,
) -> LineageInfo:
    """Return staging-phase lineage for custom staging functions."""
    target_medallion_layer = _get_datastore_config(
        datastore_config,
        staging_lakehouse_name,
        'Medallion_Layer'
    ).strip().title()

    lineage_info = LineageInfo(
        source_medallion_layer='External',
        source_type='Custom Staging Function',
        target_medallion_layer=target_medallion_layer,
        target_type='Fabric Lakehouse'
    )

    log_and_print(
        f"Custom Staging Lineage - Source: {lineage_info.source_medallion_layer} "
        f"({lineage_info.source_type}) → Target: {lineage_info.target_medallion_layer} "
        f"({lineage_info.target_type})"
    )

    return lineage_info

# ===========================================================================================
# WATERMARK AND INCREMENTAL PROCESSING CONFIGURATION
# ===========================================================================================

def parse_watermark_configuration(
    primary_config: dict,
    default_merge_type: str,
    default_watermark_column_name: str,
    staging_folder_path: str,
    using_source_folder_path: bool,
    watermark_value: str
) -> WatermarkConfig:
    """
    Parse watermark and incremental processing configuration.
    
    This function handles complex logic for watermark columns, merge strategies,
    and incremental processing settings. It considers soft deletes, batch processing,
    Change Data Feed (CDF), and layer-specific defaults.
    
    Args:
        primary_config (dict): Primary configuration dictionary
        default_merge_type (str): Default merge type from medallion layer
        default_watermark_column_name (str): Default watermark column from layer
        staging_folder_path (str): Staging folder path if specified
        using_source_folder_path (bool): Whether using folder-based ingestion
        watermark_value (str): Current watermark value
    
    Returns:
        WatermarkConfig: Watermark and merge configuration containing:
            - column_to_mark_source_data_deletion (str): Soft delete tracking column
            - delete_rows_with_value (str): Value indicating deletion
            - merge_type (str): Final merge type (may be overridden)
            - default_merge_type (str): Updated default (may be 'merge_and_delete')
            - merge_in_batches_with_columns (list): Columns for batch processing
            - strategy (str): Explicit watermark strategy
            - watermark_column_data_type (str): Watermark column data type
            - watermark_column_name (str): Watermark column name
            - use_watermark_column (bool): Whether to use column watermark filtering internally
            - watermark_value (str): Updated watermark value (may be reset)
    
    Example:
        >>> config = parse_watermark_configuration(
        ...     primary_config={'watermark_details_strategy': 'column', 'watermark_details_column_name': 'modified_date'},
        ...     default_merge_type='merge',
        ...     default_watermark_column_name='delta__modified_datetime',
        ...     staging_folder_path='',
        ...     using_source_folder_path=False,
        ...     watermark_value='2024-01-01'
        ... )
        >>> config.use_watermark_column
        True
    """
    # Runtime conflict detection - fail loudly instead of silently ignoring configs
    _validate_conflicting_watermark_configs(primary_config, table_id=None)

    overrides = []
    strategy = _resolve_watermark_strategy(primary_config)
    cdf_enabled = strategy == "change_data_feed"
    
    # Soft delete tracking
    column_to_mark_source_data_deletion = primary_config.get(
        "target_details_column_to_mark_source_data_deletion", ""
    ).strip()
    delete_rows_with_value = primary_config.get(
        "target_details_delete_rows_with_value", ""
    ).strip()
    
    # Override soft delete settings if Change Data Feed is enabled
    if cdf_enabled:
        # CDF provides _change_type column with 'delete' value for deleted records
        column_to_mark_source_data_deletion = "_change_type"
        delete_rows_with_value = "delete"
        log_and_print("Change Data Feed is enabled. Soft delete tracking set to use _change_type = 'delete' for handling deleted records.")
        overrides.append(
            "Overrode soft delete config to (_change_type='delete') because watermark_details.strategy='change_data_feed'."
        )
    
    # Override merge type if soft deletes are configured
    if delete_rows_with_value:
        default_merge_type = "merge_and_delete"
        log_and_print(f"target_details_delete_rows_with_value has a provided value from the metadata. The default value for merge type is now set to 'merge_and_delete'.")
        overrides.append(
            "Set default_merge_type='merge_and_delete' because delete_rows_with_value is configured."
        )
    
    # Get final merge type
    merge_type = primary_config.get("target_details_merge_type", default_merge_type).strip().lower()
    
    # Parse batch merge columns
    merge_in_batches_with_columns = primary_config.get("target_details_merge_in_batches_with_columns", "")
    merge_in_batches_with_columns = [col.strip() for col in merge_in_batches_with_columns.split(",") if col.strip()]
    
    # Watermark column configuration
    watermark_column_data_type = primary_config.get("watermark_details_data_type", "datetime").strip()
    watermark_column_name = primary_config.get("watermark_details_column_name", "").strip()

    if strategy == "column" and not watermark_column_name and not staging_folder_path:
        raise ValueError(
            "watermark_details.strategy='column' requires watermark_details.column_name. "
            "Do not rely on a layer default watermark column."
        )

    if strategy in ("none", "file_modified_time") and watermark_column_name:
        raise ValueError(
            f"watermark_details.strategy='{strategy}' cannot be combined with watermark_details.column_name. "
            "Use strategy='column' or remove the watermark column metadata."
        )
    
    runtime_watermark_strategy = "file_modified_time" if staging_folder_path else strategy

    # Override watermark for staging folder ingestion
    if staging_folder_path:
        watermark_column_name = ""
        watermark_column_data_type = ""
        log_and_print(
            "Setting watermark_details_column_name and watermark_column_data_type to '' because "
            "staging_folder_path is being used. Staged folder timestamps will be used for Batch watermarking."
        )
        overrides.append(
            "Cleared watermark column name/data_type for Batch because staging_folder_path uses folder timestamps."
        )
    
    # Override watermark settings if Change Data Feed is enabled
    if cdf_enabled:
        # CDF uses _commit_timestamp for watermarking automatically
        watermark_column_name = ""
        watermark_column_data_type = ""
        log_and_print("Change Data Feed is enabled. Watermark column settings are not used - CDF uses _commit_version for incremental processing.")
        overrides.append(
            "Cleared watermark column name/data_type because CDF uses Delta commit version/timestamp tracking."
        )
    
    use_watermark_column = strategy == "column" and runtime_watermark_strategy == "column"
    
    # Log the watermark setting
    if use_watermark_column:
        log_and_print(f"Watermark strategy '{strategy}' enabled. Current watermark_value: '{watermark_value}'")
    elif strategy == "change_data_feed":
        log_and_print(f"Watermark strategy 'change_data_feed' enabled. Current watermark_value: '{watermark_value}'")
    elif runtime_watermark_strategy == "file_modified_time":
        timestamp_source = (
            "staged folder timestamps for Batch checkpointing"
            if staging_folder_path
            else "source file/folder modified timestamps"
        )
        log_and_print(
            f"Watermark strategy '{strategy}' uses {timestamp_source}. Current watermark_value: '{watermark_value}'"
        )
    elif strategy == "custom":
        log_and_print(f"Watermark strategy 'custom' enabled. Passing watermark_value through unchanged: '{watermark_value}'")
    else:
        log_and_print(f"Watermark strategy '{strategy}' does not use a watermark column. Clearing watermark_value.")
        watermark_value = ""
        overrides.append(
            f"Cleared runtime watermark_value because watermark_details.strategy='{strategy}'."
        )
    
    # Reset watermark for file-based ingestion with default 1900-01-01 value
    # Empty string works for both base path (skips int comparison) and wildcard path (skips timestamp filter)
    if using_source_folder_path and watermark_value and '1900-01-01' in watermark_value:
        watermark_value = ""
        log_and_print(f"File-based ingestion with default 1900-01-01 watermark detected. Clearing watermark_value to process all files/folders.")
        overrides.append(
            "Cleared runtime watermark_value for file ingestion because watermark had default 1900-01-01 sentinel value."
        )

    _log_config_overrides(scope="parse_watermark_configuration", overrides=overrides)
    
    return WatermarkConfig(
        column_to_mark_source_data_deletion=column_to_mark_source_data_deletion,
        delete_rows_with_value=delete_rows_with_value,
        merge_type=merge_type,
        default_merge_type=default_merge_type,
        merge_in_batches_with_columns=merge_in_batches_with_columns,
        strategy=strategy,
        watermark_column_data_type=watermark_column_data_type,
        watermark_column_name=watermark_column_name,
        use_watermark_column=use_watermark_column,
        watermark_value=watermark_value
    )

# ===========================================================================================
# ADVANCED PROCESSING CONFIGURATION
# ===========================================================================================

def parse_advanced_processing_configuration(
    primary_config: dict
) -> AdvancedProcessingConfig:
    """
    Parse advanced processing settings for data quality and schema management.
    
    This function extracts configuration for:
    - Liquid clustering columns for performance optimization
    - Schema evolution failure settings
    - Duplicate primary key handling
    - Column name standardization settings (independent boolean flags)
    
    Args:
        primary_config (dict): Primary configuration dictionary
    
    Returns:
        AdvancedProcessingConfig: Advanced processing configuration containing:
            - liquid_clustering_columns (list): Columns for liquid clustering
            - fail_on_new_schema (bool): Fail when new columns detected
            - fail_on_column_data_type_change (bool): Fail on type changes
            - if_duplicate_primary_keys (str): Action for duplicate keys
            - trim (bool): Trim leading/trailing whitespace from column names
            - apply_case (str): Convert column names case ('lower', 'upper', 'title')
            - replace_non_alphanumeric_with_underscore (bool): Replace non-alphanumeric characters with underscores in column names
            - regex_find (str): Regex pattern to find in column names
            - regex_replace (str): Replacement value for regex matches
            - exact_find (str): Comma-separated exact strings to find
            - exact_replace (str): Single replacement value for exact matches
            - trim_data_in_string_columns (str): Comma-separated list of columns to trim whitespace, or "*" for all string columns
            - replace_blank_with_null_in_string_columns (str): Comma-separated list of columns to replace blanks with null, or "*" for all string columns
    """
    # Liquid clustering for performance
    liquid_clustering_columns = primary_config.get("target_details_liquid_clustering_columns", "")
    
    liquid_clustering_columns = [col.strip() for col in liquid_clustering_columns.split(",") if col.strip()]
    
    # Schema evolution settings
    fail_on_new_schema = primary_config.get(
        "target_details_fail_on_new_schema", "false"
    ).strip().lower() == "true"
    
    fail_on_column_data_type_change = primary_config.get(
        "target_details_fail_on_column_data_type_change", "false"
    ).strip().lower() == "true"
    
    # Duplicate primary key handling
    if_duplicate_primary_keys = primary_config.get(
        "target_details_if_duplicate_primary_keys", "fail"
    ).strip().lower()

    # Column name standardization - no magic defaults, user must explicitly configure
    trim_column_names = primary_config.get(
        "column_cleansing_trim", "false"
    ).strip().lower() == 'true'
    
    apply_case_column_names = primary_config.get(
        "column_cleansing_apply_case", ""
    ).strip().lower()
    
    replace_non_alphanumeric_chars_with_underscore_in_column_names = primary_config.get(
        "column_cleansing_replace_non_alphanumeric_with_underscore", "false"
    ).strip().lower() == 'true'
    
    # Regex patterns: Users provide standard regex syntax in SQL metadata (e.g., '\d+', '[0-9]+$')
    # No special escaping needed - SQL string literals preserve backslashes correctly
    regex_find_in_column_names = primary_config.get(
        "column_cleansing_regex_find", ""
    ).strip()
    
    regex_replace_in_column_names = primary_config.get(
        "column_cleansing_regex_replace", ""
    ).strip()
    
    exact_find_in_column_names = primary_config.get(
        "column_cleansing_exact_find", ""
    ).strip()
    
    exact_replace_in_column_names = primary_config.get(
        "column_cleansing_exact_replace", ""
    ).strip()

    # Data cleansing - no magic defaults, user must explicitly configure
    trim_data_in_string_columns = primary_config.get("data_cleansing_trim_data_in_string_columns", "").strip()
    replace_blank_with_null_in_string_columns = primary_config.get("data_cleansing_replace_blank_with_null_in_string_columns", "").strip()

    return AdvancedProcessingConfig(
        liquid_clustering_columns=liquid_clustering_columns,
        fail_on_new_schema=fail_on_new_schema,
        fail_on_column_data_type_change=fail_on_column_data_type_change,
        if_duplicate_primary_keys=if_duplicate_primary_keys,
        trim_column_names=trim_column_names,
        apply_case_column_names=apply_case_column_names,
        replace_non_alphanumeric_chars_with_underscore_in_column_names=replace_non_alphanumeric_chars_with_underscore_in_column_names,
        regex_find_in_column_names=regex_find_in_column_names,
        regex_replace_in_column_names=regex_replace_in_column_names,
        exact_find_in_column_names=exact_find_in_column_names,
        exact_replace_in_column_names=exact_replace_in_column_names,
        trim_data_in_string_columns=trim_data_in_string_columns,
        replace_blank_with_null_in_string_columns=replace_blank_with_null_in_string_columns
    )
    
# ===========================================================================================
# PRIMARY KEY CONFIGURATION
# ===========================================================================================

def parse_primary_key_configuration(orchestration_metadata: dict, primary_config: dict = None) -> PrimaryKeyConfig:
    """
    Parse primary key configuration from orchestration metadata.
    
    Extracts and processes the primary key column names, handling
    comma-separated lists and whitespace trimming.  When primary_config is
    provided, also validates that if_duplicate_primary_keys isn't explicitly
    set without any Primary_Keys defined (Rule 89).
    
    Args:
        orchestration_metadata (dict): Orchestration metadata dictionary
        primary_config (dict, optional): Primary configuration dictionary.
            When provided, enables Rule 89 validation against raw values.
    
    Returns:
        PrimaryKeyConfig: Primary key configuration containing:
            - primary_keys (list): Parsed list of primary key column names
    
    Example:
        >>> config = parse_primary_key_configuration(
        ...     orchestration_metadata={'Primary_Keys': 'id, customer_id, order_id'},
        ...     primary_config={}
        ... )
        >>> config.primary_keys
        ['id', 'customer_id', 'order_id']
    """
    primary_keys_raw = orchestration_metadata.get("Primary_Keys")

    primary_keys = [key.strip() for key in primary_keys_raw.split(",")] if primary_keys_raw else []

    # Rule 89: if_duplicate_primary_keys explicitly set but no primary keys defined.
    # Check the RAW value (before the "fail" default is applied in parse_advanced_processing_configuration)
    # so we only flag configs the user actually set, not framework defaults.
    if primary_config is not None:
        raw_if_dup_pks = primary_config.get("target_details_if_duplicate_primary_keys", "").strip().lower()
        if raw_if_dup_pks and not primary_keys:
            error_msg = (
                f"Conflicting config — 'if_duplicate_primary_keys' is explicitly set to "
                f"'{raw_if_dup_pks}' but no Primary_Keys are defined. "
                f"The duplicate check is silently skipped because of the `if primary_keys:` guard. "
                f"Either define Primary_Keys or remove if_duplicate_primary_keys. "
                f"Run validate_metadata_sql.py to catch this before deployment."
            )
            raise ValueError(error_msg)
    
    return PrimaryKeyConfig(
        primary_keys=primary_keys
    )

# ===========================================================================================
# ADVANCED CONFIGURATION STEPS (DATA QUALITY & TRANSFORMATION)
# ===========================================================================================

def parse_advanced_configuration_steps(advanced_config: list) -> AdvancedStepsConfig:
    """
    Parse advanced configuration steps for data quality, transformation, and post-write reconciliation.
    
    Extracts and parses JSON configuration for:
    - Data quality validation steps
    - Data transformation steps
    - Post-write reconciliation steps
    
    Args:
        advanced_config (list): List of advanced configuration dictionaries
    
    Returns:
        AdvancedStepsConfig: Parsed advanced step configuration containing:
            - data_quality_steps (list): Parsed data quality configurations
            - data_transformation_steps (list): Parsed transformation configurations
            - post_write_reconciliation_steps (list): Parsed post-write reconciliation configurations
    
    Example:
        >>> config = parse_advanced_configuration_steps(
        ...     advanced_config=[
        ...         {'Configuration_Category': 'data_quality', 'advanced_settings': '{"check": "not_null"}'},
        ...         {'Configuration_Category': 'data_transformation_steps', 'advanced_settings': '{"rename": "col1"}'}
        ...     ]
        ... )
        >>> len(config.data_quality_steps)
        1
    """
    if not advanced_config:
        return AdvancedStepsConfig(
            data_quality_steps=[],
            data_transformation_steps=[],
            post_write_reconciliation_steps=[]
        )
    
    # Parse data quality steps
    data_quality_steps = [
        json.loads(values.get('advanced_settings'))
        for values in advanced_config
        if values.get('Configuration_Category') == 'data_quality'
    ]
    
    # Parse data transformation steps
    data_transformation_steps = [
        json.loads(values.get('advanced_settings'))
        for values in advanced_config
        if values.get('Configuration_Category') == 'data_transformation_steps'
    ]

    post_write_reconciliation_steps = [
        json.loads(values.get('advanced_settings'))
        for values in advanced_config
        if values.get('Configuration_Category') == 'post_write_reconciliation'
    ]
    
    return AdvancedStepsConfig(
        data_quality_steps=data_quality_steps,
        data_transformation_steps=data_transformation_steps,
        post_write_reconciliation_steps=post_write_reconciliation_steps
    )

# ===========================================================================================
# DIMENSION TABLE AND SCD CONFIGURATION
# ===========================================================================================

def parse_dimension_table_configuration(
    primary_config: dict,
    advanced_config: list
) -> DimensionConfig:
    """
    Parse dimension table configuration for slowly changing dimensions (SCD).
    
    Extracts settings for SCD Type 2 dimension tables including:
    - Surrogate key column name from create_surrogate_key transformation
    - Surrogate key logic from attach_dimension_surrogate_key transformations
    
    Args:
        primary_config (dict): Primary configuration dictionary
        advanced_config (list): List of advanced configuration dictionaries
    
    Returns:
        DimensionConfig: Dimension/SCD configuration containing:
            - dimension_table_key_column_name (str): Surrogate key column name (from create_surrogate_key)
            - source_timestamp_column_name (str): Timestamp column used to drive SCD Type 2 scd_start_date/scd_end_date values
            - fact_table_data_load (list): Parsed surrogate key configurations for fact table loading
    
    Example:
        >>> config = parse_dimension_table_configuration(
        ...     primary_config={},
        ...     advanced_config=[{
        ...         'advanced_settings': '{"Category": "create_surrogate_key", "column_name": "customer_sk", "type": "auto_increment"}'
        ...     }]
        ... )
        >>> config.dimension_table_key_column_name
        'customer_sk'
    """
    # Extract create_surrogate_key configuration from advanced_config to get the surrogate key column name
    create_surrogate_key_config = {}
    if advanced_config:
        for item in advanced_config:
            parsed = json.loads(item.get('advanced_settings', '{}'))
            if parsed.get('Category', '') == 'create_surrogate_key':
                create_surrogate_key_config = parsed
                break
    
    # Surrogate key column name (from create_surrogate_key 'column_name', fallback to default)
    dimension_table_key_column_name = create_surrogate_key_config.get(
        "column_name", "Key_SK"
    ).strip()
    
    # SCD Type 2 comparison column - the timestamp column used for scd_start_date/scd_end_date
    source_timestamp_column_name = primary_config.get(
        "target_details_source_timestamp_column_name",
        "delta__modified_datetime"
    ).strip()
    
    # Parse attach_dimension_surrogate_key logic from advanced configuration (fact table loading)
    fact_table_data_load = []
    if advanced_config:
        fact_table_data_load = [
            json.loads(values.get('advanced_settings'))
            for values in advanced_config 
            if 'attach' in json.loads(values.get('advanced_settings')).get('Category', '').lower()
        ]
    
    return DimensionConfig(
        dimension_table_key_column_name=dimension_table_key_column_name,
        source_timestamp_column_name=source_timestamp_column_name,
        fact_table_data_load=fact_table_data_load
    )

# ===========================================================================================
# WAREHOUSE CONFIGURATION
# ===========================================================================================

def parse_warehouse_configuration(
    merge_type: str,
    datastore_config: str | list,
    target_datastore_name: str,
    spark_module
) -> WarehouseConfig:
    """
    Parse warehouse configuration and set up SQL endpoint if needed.
    
    Configures Spark to connect to Data Warehouse when merge_type is 'warehouse_spark_connector'.
    Retrieves warehouse endpoint from the Datastore_Configuration table.
    
    Args:
        merge_type (str): Merge type from target configuration
        datastore_config (str | list): Datastore configuration from Datastore_Configuration table.
                                       Either a string like "[{'Datastore_Name': 'bronze', ...}]" 
                                       or an already-parsed list.
        target_datastore_name (str): Target datastore name for warehouse
        spark_module: Spark module reference (injected for testability)
    
    Returns:
        WarehouseConfig: Warehouse configuration containing:
            - warehouse_configured (bool): Whether warehouse was configured
            - warehouse_endpoint (str, optional): Warehouse SQL endpoint if configured
    
    Example:
        >>> config = parse_warehouse_configuration(
        ...     merge_type='warehouse_spark_connector',
        ...     datastore_config="[{'Datastore_Name': 'my_warehouse', 'Endpoint': 'ws-123.datawarehouse.windows.net'}]",
        ...     target_datastore_name='my_warehouse',
        ...     spark_module=spark
        ... )
        >>> config.warehouse_configured
        True
    """
    warehouse_configured = False
    warehouse_endpoint = None
    
    if merge_type == "warehouse_spark_connector":
        # Get warehouse endpoint from datastore configuration
        warehouse_endpoint = _get_datastore_config(datastore_config, target_datastore_name, 'Endpoint')
        
        # Configure Spark to use warehouse
        spark_module.conf.set(
            f"spark.datawarehouse.{target_datastore_name}.sqlendpoint",
            f"{warehouse_endpoint},1433"
        )
        warehouse_configured = True
    
    return WarehouseConfig(
        warehouse_configured=warehouse_configured,
        warehouse_endpoint=warehouse_endpoint
    )


def _resolve_file_header_settings(primary_config: Dict[str, Any]) -> Tuple[bool, Any]:
    """Return the header flag and pandas header configuration."""
    file_has_header_row = primary_config.get("source_details_file_has_header_row", 'true').lower() == 'true'
    pandas_header_config = 0 if file_has_header_row else None
    return file_has_header_row, pandas_header_config


def _resolve_encoding(primary_config: Dict[str, Any]) -> str:
    """Return the requested file encoding with a UTF-8 default."""
    return primary_config.get("source_details_encoding", "utf-8")


def _resolve_xml_settings(primary_config: Dict[str, Any]) -> Tuple[Any, Any]:
    """Parse XML XPath and namespace strings into usable settings."""
    xml_xpath = primary_config.get("source_details_xml_xpath")
    namespace_keys = primary_config.get("source_details_xml_namespaces_keys")
    namespace_values = primary_config.get("source_details_xml_namespaces_values")

    if namespace_keys or namespace_values:
        if not namespace_keys or not namespace_values:
            raise Exception(
                "Both xml namespace keys and values must be provided when configuring XML namespaces."
            )

        namespace_key_list = [key.strip() for key in namespace_keys.split(",") if key.strip()]
        namespace_value_list = [value.strip() for value in namespace_values.split(",") if value.strip()]

        if len(namespace_key_list) != len(namespace_value_list):
            raise Exception(
                "XML namespace keys and values must have the same number of entries."
            )

        xml_namespaces = dict(zip(namespace_key_list, namespace_value_list))
    else:
        xml_namespaces = None

    return xml_xpath, xml_namespaces


def _resolve_on_bad_records_mode(on_bad_records: str) -> Any:
    """Translate configuration value into Spark read mode."""
    if not on_bad_records:
        return None

    normalized_value = on_bad_records.lower().strip()
    if normalized_value == "fail":
        return "FAILFAST"
    if normalized_value == "drop":
        return "DROPMALFORMED"
    if normalized_value == "quarantine":
        return "PERMISSIVE"

    raise Exception(
        f"Invalid on_bad_records value, {on_bad_records}. Valid value are fail, drop, and quarantine"
    )


def _build_schema_kwargs(schema: Any, mode: Any) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Construct parquet and generic schema kwargs."""
    schema_kwargs: Dict[str, Any] = {}
    if mode:
        schema_kwargs["mode"] = mode
    if mode == "PERMISSIVE":
        schema_kwargs['columnNameOfCorruptRecord'] = "delta__corrupt_record"

    if not schema:
        return {}, schema_kwargs
    
    # Convert DDL string to StructType for Spark readers
    if isinstance(schema, str):
        from pyspark.sql.types import _parse_datatype_string
        schema = _parse_datatype_string(schema)

    parquet_schema_kwargs = {"schema": schema}
    schema_kwargs["schema"] = schema

    return parquet_schema_kwargs, schema_kwargs


def extract_file_configuration(
    primary_config: Dict[str, Any],
    fail_on_new_schema: bool
) -> FileConfig:
    """Extract and process configuration parameters."""
    delimiter = primary_config.get("source_details_delimiter", ",")
    sheet_name = primary_config.get("source_details_sheet_name", "")
    file_has_header_row, pandas_header_config = _resolve_file_header_settings(primary_config)

    multiline = primary_config.get("source_details_multiline", 'false').lower() == 'true'
    schema = primary_config.get("source_details_schema")
    on_bad_records = primary_config.get("source_details_on_bad_records", "quarantine")
    mode = _resolve_on_bad_records_mode(on_bad_records)
    encoding = _resolve_encoding(primary_config)
    xml_xpath, xml_namespaces = _resolve_xml_settings(primary_config)

    allow_missing_columns = not fail_on_new_schema
    parquet_schema_kwargs, schema_kwargs = _build_schema_kwargs(schema, mode)

    process_one_file_at_a_time = primary_config.get("source_details_process_one_file_at_a_time", "false").lower() == "true"
    
    # Preserve original schema string for post-read validation (used by Parquet readers)
    # This enables schema contract validation without applying schema during read
    expected_schema_string = schema if isinstance(schema, str) else None

    return FileConfig(
        delimiter=delimiter,
        file_has_header_row=file_has_header_row,
        sheet_name=sheet_name,
        pandas_header_config=pandas_header_config,
        multiline=multiline,
        encoding=encoding,
        allow_missing_columns=allow_missing_columns,
        parquet_schema_kwargs=parquet_schema_kwargs,
        schema_kwargs=schema_kwargs,
        xml_xpath=xml_xpath,
        xml_namespaces=xml_namespaces,
        process_one_file_at_a_time=process_one_file_at_a_time,
        expected_schema_string=expected_schema_string
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1b. Warehouse Connection and Logging Functions
# 
# These functions provide pyodbc-based connections to the metadata and logging warehouses.
# They enable the notebook to:
# - Fetch metadata configuration (primary config, advanced config, orchestration, datastore details)
# - Fetch logging details (schema history, watermark values)
# - Log data movement events (Started, Processed, Failed)
# - Log schema changes
# 
# This removes the dependency on pipeline Script activities for metadata/logging operations,
# allowing the notebook to be self-contained.

# CELL ********************

# ===========================================================================================
# WAREHOUSE CONNECTION AND LOGGING FUNCTIONS
# ===========================================================================================
# These functions connect to metadata/logging warehouses via pyodbc and execute the same
# stored procedures that the pipeline's Script activities previously called.
# pyodbc, struct, and uuid are imported in NB_Helper_Functions_1.
# ===========================================================================================

@dataclass
class WarehouseConnectionConfig(_ConfigBase):
    """Connection configuration for metadata and logging warehouse endpoints.

    Single-workspace deployments commonly use the same metadata and logging
    warehouse. Decentralized data products can route metadata and logging to
    distinct warehouse names and endpoints.
    """
    metadata_endpoint: str = ""
    metadata_warehouse_name: str = ""
    central_metadata_endpoint: str = ""
    central_metadata_warehouse_name: str = ""


WAREHOUSE_CONNECTION_MAX_RETRIES = 3
WAREHOUSE_CONNECTION_BASE_DELAY_SECONDS = 1.0
WAREHOUSE_CONNECTION_MAX_DELAY_SECONDS = 4.0


def _is_transient_warehouse_connection_error(error: Exception) -> bool:
    """Return True for connection-open errors that are worth retrying briefly."""
    error_text = str(error or "").lower()

    non_retryable_signals = (
        "does not have access",
        "permission",
        "access denied",
        "login failed",
        "authentication",
        "cannot open database",
        "database does not exist",
    )
    if any(signal in error_text for signal in non_retryable_signals):
        return False

    transient_signals = (
        "too many active connections",
        "too many connections",
        "connection limit",
        "timeout expired",
        "timed out",
        "connection timeout",
        "transport-level error",
        "invalid response",
        "invalid server response",
        "server returned an invalid response",
        "temporarily not found",
        "not found temporarily",
        "temporarily unavailable",
        "try again",
        "server is busy",
        "service is busy",
        "resource limit",
        "throttl",
        "08s01",
        "08001",
        "hyt00",
        "hyt01",
    )
    if any(signal in error_text for signal in transient_signals):
        return True

    broad_non_retryable_signals = (
        "invalid",
        "not found",
        "failed to connect to fabric warehouse",
    )
    if any(signal in error_text for signal in broad_non_retryable_signals):
        return False

    return False


def _get_warehouse_connection_retry_delay_seconds(attempt_number: int) -> float:
    """Return a short capped exponential backoff with jitter for connection retries."""
    import random

    exponential_delay = min(
        WAREHOUSE_CONNECTION_BASE_DELAY_SECONDS * (2 ** max(attempt_number - 1, 0)),
        WAREHOUSE_CONNECTION_MAX_DELAY_SECONDS,
    )
    jitter = random.uniform(0.0, 0.5)
    return exponential_delay + jitter


def create_warehouse_connection(endpoint: str, warehouse_name: str):
    """
    Create a pyodbc connection to a Fabric warehouse using AAD token authentication.
    
    Uses the same pattern as NB_Setup_Feature_Workspace: gets a Fabric token via
    notebookutils.credentials.getToken("https://database.windows.net/"), encodes it as UTF-16-LE, and passes
    it via the SQL_COPT_SS_ACCESS_TOKEN connection attribute.
    
    Args:
        endpoint: SQL endpoint URL for the warehouse
        warehouse_name: Display name of the warehouse (used as DATABASE in connection string)
    
    Returns:
        pyodbc.Connection: An active connection with autocommit enabled
    """
    # Use short-lived warehouse connections. Disabling pyodbc pooling ensures a
    # closed connection is actually released instead of remaining in the driver-side
    # pool while many notebooks are running concurrently.
    if getattr(pyodbc, "pooling", None) is not False:
        pyodbc.pooling = False

    import time

    last_error = None

    for attempt_number in range(1, WAREHOUSE_CONNECTION_MAX_RETRIES + 2):
        fabric_token = notebookutils.credentials.getToken("https://database.windows.net/")

        token_bytes = fabric_token.encode("utf-16-le")
        token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)

        try:
            conn = pyodbc.connect(
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={endpoint};"
                f"DATABASE={warehouse_name};"
                f"Encrypt=Yes;TrustServerCertificate=No;",
                attrs_before={1256: token_struct},   # SQL_COPT_SS_ACCESS_TOKEN
            )
            conn.autocommit = True
            return conn
        except pyodbc.Error as conn_err:
            last_error = conn_err

            should_retry = (
                attempt_number <= WAREHOUSE_CONNECTION_MAX_RETRIES
                and _is_transient_warehouse_connection_error(conn_err)
            )

            if should_retry:
                delay_seconds = _get_warehouse_connection_retry_delay_seconds(attempt_number)
                log_and_print(
                    (
                        f"Transient warehouse connection failure on attempt {attempt_number}/"
                        f"{WAREHOUSE_CONNECTION_MAX_RETRIES + 1} for '{warehouse_name}'. "
                        f"Retrying in {delay_seconds:.2f}s. "
                        f"Error: {_sanitize_error_message(str(conn_err))}"
                    ),
                    "warn"
                )
                time.sleep(delay_seconds)
                continue

            break

    raise RuntimeError(
        f"Failed to connect to Fabric warehouse '{warehouse_name}' at endpoint '{endpoint}'.\n\n"
        f"Error: {_sanitize_error_message(str(last_error))}\n\n"
        "The identity running this notebook does not have access to the warehouse, "
        "the endpoint/name is incorrect, or the warehouse could not accept a connection.\n\n"
        "Check that:\n"
        "  1. That identity has been granted access to the warehouse.\n"
        "  2. The warehouse endpoint and name are correct (check Variable Library values).\n"
        "  3. If this was a transient capacity spike, retry the notebook run.\n\n"
        "To configure identity-based auth for notebook activities, see:\n"
        "https://blog.fabric.microsoft.com/en-US/blog/run-notebooks-in-pipelines-with-service-principal-or-workspace-identity/"
    ) from last_error


def _drain_remaining_result_sets(cursor) -> None:
    """Advance through any leftover ODBC result sets so the connection can be reused safely."""
    while cursor.nextset():
        pass


def _rows_to_list_of_dicts(cursor) -> list:
    """Convert pyodbc cursor results to a list of dicts (matching pipeline Script activity output format)."""
    try:
        if cursor.description is None:
            return []
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        _drain_remaining_result_sets(cursor)


def _rows_to_single_dict(cursor) -> dict:
    """Convert a single-row pyodbc result to a dict."""
    rows = _rows_to_list_of_dicts(cursor)
    return rows[0] if rows else {}


def fetch_metadata_from_warehouse(conn, table_id: int, target_datastore: str) -> dict:
    """
    Execute the same 5 queries the pipeline's 'Metadata' Script activity runs.
    
    Replicates the pipeline's metadata fetch by calling:
    1. Pivot_Primary_Config — pivots primary metadata into a single-row result
    2. Get_Advanced_Metadata — aggregates advanced config into JSON strings
    3. Data_Pipeline_Metadata_Orchestration — gets orchestration context (Table_ID is unique)
    4. Get_Datastore_Details — resolves target datastore to connection config, including central shortcut fallback
    5. Get_Datastore_Configuration_Catalog — full merged datastore catalog for cross-references
    
    Args:
        conn: Active pyodbc connection to the metadata warehouse
        table_id: Table ID from the orchestration metadata (must be unique in Orchestration table)
        target_datastore: Target datastore name for details lookup
    Returns:
        dict with keys: primary_config, advanced_config, orchestration_metadata,
                        target_datastore_config, all_datastore_config
    
    Raises:
        ValueError: If Table_ID returns more than one row from the Orchestration table.
                    Table_ID must be unique — duplicate rows indicate a metadata error.
    """
    cursor = conn.cursor()
    
    # 1. Pivot_Primary_Config → single row dict
    cursor.execute("EXEC [dbo].[Pivot_Primary_Config] @table_id = ?", table_id)
    primary_config = _rows_to_single_dict(cursor)
    
    # 2. Get_Advanced_Metadata → list of dicts
    cursor.execute("EXEC [dbo].[Get_Advanced_Metadata] @table_id = ?", table_id)
    advanced_config = _rows_to_list_of_dicts(cursor)
    
    # 3. Orchestration metadata → Table_ID is unique, no trigger filter needed
    cursor.execute(
        "SELECT * FROM dbo.Data_Pipeline_Metadata_Orchestration WHERE Table_ID = ?",
        table_id
    )
    orchestration_rows = _rows_to_list_of_dicts(cursor)
    if len(orchestration_rows) > 1:
        raise ValueError(
            f"Table_ID {table_id} returned {len(orchestration_rows)} rows from "
            f"Data_Pipeline_Metadata_Orchestration. Table_ID must be unique. "
            f"Remove duplicate rows for this Table_ID from the Orchestration table."
        )
    orchestration_metadata = orchestration_rows[0] if orchestration_rows else {}
    
    # 4. Get_Datastore_Details for target → single row dict
    cursor.execute("EXEC [dbo].[Get_Datastore_Details] @datastore_name = ?", target_datastore)
    target_datastore_config = _rows_to_single_dict(cursor)

    # 5. Full merged datastore catalog → list of dicts
    cursor.execute("EXEC [dbo].[Get_Datastore_Configuration_Catalog]")
    all_datastore_config = _rows_to_list_of_dicts(cursor)

    cursor.close()
    
    return {
        "primary_config": primary_config,
        "advanced_config": advanced_config,
        "orchestration_metadata": orchestration_metadata,
        "target_datastore_config": target_datastore_config,
        "all_datastore_config": all_datastore_config
    }


def fetch_watermark_details(
    conn,
    table_id: int,
    target_datastore: str,
    processing_phase: str = 'Batch',
    data_product_key: str = 'default',
    watermark_strategy: str = None,
    watermark_field_data_type: str = None
) -> dict:
    """Fetch watermark details for the requested processing phase."""
    cursor = conn.cursor()
    cursor.execute(
        "EXEC [dbo].[Get_Watermark_Value] @data_product_key = ?, @table_id = ?, @target_datastore = ?, @processing_phase = ?, @watermark_strategy = ?, @watermark_field_data_type = ?",
        data_product_key, table_id, target_datastore, processing_phase, watermark_strategy, watermark_field_data_type
    )
    watermark_details = _rows_to_single_dict(cursor)
    cursor.close()
    return watermark_details


def fetch_logging_details(
    conn,
    table_id: int,
    target_datastore: str,
    processing_phase: str = 'Batch',
    data_product_key: str = 'default',
    watermark_strategy: str = None,
    watermark_field_data_type: str = None
) -> dict:
    """
    Execute the same 2 queries the pipeline's 'Logging' Script activity runs.
    
    Replicates the pipeline's logging fetch by calling:
    1. Get_Schema_Details — latest schema hash and details
    2. Get_Watermark_Value — latest watermark and full_reload flag
    
    Args:
        conn: Active pyodbc connection to the logging warehouse
        table_id: Table ID for lookup
        target_datastore: Target datastore name for watermark filtering
    
    Returns:
        dict with keys: schema_details, watermark_details (each a single dict)
    """
    cursor = conn.cursor()
    
    # 1. Get_Schema_Details → single row dict
    cursor.execute("EXEC [dbo].[Get_Schema_Details] @data_product_key = ?, @table_id = ?", data_product_key, table_id)
    schema_details = _rows_to_single_dict(cursor)
    
    # 2. Get_Watermark_Value → single row dict
    watermark_details = fetch_watermark_details(
        conn = conn,
        table_id = table_id,
        target_datastore = target_datastore,
        processing_phase = processing_phase,
        data_product_key = data_product_key,
        watermark_strategy = watermark_strategy,
        watermark_field_data_type = watermark_field_data_type
    )
    
    cursor.close()
    
    return {
        "schema_details": schema_details,
        "watermark_details": watermark_details
    }


@dataclass
class DataMovementLogEntry(_ConfigBase):
    """
    All fields for a Log_Data_Movement stored procedure call.
    
    Built incrementally during processing:
    - At startup: set log_id, table_id, target_datastore, target_entity,
      event_start_time, trigger context, and fabric_monitor_url (built
      dynamically from notebookutils.runtime.context)
    - After config parsing: set lineage fields (source/target medallion/type)
    - After processing: set source_details, records_processed, watermark_value, etc.
    - On failure: set data_quality_warnings, data_quality_failures
    
    Example:
        >>> entry = DataMovementLogEntry(
        ...     log_id=str(uuid.uuid4()),
        ...     table_id=123,
        ...     target_datastore='silver',
        ...     target_entity='dbo.sales',
        ...     event_start_time='2024-09-04T21:43:12',
        ...     trigger_name='TR_Daily'
        ... )
        >>> log_data_movement(conn, entry, status='Started')
        >>> # ... processing ...
        >>> entry.source_details = '/Files/sales/*.parquet'
        >>> entry.records_processed = '1500'
        >>> log_data_movement(conn, entry, status='Processed')
    """
    data_product_key: str = "default"
    log_id: str = ""
    table_id: int = 0
    target_datastore: str = ""
    target_entity: str = ""
    event_start_time: str = ""
    fabric_monitor_url: str = ""
    trigger_name: str = ""
    trigger_step: int = None
    trigger_id: str = ""
    trigger_time: str = ""
    source_details: str = ""
    event_end_time: str = None
    watermark_value: str = None
    records_processed: str = None
    quarantined_records: str = None
    data_quality_warnings: str = None
    data_quality_failures: str = None
    source_medallion_layer: str = None
    source_type: str = None
    target_medallion_layer: str = None
    target_type: str = None
    # Connection refresh fields — stored so terminal-status functions can
    # obtain a fresh token if the original connection's AAD token has expired.
    central_metadata_endpoint: str = ""
    central_metadata_warehouse_name: str = ""
    processing_phase: str = 'Batch'


def log_data_movement(conn, entry: DataMovementLogEntry, status: str) -> None:
    """
    Call the Log_Data_Movement stored procedure via pyodbc.
    
    Replicates all 3 pipeline logging calls (Started, Processed, Failed) depending
    on the status parameter. The entry dataclass is built incrementally during
    processing — set trigger/context fields at startup, add results after processing.
    
    Args:
        conn: Active pyodbc connection to the logging warehouse
        entry: DataMovementLogEntry with all log fields populated
        status: One of 'Started', 'Processed', 'Failed'
    """
    cursor = conn.cursor()
    
    sql = """
    EXEC [dbo].[Log_Data_Movement]
        @log_id = ?,
        @data_product_key = ?,
        @table_id = ?,
        @source_details = ?,
        @target_datastore = ?,
        @target_entity = ?,
        @event_start_time = ?,
        @event_end_time = ?,
        @status = ?,
        @watermark_value = ?,
        @records_processed = ?,
        @quarantined_records = ?,
        @data_quality_warnings = ?,
        @data_quality_failures = ?,
        @fabric_monitor_url = ?,
        @trigger_name = ?,
        @trigger_step = ?,
        @trigger_id = ?,
        @trigger_time = ?,
        @source_medallion_layer = ?,
        @source_type = ?,
        @target_medallion_layer = ?,
        @target_type = ?,
        @processing_phase = ?
    """
    
    _VARCHAR_MAX = {"data_quality_warnings", "data_quality_failures"}

    param_map = {
        "log_id": entry.log_id,
        "data_product_key": entry.data_product_key,
        "table_id": entry.table_id,
        "source_details": entry.source_details,
        "target_datastore": entry.target_datastore,
        "target_entity": entry.target_entity,
        "event_start_time": entry.event_start_time,
        "event_end_time": entry.event_end_time,
        "status": status,
        "watermark_value": entry.watermark_value,
        "records_processed": entry.records_processed,
        "quarantined_records": entry.quarantined_records,
        "data_quality_warnings": entry.data_quality_warnings,
        "data_quality_failures": entry.data_quality_failures,
        "fabric_monitor_url": entry.fabric_monitor_url,
        "trigger_name": entry.trigger_name,
        "trigger_step": entry.trigger_step,
        "trigger_id": entry.trigger_id,
        "trigger_time": entry.trigger_time,
        "source_medallion_layer": entry.source_medallion_layer,
        "source_type": entry.source_type,
        "target_medallion_layer": entry.target_medallion_layer,
        "target_type": entry.target_type,
        "processing_phase": entry.processing_phase,
    }

    # Force varchar(max) binding for large string params to avoid ODBC Driver 18
    # sending them as legacy text/ntext types, which fail with BIN2_UTF8 collation.
    params = tuple(param_map.values())
    sizes = [
        (pyodbc.SQL_VARCHAR, 0, 0) if name in _VARCHAR_MAX else None
        for name in param_map
    ]
    cursor.setinputsizes(sizes)
    cursor.execute(sql, params)
    _drain_remaining_result_sets(cursor)
    cursor.close()
    log_and_print(f"Logged data movement: status={status}, log_id={entry.log_id}")


def persist_run_log_entries(conn, log_id: str, table_id: int, data_product_key: str = "default", source_type: str = "Notebook") -> None:
    """Persist structured log entries to Activity_Run_Logs via the Insert_Activity_Log_Entries SP.

    Reads the accumulated structured entries from get_run_log_entries(), serializes
    them to JSON, and bulk-inserts via the stored procedure. Safe to call even if
    the buffer is empty (no-ops gracefully).

    Args:
        conn: Active pyodbc connection to the logging warehouse
        log_id: The Log_ID that ties these entries to a Data_Pipeline_Logs row
        table_id: The Table_ID from orchestration metadata
        source_type: Origin label for Source_Type column (default: 'Notebook')
    """
    entries = get_run_log_entries()
    if not entries:
        return

    entries_json = json.dumps(entries, default=str)

    cursor = conn.cursor()
    sql = "EXEC [dbo].[Insert_Activity_Log_Entries] @log_id = ?, @data_product_key = ?, @table_id = ?, @entries = ?, @source_type = ?"
    cursor.setinputsizes([(pyodbc.SQL_VARCHAR, 0, 0), (pyodbc.SQL_VARCHAR, 0, 0), (pyodbc.SQL_INTEGER, 0, 0), (pyodbc.SQL_VARCHAR, 0, 0), (pyodbc.SQL_VARCHAR, 0, 0)])
    cursor.execute(sql, (log_id, data_product_key, table_id, entries_json, source_type))
    _drain_remaining_result_sets(cursor)
    cursor.close()


def log_new_schema(
    conn,
    data_product_key: str,
    table_id: int,
    datastore_name: str,
    table_name: str,
    schema_id: str,
    schema_details_dict: dict,
    fabric_monitor_url: str = "",
    end_time: str = None
) -> None:
    """
    Call the Log_New_Schema stored procedure via pyodbc.
    
    Logs new schema arrivals and schema change details to the Schema_Logs
    and Schema_Changes tables. Handles JSON serialization internally so
    callers can pass raw Python objects.
    
    Args:
        conn: Active pyodbc connection to the logging warehouse
        table_id: Table ID
        datastore_name: Target datastore name
        table_name: Target table name
        schema_id: MD5 hash of the schema
        schema_details_dict: Dict with 'schema_details' (JSON str) and 'schema_changes' (list or empty str)
        fabric_monitor_url: Fabric monitoring URL
        end_time: Processing end time (UTC string)
    """
    schema_details = schema_details_dict.get("schema_details", "")
    schema_changes_raw = schema_details_dict.get("schema_changes", "")
    schema_updates = json.dumps(schema_changes_raw) if schema_changes_raw else ""

    cursor = conn.cursor()
    
    sql = """
    EXEC [dbo].[Log_New_Schema]
        @data_product_key = ?,
        @table_id = ?,
        @datastore_name = ?,
        @table_name = ?,
        @schema_id = ?,
        @schema_details = ?,
        @schema_updates = ?,
        @fabric_monitor_url = ?,
        @end_time = ?
    """
    
    _VARCHAR_MAX = {"schema_details", "schema_updates"}

    param_map = {
        "data_product_key": data_product_key,
        "table_id": table_id,
        "datastore_name": datastore_name,
        "table_name": table_name,
        "schema_id": schema_id,
        "schema_details": schema_details,
        "schema_updates": schema_updates,
        "fabric_monitor_url": fabric_monitor_url,
        "end_time": end_time,
    }

    # Force varchar(max) binding for large string params to avoid ODBC Driver 18
    # sending them as legacy text/ntext types, which fail with BIN2_UTF8 collation.
    params = tuple(param_map.values())
    sizes = [
        (pyodbc.SQL_VARCHAR, 0, 0) if name in _VARCHAR_MAX else None
        for name in param_map
    ]
    cursor.setinputsizes(sizes)
    cursor.execute(sql, params)
    _drain_remaining_result_sets(cursor)
    cursor.close()
    log_and_print(f"Logged new schema for table_id={table_id}, schema_id={schema_id}")


def refresh_warehouse_connection(old_conn, endpoint: str, warehouse_name: str):
    """Create a new connection with a fresh AAD token, then close the old one.

    SQL tokens issued via ``notebookutils.credentials.getToken("https://database.windows.net/")`` typically
    can expire. For long-running notebooks, the original connection's
    token may have expired. This helper obtains a fresh token and returns a new
    connection, ensuring that terminal log calls (Processed / Failed) succeed
    even after extended processing.

    Args:
        old_conn: The existing pyodbc connection (will be closed safely).
        endpoint: SQL endpoint URL for the warehouse.
        warehouse_name: Display name of the warehouse.

    Returns:
        pyodbc.Connection: A new connection with a fresh token.
    """
    new_conn = create_warehouse_connection(endpoint, warehouse_name)
    close_warehouse_connection(old_conn)
    return new_conn


def close_warehouse_connection(conn) -> None:
    """Safely close a pyodbc connection."""
    if conn is not None:
        try:
            conn.close()
        except Exception as e:
            log_and_print(f"Warning closing warehouse connection: {e}", "warn")


def open_central_metadata_connection(log_entry: 'DataMovementLogEntry', existing_conn = None):
    """Open a fresh logging warehouse connection for terminal notebook logging.

    Logging connections are intentionally short-lived so that long-running notebook
    execution does not consume idle warehouse sessions while work is happening in Spark.
    Any supplied existing connection is only closed after the replacement
    connection details have been validated.
    """
    if not getattr(log_entry, 'central_metadata_endpoint', None) or not getattr(log_entry, 'central_metadata_warehouse_name', None):
        raise ValueError("Logging endpoint and warehouse name must be populated on log_entry before opening a logging connection.")

    new_conn = create_warehouse_connection(log_entry.central_metadata_endpoint, log_entry.central_metadata_warehouse_name)
    close_warehouse_connection(existing_conn)
    return new_conn


def _log_terminal_logging_error(context: str, logging_error: Exception) -> None:
    """Best-effort local logging that cannot mask the original notebook outcome."""
    message = (
        f"{context} failed while preserving the original notebook outcome. "
        f"Terminal logging error: {_sanitize_error_message(str(logging_error))}"
    )
    try:
        log_and_print(message, "error")
    except Exception:
        print(message)


def should_share_startup_warehouse_connection(
    metadata_endpoint: str,
    metadata_warehouse_name: str,
    central_metadata_endpoint: str,
    central_metadata_warehouse_name: str,
) -> bool:
    """Return True when Step 3 can reuse one connection for metadata and logging work.

    Reuse is allowed only when both endpoint and database/display name match.
    """
    return (
        str(metadata_endpoint or "").strip().lower() == str(central_metadata_endpoint or "").strip().lower()
        and str(metadata_warehouse_name or "").strip().lower() == str(central_metadata_warehouse_name or "").strip().lower()
    )


def open_startup_warehouse_connections(
    metadata_endpoint: str,
    metadata_warehouse_name: str,
    central_metadata_endpoint: str,
    central_metadata_warehouse_name: str,
):
    """Open Step 3 warehouse connections, reusing the metadata connection when possible.

    Metadata and logging may target different warehouses. Reuse the metadata
    connection only when the resolved endpoint and warehouse name both match.
    """
    metadata_conn = None
    central_metadata_endpoint = (central_metadata_endpoint or metadata_endpoint or "").strip()
    central_metadata_warehouse_name = (central_metadata_warehouse_name or metadata_warehouse_name or "").strip()

    try:
        metadata_conn = create_warehouse_connection(metadata_endpoint, metadata_warehouse_name)

        if should_share_startup_warehouse_connection(
            metadata_endpoint=metadata_endpoint,
            metadata_warehouse_name=metadata_warehouse_name,
            central_metadata_endpoint=central_metadata_endpoint,
            central_metadata_warehouse_name=central_metadata_warehouse_name,
        ):
            log_and_print("Metadata and logging target the same warehouse; reusing one startup connection.")
            return metadata_conn, metadata_conn

        central_metadata_conn = create_warehouse_connection(central_metadata_endpoint, central_metadata_warehouse_name)
        return metadata_conn, central_metadata_conn
    except Exception:
        close_warehouse_connection(metadata_conn)
        raise


def close_startup_warehouse_connections(metadata_conn, central_metadata_conn) -> None:
    """Close Step 3 startup warehouse connections without double-closing a shared connection."""
    close_warehouse_connection(metadata_conn)
    if central_metadata_conn is not metadata_conn:
        close_warehouse_connection(central_metadata_conn)


def handle_startup_failure(central_metadata_conn, log_entry: 'DataMovementLogEntry', error: Exception, step: str = None) -> None:
    """Handle Step 3 failure logging and cleanup for partially initialized startup connections."""
    if central_metadata_conn is not None and log_entry is not None:
        try:
            log_failure_and_cleanup(central_metadata_conn, log_entry, error, step=step)
        except Exception as logging_error:
            log_and_print(f"Failed to log startup failure: {logging_error}", "error")
            close_warehouse_connection(central_metadata_conn)
    elif central_metadata_conn is not None:
        close_warehouse_connection(central_metadata_conn)


def _extract_lineage_value(lineage_info, key: str):
    """Safely read lineage values from either a dict or an object with attributes."""
    if lineage_info is None:
        return None
    if isinstance(lineage_info, dict):
        return lineage_info.get(key)
    return getattr(lineage_info, key, None)


def log_failure_and_cleanup(central_metadata_conn, log_entry: 'DataMovementLogEntry', error: Exception, step: str = None) -> None:
    """Log a processing failure to Data_Pipeline_Logs and close the logging connection.

    Handles both DataQualityFailureError (with DQ-specific fields) and generic exceptions.
    Call this from except blocks before re-raising. Always closes the connection.

    When logging metadata is available, opens a fresh short-lived logging connection
    before writing terminal failure state. This avoids relying on a long-lived
    warehouse session that may have gone stale during notebook execution.

    Args:
        central_metadata_conn: Active pyodbc connection to the logging warehouse
        log_entry: DataMovementLogEntry instance being built during this run
        error: The caught exception
        step: Optional notebook step identifier (e.g., "Step 4: Parse Configuration").
              When provided, prepended to the error message as ``[<step>] <error>``
              so downstream consumers (observability agent, HTML reports) can immediately
              see which processing phase failed without opening the Spark monitor.
    """
    if getattr(log_entry, '_terminal_status_logged', False):
        close_warehouse_connection(central_metadata_conn)
        return

    try:
        # Open a fresh short-lived connection for terminal logging.
        if log_entry.central_metadata_endpoint:
            central_metadata_conn = open_central_metadata_connection(log_entry, central_metadata_conn)

        end_time = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
        log_entry.event_end_time = end_time

        # Capture the actual error message for logging, with optional step prefix.
        # Sanitize to redact secrets and cap length before persisting to warehouse.
        error_text = _sanitize_error_message(str(error))
        if step:
            error_text = f"[{step}] {error_text}"

        if isinstance(error, DataQualityFailureError):
            log_entry.source_details = getattr(error, 'source_details', None)
            log_entry.data_quality_warnings = json.dumps(error.dq_warnings) if error.dq_warnings else None
            log_entry.data_quality_failures = json.dumps(error.dq_failures) if error.dq_failures else None

        if hasattr(error, 'lineage_info') and getattr(error, 'lineage_info', None):
            lineage_info = error.lineage_info
            log_entry.source_medallion_layer = _extract_lineage_value(lineage_info, "source_medallion_layer")
            log_entry.source_type = _extract_lineage_value(lineage_info, "source_type")
            log_entry.target_medallion_layer = _extract_lineage_value(lineage_info, "target_medallion_layer")
            log_entry.target_type = _extract_lineage_value(lineage_info, "target_type")

        # Persist the actual failure message into the structured run-log buffer
        # before Activity_Run_Logs is flushed to the warehouse.
        log_and_print(error_text, "error")

        log_data_movement(central_metadata_conn, log_entry, status='Failed')
        persist_run_log_entries(central_metadata_conn, log_entry.log_id, log_entry.table_id, log_entry.data_product_key)
        setattr(log_entry, '_terminal_status_logged', True)
    except Exception as logging_error:
        _log_terminal_logging_error("Failure logging", logging_error)
    finally:
        close_warehouse_connection(central_metadata_conn)


def log_no_data_and_cleanup(central_metadata_conn, log_entry: 'DataMovementLogEntry', error: 'NoDataFoundError') -> None:
    """Log a no-data result as Processed with 0 records and close the logging connection.

    Call this when NoDataFoundError is caught. After this call, exit the notebook
    gracefully since there is nothing left to process.

    When logging metadata is available, opens a fresh short-lived logging connection
    before writing terminal processed state. This avoids relying on a long-lived
    warehouse session that may have gone stale during notebook execution.

    Args:
        central_metadata_conn: Active pyodbc connection to the logging warehouse
        log_entry: DataMovementLogEntry instance being built during this run
        error: The caught NoDataFoundError (carries watermark_value, source_details, lineage_info)
    """
    if getattr(log_entry, '_terminal_status_logged', False):
        close_warehouse_connection(central_metadata_conn)
        return

    try:
        # Open a fresh short-lived connection for terminal logging.
        if log_entry.central_metadata_endpoint:
            central_metadata_conn = open_central_metadata_connection(log_entry, central_metadata_conn)

        end_time = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
        new_watermark_value = convert_datetime_to_string(value=error.watermark_value)
        log_entry.source_details = error.source_details
        log_entry.event_end_time = end_time
        log_entry.watermark_value = str(new_watermark_value) if new_watermark_value else None
        log_entry.records_processed = "0"
        log_entry.quarantined_records = "0"

        if hasattr(error, 'lineage_info') and getattr(error, 'lineage_info', None):
            lineage_info = error.lineage_info
            log_entry.source_medallion_layer = _extract_lineage_value(lineage_info, "source_medallion_layer")
            log_entry.source_type = _extract_lineage_value(lineage_info, "source_type")
            log_entry.target_medallion_layer = _extract_lineage_value(lineage_info, "target_medallion_layer")
            log_entry.target_type = _extract_lineage_value(lineage_info, "target_type")

        log_and_print("No new data found — logged as Processed with 0 records.")
        log_data_movement(central_metadata_conn, log_entry, status='Processed')
        persist_run_log_entries(central_metadata_conn, log_entry.log_id, log_entry.table_id, log_entry.data_product_key)
        setattr(log_entry, '_terminal_status_logged', True)
    except Exception as logging_error:
        _log_terminal_logging_error("No-data terminal logging", logging_error)
    finally:
        close_warehouse_connection(central_metadata_conn)


def finalize_staging_only_and_build_exit_value(
    central_metadata_conn,
    log_entry: 'DataMovementLogEntry',
    staging_result: dict
) -> str:
    """Finalize NB_Batch_Processing as a staging-only success and return a stable exit payload."""
    if log_entry.central_metadata_endpoint:
        central_metadata_conn = open_central_metadata_connection(log_entry, central_metadata_conn)

    end_time = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
    rows_copied = int(staging_result.get('rows_copied', 0) or 0)
    next_watermark_value = staging_result.get('next_watermark_value', '')

    log_entry.processing_phase = 'Staging'
    log_entry.source_details = staging_result.get('source_details')
    log_entry.event_end_time = end_time
    log_entry.watermark_value = str(next_watermark_value) if next_watermark_value else None
    log_entry.records_processed = str(rows_copied)
    log_entry.quarantined_records = "0"

    log_data_movement(central_metadata_conn, log_entry, status='Processed')
    persist_run_log_entries(central_metadata_conn, log_entry.log_id, log_entry.table_id, log_entry.data_product_key)
    setattr(log_entry, '_terminal_status_logged', True)
    close_warehouse_connection(central_metadata_conn)

    return build_custom_staging_exit_value(rows_copied, next_watermark_value)


def log_success_and_cleanup(
    central_metadata_conn,
    log_entry: 'DataMovementLogEntry',
    source_details: str,
    new_watermark_value,
    total_records_processed: int,
    quarantined_records: int,
    dq_warnings: list,
    new_schema: bool,
    table_id: int,
    target_datastore: str,
    target_entity: str,
    new_data_schema_hash: str,
    new_schema_details: dict,
    fabric_monitor_url: str,
) -> None:
    """Log terminal success using a fresh short-lived warehouse connection and close it."""
    if getattr(log_entry, '_terminal_status_logged', False):
        close_warehouse_connection(central_metadata_conn)
        return

    central_metadata_conn = open_central_metadata_connection(log_entry, central_metadata_conn)

    try:
        end_time = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
        log_entry.source_details = source_details
        log_entry.event_end_time = end_time
        log_entry.watermark_value = str(new_watermark_value) if new_watermark_value else None
        log_entry.records_processed = str(total_records_processed)
        log_entry.quarantined_records = str(quarantined_records)
        log_entry.data_quality_warnings = json.dumps(dq_warnings) if dq_warnings else None

        if new_schema:
            log_new_schema(
                conn=central_metadata_conn,
                data_product_key=log_entry.data_product_key,
                table_id=table_id,
                datastore_name=target_datastore,
                table_name=target_entity,
                schema_id=new_data_schema_hash,
                schema_details_dict=new_schema_details,
                fabric_monitor_url=fabric_monitor_url,
                end_time=end_time
            )

        log_data_movement(central_metadata_conn, log_entry, status='Processed')
        persist_run_log_entries(central_metadata_conn, log_entry.log_id, log_entry.table_id, log_entry.data_product_key)
        setattr(log_entry, '_terminal_status_logged', True)
        log_and_print(f"Processing complete. Records: {total_records_processed}, Quarantined: {quarantined_records}")
    finally:
        close_warehouse_connection(central_metadata_conn)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Spark Session Configuration
# 
# This section configures the Spark session with optimized settings based on the target lakehouse layer. Each layer (Bronze, Silver, Gold) has specific performance characteristics and requirements that are addressed through tailored Spark configurations.
# 
# ### Configuration Strategy
# - **Bronze**: Optimized for high-speed ingestion with minimal transformations
# - **Silver**: Balanced configuration for both read and write operations
# - **Gold**: Optimized for analytical query performance and complex transformations

# CELL ********************

# ===========================================================================================
# PERFORMANCE OPTIMIZATION SETTINGS
# ===========================================================================================

def parse_performance_configuration(
    primary_config: dict,
    target_datastore_medallion_name: str
) -> PerformanceConfig:
    """
    Parse performance optimization configuration including compute statistics.
    
    Extracts configuration for:
    - Column-level statistics computation
    - Number of columns for statistics (if configured)
    - Spark configuration enablement for lakehouse tables
    
    Args:
        primary_config (dict): Primary configuration dictionary
        target_datastore_medallion_name (str): Target medallion layer name (bronze/silver/gold)
    
    Returns:
        PerformanceConfig: Performance optimization settings containing:
            - compute_statistics_on_columns (str): Comma-separated column names
            - compute_statistics_on_first_n_columns (str): Number of columns for stats
            - column_stats_configured (bool): Whether statistics are configured
            - use_spark_config_for_lakehouse (str): Layer name for Spark config enablement
    
    Example:
        >>> config = parse_performance_configuration(
        ...     primary_config={
        ...         'target_details_compute_statistics_on_columns': 'col1,col2',
        ...         'target_details_use_spark_config_for_lakehouse': 'silver'
        ...     },
        ...     target_datastore_medallion_name='silver'
        ... )
        >>> config.column_stats_configured
        True
    """
    # Column statistics configuration
    compute_statistics_on_columns = primary_config.get(
        "target_details_compute_statistics_on_columns", ""
    )
    
    compute_statistics_on_first_n_columns = primary_config.get(
        "target_details_compute_statistics_on_first_n_columns", ""
    )

    # Rule 88: Both compute_statistics configs are set — only one will be used.
    # Check raw values here before any defaults are applied downstream.
    if compute_statistics_on_columns and compute_statistics_on_first_n_columns:
        error_msg = (
            f"Conflicting config — both 'compute_statistics_on_columns' "
            f"('{compute_statistics_on_columns}') and 'compute_statistics_on_first_n_columns' "
            f"('{compute_statistics_on_first_n_columns}') are set. Only "
            f"'compute_statistics_on_columns' will be used and "
            f"'compute_statistics_on_first_n_columns' is silently ignored. Set only one. "
            f"Run validate_metadata_sql.py to catch this before deployment."
        )
        raise ValueError(error_msg)
    
    column_stats_configured = bool(compute_statistics_on_columns) or bool(compute_statistics_on_first_n_columns)
    
    # Spark configuration enablement (target layer for which to enable Spark configs)
    use_spark_config_for_lakehouse = primary_config.get(
        "target_details_use_spark_config_for_lakehouse",
        target_datastore_medallion_name
    ).strip().lower()
    
    return PerformanceConfig(
        compute_statistics_on_columns=compute_statistics_on_columns,
        compute_statistics_on_first_n_columns=compute_statistics_on_first_n_columns,
        column_stats_configured=column_stats_configured,
        use_spark_config_for_lakehouse=use_spark_config_for_lakehouse
    )

# ===========================================================================================
# OTHER SPARK CONFIG
# ===========================================================================================

def parse_spark_configuration(primary_config: dict) -> SparkConfig:
    """
    Parse Spark configuration settings for Delta table operations.
    
    Extracts configuration for:
    - Change data feed enablement
    - Timestamp rebase mode for write operations (from target_details)
    - Timestamp rebase mode for read operations (from source_details)
    
    Args:
        primary_config (dict): Primary configuration dictionary
    
    Returns:
        SparkConfig: Spark setting configuration containing:
            - enable_change_data_feed (bool): Enable CDC on Delta tables
            - spark_timestamp_rebase_mode_write (str): Write rebase mode
            - spark_timestamp_rebase_mode_read (str): Read rebase mode
    
    Example:
        >>> config = parse_spark_configuration(
        ...     primary_config={
        ...         'target_details_enable_change_data_feed': 'true',
        ...         'target_details_spark_timestamp_rebase_mode': 'LEGACY'
        ...     }
        ... )
        >>> config.enable_change_data_feed
        True
    """
    # Change data feed for CDC
    enable_change_data_feed = primary_config.get(
        "target_details_enable_change_data_feed", "false"
    ).strip().lower() == "true"
    
    # Default timestamp rebase mode
    default_timestamp_rebase_mode = "CORRECTED"
    
    # Timestamp rebase modes for legacy Parquet compatibility
    # Write mode is stored in target_details
    spark_timestamp_rebase_mode_write = primary_config.get(
        "target_details_spark_timestamp_rebase_mode",
        default_timestamp_rebase_mode
    ).strip().upper()
    
    # Read mode is stored in source_details
    spark_timestamp_rebase_mode_read = primary_config.get(
        "source_details_spark_timestamp_rebase_mode",
        default_timestamp_rebase_mode
    ).strip().upper()
    
    return SparkConfig(
        enable_change_data_feed=enable_change_data_feed,
        spark_timestamp_rebase_mode_write=spark_timestamp_rebase_mode_write,
        spark_timestamp_rebase_mode_read=spark_timestamp_rebase_mode_read
    )

def parse_bronze_layer_spark_configuration() -> LayerSparkConfig:
    """
    Parse Spark configuration settings optimized for Bronze layer ingestion.
    
    Bronze layer is optimized for high-speed data ingestion with minimal processing overhead.
    Configuration focuses on fast writes while maintaining data quality for downstream processing.
    
    Args:
        None
    Returns:
        LayerSparkConfig: Bronze layer Spark configuration:
            - optimize_write_enabled (bool): Whether to enable optimize write
            - v_order_enabled (bool): Whether to enable V-Order Parquet optimization
            - checkpoint_interval (str): Delta checkpoint interval
    
    Example:
        >>> config = parse_bronze_layer_spark_configuration(
        ...     staging_folder_path = "/lakehouse/Bronze_Files/staging",
        ...     column_stats_configured = False
        ... )
        >>> print(config.optimize_write_enabled)
        False
    
    Reference:
        https://support.fabric.microsoft.com/en-us/blog/optimizing-spark-compute-for-medallion-architectures-in-microsoft-fabric
    """
    log_and_print("Configuring table properties for Bronze layer")
    
    # Bronze layer optimizations - fast ingestion focused
    optimize_write_enabled = False
    v_order_enabled = False
    checkpoint_interval = "25"
    
    return LayerSparkConfig(
        optimize_write_enabled=optimize_write_enabled,
        v_order_enabled=v_order_enabled,
        checkpoint_interval=checkpoint_interval
    )


def parse_silver_layer_spark_configuration() -> LayerSparkConfig:
    """
    Parse Spark configuration settings optimized for Silver layer processing.
    
    Silver layer uses a balanced configuration for data cleansing, transformation,
    and storage efficiency. Configuration balances read and write performance.
    
    Args:
        None
    
    Returns:
        LayerSparkConfig: Silver layer Spark configuration:
            - optimize_write_enabled (bool): Whether to enable optimize write
            - v_order_enabled (bool): Whether to enable V-Order Parquet optimization
            - checkpoint_interval (str): Delta checkpoint interval
    
    Example:
        >>> config = parse_silver_layer_spark_configuration()
        >>> print(config.optimize_write_enabled)
        True
    
    Reference:
        https://support.fabric.microsoft.com/en-us/blog/optimizing-spark-compute-for-medallion-architectures-in-microsoft-fabric
    """
    log_and_print("Configuring table properties for Silver layer")
    
    # Silver layer optimizations - balanced approach
    optimize_write_enabled = True
    v_order_enabled = False
    
    return LayerSparkConfig(
        optimize_write_enabled=optimize_write_enabled,
        v_order_enabled=v_order_enabled,
        checkpoint_interval=None
    )


def parse_gold_layer_spark_configuration() -> LayerSparkConfig:
    """
    Parse Spark configuration settings optimized for Gold layer analytics.
    
    Gold layer is optimized for analytical query performance and large-scale aggregations.
    Configuration prioritizes read performance for business intelligence and reporting workloads.
    
    Args:
        None
    
    Returns:
        LayerSparkConfig: Gold layer Spark configuration:
            - optimize_write_enabled (bool): Whether to enable optimize write
            - v_order_enabled (bool): Whether to enable V-Order Parquet optimization
            - checkpoint_interval (str): Delta checkpoint interval
    
    Example:
        >>> config = parse_gold_layer_spark_configuration()
        >>> print(config.optimize_write_enabled)
        True
        >>> print(config.v_order_enabled)
        True
    
    Reference:
        https://support.fabric.microsoft.com/en-us/blog/optimizing-spark-compute-for-medallion-architectures-in-microsoft-fabric
    """
    log_and_print("Configuring table properties for Gold layer")
    
    # Gold layer optimizations - read performance focused
    optimize_write_enabled = True
    v_order_enabled = True  # Enable V-Order for optimal query performance
    
    return LayerSparkConfig(
        optimize_write_enabled=optimize_write_enabled,
        v_order_enabled=v_order_enabled,
        checkpoint_interval=None
    )


def apply_spark_configurations(
    spark_timestamp_rebase_mode_write: str,
    spark_timestamp_rebase_mode_read: str,
    use_spark_config_for_lakehouse: str
) -> 'LayerSparkConfig':
    """Apply session-level Spark configs and return the layer configuration.

    Settings that have Delta table-level equivalents (enableChangeDataFeed,
    enableDeletionVectors, columnMapping.mode, autoCompact, optimizeWrite,
    checkpointInterval) are NO LONGER set here.  They are applied as
    TBLPROPERTIES at table creation time via build_table_properties() +
    create_delta_table().  This makes the notebook safe under Spark
    high-concurrency mode where multiple notebooks share one SparkSession.

    Args:
        spark_timestamp_rebase_mode_write (str): Rebase mode for timestamps when writing Parquet
        spark_timestamp_rebase_mode_read (str): Rebase mode for timestamps when reading Parquet
        use_spark_config_for_lakehouse (str): Medallion layer name (bronze/silver/gold)

    Returns:
        LayerSparkConfig: The parsed layer configuration so the caller can pass it
            to build_table_properties().
    """
    # Set Spark configuration for datetime and int96 rebase mode when writing Parquet files
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", spark_timestamp_rebase_mode_write)
    log_and_print(f"Spark Config: spark.sql.parquet.datetimeRebaseModeInWrite = {spark_timestamp_rebase_mode_write}")
    spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", spark_timestamp_rebase_mode_write)
    log_and_print(f"Spark Config: spark.sql.parquet.int96RebaseModeInWrite = {spark_timestamp_rebase_mode_write}")

    # Set Spark configuration for datetime and int96 rebase mode when reading Parquet files
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", spark_timestamp_rebase_mode_read)
    log_and_print(f"Spark Config: spark.sql.parquet.datetimeRebaseModeInRead = {spark_timestamp_rebase_mode_read}")
    spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", spark_timestamp_rebase_mode_read)
    log_and_print(f"Spark Config: spark.sql.parquet.int96RebaseModeInRead = {spark_timestamp_rebase_mode_read}")

    # Schema evolution settings
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "True")
    log_and_print("Spark Config: spark.databricks.delta.schema.autoMerge.enabled = True")

    # This cuts the overhead of Delta table snapshot generation 
    # (the process of identifying and caching the list of files that are active in the version of the table being queried) by ~50%.
    spark.conf.set("spark.microsoft.delta.snapshot.driverMode.enabled", True)
    log_and_print("Spark Config: spark.microsoft.delta.snapshot.driverMode.enabled = True")

    # SQL compliance settings
    spark.conf.set("spark.sql.ansi.enabled", "True")
    log_and_print("Spark Config: spark.sql.ansi.enabled = True")

    # Performance settings - compaction
    # https://learn.microsoft.com/en-us/fabric/data-engineering/table-compaction?tabs=sparksql
    spark.conf.set("spark.microsoft.delta.optimize.fast.enabled", "True")
    log_and_print("Spark Config: spark.microsoft.delta.optimize.fast.enabled = True")

    spark.conf.set("spark.microsoft.delta.optimize.fileLevelTarget.enabled", "True")
    log_and_print("Spark Config: spark.microsoft.delta.optimize.fileLevelTarget.enabled = True")

    # https://learn.microsoft.com/en-us/fabric/data-engineering/tune-file-size?tabs=sparksql#adaptive-target-file-size
    spark.conf.set("spark.microsoft.delta.targetFileSize.adaptive.enabled", "True")
    log_and_print("Spark Config: spark.microsoft.delta.targetFileSize.adaptive.enabled = True")

    # Parse layer-specific configuration based on medallion layer
    if use_spark_config_for_lakehouse == 'bronze':
        layer_config = parse_bronze_layer_spark_configuration()

    elif use_spark_config_for_lakehouse == 'silver':
        layer_config = parse_silver_layer_spark_configuration()

    elif use_spark_config_for_lakehouse == 'gold':
        layer_config = parse_gold_layer_spark_configuration()
    else:
        raise Exception(f"`target_details_use_spark_config_for_lakehouse` is set to '{use_spark_config_for_lakehouse}', but the only allowed values are `bronze`, `silver`, or `gold`.")

    return layer_config

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Date Dimension Table Creation
# 
# This section provides utilities for creating standard date dimension tables, which are fundamental components of any star schema. The date dimension enables time-based analysis and provides a rich set of attributes for temporal reporting.
# 
# Key features:
# - **Comprehensive Date Attributes**: Year, quarter, month, week, day attributes
# - **Business Calendar Support**: Weekend flags, month names, week numbers
# - **Sorting Columns**: Pre-calculated sort orders for proper visualization
# - **Extended Date Range**: Covers 100+ years (1950-2050) by default
# - **Null Row Support**: Includes -1 surrogate key for unknown dates

# CELL ********************

def create_date_dimension(
    fact_table_data_load: list,
    target_datastore_workspace_id: str,
    target_datastore_id: str,
    date_table_schema_name: str = 'dbo',
    date_table_name: str = 'dim_date',
    date_dimension_table_key_column_name: str = "date_sk"
):
    """
    Create a comprehensive date dimension table for analytical workloads.

    This function generates a date dimension with a century of dates and rich
    temporal attributes. The table serves as a conformed dimension across all
    fact tables, enabling consistent time-based analysis and reporting.

    The dimension includes pre-calculated attributes to optimize query performance
    and simplify report development. All common date hierarchies and groupings
    are materialized to avoid runtime calculations.

    Args:
        fact_table_data_load (list): Parsed surrogate key configurations from metadata for fact table loading.
                                     If empty, function exits without creating date dimension.
        target_datastore_workspace_id (str): Fabric workspace GUID where date dimension will be created
        target_datastore_id (str): Lakehouse GUID where date dimension will be created
        date_table_schema_name (str): Schema name for the date dimension table (default: 'dbo')
        date_table_name (str): Table name for the date dimension (default: 'dim_date')
        date_dimension_table_key_column_name (str): Name for the surrogate key column 
                                                        in YYYYMMDD integer format (default: 'Key_SK')

    Table Structure:
        - Surrogate Key: Integer in YYYYMMDD format for efficient joins
        - Date Attributes: Full date, year, quarter, month, day
        - Descriptive Fields: Month names, day names, formatted strings
        - Business Flags: Weekend indicator for business day calculations
        - Sort Columns: Negative values for descending order in reports
        - Hierarchies: Year-Month combinations for drill-down analysis
        - Null Row: -1 surrogate key for unknown/missing dates in fact tables

    Performance Considerations:
        - Static table with ~36,500 rows (100 years: 1950-2050)
        - Integer surrogate key for optimal join performance
        - Pre-calculated attributes eliminate runtime functions
        - Suitable for broadcasting in distributed queries

    Behavior:
        - Only creates dimension if attach_dimension_surrogate_key is configured (fact tables only)
        - Skips creation for dimension tables (which use create_surrogate_key)
        - Skips creation if date dimension table already exists at the target path
        - Creates table in the target lakehouse specified by workspace and datastore IDs

    Usage Example:
        Date dimensions are typically joined to fact tables:
        ```sql
        SELECT d.Month_Name, SUM(f.Sales_Amount)
        FROM fact_sales f
        JOIN dim_date d ON f.date_sk = d.date_sk
        GROUP BY d.Month_Name, d.Sort_Month
        ORDER BY d.Sort_Month DESC
        ```
    """
    # Exit early if no attach_dimension_surrogate_key logic is configured
    # Only fact tables (with attach_dimension_surrogate_key) need the date dimension
    # Dimension tables (with create_surrogate_key) do not need it auto-created
    if not fact_table_data_load:
        return
    
    # Construct ABFSS path to date dimension table location
    date_dimension_path = (
        f"abfss://{target_datastore_workspace_id}@onelake.dfs.fabric.microsoft.com/"
        f"{target_datastore_id}/Tables/{date_table_schema_name}/{date_table_name}"
    )
    
    # Skip creation if date dimension already exists
    date_dimension_table_exists = notebookutils.fs.exists(date_dimension_path)

    if date_dimension_table_exists:
        return 
    
    creating_date_dimension_log_info = "Creating date dimension table."
    log_and_print(creating_date_dimension_log_info)

    # Define date range spanning 100 years (1950-2050)
    start_date = date(1950, 1, 1)
    end_date = date(2050, 12, 31)
    
    # Generate DataFrame with sequential dates across the entire range
    date_df = spark.createDataFrame(
        [(start_date + timedelta(days=i),)
        for i in range((end_date - start_date).days + 1)],
        ["date"]
    )
    
    # Transform into comprehensive date dimension with all analytical attributes
    # Surrogate key uses YYYYMMDD integer format for efficient joins
    date_dim_df = date_df.withColumn(date_dimension_table_key_column_name, f.date_format("date", "yyyyMMdd").cast("int")) \
        .withColumn("Date", f.col("date")) \
        .withColumn("Date_Text", f.date_format("date", "yyyy-MM-dd")) \
        .withColumn("Year", f.year("date")) \
        .withColumn("Quarter", f.quarter("date")) \
        .withColumn("Month", f.month("date")) \
        .withColumn("Month_Name", f.date_format("date", "MMMM")) \
        .withColumn("Month_Name_Abbrev", f.date_format("date", "MMM")) \
        .withColumn("Day", f.dayofmonth("date")) \
        .withColumn("Day_Name", f.date_format("date", "EEEE")) \
        .withColumn("Day_Of_Week", f.dayofweek("date")) \
        .withColumn("Week_Of_Year", f.weekofyear("date")) \
        .withColumn("Is_Weekend", f.expr("CASE WHEN dayofweek(date) IN (1, 7) THEN TRUE ELSE FALSE END")) \
        .withColumn("Month_Year", f.concat_ws(", ", f.date_format("date", "MMM"), f.year("date"))) \
        .withColumn("Sort_Year", -f.year("date")) \
        .withColumn("Sort_Quarter", -f.quarter("date")) \
        .withColumn("Sort_Month", -f.month("date")) \
        .withColumn("Sort_Day", -f.date_format("date", "yyyyMMdd").cast("int")) \
        .withColumn("Sort_Day_Of_Week", -f.dayofweek("date")) \
        .withColumn("Sort_Week_Of_Year", -f.weekofyear("date")) \
        .withColumn("Sort_Year_Month", (f.year("date") * 100 + f.month("date")) * -1)

    def set_all_columns_nullable(schema):
        """Make all columns nullable to support null row addition."""
        new_fields = [StructField(field.name, field.dataType, True) for field in schema.fields]
        return StructType(new_fields)

    # Update schema to allow nulls (required for adding null row with -1 key)
    updated_schema = set_all_columns_nullable(date_dim_df.schema)
    date_dim_df = spark.createDataFrame(date_dim_df.rdd, schema = updated_schema)
    
    # Add null row with -1 surrogate key for unknown/missing dates in fact tables
    date_dim_df_null_row = spark.createDataFrame([(-1,)], [date_dimension_table_key_column_name])
    date_dim_df = date_dim_df.unionByName(date_dim_df_null_row, allowMissingColumns = True)

    # Write to target lakehouse as foundational dimension table
    date_dim_df.write.mode("overwrite").option("overwriteSchema", "true").save(date_dimension_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Slowly Changing Dimensions (SCD) Type 2 Support
# 
# This section provides utility functions for implementing Type 2 Slowly Changing Dimensions, which track historical changes in dimension data over time.
# 
# ### SCD2 Column Addition Function
# Adds the necessary tracking columns for SCD2 implementation:
# - **scd_start_date**: When the record version became effective
# - **scd_end_date**: When the record version was superseded (NULL for current records)
# - **scd_active**: Flag indicating if this is the current version (1) or historical (0)

# CELL ********************

def _add_scd2_columns_with_deletion_tracking(
    new_data,
    column_to_mark_source_data_deletion: str,
    delete_rows_with_value: str,
    source_timestamp_column_name: str
):
    """
    Add SCD2 columns with deletion tracking logic.
    
    Args:
        new_data (DataFrame): Input dimension data
        column_to_mark_source_data_deletion (str): Column indicating if record was deleted at source
        delete_rows_with_value (str): Value of column when data is deleted
        source_timestamp_column_name (str): Column containing the business timestamp for versioning
    
    Returns:
        DataFrame: DataFrame with SCD2 columns added
    """
    delete_marker_literal = _build_delete_marker_literal_for_dataframe(
        delete_rows_with_value=delete_rows_with_value,
        dataframe=new_data,
        column_name=column_to_mark_source_data_deletion
    )

    new_data = (
        new_data.withColumn("scd_start_date", f.col(source_timestamp_column_name))
        .withColumn("scd_end_date",
            f.when(
                f.col(column_to_mark_source_data_deletion) == delete_marker_literal, f.col(source_timestamp_column_name)
            ).otherwise(f.lit(None).cast(TimestampType())),
        )
        .withColumn("scd_active", f.when(f.col(column_to_mark_source_data_deletion) == delete_marker_literal, 0).otherwise(1))
    )
    return new_data


def _build_delete_marker_literal_for_dataframe(delete_rows_with_value, dataframe, column_name: str):
    """Cast a configured delete marker literal to the DataFrame column type."""
    column_data_type = None
    for field in dataframe.schema.fields:
        if field.name == column_name or field.name.lower() == column_name.lower():
            column_data_type = field.dataType
            break

    literal = f.lit(delete_rows_with_value)
    return literal.cast(column_data_type) if column_data_type is not None else literal


def _add_scd2_columns_without_deletion_tracking(
    new_data,
    source_timestamp_column_name: str
):
    """
    Add SCD2 columns without deletion tracking (all records treated as active).
    
    Args:
        new_data (DataFrame): Input dimension data
        source_timestamp_column_name (str): Column containing the business timestamp for versioning
    
    Returns:
        DataFrame: DataFrame with SCD2 columns added
    """
    new_data = (
        new_data.withColumn("scd_start_date", f.col(source_timestamp_column_name))
        .withColumn("scd_end_date", f.lit(None).cast(TimestampType()))
        .withColumn("scd_active", f.lit(1))
    )
    return new_data


def add_scd2_columns_for_dimensions(
    new_data,
    column_to_mark_source_data_deletion: str,
    delete_rows_with_value: str,
    source_timestamp_column_name: str,
    lakehouse_table_output: bool,
    first_run: bool,
    merge_type: str
):
    """
    Add SCD Type 2 tracking columns to dimension data for historical change tracking.

    This function enhances dimension tables with temporal columns required for SCD2 
    implementation, enabling point-in-time analysis and historical reporting.

    Args:
        new_data (DataFrame): Input dimension data requiring SCD2 columns
        column_to_mark_source_data_deletion (str): Column indicating if record was deleted at source
        delete_rows_with_value (str): value of column when data is deleted
        source_timestamp_column_name (str): Column containing the business timestamp for versioning
        lakehouse_table_output (bool): Whether output is a lakehouse table
        first_run (bool): Whether this is the first run for the table
        merge_type (str): The merge type for the table (SCD2 columns added when merge_type is 'scd2')

    Returns:
        DataFrame: Enhanced DataFrame with SCD2 tracking columns:
                  - scd_start_date: Record version effective date
                  - scd_end_date: Record version end date (NULL for current)
                  - scd_active: Current version indicator (1=current, 0=historical)

    Implementation Notes:
        - Deleted records get both start and end dates set to the source_timestamp_column_name value
        - Active records have NULL end dates to indicate they are current
        - The source_timestamp_column_name column serves as the version timestamp
    """

    if not lakehouse_table_output or not first_run or merge_type != "scd2":
        return new_data

    adding_scd2_columns_log_info = "Adding scd2 columns to dataframe before delta table creation."
    log_and_print(adding_scd2_columns_log_info)
    
    if column_to_mark_source_data_deletion:
        new_data = _add_scd2_columns_with_deletion_tracking(
            new_data=new_data,
            column_to_mark_source_data_deletion=column_to_mark_source_data_deletion,
            delete_rows_with_value=delete_rows_with_value,
            source_timestamp_column_name=source_timestamp_column_name
        )
    else:
        new_data = _add_scd2_columns_without_deletion_tracking(
            new_data=new_data,
            source_timestamp_column_name=source_timestamp_column_name
        )
    
    return new_data

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. SCD2 Post-Processing Updates
# 
# After initial SCD2 processing, this function ensures that single-version dimension records have their start dates set to the beginning of time (1900-01-01). This ensures these records are always available for historical fact table joins.

# CELL ********************

def post_processing_scd2_update(
    primary_keys: list,
    target_abfss_path: str,
    lakehouse_table_output: bool,
    merge_type: str,
    total_records_processed: int
) -> None:
    """
    Adjust SCD2 start dates for single-version dimension records to ensure historical availability.

    This post-processing step identifies dimension records that have only one version and sets 
    their start date to 1900-01-01. This ensures these records are available for all historical 
    fact data, preventing join failures for early transactions.

    Only runs when all conditions are met: output is a lakehouse table, merge_type is 'scd2',
    and records were actually processed.

    Args:
        primary_keys (list): Natural/business key columns identifying unique dimension members
        target_abfss_path (str): Target table name abfss path
        lakehouse_table_output (bool): Whether output is a lakehouse table
        merge_type (str): The merge type for the table (post-processing runs when merge_type is 'scd2')
        total_records_processed (int): Number of records processed

    Business Logic:
        - Single-version records represent dimension members that have never changed
        - Setting their start date to 1900-01-01 ensures they match all historical facts
        - Multi-version records retain their actual change dates for accurate history

    Performance Considerations:
        - Only processes records with exactly one version
        - Uses bulk merge operation for efficiency
        - Minimal impact on large dimension tables
    """
    if not (lakehouse_table_output and merge_type == "scd2" and total_records_processed > 0):
        return

    log_and_print("Running post processing for SCD2 dimension tables.")
    primary_key_columns = ', '.join([f"`{primary_key}`" for primary_key in primary_keys])
    primary_key_join_condition = ' AND '.join([
        f"target_df.`{primary_key}` <=> single_version_keys.`{primary_key}`"
        for primary_key in primary_keys
    ])

    # Identify dimension records with only one version
    dimensions_with_one_row = spark.sql(
        f"""
        WITH single_version_keys AS (
            SELECT      {primary_key_columns}
            FROM        delta.`{target_abfss_path}`
            GROUP BY    {primary_key_columns}
            HAVING      COUNT(*) = 1
        )
        SELECT      target_df.*
        FROM        delta.`{target_abfss_path}` AS target_df
        INNER JOIN  single_version_keys
        ON          {primary_key_join_condition}
        """)
    
    # Set start date to beginning of time for universal availability
    # NOTE: Use 'MM' (month), not 'mm' (minute). With 'mm' Spark silently drops the month
    # component because to_date ignores minute, defaulting month to January.
    dimensions_with_one_row = dimensions_with_one_row.withColumn("scd_start_date", f.to_date(f.lit('1900-01-01'), 'yyyy-MM-dd'))

    # Get Delta table reference for merge operation
    target_table = DeltaTable.forPath(spark, target_abfss_path)

    # Define merge aliases
    target_alias = "target_df"
    source_alias = "source_df"

    # Build null-safe merge condition
    merge_condition = get_merge_condition(
        merge_keys = primary_keys, right_alias = source_alias, left_alias = target_alias
    )
    
    # Execute merge to update start dates
    target_table.alias(target_alias).merge(
        source=dimensions_with_one_row.alias(source_alias), condition=merge_condition
    ).whenMatchedUpdate(
        set={"scd_start_date": f"{source_alias}.scd_start_date"}
    ).execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_schema_changes(new_schema: list, old_schema: list):
    """
    Compare two schemas and identify all differences including new, dropped, and changed columns.

    This function performs a comprehensive schema comparison to detect evolution patterns,
    which is critical for maintaining data quality and managing schema drift in the lakehouse.

    Args:
        new_schema (list): Current schema as list of (column_name, data_type) tuples
        old_schema (list): Previous schema as list of (column_name, data_type) tuples

    Returns:
        list: List of dictionaries describing schema changes:
              - column: Column name affected
              - data_type: Data type information (varies by change type)
              - change_type: Type of change ('Column Added', 'Column Dropped', 'Column Type Changed')

    Schema Change Detection:
        - Column Added: Present in new schema but not in old
        - Column Dropped: Present in old schema but not in new
        - Column Type Changed: Same column name but different data type

    Usage:
        Schema changes are logged for audit trails and can trigger different behaviors
        based on configuration (fail, warn, or auto-adapt).
    """
    # Convert schemas to dictionaries for efficient comparison
    old_schema_dict = dict(old_schema)
    new_schema_dict = dict(new_schema)

    # Identify additions - columns in new schema not in old
    new_columns = [{"column": col, "data_type": new_schema_dict[col], "change_type": "Column Added"} 
                for col in set(new_schema_dict.keys()) - set(old_schema_dict.keys())]

    # Identify deletions - columns in old schema not in new
    dropped_columns = [{"column": col, "data_type": old_schema_dict[col], "change_type": "Column Dropped"} 
                    for col in set(old_schema_dict.keys()) - set(new_schema_dict.keys())]

    # Identify type changes - same column name but different data type
    data_type_changes = [{"column": col, "data_type": f"{old_schema_dict[col]} --> {new_schema_dict[col]}", 
                        "change_type": "Column Type Changed"} 
                        for col in old_schema_dict.keys() & new_schema_dict.keys() 
                        if old_schema_dict[col] != new_schema_dict[col]]

    # Combine all changes into a single comprehensive list
    schema_changes = new_columns + dropped_columns + data_type_changes

    return schema_changes

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Spark Table Metadata Management
# 
# This section handles the updating of column descriptions in Spark tables, particularly for documenting primary keys. This enhances table discoverability and understanding for downstream consumers.

# CELL ********************

def _spark_sql_string_literal(value: str) -> str:
    """Format a Python string as a Spark SQL single-quoted literal."""
    return "'" + str(value or "").replace("'", "''") + "'"


def update_spark_column_descriptions(column_descriptions: list, primary_keys: list, target_datastore_name: str, target_table_name: str, target_workspace_name: str):
    """
    Update Spark table column descriptions to indicate primary key status.

    This function enhances table metadata by adding or removing "PRIMARY KEY" indicators
    in column descriptions, improving documentation and data discovery capabilities.

    Args:
        column_descriptions (list): Current column metadata from DESCRIBE TABLE
        primary_keys (list): List of columns that are primary keys
        target_datastore_name (str): Target lakehouse name
        target_table_name (str): Target table name
        target_workspace_name (str): Workspace containing the lakehouse

    Column Description Logic:
        - Primary key columns get "PRIMARY KEY" prefix (if not already present)
        - Non-primary key columns have "PRIMARY KEY" removed (if present)
        - Existing descriptions are preserved and appended after primary key indicator

    Benefits:
        - Improves table documentation for data consumers
        - Enables programmatic discovery of primary keys
        - Maintains consistency across all tables in the platform
    """
    if not primary_keys:
        primary_keys = []

    for column_description in column_descriptions:
        col_name = column_description['col_name']
        comment = column_description['comment']

        if not comment:
            comment = ""
        else:
            comment = comment.strip()

        if col_name in primary_keys and 'PRIMARY KEY' not in comment:
            if not comment:
                comment = "PRIMARY KEY"
            else:
                comment = f"PRIMARY KEY; {comment}"
            spark.sql(f"ALTER TABLE `{target_workspace_name}`.{target_datastore_name}.{target_table_name} ALTER COLUMN `{col_name}` COMMENT {_spark_sql_string_literal(comment)}")

        if col_name not in primary_keys and 'PRIMARY KEY' in comment:
            comment = comment.replace("PRIMARY KEY; ", "").replace("PRIMARY KEY", "")
            spark.sql(f"ALTER TABLE `{target_workspace_name}`.{target_datastore_name}.{target_table_name} ALTER COLUMN `{col_name}` COMMENT {_spark_sql_string_literal(comment)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Schema Hashing Utilities
# 
# This section provides utilities for generating deterministic hash values from schema definitions. These hashes are used to detect schema changes efficiently without comparing full schema strings.

# CELL ********************

def get_md5_of_string(input_string):
    """
    Generate MD5 hash of a string for schema comparison and change detection.

    This function creates a deterministic hash value that uniquely represents a schema,
    enabling efficient comparison of schemas across different runs without storing or
    comparing the full schema definition.

    Args:
        input_string (str): String representation of schema or any content to hash

    Returns:
        str: 32-character hexadecimal MD5 hash

    Use Cases:
        - Schema change detection: Compare hash values to detect any schema modifications
        - Schema versioning: Track schema evolution over time
        - Efficient storage: Store compact hash instead of full schema definition

    Note:
        MD5 is used for its speed and deterministic output. Cryptographic security
        is not required for this use case.
    """
    return hashlib.md5(input_string.encode("UTF-8")).hexdigest()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Entity Resolution Functions
# 
# This section contains advanced functions for Master Data Management (MDM) and entity resolution. These functions enable matching and linking of records across different data sources using various comparison techniques including exact matching, fuzzy matching, and phonetic algorithms.
# 
# ### Key Capabilities
# - **N-Gram Filtering**: Pre-filters potential matches using MinHash LSH for scalability
# - **Multiple Comparison Types**: Exact, fuzzy (Levenshtein), phonetic (Soundex), and numeric percent difference
# - **Configurable Matching Logic**: Define auto-match and manual review thresholds
# - **Self-Join Support**: Compare records within the same dataset for deduplication

# CELL ********************

def ngram_matching(
    primary_dataset_df: DataFrame,
    secondary_dataset_df: DataFrame,
    primary_dataset_id_column: str,
    secondary_dataset_id_column: str
    ) -> DataFrame:
    """
    Perform efficient N-gram based pre-filtering for fuzzy matching using MinHash LSH.

    This function implements a scalable approach to identify potential matches between
    two datasets using Locality Sensitive Hashing (LSH). It significantly reduces the
    comparison space for expensive fuzzy matching operations.

    Args:
        primary_dataset_df (DataFrame): First dataset with 'id' and 'name' columns
        secondary_dataset_df (DataFrame): Second dataset with 'id' and 'name' columns
        primary_dataset_id_column (str): Name for primary dataset ID in output
        secondary_dataset_id_column (str): Name for secondary dataset ID in output

    Returns:
        DataFrame: Potential matches with columns [primary_dataset_id_column, secondary_dataset_id_column]

    Algorithm Steps:
        1. Tokenize text into words for comparison
        2. Convert words to TF (Term Frequency) feature vectors
        3. Apply MinHash LSH for efficient similarity detection
        4. Find pairs with Jaccard distance ≤ 0.8 (configurable threshold)

    Performance Benefits:
        - Reduces O(n²) comparisons to approximately O(n)
        - Enables fuzzy matching on large datasets (millions of records)
        - Configurable threshold balances precision vs. recall
    """
    # Step 1: Tokenize names into words for comparison
    tokenizer = Tokenizer(inputCol="name", outputCol="words")
    primary_dataset_words = tokenizer.transform(primary_dataset_df)
    secondary_dataset_words = tokenizer.transform(secondary_dataset_df)
    
    # Step 2: Convert words to Term Frequency feature vectors
    # Using 10,000 features provides good balance of accuracy and performance
    hash_tf = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=10000)
    primary_dataset_featurized = hash_tf.transform(primary_dataset_words)
    secondary_dataset_featurized = hash_tf.transform(secondary_dataset_words)
    
    # Step 3: Fit MinHash LSH model for similarity detection
    # 5 hash tables provides good accuracy with reasonable performance
    mh = MinHashLSH(inputCol="rawFeatures", outputCol="hashes", numHashTables=5)
    model = mh.fit(primary_dataset_featurized)
    
    # Step 4: Transform both datasets to include hash signatures
    primary_dataset_hashed = model.transform(primary_dataset_featurized)
    secondary_dataset_hashed = model.transform(secondary_dataset_featurized)
    
    # Step 5: Find similar pairs using approximate similarity join
    # Threshold of 0.8 captures most relevant matches while filtering noise
    similar_pairs = model.approxSimilarityJoin(
        primary_dataset_hashed, secondary_dataset_hashed, threshold=.3, distCol="JaccardDistance"
    )
    
    # Step 6: Extract and rename ID columns for output
    result = similar_pairs.select(
        f.col("datasetA.id").alias(primary_dataset_id_column),
        f.col("datasetB.id").alias(secondary_dataset_id_column)
    )

    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def _get_id_columns(
    config: Dict[str, Any]
) -> Tuple[str, str]:
    """Get primary and secondary dataset ID column names."""
    primary_id = f"{config['primary_dataset_alias']}_match_id"
    secondary_id = f"{config['secondary_dataset_alias']}_match_id"
    return primary_id, secondary_id

def _apply_self_comparison_filter(
    df_comparison: DataFrame, 
    config: Dict[str, Any]
) -> DataFrame:
    """Apply self-comparison filter to avoid duplicate comparisons."""
    if config['is_self_comparison']:
        primary_id, secondary_id = _get_id_columns(
            config = config
        )
        return df_comparison.filter(f.col(primary_id) < f.col(secondary_id))
    return df_comparison

def _merge_comparison_results(
    df_comparison_all: DataFrame, 
    df_comparison: DataFrame, 
    config: Dict[str, Any], 
    join_type: str = "FULL OUTER JOIN"
) -> DataFrame:
    """Merge new comparison results with accumulated results using SQL."""
    primary_id, secondary_id = _get_id_columns(
        config = config
    )
    id_columns = (primary_id, secondary_id)
    
    df_comparison_all_non_id_cols = [c for c in df_comparison_all.columns if c not in id_columns]
    df_comparison_non_id_cols = [c for c in df_comparison.columns if c not in id_columns]
    non_id_cols = df_comparison_non_id_cols + df_comparison_all_non_id_cols
    non_id_cols_str = ', '.join(non_id_cols)

    # Use unique view names to avoid collisions in HC mode
    view_suffix = f"_{uuid.uuid4().hex[:8]}"
    df_comparison.createOrReplaceTempView(f'df_comparison{view_suffix}')
    df_comparison_all.createOrReplaceTempView(f'df_comparison_all{view_suffix}')

    return spark.sql(f"""
        SELECT      COALESCE(a.{primary_id}, b.{primary_id}) `{primary_id}`
                    ,COALESCE(a.{secondary_id}, b.{secondary_id}) `{secondary_id}`
                    ,{non_id_cols_str}
        FROM        df_comparison_all{view_suffix} as a
        {join_type} df_comparison{view_suffix} as b
        ON          a.{primary_id} = b.{primary_id}
        AND         a.{secondary_id} = b.{secondary_id}
    """)

def _has_comparison_rows(
    df_comparison_all: DataFrame
) -> bool:
    """Check whether accumulated comparison results have any rows without scanning for an exact count."""
    return bool(df_comparison_all.take(1))

def _extract_entity_configuration(
    data_transformation_config: Dict[str, Any]
) -> Dict[str, Any]:
    """Extract and validate configuration parameters for entity resolution."""
    secondary_dataset_query = data_transformation_config.get("secondary_dataset_query")
    is_self_comparison = not bool(secondary_dataset_query)
    
    primary_dataset_comparison_fields = [field.strip() for field in data_transformation_config.get("primary_dataset_comparison_fields").split(",")]
    
    if is_self_comparison:
        secondary_dataset_comparison_fields = primary_dataset_comparison_fields
    else:
        secondary_dataset_comparison_fields = [field.strip() for field in data_transformation_config.get("secondary_dataset_comparison_fields").split(",")]

    comparison_types = [ctype.strip() for ctype in data_transformation_config.get("comparison_types").split(",")]

    Invalid_comparison_types = set(comparison_types) - set(['fuzzy', 'exact', 'soundex', 'percent_difference'])
    if Invalid_comparison_types:
        raise Exception(f"The following comparison types were invalid: {Invalid_comparison_types}. " +
                       "The only valid comparison types are `fuzzy`, `exact`, and `soundex`, `percent_difference`.")

    return {
        'secondary_dataset_query': secondary_dataset_query,
        'is_self_comparison': is_self_comparison,
        'primary_dataset_comparison_fields': primary_dataset_comparison_fields,
        'secondary_dataset_comparison_fields': secondary_dataset_comparison_fields,
        'comparison_types': comparison_types,
        'primary_dataset_alias': data_transformation_config.get("primary_dataset_alias").strip(),
        'secondary_dataset_alias': data_transformation_config.get("secondary_dataset_alias").strip(),
        'only_fuzzy_match_if_one_exact_match': data_transformation_config.get("only_fuzzy_match_if_one_exact_match", "true").strip().lower() == "true",
        'use_ngram_filtering_for_fuzzy_comparisons': data_transformation_config.get("use_ngram_filtering_for_fuzzy_comparisons", "false").strip().lower() == "true",
        'auto_match_logic': data_transformation_config.get("auto_match_logic"),
        'match_with_manual_review_logic': data_transformation_config.get("match_with_manual_review_logic").strip()
    }

def _prepare_datasets(
    primary_dataset_df: DataFrame, 
    config: Dict[str, Any]
) -> Tuple[DataFrame, DataFrame, List[Tuple[str, str, str]]]:
    """Prepare primary and secondary datasets with unique identifiers and aliased columns."""
    df1 = primary_dataset_df.alias(config['primary_dataset_alias']).withColumn("match_id", f.monotonically_increasing_id())

    if config['is_self_comparison']:
        df2 = df1.select("*").alias(config['secondary_dataset_alias'])
    else:
        df2 = spark.sql(config['secondary_dataset_query']).alias(config['secondary_dataset_alias']).withColumn("match_id", f.monotonically_increasing_id())

    # Alias columns with dataset prefixes
    df1_cols = [f.col(c).alias(f"{config['primary_dataset_alias']}_{c}") for c in df1.columns]
    df1 = df1.select(*df1_cols)
    df2_cols = [f.col(c).alias(f"{config['secondary_dataset_alias']}_{c}") for c in df2.columns]
    df2 = df2.select(*df2_cols)

    comparisons = list(zip(config['primary_dataset_comparison_fields'], config['secondary_dataset_comparison_fields'], config['comparison_types']))

    return df1, df2, comparisons

def _perform_exact_soundex_comparisons(
    df1: DataFrame, 
    df2: DataFrame, 
    comparisons: List[Tuple[str, str, str]], 
    config: Dict[str, Any]
) -> DataFrame:
    """Perform exact and soundex comparisons between datasets."""
    equality_comparisons = [(comparison[0], comparison[1], comparison[2]) 
                           for comparison in comparisons 
                           if comparison[2] in ('soundex', 'exact')]

    primary_id, secondary_id = _get_id_columns(
        config = config
    )
    df_comparison_all = spark.createDataFrame([], f"{primary_id} int, {secondary_id} int")

    for field1, field2, comparison_type in equality_comparisons:
        df1_field = f"{config['primary_dataset_alias']}_{field1}"
        df2_field = f"{config['secondary_dataset_alias']}_{field2}"

        if comparison_type == "exact":
            df1_comparison = df1.select(f.col(primary_id), f.col(df1_field))
            df2_comparison = df2.select(f.col(secondary_id), f.col(df2_field))
        elif comparison_type == "soundex":
            df1_comparison = df1.select(f.col(primary_id), f.soundex(f.col(df1_field)).alias(df1_field))
            df2_comparison = df2.select(f.col(secondary_id), f.soundex(f.col(df2_field)).alias(df2_field))

        df_comparison = df1_comparison.join(df2_comparison, f.col(df1_field) == f.col(df2_field), "inner")
        df_comparison = _apply_self_comparison_filter(
            df_comparison = df_comparison,
            config = config
        )
        df_comparison = df_comparison.withColumn(f"{df1_field}_{df2_field}_{comparison_type}", f.lit(1))
        df_comparison_all = _merge_comparison_results(
            df_comparison_all = df_comparison_all,
            df_comparison = df_comparison,
            config = config
        )

    return df_comparison_all

def _perform_fuzzy_comparisons(
    df1: DataFrame, 
    df2: DataFrame, 
    comparisons: List[Tuple[str, str, str]], 
    df_comparison_all: DataFrame, 
    config: Dict[str, Any]
) -> DataFrame:
    """Perform fuzzy matching using Levenshtein distance."""
    fuzzy_comparisons = [(comparison[0], comparison[1], comparison[2]) 
                         for comparison in comparisons 
                         if comparison[2] == 'fuzzy']

    if not fuzzy_comparisons:
        return df_comparison_all

    primary_id, secondary_id = _get_id_columns(
        config = config
    )

    # Row existence is monotonic across comparison merges, so keep the flag once it becomes true.
    has_existing_matches = _has_comparison_rows(df_comparison_all)

    for fuzzy_index, (field1, field2, _) in enumerate(fuzzy_comparisons):
        df1_field = f"{config['primary_dataset_alias']}_{field1}"
        df2_field = f"{config['secondary_dataset_alias']}_{field2}"

        # Prepare cleaned fields for fuzzy comparison
        df1_comparison = df1.select(f.col(primary_id), f.col(df1_field)).withColumn(
            f"{df1_field}_clean4fuzz", 
            f.regexp_replace(f.regexp_replace(f.trim(f.lower(f.col(df1_field))), r"[^\w\s]", " "), r"\s+", " ")
        )
        df2_comparison = df2.select(f.col(secondary_id), f.col(df2_field)).withColumn(
            f"{df2_field}_clean4fuzz", 
            f.regexp_replace(f.regexp_replace(f.trim(f.lower(f.col(df2_field))), r"[^\w\s]", " "), r"\s+", " ")
        )
        
        # Decide which comparison strategy to use
        if config['only_fuzzy_match_if_one_exact_match'] and has_existing_matches:
            # Option 1: Join against existing exact/soundex matches (when flag is enabled)
            df_comparison = df_comparison_all.join(df1_comparison, primary_id, 'left') \
                                             .join(df2_comparison, secondary_id, 'left') \
                                             .select(primary_id, secondary_id, df1_field, df2_field,
                                                     f"{df1_field}_clean4fuzz", f"{df2_field}_clean4fuzz")
        elif config['use_ngram_filtering_for_fuzzy_comparisons']:
            # Use n-gram pre-filtering for scalability (only when explicitly enabled or auto-determined)
            df1_ngram = df1_comparison.select(f.col(primary_id).alias('id'), f.col(f"{df1_field}_clean4fuzz").alias('name'))
            df2_ngram = df2_comparison.select(f.col(secondary_id).alias('id'), f.col(f"{df2_field}_clean4fuzz").alias('name'))

            ngram_matches = ngram_matching(
                primary_dataset_df = df1_ngram,
                secondary_dataset_df = df2_ngram,
                primary_dataset_id_column = primary_id,
                secondary_dataset_id_column = secondary_id
            )
            df_comparison = ngram_matches.join(df1_comparison, primary_id, 'left') \
                                         .join(df2_comparison, secondary_id, 'left') \
                                         .select(primary_id, secondary_id, df1_field, df2_field, 
                                                f"{df1_field}_clean4fuzz", f"{df2_field}_clean4fuzz")
        else:
            # No n-gram filtering - use direct cross join
            df_comparison = df1_comparison.crossJoin(df2_comparison) \
                                          .select(primary_id, secondary_id, df1_field, df2_field,
                                                  f"{df1_field}_clean4fuzz", f"{df2_field}_clean4fuzz")

        df_comparison = _apply_self_comparison_filter(
            df_comparison = df_comparison,
            config = config
        )
        
        left_clean = f.coalesce(f.col(f"{df1_field}_clean4fuzz"), f.lit(""))
        right_clean = f.coalesce(f.col(f"{df2_field}_clean4fuzz"), f.lit(""))
        max_clean_length = f.greatest(f.length(left_clean), f.length(right_clean))

        # Calculate normalized Levenshtein similarity. Empty/empty is a perfect match.
        df_comparison = df_comparison.withColumn(f"{df1_field}_{df2_field}_fuzzy", 
            f.when(max_clean_length == 0, f.lit(1.0))
             .otherwise(1 - (f.levenshtein(left_clean, right_clean) / max_clean_length)))

        df_comparison_all = _merge_comparison_results(
            df_comparison_all = df_comparison_all,
            df_comparison = df_comparison,
            config = config
        )

        if not has_existing_matches and fuzzy_index < len(fuzzy_comparisons) - 1:
            has_existing_matches = _has_comparison_rows(df_comparison_all)

    return df_comparison_all

def _perform_percent_difference_comparisons(
    df1: DataFrame, 
    df2: DataFrame, 
    comparisons: List[Tuple[str, str, str]], 
    df_comparison_all: DataFrame, 
    config: Dict[str, Any]
) -> DataFrame:
    """Perform percent difference comparisons for numeric fields."""
    percent_difference_comparisons = [(comparison[0], comparison[1], comparison[2]) 
                                     for comparison in comparisons 
                                     if comparison[2] == 'percent_difference']

    if not percent_difference_comparisons:
        return df_comparison_all

    primary_id, secondary_id = _get_id_columns(
        config = config
    )

    has_existing_matches = _has_comparison_rows(df_comparison_all)

    for percent_index, (field1, field2, _) in enumerate(percent_difference_comparisons):
        df1_field = f"{config['primary_dataset_alias']}_{field1}"
        df2_field = f"{config['secondary_dataset_alias']}_{field2}"

        df1_comparison = df1.select(f.col(primary_id), f.col(df1_field))
        df2_comparison = df2.select(f.col(secondary_id), f.col(df2_field))
        
        if has_existing_matches:
            # Join against existing comparisons
            df_comparison = df_comparison_all.join(df1_comparison, primary_id, 'left') \
                                             .join(df2_comparison, secondary_id, 'left') \
                                             .select(primary_id, secondary_id, df1_field, df2_field)
        else:
            # No existing comparisons - do cross join
            df_comparison = df1_comparison.crossJoin(df2_comparison) \
                                          .select(primary_id, secondary_id, df1_field, df2_field)

        df_comparison = _apply_self_comparison_filter(
            df_comparison = df_comparison,
            config = config
        )
        
        # Calculate percent difference
        df_comparison = df_comparison.withColumn(
            f"{df1_field}_{df2_field}_percent_difference",
            f.abs((f.col(df2_field) - f.col(df1_field)) / f.col(df1_field) * 100)
        )

        df_comparison_all = _merge_comparison_results(
            df_comparison_all = df_comparison_all,
            df_comparison = df_comparison,
            config = config,
            join_type = "LEFT JOIN" if has_existing_matches else "FULL OUTER JOIN"
        )

        if not has_existing_matches and percent_index < len(percent_difference_comparisons) - 1:
            has_existing_matches = _has_comparison_rows(df_comparison_all)

    return df_comparison_all

def _apply_matching_rules(
    df_comparison_all: DataFrame, 
    config: Dict[str, Any]
) -> DataFrame:
    """Apply auto-match and manual review logic to categorize results."""
    df_comparison_all = df_comparison_all.alias('df_comparison_all')
    primary_id, secondary_id = _get_id_columns(
        config = config
    )

    auto_matches = df_comparison_all.filter(config['auto_match_logic']).alias('auto_matches')
    auto_matches = auto_matches.select(f.lit("Auto_Match").alias("match_outcome"), "*")

    non_auto_matches = df_comparison_all.join(
        auto_matches,
        on=(f.col(f"df_comparison_all.{primary_id}") == f.col(f"auto_matches.{primary_id}")) &
           (f.col(f"df_comparison_all.{secondary_id}") == f.col(f"auto_matches.{secondary_id}")),
        how="leftanti"
    ).select(df_comparison_all["*"])

    manual_review_matches = non_auto_matches.filter(config['match_with_manual_review_logic'])
    manual_review_matches = manual_review_matches.select(f.lit("Match_For_Manual_Review").alias("match_outcome"), "*")

    return auto_matches.unionByName(manual_review_matches).alias('matches')

def _apply_transitive_mapping(
    matches_full_dataset: DataFrame, 
    config: Dict[str, Any]
) -> DataFrame:
    """Assign transitive match groups with role-aware sparse graph components."""
    from scipy.sparse import coo_matrix
    from scipy.sparse.csgraph import connected_components
    from pyspark.sql.types import LongType, StructField, StructType
    
    primary_id, secondary_id = _get_id_columns(
        config = config
    )
    mapping_pairs = matches_full_dataset.select(primary_id, secondary_id).toPandas().values.tolist()

    node_indexes = {}
    edge_source_indexes = []
    edge_target_indexes = []
    is_self_comparison = config['is_self_comparison']
    for primary_value, secondary_value in mapping_pairs:
        primary_node = primary_value if is_self_comparison else ("primary", primary_value)
        secondary_node = secondary_value if is_self_comparison else ("secondary", secondary_value)
        primary_index = node_indexes.setdefault(primary_node, len(node_indexes))
        secondary_index = node_indexes.setdefault(secondary_node, len(node_indexes))
        edge_source_indexes.append(primary_index)
        edge_target_indexes.append(secondary_index)

    group_map = {}
    if node_indexes:
        graph = coo_matrix(
            (
                [True] * len(edge_source_indexes),
                (edge_source_indexes, edge_target_indexes),
            ),
            shape=(len(node_indexes), len(node_indexes)),
            dtype=bool,
        ).tocsr()
        _, component_labels = connected_components(graph, directed=False, return_labels=True)

        component_group_ids = {}
        for node, node_index in node_indexes.items():
            component_label = int(component_labels[node_index])
            group_id = component_group_ids.setdefault(component_label, len(component_group_ids) + 1)
            if is_self_comparison or node[0] == "primary":
                record_id = node if is_self_comparison else node[1]
                group_map[int(record_id)] = group_id

    mapping_schema = StructType([
        StructField("id", LongType(), False),
        StructField("match_group_id", LongType(), False)
    ])
    mapping_groups = spark.createDataFrame(
        [(record_id, group_id) for record_id, group_id in group_map.items()],
        mapping_schema,
    )

    return matches_full_dataset.join(
        mapping_groups,
        matches_full_dataset[primary_id] == mapping_groups['id'],
        how="left"
    ).select(mapping_groups["match_group_id"], matches_full_dataset["*"])

def _assemble_final_results(
    df1: DataFrame, 
    df2: DataFrame, 
    matches: DataFrame, 
    matches_full_dataset_grouped: DataFrame, 
    config: Dict[str, Any]
) -> DataFrame:
    """Combine matched and unmatched records into final result set."""
    primary_id, secondary_id = _get_id_columns(
        config = config
    )

    # Join matches with original data
    matches_columns = set(matches.columns)
    df1_columns = [col.lower() for col in df1.columns if col.lower() not in matches_columns]
    df2_columns = [col for col in df2.columns if col.lower() not in matches_columns]

    matches_full_dataset = matches.join(df1, primary_id, 'left') \
                                  .join(df2, secondary_id, 'left') \
                                  .select(matches["*"], *df1_columns, *df2_columns)

    # Identify unmatched records
    if config['is_self_comparison']:
        df1_no_matches = df1.join(matches_full_dataset_grouped, 
                                 on=(df1[primary_id] == matches_full_dataset_grouped[primary_id]) | 
                                    (df1[primary_id] == matches_full_dataset_grouped[secondary_id]),
                                 how='leftanti')
    else:
        df1_no_matches = df1.join(matches_full_dataset_grouped, primary_id, 'leftanti')
    
    df1_no_matches = df1_no_matches.withColumn('match_outcome', f.lit('No_Match'))
    all_data = matches_full_dataset_grouped.unionByName(df1_no_matches, allowMissingColumns=True)

    if not config['is_self_comparison']:
        df2_no_matches = df2.join(matches_full_dataset_grouped, secondary_id, 'leftanti') \
                            .withColumn('match_outcome', f.lit('No_Match'))
        all_data = all_data.unionByName(df2_no_matches, allowMissingColumns=True)

    # Clean up intermediate columns
    all_data_columns = [col for col in all_data.columns if not col.endswith('_clean4fuzz')]
    return all_data.select(*all_data_columns)

def entity_resolution(
    primary_dataset_df: DataFrame, 
    data_transformation_config: Dict[str, Any]
) -> DataFrame:
    """
    Comprehensive entity resolution with multiple comparison techniques for record matching.

    This function implements a sophisticated entity matching system that combines multiple
    comparison techniques to identify duplicate or related records across datasets. It supports
    both cross-dataset matching and self-deduplication scenarios.

    Args:
        primary_dataset_df (DataFrame): Primary dataset for entity resolution
        data_transformation_config (dict): Configuration dictionary containing:
            - secondary_dataset_query: SQL query for secondary dataset (optional)
            - primary_dataset_comparison_fields: Comma-separated field names
            - secondary_dataset_comparison_fields: Comma-separated field names
            - comparison_types: Comma-separated comparison types
            - primary_dataset_alias: Alias for primary dataset
            - secondary_dataset_alias: Alias for secondary dataset
            - only_fuzzy_match_if_one_exact_match: "true"/"false" (default: "true")
            - use_ngram_filtering_for_fuzzy_comparisons: "true"/"false" (default: "false")
                * "true": Always use MinHash LSH n-gram pre-filtering for fuzzy matches
                * "false": Never use n-gram filtering (direct cross join + Levenshtein for fuzzy)
            - auto_match_logic: SQL expression for automatic matches
            - match_with_manual_review_logic: SQL expression for review matches

    Returns:
        DataFrame: Complete dataset with match outcomes:
            - All records from primary dataset
            - match_outcome column: 'Auto_Match', 'Match_For_Manual_Review', or 'No_Match'
            - Comparison score columns for each field comparison
            - Matched records from secondary dataset (if applicable)

    Comparison Types Supported:
        1. exact: Direct equality comparison
        2. soundex: Phonetic matching for names
        3. fuzzy: Levenshtein distance-based similarity (0-1 scale)
        4. percent_difference: Numeric difference as percentage

    N-gram Filtering Behavior (for fuzzy matching only):
        - Word-level tokenization works well for multi-word text (e.g., company names)
        - Single-word comparisons (e.g., "Robert" vs "Roberta") may not benefit from n-grams
        - For small datasets, cross join is often faster than n-gram overhead
        - N-gram filtering only applies to fuzzy (Levenshtein) comparisons, not exact/soundex
        - Default is "false" - use "true" only for large multi-word text datasets

    Processing Flow:
        1. Prepare datasets with unique identifiers
        2. Perform exact and soundex comparisons
        3. Apply fuzzy matching (with optional pre-filtering)
        4. Calculate percent differences for numeric fields
        5. Apply matching rules to categorize results
        6. Combine matched and unmatched records

    Performance Optimizations:
        - Optional n-gram pre-filtering for large-scale fuzzy matching
        - Incremental join approach to build comparison results
        - Self-join optimization to avoid duplicate comparisons
    """
    running_entity_resolution_log_info = f"Running entity resolution logic."
    log_and_print(running_entity_resolution_log_info)
    
    config = _extract_entity_configuration(
        data_transformation_config = data_transformation_config
    )
    df1, df2, comparisons = _prepare_datasets(
        primary_dataset_df = primary_dataset_df,
        config = config
    )
    
    # Perform all comparison types
    df_comparison_all = _perform_exact_soundex_comparisons(
        df1 = df1,
        df2 = df2,
        comparisons = comparisons,
        config = config
    )
    df_comparison_all = _perform_fuzzy_comparisons(
        df1 = df1,
        df2 = df2,
        comparisons = comparisons,
        df_comparison_all = df_comparison_all,
        config = config
    )
    df_comparison_all = _perform_percent_difference_comparisons(
        df1 = df1,
        df2 = df2,
        comparisons = comparisons,
        df_comparison_all = df_comparison_all,
        config = config
    )
    
    matches = _apply_matching_rules(
        df_comparison_all = df_comparison_all,
        config = config
    )
    
    # Join with original data and apply transitive mapping
    primary_id, secondary_id = _get_id_columns(
        config = config
    )
    matches_columns = set(matches.columns)
    df1_columns = [col.lower() for col in df1.columns if col.lower() not in matches_columns]
    df2_columns = [col for col in df2.columns if col.lower() not in matches_columns]

    matches_full_dataset = matches.join(df1, primary_id, 'left') \
                                  .join(df2, secondary_id, 'left') \
                                  .select(matches["*"], *df1_columns, *df2_columns)
    
    matches_full_dataset_grouped = _apply_transitive_mapping(
        matches_full_dataset = matches_full_dataset,
        config = config
    )
    
    return _assemble_final_results(
        df1 = df1,
        df2 = df2,
        matches = matches,
        matches_full_dataset_grouped = matches_full_dataset_grouped,
        config = config
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================================
# CUSTOM FUNCTION LOADING CONSTANTS
# ===========================================================================================

# Paths for custom function .py files selected by metadata scope.
CUSTOM_FUNCTIONS_FOLDER = "custom_functions"
CENTRAL_CUSTOM_FUNCTIONS_FOLDER = "central_custom_functions"
CUSTOM_FUNCTIONS_WORKSPACE_ID = ""
CUSTOM_FUNCTION_LOAD_MODE = "file"
CUSTOM_FUNCTION_SOURCE_MAX_BYTES = 20 * 1024 * 1024


def configure_custom_function_runtime(workspace_id: str = None, load_mode: str = "file") -> None:
    """Set the product-scoped custom function loading runtime."""
    global CUSTOM_FUNCTIONS_WORKSPACE_ID, CUSTOM_FUNCTION_LOAD_MODE

    CUSTOM_FUNCTIONS_WORKSPACE_ID = str(workspace_id or "").strip()
    normalized_load_mode = str(load_mode or "file").strip().lower()
    if normalized_load_mode not in {"file", "notebook"}:
        raise ValueError("custom_function_load_mode must be one of: file, notebook.")
    CUSTOM_FUNCTION_LOAD_MODE = normalized_load_mode


def read_text_file(file_path: str, max_bytes: int = CUSTOM_FUNCTION_SOURCE_MAX_BYTES) -> str:
    """Read local or ABFSS text content."""
    if str(file_path or "").lower().startswith("abfss://"):
        return notebookutils.fs.head(file_path, max_bytes)

    with open(file_path, 'r', encoding='utf-8') as f_handle:
        return f_handle.read()


def _get_custom_function_file_candidates(
    module_name: str,
    file_base_path: str = "",
    folder_names=None,
) -> list:
    """Return file paths for the selected custom function folder."""
    selected_folders = tuple(folder_names or (CUSTOM_FUNCTIONS_FOLDER,))
    normalized_base_path = str(file_base_path or "").strip().rstrip('/')
    if normalized_base_path:
        # The caller's base path ends with the product custom functions folder;
        # swapping that final segment keeps both candidates under the same Files root.
        files_root = normalized_base_path.rsplit('/', 1)[0]
        return [
            (
                f"{files_root}/{folder_name}/{module_name}.py",
                f"{files_root}/{folder_name}/{module_name}.py",
            )
            for folder_name in selected_folders
        ]

    return [
        (
            f"/lakehouse/default/Files/{folder_name}/{module_name}.py",
            f"Files/{folder_name}/{module_name}.py",
        )
        for folder_name in selected_folders
    ]


def instantiate_notebook(
    notebook_name: str,
    max_retries: int = 3,
    function_scope: str = "local",
) -> None:
    """
    Load custom functions from the explicitly selected local or central scope.
    
    This function supports two loading modes configured by
    configure_custom_function_runtime(load_mode=...):
    1. **file**: Load a local .py file from Files/custom_functions.
    2. **notebook**: Load a local notebook definition through the Fabric API.

    Central scope always loads the deployed .py file through the product
    lakehouse's Files/central_custom_functions shortcut.

    Args:
        notebook_name (str): Name of the custom function module/notebook to load.
                            For .py files, this is the filename without extension.
                            For notebooks, this is the Fabric notebook name.
        max_retries (int): Maximum retry attempts for notebook API loading (default: 3)
        function_scope (str): local (default) or central.
    
    Example:
        >>> # In production (loads from Files/custom_functions/NB_Custom_Products.py)
        >>> instantiate_notebook("NB_Custom_Products")
        >>> 
        >>> # In dev (loads the live local notebook definition)
        >>> instantiate_notebook("NB_Custom_Products")

        >>> # Shared function (loads the central shortcut file in every environment)
        >>> instantiate_notebook("NB_Shared_Products", function_scope="central")
        >>> 
        >>> # Now custom functions are available
        >>> result = my_custom_transformation(df)
    
    Note:
        - Production: local .py files are read and executed in the shared namespace
        - Development: local notebook definitions are loaded through the notebook API
        - Central: shared .py files are read through the central shortcut
        - Both paths execute in the shared session namespace so custom functions
          can reference helpers from NB_Helper_Functions (e.g., _mount_lakehouse_for_local_access)
    """
    normalized_scope = str(function_scope or "local").strip().lower()
    if normalized_scope not in {"local", "central"}:
        raise ValueError("function_scope must be one of: local, central.")

    if normalized_scope == "central":
        if _try_load_from_lakehouse_file(
            notebook_name,
            folder_names=(CENTRAL_CUSTOM_FUNCTIONS_FOLDER,),
        ):
            return

        try:
            shortcut_exists = notebookutils.fs.exists(f"Files/{CENTRAL_CUSTOM_FUNCTIONS_FOLDER}")
        except Exception:
            shortcut_exists = False
        if not shortcut_exists:
            raise FileNotFoundError(
                f"Central custom function shortcut 'Files/{CENTRAL_CUSTOM_FUNCTIONS_FOLDER}' is missing. "
                "Deploy or reconcile the product workspace before running central functions."
            )
        raise FileNotFoundError(
            f"Central custom function '{notebook_name}' was not found at "
            f"Files/{CENTRAL_CUSTOM_FUNCTIONS_FOLDER}/{notebook_name}.py. "
            "Publish the function from the central workspace first."
        )

    if CUSTOM_FUNCTION_LOAD_MODE == "notebook":
        log_and_print(f"Loading local custom function '{notebook_name}' from notebook API (notebook mode).")
        _load_via_notebook_api(notebook_name, max_retries)
        return

    if _try_load_from_lakehouse_file(
        notebook_name,
        folder_names=(CUSTOM_FUNCTIONS_FOLDER,),
    ):
        return

    searched_paths = ', '.join(
        exists_path
        for _, exists_path in _get_custom_function_file_candidates(
            notebook_name,
            folder_names=(CUSTOM_FUNCTIONS_FOLDER,),
        )
    )
    raise FileNotFoundError(
        f"Local custom function '{notebook_name}' was not found in [{searched_paths}] "
        "and custom_function_load_mode='file'."
    )


def _try_load_from_lakehouse_file(module_name: str, folder_names=None) -> bool:
    """
    Load a custom function from the first matching lakehouse Files folder.
    
    This is the preferred production method - .py files are deployed via CI/CD
    and executed in the current IPython session so that custom functions can
    reference helper functions from NB_Helper_Functions (e.g., log_and_print,
    _mount_lakehouse_for_local_access, _get_datastore_config).
    
    Args:
        module_name (str): Name of the module (without .py extension)
    
    Returns:
        bool: True if successfully loaded, False if neither file exists
    
    Note:
        The .py file is read and executed via IPython's run_cell() rather than
        importlib. This ensures functions are defined in the shared session
        namespace where they can access helper functions from other notebooks.
        Product-local Files/custom_functions overrides Files/central_custom_functions.
    """
    for py_file_path, exists_path in _get_custom_function_file_candidates(
        module_name,
        folder_names=folder_names,
    ):
        try:
            file_exists = notebookutils.fs.exists(exists_path)
        except Exception:
            file_exists = False

        if not file_exists:
            continue

        log_and_print(f"Loading custom functions from '{py_file_path}' (production mode).")

        # Once a higher-precedence file exists, read and execution failures must
        # surface instead of silently selecting a lower-precedence implementation.
        code = read_text_file(py_file_path)
        compiled = compile(code, py_file_path, 'exec')
        exec(compiled, globals())

        log_and_print(f"Successfully loaded custom functions from '{module_name}.py'.")
        return True

    return False


def _load_via_notebook_api(
    notebook_name: str,
    max_retries: int
) -> None:
    """
    Load custom functions using the Fabric notebook getDefinition() API.
    
    This is the notebook loading method used when custom_function_load_mode is
    set to 'notebook'. Retrieves notebook content via API and executes code
    cells via IPython.
    
    In production environments, .py files should be deployed to the lakehouse
    Files folder, which loads directly from disk instead of this method.
    
    Args:
        notebook_name (str): Name of the Fabric notebook to load
        max_retries (int): Maximum number of retry attempts
    
    Raises:
        Exception: If all retry attempts fail
    """
    last_exception = None
    
    for attempt in range(1, max_retries + 1):
        log_and_print(f"Instantiating notebook '{notebook_name}' via API (attempt {attempt}/{max_retries}).")

        try:
            code_cells = _get_notebook_code_cells(notebook_name)
        except Exception as e:
            last_exception = e
            log_and_print(f"Attempt {attempt}/{max_retries} failed to retrieve the notebook: {str(e)}")
        else:
            try:
                _execute_code_cells(code_cells)
            except Exception as e:
                last_exception = e
                log_and_print(f"Attempt {attempt}/{max_retries} failed while executing the notebook: {str(e)}")
            else:
                log_and_print(f"Successfully instantiated notebook '{notebook_name}'.")
                return

        if attempt < max_retries:
            wait_time = 2 ** attempt  # Exponential backoff: 2, 4, 8 seconds
            log_and_print(f"Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
    
    raise Exception(f"Failed to instantiate notebook '{notebook_name}' after {max_retries} attempts. Last error: {str(last_exception)}")


def _get_notebook_code_cells(notebook_name: str) -> list:
    """
    Retrieve and parse a Fabric notebook, returning only the code cells.
    
    Args:
        notebook_name (str): Name of the Fabric notebook to retrieve
        
    Returns:
        list: List of code cell dictionaries from the notebook
    """
    # Get notebook definition (returns .ipynb JSON format)
    if CUSTOM_FUNCTIONS_WORKSPACE_ID:
        notebook_definition = notebookutils.notebook.getDefinition(
            notebook_name,
            workspaceId=CUSTOM_FUNCTIONS_WORKSPACE_ID,
            format="ipynb"
        )
    else:
        notebook_definition = notebookutils.notebook.getDefinition(notebook_name)
    log_and_print(f"Notebook definition retrieved ({len(notebook_definition)} characters total, showing first 500): {notebook_definition[:500]}...")
    
    # Parse the notebook JSON and extract code cells
    notebook_json = json.loads(notebook_definition)
    cells = notebook_json.get('cells', [])
    code_cells = [cell for cell in cells if cell.get('cell_type') == 'code']
    
    log_and_print(f"Found {len(code_cells)} code cells to execute.")
    return code_cells


def _execute_code_cells(code_cells: list) -> None:
    """
    Execute a list of notebook code cells using IPython's run_cell().
    
    Args:
        code_cells (list): List of cell dictionaries with 'source' keys
    """
    ipython = get_ipython()
    
    for cell in code_cells:
        source = cell.get('source', [])
        # Handle both list of lines and single string formats
        code = ''.join(source) if isinstance(source, list) else source
        if code.strip():  # Skip empty cells
            result = ipython.run_cell(code)
            if result.error_in_exec is not None:
                raise result.error_in_exec

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def drop_external_table_for_shortcut(
    target_table_name: str,
    first_run: bool,
    output_external_location: bool, 
    lakehouse_table_output: bool
) -> None:
    """
    Drop external table reference to enable OneLake shortcut creation.
    
    After writing to an external location (e.g., ADLS Gen2), the table metadata
    must be dropped to allow OneLake shortcuts to be created for the Delta files.
    
    Args:
        target_table_name (str): Fully qualified table name (workspace.datastore.schema.table)
        first_run (bool): first run for dataset
        output_external_location (bool), output is external location in adls gen2 
        lakehouse_table_output: output is lakehouse table
    Returns:
        None
    
    """
    if first_run and output_external_location and lakehouse_table_output:
        log_and_print("Dropping external table to enable OneLake shortcut creation.")
        spark.sql(f"DROP TABLE IF EXISTS {target_table_name}")

def check_recent_vacuum_operations(
    target_table_name: str,
    history_limit: int = 50
) -> tuple:
    """
    Check table history for recent vacuum operations.
    
    This function queries the Delta table history to determine if vacuum operations
    have been run recently, which helps avoid redundant vacuum executions.
    
    Args:
        target_table_name (str): Fully qualified table name
        history_limit (int): Number of recent operations to check (default: 50)
    
    Returns:
        tuple: (total_operations, number_of_vacuum_operations)
            - total_operations (int): Total number of operations in table history
            - number_of_vacuum_operations (int): Count of VACUUM START operations
    
    """
    table_operations = spark.sql(
        f"DESCRIBE HISTORY {target_table_name}"
    )
    
    total_operations = table_operations.count()
    
    number_of_vacuum_operations = (
        table_operations
        .orderBy("timestamp", ascending = False)
        .limit(history_limit)
        .filter("operation = 'VACUUM START'")
        .count()
    )
    
    return total_operations, number_of_vacuum_operations

def execute_vacuum_on_table(target_abfss_path: str) -> None:
    """
    Execute vacuum operation on a Delta table to remove old files.
    
    Vacuum removes data files that are no longer referenced by the Delta table,
    freeing up storage space. Files are removed based on the retention policy.
    
    Args:
        target_abfss_path (str): ABFSS path to the target Delta table
    
    Returns:
        None
    
    Note:
        Default retention period is 7 days. Files older than this will be removed.
    
    Example:
        >>> execute_vacuum_on_table(
        ...     target_abfss_path = 'abfss://workspace@onelake.dfs.fabric.microsoft.com/Gold.Lakehouse/Tables/fact_sales'
        ... )
    """
    log_and_print("Running vacuum command against table.")
    target_table = DeltaTable.forPath(spark, target_abfss_path)
    target_table.vacuum()

def execute_vacuum_if_needed(
    lakehouse_table_output: bool,
    target_table_name: str,
    target_abfss_path: str,
    min_operations_threshold: int = 50
) -> None:
    """
    Execute vacuum operation only if needed based on table history.
    
    This function checks if vacuum has been run recently and only executes it
    if no recent vacuum operations are found and the table has sufficient history.
    
    Args:
        target_table_name (str): Fully qualified table name
        target_abfss_path (str): ABFSS path to the target Delta table
        min_operations_threshold (int): Minimum operations before vacuum (default: 50)
    
    Returns:
        None
    
    """
    if not lakehouse_table_output:
        return

    total_operations, number_of_vacuum_operations = check_recent_vacuum_operations(
        target_table_name = target_table_name
    )
    
    # Run vacuum if no recent operations found and table has sufficient history
    if number_of_vacuum_operations == 0 and total_operations > min_operations_threshold:
        execute_vacuum_on_table(target_abfss_path = target_abfss_path)
    elif number_of_vacuum_operations > 0:
        log_and_print(f"Skipping vacuum - {number_of_vacuum_operations} recent vacuum operation(s) detected.")
    else:
        log_and_print(f"Skipping vacuum - table has only {total_operations} operations (threshold: {min_operations_threshold}).")

def cleanup_temporary_files(
    file_staging_path: str,
    clean_up_temporary_path: bool
) -> None:
    """
    Remove temporary files created during staged ingestion flows.
    
    Some ingestion paths stage intermediate files in a temporary area before Spark
    reads them. This function cleans up those temporary files when configured.
    
    Args:
        file_staging_path (str): Path to the temporary file staging directory
        clean_up_temporary_path (bool): whether there's temporary data to delete
    
    Returns:
        None
    
    """
    if clean_up_temporary_path:
        log_and_print("Deleting temporary staged files used during ingestion.")
        notebookutils.fs.rm(file_staging_path, True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def create_schema_if_not_exists(
    target_abfss_path: str,
    lakehouse_table_output: bool
) -> None:
    """    
    This function parses the target_abfss_path to extract the schema,
    then creates the schema in the target datastore using notebookutils if it doesn't already exist.
    
    Args:
        target_abfss_path (str): Abfss path for schema
        lakehouse_table_output (bool): Whether output is lakehouse table
    
    Note:
        - Idempotent operation - safe to call multiple times
    """
    if not lakehouse_table_output:
        return

    # Extract path without table name
    schema_abfss_path = target_abfss_path.rsplit('/', 1)[0]
    
    schema_name = schema_abfss_path.rsplit('/', 1)[1]

    log_and_print(f"Creating schema, `{schema_name}`, for lakehouse if it doesn't exist.")

    notebookutils.fs.mkdirs(schema_abfss_path)  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def drop_table_for_full_reload(
    target_table_name: str
) -> None:
    """
    Drop the target table to prepare for a full reload.
    
    This function drops the existing table if it exists, allowing a complete
    refresh of the data. Used when full_reload = 'Yes' in metadata configuration.
    
    Args:
        target_table_name (str): Fully qualified table name (schema.table)
    
    Returns:
        None
    
    Example:
        >>> drop_table_for_full_reload(
        ...     target_workspace_name = 'Analytics Workspace',
        ...     target_table_name = '`workspacename`.datastorename.dbo.customers'
        ... )
    """
    full_reload_log_info = "Running full reload process."
    log_and_print(full_reload_log_info)
    
    drop_table_statement = f"DROP TABLE IF EXISTS {target_table_name}"
    spark.sql(drop_table_statement)


def check_target_table_exists(
    target_abfss_path: str
) -> bool:
    """
    Check if the target table exists in the specified workspace and datastore.
    
    This function verifies table existence using Spark's catalog, which is used
    to determine if this is a first-time load or an incremental update.
    
    Args:
        target_abfss_path (str): abfss path for target table
    
    Returns:
        bool: True if table exists, False otherwise
    
    Example:
        >>> exists = check_target_table_exists(
        ...     target_abfss_path = ''
        ... )
        >>> print(f"Table exists: {exists}")
    """
    try:
        path_exists = notebookutils.fs.exists(target_abfss_path)
    except Exception as error:
        raise RuntimeError(
            f"Unable to check target path existence for '{target_abfss_path}'."
        ) from error

    if path_exists:
        try:
            target_table_exists = DeltaTable.isDeltaTable(spark, target_abfss_path)
        except Exception as error:
            log_and_print(f"Target path exists but is not readable as a Delta table: {error}", "warn")
            target_table_exists = False
    else:
        target_table_exists = False
    table_exists_log_info = f"Table exists: {target_table_exists}"
    log_and_print(table_exists_log_info)
    return target_table_exists

def determine_first_run_and_table_existence(
    target_table_name: str,
    full_reload: str,
    watermark_value: str,
    target_abfss_path: str,
    lakehouse_table_output: bool
) -> tuple:
    """
    Determine if this is a first run and whether the target table exists.
    
    This orchestrator function handles the full reload workflow and table existence
    checking logic, coordinating between different helper functions.
    
    Args:
        target_table_name (str): Name of the target table
        full_reload (str): 'Yes' to drop and recreate table, otherwise incremental
        watermark_value (str): watermark value for incremental data processing
        target_abfss_path (str): abfss path for target table
        lakehouse_table_output (bool): whether target is a lakehouse table
    Returns:
        tuple: (first_run, target_table_exists, watermark_value)
            - first_run (bool): True if this is the first load for the table
            - target_table_exists (bool): True if the target table currently exists
            - watermark_value (str): Reset to empty string for full reload
    
    Example:
        >>> first_run, exists, watermark = determine_first_run_and_table_existence(
        ...     target_table_name = 'workspacename.datastorename.dbo.customers',
        ...     full_reload = 'No',
        ...     watermark_value = '',
        ...     target_abfss_path = 'abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse/Tables/dbo/customers',
        ...     lakehouse_table_output = True,
        ... )
    """
    # Initialize variables
    first_run = False
    target_table_exists = False
    
    if not lakehouse_table_output:
        # No target table specified (e.g., writing to files or warehouse)
        return first_run, target_table_exists, watermark_value
    
    # Check target table existence and handle full reload
    if full_reload == 'Yes':
        drop_table_for_full_reload(
            target_table_name = target_table_name
        )
        # Reset watermark for full reload
        watermark_value = ""
        # Reset first run flag for full reload
        first_run = True
        target_table_exists = False
    else:
        target_table_exists = check_target_table_exists(
            target_abfss_path = target_abfss_path
        )
        # Set first run flag based on table existence
        first_run = not target_table_exists
    
    return first_run, target_table_exists, watermark_value


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================================
# CUSTOM STAGING PROCESSING FUNCTIONS
# ===========================================================================================
# Functions used by NB_Batch_Processing to build metadata,
# load/execute custom staging functions, and validate results. Keeping logic in functions
# enables unit testing.
# ===========================================================================================

def parse_custom_staging_parameters(
    watermark_value: str,
    primary_config: dict,
    staging_datastore_config: dict,
    source_datastore_config: dict,
    orchestration_metadata: dict,
    target_folderpath_w_ingestion_type: str
) -> CustomStagingParams:
    """
    Validate custom staging metadata inputs and build the metadata
    dictionary that gets passed to the custom staging function.
    
    Args:
        watermark_value (str): Previous high watermark for incremental processing
        primary_config (dict): Source details configuration
        staging_datastore_config (dict): Staging datastore configuration
        source_datastore_config (dict): Source datastore configuration
        orchestration_metadata (dict): Orchestration metadata
        target_folderpath_w_ingestion_type (str): Target staging folder path with timestamp
    
    Returns:
        CustomStagingParams: Parsed custom staging parameters containing:
            - metadata (dict): Bundled metadata dictionary for the custom function
            - primary_config (dict): Parsed primary configuration
            - custom_staging_function (str): Name of the custom function to call
            - custom_staging_function_notebook (str): Name of the notebook containing the function
    
    Raises:
        TypeError: If any configuration input is not already a dictionary
        ValueError: If required custom staging function or notebook attributes are missing
    
    Example:
        >>> result = parse_custom_staging_parameters(
        ...     watermark_value='2024-01-01',
        ...     primary_config={'source_details_custom_staging_function': 'my_func', 'source_details_custom_staging_function_notebook': 'NB_My_Staging'},
        ...     staging_datastore_config={},
        ...     source_datastore_config={},
        ...     orchestration_metadata={},
        ...     target_folderpath_w_ingestion_type='staging/data/20240101'
        ... )
        >>> result.custom_staging_function
        'my_func'
    """
    for config_name, config_value in (
        ('primary_config', primary_config),
        ('staging_datastore_config', staging_datastore_config),
        ('source_datastore_config', source_datastore_config),
        ('orchestration_metadata', orchestration_metadata),
    ):
        if not isinstance(config_value, dict):
            raise TypeError(
                f"{config_name} must be a dict. "
                f"Received {type(config_value).__name__}."
            )

    # Build metadata dictionary
    metadata = {
        "watermark_value": watermark_value,
        "primary_config": primary_config,
        "staging_datastore_config": staging_datastore_config,
        "source_datastore_config": source_datastore_config,
        "orchestration_metadata": orchestration_metadata,
        "target_folderpath_w_ingestion_type": target_folderpath_w_ingestion_type
    }

    # Extract custom function configuration
    custom_staging_function = primary_config.get('source_details_custom_staging_function', '')
    custom_staging_function_notebook = primary_config.get('source_details_custom_staging_function_notebook', '')

    if not custom_staging_function:
        raise ValueError(
            "Missing required metadata attribute 'source_details_custom_staging_function'. "
            "This must be set in the Source_Details table to specify the custom function name."
        )

    if not custom_staging_function_notebook:
        raise ValueError(
            "Missing required metadata attribute 'source_details_custom_staging_function_notebook'. "
            "This must be set in the Source_Details table to specify the notebook containing the custom function."
        )

    log_and_print(f"Custom staging function: {custom_staging_function}")
    log_and_print(f"Custom staging notebook: {custom_staging_function_notebook}")

    return CustomStagingParams(
        metadata=metadata,
        primary_config=primary_config,
        custom_staging_function=custom_staging_function,
        custom_staging_function_notebook=custom_staging_function_notebook
    )


def parse_custom_staging_run_time(trigger_time: str) -> datetime:
    """Parse orchestration trigger_time into the deterministic run timestamp."""
    normalized_trigger_time = str(trigger_time or '').strip()
    if not normalized_trigger_time:
        raise ValueError(
            "Missing trigger_time for custom staging output path. "
            "Retries must reuse the orchestration trigger timestamp."
        )

    if normalized_trigger_time.endswith('Z'):
        normalized_trigger_time = f"{normalized_trigger_time[:-1]}+00:00"

    try:
        run_time = datetime.fromisoformat(normalized_trigger_time)
    except ValueError as exc:
        raise ValueError(
            f"Invalid trigger_time '{trigger_time}'. Expected an ISO 8601 timestamp."
        ) from exc

    if run_time.tzinfo is not None:
        run_time = run_time.astimezone(timezone.utc)

    return run_time


def build_custom_staging_output_paths(
    staging_folder_path: str,
    run_time: datetime = None
) -> str:
    """Build the timestamped folder path used by custom staging."""
    if run_time is None:
        raise ValueError("Missing run_time for custom staging output path.")

    timestamp = run_time.strftime('%Y%m%d%H%M%S')
    normalized_path = (staging_folder_path or '').strip().rstrip('/')

    if normalized_path:
        return f"{normalized_path}/{timestamp}"

    return f"{timestamp}"


def resolve_custom_staging_output_abfss_path(
    staging_datastore_config: dict,
    target_folderpath_w_ingestion_type: str
) -> str:
    """Resolve the custom staging output folder into an ABFSS path."""
    try:
        workspace_id = str(staging_datastore_config['Workspace_ID']).strip()
        datastore_id = str(staging_datastore_config['Datastore_ID']).strip()
    except KeyError as exc:
        raise KeyError(
            "staging_datastore_config must include both 'Workspace_ID' and 'Datastore_ID'."
        ) from exc

    if not workspace_id or not datastore_id:
        raise ValueError(
            "staging_datastore_config must include non-empty 'Workspace_ID' and 'Datastore_ID' values."
        )

    normalized_target_path = str(target_folderpath_w_ingestion_type or '').strip().lstrip('/')

    return (
        f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/"
        f"{datastore_id}/Files/{normalized_target_path}"
    )


def validate_custom_staging_output(
    staging_datastore_config: dict,
    staging_lakehouse_name: str,
    target_folderpath_w_ingestion_type: str,
    rows_copied: int,
    watermark_value: str,
    lineage_info
) -> dict:
    """Validate custom staging output files before file ingestion continues."""
    target_folder_abfss_path = resolve_custom_staging_output_abfss_path(
        staging_datastore_config = staging_datastore_config,
        target_folderpath_w_ingestion_type = target_folderpath_w_ingestion_type
    )
    target_folder_exists = notebookutils.fs.exists(target_folder_abfss_path)
    staged_entries = notebookutils.fs.ls(target_folder_abfss_path) if target_folder_exists else []

    if any(getattr(entry, 'isDir', False) for entry in staged_entries):
        raise ValueError(
            f"Custom staging output in '{target_folder_abfss_path}' must contain direct child files only."
        )

    staged_files = [getattr(entry, 'path', None) for entry in staged_entries if getattr(entry, 'path', None)]

    if rows_copied > 0 and not staged_files:
        raise ValueError(
            f"Custom staging for lakehouse '{staging_lakehouse_name}' reported rows_copied > 0 but wrote no files "
            f"to '{target_folder_abfss_path}'."
        )

    if not staged_files:
        raise NoDataFoundError(
            f"No data found in custom staging output for lakehouse '{staging_lakehouse_name}'.",
            watermark_value,
            target_folder_abfss_path,
            lineage_info
        )

    if rows_copied == 0:
        log_and_print(
            f"Custom staging reported rows_copied = 0. Continuing because files were discovered in '{target_folder_abfss_path}'.",
            'warn'
        )

    return {
        'target_folder_abfss_path': target_folder_abfss_path,
        'staged_files': staged_files
    }


def validate_custom_staging_configuration(
    custom_staging_function: str,
    custom_staging_function_notebook: str
) -> None:
    """Require custom staging metadata to specify both function and notebook together."""
    if not (custom_staging_function or custom_staging_function_notebook):
        return

    if not custom_staging_function:
        raise ValueError(
            "Custom staging requires both 'source_details_custom_staging_function' and "
            "'source_details_custom_staging_function_notebook'. Missing "
            "'source_details_custom_staging_function'."
        )

    if not custom_staging_function_notebook:
        raise ValueError(
            "Custom staging requires both 'source_details_custom_staging_function' and "
            "'source_details_custom_staging_function_notebook'. Missing "
            "'source_details_custom_staging_function_notebook'."
        )


def execute_custom_staging_function(
    custom_staging_function: str,
    custom_staging_function_notebook: str,
    metadata: dict,
    spark_session,
    session_globals: dict
) -> dict:
    """
    Load and execute a custom staging function, validating its result.
    
    This function:
    1. Uses instantiate_notebook() to load the user's notebook into the session
    2. Calls the custom function with (metadata, spark) arguments
    3. Validates the result is a dict with required keys
    4. Returns the validated result
    
    Args:
        custom_staging_function (str): Name of the Python function to call
        custom_staging_function_notebook (str): Name of the notebook containing the function
        metadata (dict): Bundled metadata dictionary to pass to the custom function
        spark_session: The active SparkSession
        session_globals (dict): The globals() dict from the calling notebook session,
                                used to look up the function after instantiate_notebook() loads it
    
    Returns:
        dict: Dictionary containing:
            - rows_copied (int|str): Number of records staged
            - next_watermark_value (str): Updated watermark for next incremental run
    
    Raises:
        TypeError: If the custom function does not return a dict
        KeyError: If the custom function is not found in session globals after loading
    
    Example:
        >>> result = execute_custom_staging_function(
        ...     custom_staging_function='my_staging_func',
        ...     custom_staging_function_notebook='NB_My_Staging',
        ...     metadata={'watermark_value': '', 'primary_config': {}},
        ...     spark_session=spark,
        ...     session_globals=globals()
        ... )
        >>> result['rows_copied']
        100
    """
    # Load custom function notebook into session
    instantiate_notebook(
        notebook_name=custom_staging_function_notebook,
        function_scope=(metadata.get('primary_config') or {}).get(
            'source_details_custom_staging_function_scope',
            'local',
        ),
    )

    # Call custom function
    log_and_print(f"Calling custom staging function: {custom_staging_function}")
    function_kwargs = {"metadata": metadata, "spark": spark_session}
    result = session_globals[custom_staging_function](**function_kwargs)

    # Validate result
    if not isinstance(result, dict):
        raise TypeError(
            f"Custom staging function '{custom_staging_function}' must return a dict with "
            f"'rows_copied' and 'next_watermark_value' keys. Got {type(result).__name__} instead."
        )

    rows_copied = result.get('rows_copied', 0)
    next_watermark_value = result.get('next_watermark_value', '')

    log_and_print(f"Custom staging complete — rows_copied: {rows_copied}, next_watermark_value: {next_watermark_value}")

    return {
        'rows_copied': rows_copied,
        'next_watermark_value': next_watermark_value
    }

def execute_custom_staging_for_batch(
    central_metadata_conn,
    batch_log_entry: DataMovementLogEntry,
    primary_config: dict,
    datastore_config: str | list | dict,
    orchestration_metadata: dict,
    spark_session,
    session_globals: dict,
    staging_only: bool = False
) -> dict:
    """Run a custom staging function inside NB_Batch_Processing before file ingestion."""
    batch_data_product_key = getattr(batch_log_entry, 'data_product_key', None) or 'default'
    staging_lakehouse_name = primary_config.get('source_details_staging_lakehouse_name', '').strip().lower()
    staging_folder_path = primary_config.get('source_details_staging_folder_path', '').strip()
    custom_staging_function = primary_config.get('source_details_custom_staging_function', '').strip()
    custom_staging_function_notebook = primary_config.get('source_details_custom_staging_function_notebook', '').strip()

    validate_custom_staging_configuration(
        custom_staging_function = custom_staging_function,
        custom_staging_function_notebook = custom_staging_function_notebook
    )

    if not staging_lakehouse_name:
        raise ValueError("Missing required metadata attribute 'source_details_staging_lakehouse_name' for custom staging.")

    if not staging_folder_path:
        raise ValueError("Missing required metadata attribute 'source_details_staging_folder_path' for custom staging.")

    staging_central_metadata_conn = central_metadata_conn
    owns_staging_central_metadata_conn = False
    if getattr(batch_log_entry, 'central_metadata_endpoint', None) and getattr(batch_log_entry, 'central_metadata_warehouse_name', None):
        try:
            staging_central_metadata_conn = open_central_metadata_connection(batch_log_entry, central_metadata_conn)
            owns_staging_central_metadata_conn = True
        except Exception as exc:
            if central_metadata_conn is None:
                raise
            log_and_print(
                f"Falling back to existing logging connection for custom staging because a fresh connection could not be opened: {exc}",
                'warn'
            )
    elif staging_central_metadata_conn is None:
        raise ValueError(
            "Custom staging requires either a valid central_metadata_conn or logging endpoint and warehouse name on the batch log entry."
        )

    try:
        custom_staging_run_time = parse_custom_staging_run_time(batch_log_entry.trigger_time)
        target_folderpath_w_ingestion_type = build_custom_staging_output_paths(
            staging_folder_path = staging_folder_path,
            run_time = custom_staging_run_time
        )

        staging_watermark_details = fetch_watermark_details(
            conn = staging_central_metadata_conn,
            table_id = int(orchestration_metadata.get('Table_ID')),
            target_datastore = staging_lakehouse_name,
            processing_phase = 'Staging',
            data_product_key = batch_data_product_key,
            watermark_strategy = primary_config.get('watermark_details_strategy'),
            watermark_field_data_type = primary_config.get('watermark_details_data_type')
        )
        staging_watermark_value = staging_watermark_details.get('watermark_value', '')

        staging_datastore_config = get_datastore_entry(datastore_config, staging_lakehouse_name)
        source_datastore_name = primary_config.get('source_details_datastore_name', staging_lakehouse_name).strip().lower() or staging_lakehouse_name
        source_datastore_config = get_datastore_entry(datastore_config, source_datastore_name)
        staging_lineage_info = extract_custom_staging_lineage_information(
            staging_lakehouse_name = staging_lakehouse_name,
            datastore_config = datastore_config
        )

        metadata = {
            'watermark_value': staging_watermark_value,
            'primary_config': primary_config,
            'staging_datastore_config': staging_datastore_config,
            'source_datastore_config': source_datastore_config,
            'orchestration_metadata': orchestration_metadata,
            'target_folderpath_w_ingestion_type': target_folderpath_w_ingestion_type,
        }

        staging_source_details = f"Custom staging function, {custom_staging_function}"
        if custom_staging_function_notebook:
            staging_source_details += f", in {custom_staging_function_notebook}"

        target_folder_abfss_path = resolve_custom_staging_output_abfss_path(
            staging_datastore_config = staging_datastore_config,
            target_folderpath_w_ingestion_type = target_folderpath_w_ingestion_type
        )
        # Cleanup is scoped to this table/run retry folder: metadata validation requires
        # unique staging_folder_path values across Table_IDs, and runtime appends trigger_time.
        if notebookutils.fs.exists(target_folder_abfss_path):
            notebookutils.fs.rm(target_folder_abfss_path, True)

        staging_start_time = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
        staging_log_entry = DataMovementLogEntry(
            data_product_key = batch_data_product_key,
            log_id = str(uuid.uuid4()),
            table_id = int(orchestration_metadata.get('Table_ID')),
            target_datastore = staging_lakehouse_name,
            target_entity = target_folderpath_w_ingestion_type,
            event_start_time = staging_start_time,
            fabric_monitor_url = batch_log_entry.fabric_monitor_url,
            trigger_name = batch_log_entry.trigger_name,
            trigger_step = batch_log_entry.trigger_step,
            trigger_id = batch_log_entry.trigger_id,
            trigger_time = batch_log_entry.trigger_time,
            source_details = staging_source_details,
            source_medallion_layer = staging_lineage_info.source_medallion_layer,
            source_type = staging_lineage_info.source_type,
            target_medallion_layer = staging_lineage_info.target_medallion_layer,
            target_type = staging_lineage_info.target_type,
            central_metadata_endpoint = batch_log_entry.central_metadata_endpoint,
            central_metadata_warehouse_name = batch_log_entry.central_metadata_warehouse_name,
            processing_phase = 'Staging'
        )
        log_data_movement(staging_central_metadata_conn, staging_log_entry, status='Started')

        try:
            staging_result = execute_custom_staging_function(
                custom_staging_function = custom_staging_function,
                custom_staging_function_notebook = custom_staging_function_notebook,
                metadata = metadata,
                spark_session = spark_session,
                session_globals = session_globals
            )
        except Exception:
            staging_log_entry.event_end_time = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
            log_data_movement(staging_central_metadata_conn, staging_log_entry, status='Failed')
            raise

        rows_copied = int(staging_result.get('rows_copied', 0) or 0)
        next_watermark_value = staging_result.get('next_watermark_value', '')

        try:
            validate_custom_staging_output(
                staging_datastore_config = staging_datastore_config,
                staging_lakehouse_name = staging_lakehouse_name,
                target_folderpath_w_ingestion_type = target_folderpath_w_ingestion_type,
                rows_copied = rows_copied,
                watermark_value = next_watermark_value,
                lineage_info = staging_lineage_info
            )
        except NoDataFoundError:
            staging_log_entry.event_end_time = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
            staging_log_entry.watermark_value = str(next_watermark_value) if next_watermark_value else None
            staging_log_entry.records_processed = str(rows_copied)
            log_data_movement(staging_central_metadata_conn, staging_log_entry, status='Processed')
            raise
        except Exception:
            staging_log_entry.event_end_time = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
            staging_log_entry.watermark_value = str(next_watermark_value) if next_watermark_value else None
            staging_log_entry.records_processed = str(rows_copied)
            log_data_movement(staging_central_metadata_conn, staging_log_entry, status='Failed')
            raise

        staging_log_entry.event_end_time = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
        staging_log_entry.watermark_value = str(next_watermark_value) if next_watermark_value else None
        staging_log_entry.records_processed = str(rows_copied)
        log_data_movement(staging_central_metadata_conn, staging_log_entry, status='Processed')

        if staging_only:
            batch_log_entry.source_medallion_layer = staging_lineage_info.source_medallion_layer
            batch_log_entry.source_type = staging_lineage_info.source_type
            batch_log_entry.target_medallion_layer = staging_lineage_info.target_medallion_layer
            batch_log_entry.target_type = staging_lineage_info.target_type

        return {
            'rows_copied': rows_copied,
            'next_watermark_value': next_watermark_value,
            'source_details': staging_source_details,
            'target_folderpath_w_ingestion_type': target_folderpath_w_ingestion_type
        }
    finally:
        if owns_staging_central_metadata_conn:
            close_warehouse_connection(staging_central_metadata_conn)


def handle_custom_staging(
    central_metadata_conn,
    batch_log_entry: DataMovementLogEntry,
    source_config,
    primary_config: dict,
    datastore_config: str | list | dict,
    orchestration_metadata: dict,
    spark_session,
    session_globals: dict
) -> dict | None:
    """Execute custom staging when both custom staging metadata attributes are configured."""
    custom_staging_function = primary_config.get('source_details_custom_staging_function', '').strip()
    custom_staging_function_notebook = primary_config.get('source_details_custom_staging_function_notebook', '').strip()

    if not (custom_staging_function or custom_staging_function_notebook):
        return None

    validate_custom_staging_configuration(
        custom_staging_function = custom_staging_function,
        custom_staging_function_notebook = custom_staging_function_notebook
    )

    exit_after_staging = (primary_config.get('source_details_exit_after_staging', '').strip().lower() == 'true')
    staging_result = execute_custom_staging_for_batch(
        central_metadata_conn = central_metadata_conn,
        batch_log_entry = batch_log_entry,
        primary_config = primary_config,
        datastore_config = datastore_config,
        orchestration_metadata = orchestration_metadata,
        spark_session = spark_session,
        session_globals = session_globals,
        staging_only = exit_after_staging
    )

    if exit_after_staging:
        notebookutils.notebook.exit(
            finalize_staging_only_and_build_exit_value(
                central_metadata_conn = central_metadata_conn,
                log_entry = batch_log_entry,
                staging_result = staging_result
            )
        )

    return staging_result


def build_custom_staging_exit_value(rows_copied, next_watermark_value: str) -> str:
    """
    Build the JSON exit value string for the orchestration pipeline.
    
    The caller reads these values from the notebook exitValue JSON.
    
    Args:
        rows_copied: Number of records staged (int or str)
        next_watermark_value (str): Updated watermark for next incremental run
    
    Returns:
        str: JSON string with rows_copied and next_watermark_value
    
    Example:
        >>> build_custom_staging_exit_value(100, '2024-01-15')
        '{"rows_copied": "100", "next_watermark_value": "2024-01-15"}'
    """
    return json.dumps({
        "rows_copied": str(rows_copied),
        "next_watermark_value": str(next_watermark_value)
    })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
