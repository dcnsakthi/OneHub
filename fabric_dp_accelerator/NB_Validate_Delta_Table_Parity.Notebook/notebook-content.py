# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "environment": {
# META       "environmentId": "1c72553b-20d4-82de-4da9-4bfc55b0dd2a",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

mapping_id = ""
source_table_name = ""
target_table_name = ""
key_columns_json = "[]"
ignored_columns_json = "[]"
source_watermark_column = ""
target_watermark_column = ""
max_columns = 64

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import hashlib
import re
import time
from datetime import datetime, timezone
from typing import Any

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, MapType, NullType, StructType


spark: Any = globals()["spark"]
notebookutils: Any = globals()["notebookutils"]

SYSTEM_COLUMN_NAMES = {
    "_change_type",
    "_commit_version",
    "_commit_timestamp",
}


def _is_system_column(column_name):
    folded = column_name.casefold()
    return folded.startswith("delta__") or folded in SYSTEM_COLUMN_NAMES


def _is_comparable_type(data_type):
    return not isinstance(data_type, (ArrayType, MapType, NullType, StructType))


# PARITY_TYPE_HELPERS_BEGIN
INTEGRAL_RANK = {
    "tinyint": 1,
    "byte": 1,
    "smallint": 2,
    "short": 2,
    "int": 3,
    "integer": 3,
    "bigint": 4,
    "long": 4,
}
INTEGRAL_TYPE_BY_RANK = {1: "tinyint", 2: "smallint", 3: "int", 4: "bigint"}
INTEGRAL_PRECISION = {
    "tinyint": (3, 0),
    "byte": (3, 0),
    "smallint": (5, 0),
    "short": (5, 0),
    "int": (10, 0),
    "integer": (10, 0),
    "bigint": (19, 0),
    "long": (19, 0),
}
FLOATING_TYPES = {"float", "double"}
TEMPORAL_TYPES = {"date", "timestamp", "timestamp_ntz", "timestamp_ltz"}
DECIMAL_TYPE_PATTERN = re.compile(r"^decimal\((\d+),(\d+)\)$")


def _normalize_simple_type(simple_name):
    return str(simple_name or "").casefold().replace(" ", "")


def _parsed_spark_type(simple_name):
    folded = _normalize_simple_type(simple_name)
    match = DECIMAL_TYPE_PATTERN.match(folded)
    if match:
        return "decimal", int(match.group(1)), int(match.group(2))
    if folded == "decimal":
        return "decimal", 10, 0
    return folded, None, None


def _decimal_spec_for_numeric(root, precision, scale):
    if root == "decimal":
        return int(precision or 10), int(scale or 0)
    if root in INTEGRAL_PRECISION:
        return INTEGRAL_PRECISION[root]
    return 38, 18


def _is_numeric_root(root):
    return root == "decimal" or root in INTEGRAL_RANK or root in FLOATING_TYPES


def _comparison_type(source_simple, target_simple):
    source_simple = _normalize_simple_type(source_simple)
    target_simple = _normalize_simple_type(target_simple)
    if source_simple == target_simple:
        return "identical", None
    source_root, source_precision, source_scale = _parsed_spark_type(source_simple)
    target_root, target_precision, target_scale = _parsed_spark_type(target_simple)
    if source_root in TEMPORAL_TYPES and target_root in TEMPORAL_TYPES:
        return "compatible", "timestamp"
    if not (_is_numeric_root(source_root) and _is_numeric_root(target_root)):
        return "incompatible", None
    if source_root in FLOATING_TYPES or target_root in FLOATING_TYPES:
        if source_root in FLOATING_TYPES and target_root in FLOATING_TYPES:
            return "compatible", "double"
        decimal_root, decimal_precision, decimal_scale = (
            (target_root, target_precision, target_scale)
            if source_root in FLOATING_TYPES
            else (source_root, source_precision, source_scale)
        )
        if decimal_root == "decimal":
            precision, scale = _decimal_spec_for_numeric(
                decimal_root, decimal_precision, decimal_scale
            )
            return "compatible", f"decimal({precision},{scale})"
        return "incompatible", None
    if source_root in INTEGRAL_RANK and target_root in INTEGRAL_RANK:
        rank = max(INTEGRAL_RANK[source_root], INTEGRAL_RANK[target_root])
        return "compatible", INTEGRAL_TYPE_BY_RANK[rank]
    source_precision, source_scale = _decimal_spec_for_numeric(
        source_root, source_precision, source_scale
    )
    target_precision, target_scale = _decimal_spec_for_numeric(
        target_root, target_precision, target_scale
    )
    scale = max(source_scale, target_scale)
    integer_digits = max(source_precision - source_scale, target_precision - target_scale)
    precision = integer_digits + scale
    if precision > 38:
        return "incompatible", None
    return "compatible", f"decimal({precision},{scale})"


def _comparison_mode(source_simple, target_simple, kind):
    if kind not in {"identical", "compatible"}:
        return None
    source_root, _, _ = _parsed_spark_type(source_simple)
    target_root, _, _ = _parsed_spark_type(target_simple)
    roots = {source_root, target_root}
    if "decimal" in roots and roots.intersection(FLOATING_TYPES):
        return "decimal-quantized"
    return "exact"


def _decimal_quantization_tolerance(cast_type, comparison_mode):
    if comparison_mode != "decimal-quantized":
        return None
    root, _, scale = _parsed_spark_type(cast_type)
    if root != "decimal":
        return None
    scale = int(scale or 0)
    return "0.5" if scale == 0 else f"0.{('0' * scale)}5"
# PARITY_TYPE_HELPERS_END


def _field_map(dataframe, table_label):
    fields = {}
    duplicates = []
    for field in dataframe.schema.fields:
        folded = field.name.casefold()
        if folded in fields:
            duplicates.append(field.name)
        else:
            fields[folded] = field
    if duplicates:
        raise ValueError(
            f"{table_label} schema contains case-insensitive duplicate columns: {duplicates}."
        )
    return fields


def _schema_signature(dataframe, ignored_names):
    return {
        field.name.casefold(): {
            "type": field.dataType.simpleString(),
            "nullable": bool(field.nullable),
        }
        for field in dataframe.schema.fields
        if field.name.casefold() not in ignored_names and not _is_system_column(field.name)
    }


def _schema_difference_kind(source_value, target_value):
    if source_value is None or target_value is None:
        return "missing", None
    kind, cast_type = _comparison_type(source_value.get("type"), target_value.get("type"))
    if bool(source_value.get("nullable")) != bool(target_value.get("nullable")):
        return "nullable", cast_type
    if kind == "identical":
        return "identical", None
    return kind, cast_type


def _schema_matches(source_signature, target_signature):
    if set(source_signature) != set(target_signature):
        return False
    for column, source_value in source_signature.items():
        kind, _ = _schema_difference_kind(source_value, target_signature[column])
        if kind in {"missing", "incompatible", "nullable"}:
            return False
    return True


def _schema_evidence(source_signature, target_signature, difference_limit=100):
    source_json = json.dumps(source_signature, sort_keys=True, separators=(",", ":"))
    target_json = json.dumps(target_signature, sort_keys=True, separators=(",", ":"))
    differences = []
    for column in sorted(set(source_signature) | set(target_signature)):
        source_value = source_signature.get(column)
        target_value = target_signature.get(column)
        kind, cast_type = _schema_difference_kind(source_value, target_value)
        if kind == "identical":
            continue
        differences.append(
            {
                "column": column,
                "kind": kind,
                "source": source_value,
                "target": target_value,
                "comparisonType": cast_type,
            }
        )
    return {
        "sourceSummary": {
            "columnCount": len(source_signature),
            "sha256": hashlib.sha256(source_json.encode("utf-8")).hexdigest(),
        },
        "targetSummary": {
            "columnCount": len(target_signature),
            "sha256": hashlib.sha256(target_json.encode("utf-8")).hexdigest(),
        },
        "differences": differences[:difference_limit],
        "differenceCount": len(differences),
        "differencesTruncated": len(differences) > difference_limit,
        "compatibleDifferenceCount": sum(1 for item in differences if item["kind"] == "compatible"),
        "incompatibleDifferenceCount": sum(1 for item in differences if item["kind"] in {"missing", "incompatible", "nullable"}),
    }


def _bounded_evidence(values, limit=100):
    return {
        "values": values[:limit],
        "count": len(values),
        "truncated": len(values) > limit,
    }


def _column_plan(source_field, target_field):
    source_type = source_field.dataType.simpleString()
    target_type = target_field.dataType.simpleString()
    kind, cast_type = _comparison_type(source_type, target_type)
    comparison_mode = _comparison_mode(source_type, target_type, kind)
    return {
        "name": source_field.name,
        "sourceType": source_type,
        "targetType": target_type,
        "comparisonKind": kind,
        "castType": cast_type,
        "comparisonMode": comparison_mode,
        "quantizationTolerance": _decimal_quantization_tolerance(
            cast_type, comparison_mode
        ),
    }


def _select_comparison_columns(source, target, key_columns, ignored_names, column_limit):
    source_fields = _field_map(source, "Source")
    target_fields = _field_map(target, "Target")
    selected = []
    selected_folded = set()
    missing_keys = []
    unsupported_keys = []
    excluded_keys = []
    incompatible_types = []
    compatible_types = []

    def consider_field(source_field, target_field):
        plan = _column_plan(source_field, target_field)
        if plan["comparisonKind"] == "incompatible":
            incompatible_types.append(
                {
                    "column": plan["name"],
                    "sourceType": plan["sourceType"],
                    "targetType": plan["targetType"],
                }
            )
            return False
        if plan["comparisonKind"] == "compatible":
            compatible_types.append(
                {
                    "column": plan["name"],
                    "sourceType": plan["sourceType"],
                    "targetType": plan["targetType"],
                    "comparisonType": plan["castType"],
                    "comparisonMode": plan["comparisonMode"],
                    "quantizationTolerance": plan["quantizationTolerance"],
                }
            )
        selected.append(plan)
        selected_folded.add(source_field.name.casefold())
        return True

    for key in key_columns:
        folded = key.casefold()
        if folded in ignored_names or _is_system_column(key):
            excluded_keys.append(key)
            continue
        source_field = source_fields.get(folded)
        target_field = target_fields.get(folded)
        if source_field is None or target_field is None:
            missing_keys.append(key)
            continue
        if not _is_comparable_type(source_field.dataType) or not _is_comparable_type(target_field.dataType):
            unsupported_keys.append(key)
            continue
        consider_field(source_field, target_field)

    if missing_keys or unsupported_keys or excluded_keys or incompatible_types:
        raise ValueError(
            "Configured keys must exist on both tables, remain in scope, and use comparable primitive types: "
            f"missing={missing_keys}, unsupported={unsupported_keys}, excluded={excluded_keys}, "
            f"incompatibleTypes={incompatible_types}."
        )
    if len(selected) > column_limit:
        raise ValueError(
            f"Configured key count {len(selected)} exceeds max_columns={column_limit}."
        )

    ignored_system = []
    ignored_configured = []
    unsupported_complex = []
    omitted_by_limit = []
    for field in source.schema.fields:
        folded = field.name.casefold()
        if folded in selected_folded:
            continue
        if _is_system_column(field.name):
            ignored_system.append(field.name)
            continue
        if folded in ignored_names:
            ignored_configured.append(field.name)
            continue
        target_field = target_fields.get(folded)
        if target_field is None:
            continue
        if not _is_comparable_type(field.dataType) or not _is_comparable_type(target_field.dataType):
            unsupported_complex.append(field.name)
            continue
        plan = _column_plan(field, target_field)
        if plan["comparisonKind"] == "incompatible":
            incompatible_types.append(
                {
                    "column": plan["name"],
                    "sourceType": plan["sourceType"],
                    "targetType": plan["targetType"],
                }
            )
            continue
        if len(selected) >= column_limit:
            omitted_by_limit.append(field.name)
            continue
        consider_field(field, target_field)

    return {
        "selected": selected,
        "ignoredSystem": ignored_system,
        "ignoredConfigured": ignored_configured,
        "unsupportedComplex": unsupported_complex,
        "compatibleTypes": compatible_types,
        "incompatibleTypes": incompatible_types,
        "omittedByLimit": omitted_by_limit,
    }


def _column(column_name):
    return F.col(f"`{column_name.replace('`', '``')}`")


def _internal_column_name(source, target, base_name):
    existing = {
        field.name.casefold()
        for dataframe in (source, target)
        for field in dataframe.schema.fields
    }
    candidate = base_name
    while candidate.casefold() in existing:
        candidate += "_"
    return candidate


def _projected_column(column_name, field_names, plan):
    expression = _column(field_names[column_name.casefold()])
    if plan.get("castType"):
        expression = expression.cast(plan["castType"])
    return expression.alias(plan["name"])


def _iso_utc(value):
    if value is None:
        return None
    if hasattr(value, "tzinfo") and value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    if hasattr(value, "astimezone"):
        return value.astimezone(timezone.utc).isoformat()
    return str(value)


def _resolve_column(dataframe, table_label, column_name):
    requested = str(column_name or "").strip()
    if not requested:
        return None
    fields = _field_map(dataframe, table_label)
    field = fields.get(requested.casefold())
    if field is None:
        raise ValueError(
            f"{table_label} watermark column '{requested}' was not found."
        )
    return field.name


def _max_watermark(dataframe, column_name):
    if not column_name:
        return None
    value = dataframe.agg(F.max(_column(column_name)).alias("watermark")).first()
    if value is None:
        return None
    return _iso_utc(value["watermark"])


def _validate_decimal_quantized_values(source, target, selected_plans):
    source_fields = {key: field.name for key, field in _field_map(source, "Source").items()}
    target_fields = {key: field.name for key, field in _field_map(target, "Target").items()}
    for dataframe, fields, side, type_key in (
        (source, source_fields, "Source", "sourceType"),
        (target, target_fields, "Target", "targetType"),
    ):
        invalid_condition = None
        floating_columns = []
        for plan in selected_plans:
            if plan.get("comparisonMode") != "decimal-quantized":
                continue
            root, _, _ = _parsed_spark_type(plan[type_key])
            if root not in FLOATING_TYPES:
                continue
            column_name = fields[plan["name"].casefold()]
            expression = _column(column_name)
            condition = expression.isNotNull() & (
                F.isnan(expression) | (F.abs(expression) == F.lit(float("inf")))
            )
            invalid_condition = condition if invalid_condition is None else (invalid_condition | condition)
            floating_columns.append(column_name)
        if invalid_condition is not None and dataframe.where(invalid_condition).limit(1).count():
            raise ValueError(
                f"{side} floating columns contain NaN or infinity and cannot be "
                f"decimal-quantized safely: {floating_columns}."
            )


def _exact_projected_comparison(source, target, selected_plans):
    source_fields = {key: field.name for key, field in _field_map(source, "Source").items()}
    target_fields = {key: field.name for key, field in _field_map(target, "Target").items()}
    source_count_column = _internal_column_name(source, target, "__migration_source_count")
    target_count_column = _internal_column_name(source, target, "__migration_target_count")
    selected_names = [plan["name"] for plan in selected_plans]
    source_projection = [_projected_column(plan["name"], source_fields, plan) for plan in selected_plans]
    target_projection = [_projected_column(plan["name"], target_fields, plan) for plan in selected_plans]
    source_tagged = (
        source.select(*source_projection)
        .withColumn(source_count_column, F.lit(1).cast("long"))
        .withColumn(target_count_column, F.lit(0).cast("long"))
    )
    target_tagged = (
        target.select(*target_projection)
        .withColumn(source_count_column, F.lit(0).cast("long"))
        .withColumn(target_count_column, F.lit(1).cast("long"))
    )
    grouped = (
        source_tagged.unionByName(target_tagged)
        .groupBy(*[_column(name) for name in selected_names])
        .agg(
            F.sum(source_count_column).alias("source_count"),
            F.sum(target_count_column).alias("target_count"),
        )
    )
    summary = grouped.agg(
        F.coalesce(F.sum("source_count"), F.lit(0)).alias("source_rows"),
        F.coalesce(F.sum("target_count"), F.lit(0)).alias("target_rows"),
        F.coalesce(F.sum(F.least("source_count", "target_count")), F.lit(0)).alias("matched_rows"),
        F.coalesce(
            F.sum(F.greatest(F.col("source_count") - F.col("target_count"), F.lit(0))),
            F.lit(0),
        ).alias("source_only_rows"),
        F.coalesce(
            F.sum(F.greatest(F.col("target_count") - F.col("source_count"), F.lit(0))),
            F.lit(0),
        ).alias("target_only_rows"),
    ).first()
    values = summary.asDict()
    source_rows = int(values["source_rows"])
    target_rows = int(values["target_rows"])
    source_only = int(values["source_only_rows"])
    target_only = int(values["target_only_rows"])
    different_rows = max(source_only, target_only)
    denominator = max(source_rows, target_rows)
    return {
        "sourceRows": source_rows,
        "targetRows": target_rows,
        "matchedRows": int(values["matched_rows"]),
        "sourceOnlyRows": source_only,
        "targetOnlyRows": target_only,
        "differentRows": different_rows,
        "differencePercent": 0.0 if denominator == 0 else (different_rows * 100.0 / denominator),
    }


started = time.perf_counter()
receipt: dict[str, object]
try:
    spark.conf.set("spark.native.enabled", "true")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
    spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
    keys = json.loads(key_columns_json or "[]")
    ignored = {str(value).casefold() for value in json.loads(ignored_columns_json or "[]")}
    source_df = spark.table(source_table_name)
    target_df = spark.table(target_table_name)
    source_watermark_name = _resolve_column(source_df, "Source", source_watermark_column)
    target_watermark_name = _resolve_column(target_df, "Target", target_watermark_column)
    source_last_write_utc = (
        _max_watermark(source_df, source_watermark_name) if source_watermark_name else None
    )
    target_last_write_utc = (
        _max_watermark(target_df, target_watermark_name) if target_watermark_name else None
    )
    source_schema_signature = _schema_signature(source_df, ignored)
    target_schema_signature = _schema_signature(target_df, ignored)
    schema_evidence = _schema_evidence(source_schema_signature, target_schema_signature)
    schema_matches = _schema_matches(source_schema_signature, target_schema_signature)
    selection = _select_comparison_columns(
        source_df,
        target_df,
        [str(value) for value in keys],
        ignored,
        int(max_columns),
    )
    if not selection["selected"]:
        raise ValueError("No comparable business columns remain after exclusions.")
    _validate_decimal_quantized_values(source_df, target_df, selection["selected"])
    comparison = _exact_projected_comparison(source_df, target_df, selection["selected"])
    status = "Passed" if schema_matches and comparison["differentRows"] == 0 else "Failed"
    ignored_system = _bounded_evidence(selection["ignoredSystem"])
    ignored_configured = _bounded_evidence(selection["ignoredConfigured"])
    unsupported_complex = _bounded_evidence(selection["unsupportedComplex"])
    compatible_types = _bounded_evidence(selection["compatibleTypes"])
    decimal_quantized_types = _bounded_evidence([
        item
        for item in selection["compatibleTypes"]
        if item.get("comparisonMode") == "decimal-quantized"
    ])
    incompatible_types = _bounded_evidence(selection["incompatibleTypes"])
    omitted_by_limit = _bounded_evidence(selection["omittedByLimit"])
    receipt = {
        "process": "DELTA_TABLE_PARITY",
        "mappingId": mapping_id,
        "status": status,
        "schemaMatches": schema_matches,
        "sourceSchema": schema_evidence["sourceSummary"],
        "targetSchema": schema_evidence["targetSummary"],
        "schemaDifferences": schema_evidence["differences"],
        "schemaDifferenceCount": schema_evidence["differenceCount"],
        "schemaDifferencesTruncated": schema_evidence["differencesTruncated"],
        "schemaCompatibleDifferenceCount": schema_evidence["compatibleDifferenceCount"],
        "schemaIncompatibleDifferenceCount": schema_evidence["incompatibleDifferenceCount"],
        **comparison,
        "comparedColumns": [plan["name"] for plan in selection["selected"]],
        "comparedColumnCount": len(selection["selected"]),
        "maxColumns": int(max_columns),
        "ignoredSystemColumns": ignored_system["values"],
        "ignoredSystemColumnCount": ignored_system["count"],
        "ignoredSystemColumnsTruncated": ignored_system["truncated"],
        "ignoredConfiguredColumns": ignored_configured["values"],
        "ignoredConfiguredColumnCount": ignored_configured["count"],
        "ignoredConfiguredColumnsTruncated": ignored_configured["truncated"],
        "unsupportedComplexColumns": unsupported_complex["values"],
        "unsupportedComplexColumnCount": unsupported_complex["count"],
        "unsupportedComplexColumnsTruncated": unsupported_complex["truncated"],
        "compatibleTypeColumns": compatible_types["values"],
        "compatibleTypeColumnCount": compatible_types["count"],
        "compatibleTypeColumnsTruncated": compatible_types["truncated"],
        "decimalQuantizedColumns": decimal_quantized_types["values"],
        "decimalQuantizedColumnCount": decimal_quantized_types["count"],
        "decimalQuantizedColumnsTruncated": decimal_quantized_types["truncated"],
        "incompatibleTypeColumns": incompatible_types["values"],
        "incompatibleTypeColumnCount": incompatible_types["count"],
        "incompatibleTypeColumnsTruncated": incompatible_types["truncated"],
        "omittedByColumnLimit": omitted_by_limit["values"],
        "omittedByColumnLimitCount": omitted_by_limit["count"],
        "omittedByColumnLimitTruncated": omitted_by_limit["truncated"],
        "comparisonScope": (
            "decimal-quantized-selected-column-multiset"
            if decimal_quantized_types["count"]
            else "exact-selected-column-multiset"
        ),
        "sourceWatermarkColumn": source_watermark_name,
        "targetWatermarkColumn": target_watermark_name,
        "sourceLastWriteUtc": source_last_write_utc,
        "targetLastWriteUtc": target_last_write_utc,
        "nativeExecutionEnabled": spark.conf.get("spark.native.enabled", "false").casefold() == "true",
        "ansiEnabled": spark.conf.get("spark.sql.ansi.enabled", "false").casefold() == "true",
    }
except Exception as exc:
    receipt = {
        "process": "DELTA_TABLE_PARITY",
        "mappingId": mapping_id,
        "status": "Blocked",
        "errorType": type(exc).__name__,
        "errorMessage": str(exc),
    }

receipt["durationMs"] = int((time.perf_counter() - started) * 1000)
receipt["completedUtc"] = datetime.now(timezone.utc).isoformat()
notebookutils.notebook.exit(json.dumps(receipt, sort_keys=True, default=str))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
