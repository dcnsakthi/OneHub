# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Data Pipeline Lineage Generator
# 
# This notebook is a thin central-workspace orchestrator. Runtime logic lives in
# `NB_Helper_Functions_Lineage` so lineage generation remains testable and reusable.
# The helper reads metadata for the supplied data product context, then writes
# lineage records back to the central `Data_Pipeline_Lineage` table.

# PARAMETERS CELL ********************

# Product metadata context. Use "default" for the core workspace, or pass a product key
# plus product metadata values from VL_Data_Product_Context for a data product workspace.
data_product_key = "default"
metadata_warehouse_name = "metadata_warehouse"
metadata_warehouse_id = "23dfa05f-6adf-491b-a47a-0cce7bf9b14c"
metadata_warehouse_endpoint = "zhjurl47bhxe7p63ls3uizuu7i-s536l56dwu6uhclskrxyzxdxlq.datawarehouse.fabric.microsoft.com"
metadata_workspace_id = "76e43058-b99b-403a-8603-174631f16837"
metadata_lakehouse_id = ""
metadata_lakehouse_workspace_id = ""

# Central metadata context. Leave blank to use the metadata values in single-workspace deployments.
central_metadata_warehouse_name = ""
central_metadata_warehouse_id = ""
central_metadata_warehouse_endpoint = ""
central_metadata_workspace_id = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Load Helper Functions

# CELL ********************

%run NB_Helper_Functions_1

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

# CELL ********************

%run NB_Helper_Functions_Lineage

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Helper modules loaded")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Generate and Persist Lineage

# CELL ********************

lineage_result = generate_and_persist_lineage(
    spark=spark,
    notebookutils=notebookutils,
    data_product_key=data_product_key,
    metadata_warehouse_name=metadata_warehouse_name,
    metadata_warehouse_id=metadata_warehouse_id,
    metadata_warehouse_endpoint=metadata_warehouse_endpoint,
    metadata_workspace_id=metadata_workspace_id,
    metadata_lakehouse_id=metadata_lakehouse_id,
    metadata_lakehouse_workspace_id=metadata_lakehouse_workspace_id,
    central_metadata_warehouse_name=central_metadata_warehouse_name,
    central_metadata_warehouse_id=central_metadata_warehouse_id,
    central_metadata_warehouse_endpoint=central_metadata_warehouse_endpoint,
    central_metadata_workspace_id=central_metadata_workspace_id,
)

print("Lineage generation complete")
print(f"   Data product: {lineage_result['data_product_key']}")
if lineage_result.get("data_product_key") == "all":
    # Cross-product fan-out returns a per-product summary, not single-run version fields.
    print(f"   Products processed: {lineage_result.get('product_count', 0)}")
    for product_result in lineage_result.get("products", []):
        if product_result.get("error"):
            print(f"   - {product_result['data_product_key']}: ERROR {product_result['error']}")
        else:
            print(
                f"   - {product_result['data_product_key']}: "
                f"version {product_result.get('lineage_version')}, "
                f"rows appended {product_result.get('rows_to_append', 0)}"
            )
else:
    print(f"   Previous version: {lineage_result['previous_version']}")
    print(f"   Current version: {lineage_result['lineage_version']}")
print(f"   Rows appended: {lineage_result['rows_to_append']}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
