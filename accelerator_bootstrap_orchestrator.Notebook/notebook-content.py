# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# MARKDOWN ********************

# # Fabric Artifacts Deployment Orchestrator
# 
# This notebook orchestrates the deployment of Microsoft Fabric artifacts (lakehouses, warehouses, pipelines, notebooks, semantic models, and reports) to target workspaces.
# 
# ## Overview
# This orchestrator notebook configures the deployment parameters and then calls the `create_accelerator_artifacts` notebook to perform the actual deployment operations.
# 
# ## Prerequisites
# - Access to target Microsoft Fabric workspaces
# - Appropriate permissions to create artifacts in the target workspaces
# - Valid workspace IDs for all target locations

# MARKDOWN ********************

# ## Step 1: Configure Lakehouse and Warehouse Deployment
# 
# Define the **names** and target **workspace IDs** for each lakehouse and warehouse to be deployed. 
# 
# **You can customize:**
# - **Names**: Change the lakehouse/warehouse names to match your naming conventions (e.g., "bronze" → "bronze_sales")
# - **Workspace IDs (Optional)**: Set the workspace ID where each artifact should be created. If left empty, the current workspace will be used
# - **Folder**: Artifacts default to `fabric_dp_accelerator`; set `folder_name` to an empty string to deploy at workspace root
# 
# The system will:
# - Create lakehouses if they don't exist in the target workspace
# - Create warehouses if they don't exist in the target workspace
# - Skip creation if items already exist (case-insensitive comparison)
# - Place all artifacts in the specified folder if `folder_name` is provided
# - Use the current workspace for any workspace ID left empty
# 
# **Lakehouses:**
# - **Bronze**: Raw data ingestion layer
# - **Silver**: Cleaned and transformed data layer  
# - **Gold**: Business-ready aggregated data layer
# - **Metadata**: Storage for configuration and metadata files
# 
# **Warehouses:**
# - **Metadata**: Central metadata storage and logging for metadata-driven ingestion
# 
# **Note:** You can customize the names, workspace IDs, and `folder_name` to match your deployment.


# MARKDOWN ********************

# ## Step 2: Configure Workspace IDs for Compute and Artifacts
# 
# Define the workspace IDs for hosting different types of artifacts:
# 
# - **spark_compute_workspace_id**: Where notebooks will be deployed
# - **pipeline_workspace_id**: Where data pipelines will be deployed  
# - **report_semantic_model_workspace_id**: Where semantic models and reports will be deployed for data pipeline monitoring
# 
# **Note:** These can all point to the same workspace or be distributed across different workspaces based on your organization's requirements. If a `folder_name` is specified in Step 1, all artifacts will be organized within that folder in their respective workspaces.


# MARKDOWN ********************

# ### Default Workspace IDs
# 
# If workspace IDs are not provided, they will default to the current workspace ID. This ensures that all artifacts are deployed to the appropriate environment even if specific workspace IDs are not explicitly defined.
# 
# The following logic is applied:
# - **Lakehouses (Bronze, Silver, Gold, Metadata)**: Default to the current workspace ID if not specified.
# - **Compute and Artifacts Workspaces**: Spark compute, pipelines, and semantic models will also default to the current workspace ID if left empty.
# 
# This approach simplifies deployment by reducing the need for manual input while maintaining flexibility for customization.


# PARAMETERS CELL ********************

# OPTIONAL: input folder name to store all created resources for each workspace
# Set to an empty string to deploy to the workspace root.
folder_name = "fabric_dp_accelerator"

# step 1
bronze_lakehouse_name = "bronze"
bronze_lakehouse_workspace_id = ""

silver_lakehouse_name = "silver"
silver_lakehouse_workspace_id = ""

gold_lakehouse_name = "gold"
gold_lakehouse_workspace_id = ""

metadata_workspace_id = ""
metadata_warehouse_name = "metadata_warehouse"
metadata_lakehouse_name = "metadata_lakehouse"

# step 2
spark_compute_workspace_id = ""
pipeline_workspace_id = ""
report_semantic_model_workspace_id = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3: Execute Deployment
# 
# The cell below runs the `create_accelerator_artifacts` notebook which performs the following operations:
# 
# 1. **Deploy Lakehouses**: Creates Bronze, Silver, Gold, and `metadata_lakehouse` lakehouses in their respective workspaces
# 2. **Deploy Warehouses**: Creates the Metadata warehouse for centralized metadata storage
# 3. **Deploy Notebooks**: Deploys all helper notebooks and data processing notebooks to the spark compute workspace
# 4. **Deploy Pipelines**: Deploys orchestration and data ingestion pipelines to the pipeline workspace
# 5. **Deploy Semantic Models**: Deploys Power BI semantic models for the following reports
# 6. **Deploy Reports**: Deploys pre-built Power BI reports for monitoring and analysis:
#    - **Data Pipeline Monitoring**: Monitors data pipelines and data quality notifications for every table and trigger
#    - **Exploratory Data Analysis**: Provides summary statistics for all ingested lakehouse tables
#    - **Data Lineage**: Visualizes data flow and dependencies across Bronze, Silver, and Gold layers
# 
# The deployment process will:
# - Skip items that already exist in target workspaces
# - Update connection strings and references to point to the newly created lakehouses and warehouses
# - Preserve pipeline logic while adapting to the new environment
# 
# **Execution Time:** ~5 minutes.

# CELL ********************

%run create_accelerator_artifacts
