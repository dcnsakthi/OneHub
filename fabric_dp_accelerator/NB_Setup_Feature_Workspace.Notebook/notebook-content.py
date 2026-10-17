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

# # 🚀 Feature Workspace Setup
# 
# **One-click setup** for your feature workspace after Git sync. This notebook:
# 
# | Step | What It Does |
# |------|-------------|
# | **1** | Discovers all workspace artifacts (warehouse, lakehouses, notebooks, environment) |
# | **2** | Updates `FeatureWorkspaceConfig` variable overrides in `VL_Core_Workspace_Context` |
# | **3** | Activates the value set so overrides take effect |
# | **4** | Executes your datastore configuration notebook against the warehouse |
# | **5** | Executes your selected metadata SQL notebook(s) against the warehouse |
# 
# ### How to Use
# 1. **Edit the configuration cell below** — set your datastore notebook name and metadata notebook names
# 2. **Run All** — the notebook handles everything else automatically
# 
# > ⚠️ **Prerequisites**: Your feature workspace must already be synced from Git and contain the expected artifacts.

# PARAMETERS CELL ********************

# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║                        📋 CONFIGURATION — EDIT ME                          ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

# ── Datastore Notebook ────────────────────────────────────────────────────────
# Name of the datastore SQL notebook to execute (in the "datastores" folder).
# This inserts environment-specific Lakehouse/Warehouse GUIDs into the
# Datastore_Configuration table.
# Examples: "datastore_DEV", "datastore_QA", "datastore_PROD"
DATASTORE_NOTEBOOK = "datastore_DEV"

# ── Metadata Notebooks ───────────────────────────────────────────────────────
# List the metadata SQL notebook(s) to execute (in the "metadata" folder).
# These insert pipeline orchestration records into the warehouse.
# Add as many as you need — they will run in order.
METADATA_NOTEBOOKS = [
    # "metadata_YourTriggerName",
    # "metadata_YourTriggerName2"
]

# ── Advanced (usually no changes needed) ─────────────────────────────────────
# Warehouse display name (case-insensitive match)
METADATA_WAREHOUSE_NAME = "metadata_warehouse"

# Lakehouse display name (case-insensitive match)
METADATA_LAKEHOUSE_NAME = "metadata_lakehouse"

# Variable Library display name
VARIABLE_LIBRARY_NAME = "VL_Core_Workspace_Context"

# Value set name (matches the JSON file in the Variable Library)
VALUE_SET_NAME = "FeatureWorkspaceConfig"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 1 — Discover Workspace Artifacts
# 
# Auto-detects the current workspace ID, then queries the Fabric REST API to find:
# - **Warehouse** → ID + SQL endpoint
# - **Lakehouses** → metadata_files ID
# - **Notebooks** → IDs for batch processing, EDA, error logs, custom staging
# - **Environment** → default Spark environment ID
# - **Variable Library** → ID for updating overrides

# CELL ********************

import requests, json, base64, time, urllib.parse
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pyspark.sql.types import StructType, StructField, StringType

# ── Resilient HTTP session (timeout + retry on 429 / 5xx) ───────────────────
DEFAULT_TIMEOUT = 30  # seconds per request

class _TimeoutAdapter(HTTPAdapter):
    """HTTPAdapter that injects a default timeout on every request."""
    def __init__(self, *args, timeout=DEFAULT_TIMEOUT, **kwargs):
        self.timeout = timeout
        super().__init__(*args, **kwargs)
    def send(self, request, **kwargs):
        kwargs.setdefault("timeout", self.timeout)
        return super().send(request, **kwargs)

_retry_strategy = Retry(
    total=4,                                     # up to 4 retries
    backoff_factor=1,                             # 1s, 2s, 4s, 8s
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET", "POST", "PATCH"],
    respect_retry_after_header=True,              # honour Retry-After from 429
)

session = requests.Session()
_adapter = _TimeoutAdapter(max_retries=_retry_strategy)
session.mount("https://", _adapter)
session.mount("http://", _adapter)

# ── Auth & workspace context ─────────────────────────────────────────────────
fabric_token = notebookutils.credentials.getToken("pbi")
headers = {
    "Authorization": f"Bearer {fabric_token}",
    "Content-Type": "application/json",
}
session.headers.update(headers)
BASE_URL = "https://api.fabric.microsoft.com/v1"

workspace_id = notebookutils.runtime.context.get("currentWorkspaceId")
workspace_name = notebookutils.runtime.context.get("currentWorkspaceName")
print(f"🏢 Workspace: {workspace_name} ({workspace_id})")

# ── Helper: list items by type ───────────────────────────────────────────────
def list_items(item_type):
    """Return {displayName: id} dict for all items of a given type."""
    first_url = f"{BASE_URL}/workspaces/{workspace_id}/items?type={item_type}"
    url = first_url
    items = {}
    while url:
        resp = session.get(url)
        resp.raise_for_status()
        payload = resp.json()
        items.update({item["displayName"]: item["id"] for item in payload.get("value", [])})
        continuation_uri = payload.get("continuationUri")
        continuation_token = payload.get("continuationToken")
        if continuation_uri:
            url = continuation_uri
        elif continuation_token:
            encoded_token = urllib.parse.quote(str(continuation_token), safe="")
            url = f"{first_url}&continuationToken={encoded_token}"
        else:
            url = None
    return items

def wait_for_sql_endpoint(url, property_path, label, max_attempts=60):
    for _ in range(max_attempts):
        resp = session.get(url)
        resp.raise_for_status()
        connection_string = resp.json()
        for property_name in property_path:
            connection_string = (connection_string or {}).get(property_name)
        if connection_string:
            return connection_string
        time.sleep(2)
    raise RuntimeError(f"{label} SQL endpoint was not ready after {max_attempts * 2} seconds.")

# ── Helper: get warehouse SQL endpoint ───────────────────────────────────────
def get_warehouse_endpoint(warehouse_id):
    """Return the SQL connection string for a warehouse."""
    return wait_for_sql_endpoint(
        f"{BASE_URL}/workspaces/{workspace_id}/warehouses/{warehouse_id}",
        ("properties", "connectionString"),
        f"Warehouse '{warehouse_id}'",
    )

def get_lakehouse_endpoint(lakehouse_id):
    """Return the SQL connection string for a lakehouse."""
    return wait_for_sql_endpoint(
        f"{BASE_URL}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}",
        ("properties", "sqlEndpointProperties", "connectionString"),
        f"Lakehouse '{lakehouse_id}'",
    )

# ── Discover all artifact types ──────────────────────────────────────────────
print("\n🔍 Discovering workspace artifacts...")

warehouses   = list_items("Warehouse")
lakehouses   = list_items("Lakehouse")
notebooks    = list_items("Notebook")
var_libs     = list_items("VariableLibrary")

# Environments use a dedicated API endpoint (not the generic items API)
env_url = f"{BASE_URL}/workspaces/{workspace_id}/environments"
env_resp = session.get(env_url)
env_resp.raise_for_status()
environments = {e["displayName"]: e["id"] for e in env_resp.json().get("value", [])}

# ── Resolve key artifacts ────────────────────────────────────────────────────
# Warehouse
wh_match = next(
    ((name, wid) for name, wid in warehouses.items()
     if name.lower() == METADATA_WAREHOUSE_NAME.lower()),
    None,
)
if not wh_match:
    raise RuntimeError(
        f"❌ Warehouse '{METADATA_WAREHOUSE_NAME}' not found. "
        f"Available: {list(warehouses.keys())}"
    )
warehouse_display_name, warehouse_id_resolved = wh_match
warehouse_endpoint = get_warehouse_endpoint(warehouse_id_resolved)

print(f"   ✅ Warehouse: {warehouse_display_name} → {warehouse_id_resolved}")
print(f"      Endpoint:  {warehouse_endpoint}")

# Metadata Files lakehouse
lakehouse_names_lower = {k.lower(): (k, v) for k, v in lakehouses.items()}
metadata_lakehouse = lakehouse_names_lower.get(METADATA_LAKEHOUSE_NAME.lower())
if not metadata_lakehouse:
    print(f"   ⚠️  Lakehouse '{METADATA_LAKEHOUSE_NAME}' not found — will skip that override")
    metadata_lakehouse_id = None
else:
    metadata_lakehouse_id = metadata_lakehouse[1]
    print(f"   ✅ Lakehouse (metadata_files): {metadata_lakehouse[0]} → {metadata_lakehouse_id}")

CENTRAL_DATASTORE_TABLE_NAME = "central_datastore_configuration"
CENTRAL_DATASTORE_SCHEMA = StructType([
    StructField("Datastore_Name", StringType(), True),
    StructField("Datastore_Type", StringType(), True),
    StructField("Datastore_ID", StringType(), True),
    StructField("Workspace_ID", StringType(), True),
    StructField("Workspace_Name", StringType(), True),
    StructField("Medallion_Layer", StringType(), True),
    StructField("Endpoint", StringType(), True),
    StructField("Connection_ID", StringType(), True),
])

def _ensure_central_datastore_object(lakehouse_id, source_warehouse_id, table_path):
    """Make metadata_lakehouse/Tables/dbo/central_datastore_configuration resolve.

    Preferred: a OneLake shortcut to this workspace's own metadata warehouse
    Datastore_Configuration, so Get_Datastore_Details' central fallback returns the real
    central catalog. Get_Datastore_Details checks the local warehouse table first, so
    locally-defined datastores still override by name. Falls back to an empty managed
    table so the static SQL reference always resolves even if the shortcut cannot be made.
    """
    source_path = "Tables/dbo/Datastore_Configuration"
    get_url = f"{BASE_URL}/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts/Tables/dbo/{CENTRAL_DATASTORE_TABLE_NAME}"
    create_url = f"{BASE_URL}/workspaces/{workspace_id}/items/{lakehouse_id}/shortcuts?shortcutConflictPolicy=Abort"
    body = {
        "path": "Tables/dbo",
        "name": CENTRAL_DATASTORE_TABLE_NAME,
        "target": {"oneLake": {"workspaceId": workspace_id, "itemId": source_warehouse_id, "path": source_path}},
    }
    try:
        if not source_warehouse_id:
            raise RuntimeError("metadata warehouse id unavailable")
        shortcut_current = False
        existing = session.get(get_url)
        if existing.status_code == 200:
            target = existing.json().get("target", {}).get("oneLake", {})
            shortcut_current = (
                target.get("workspaceId") == workspace_id
                and target.get("itemId") == source_warehouse_id
                and target.get("path") == source_path
            )
            if not shortcut_current:
                delete_resp = session.delete(get_url)
                if delete_resp.status_code == 202:
                    # Async delete: wait until the shortcut is actually gone before recreating so the
                    # POST below cannot race the delete and fail with a conflict.
                    for _ in range(30):
                        time.sleep(2)
                        delete_poll = session.get(get_url)
                        if delete_poll.status_code == 404:
                            break
                        if delete_poll.status_code != 200:
                            delete_poll.raise_for_status()
                    else:
                        raise RuntimeError("Shortcut delete (202) did not complete before recreate.")
                elif delete_resp.status_code not in (200, 204):
                    delete_resp.raise_for_status()
        elif existing.status_code == 404:
            # No shortcut yet; clear any empty managed table occupying the name.
            try:
                notebookutils.fs.rm(table_path, True)
            except Exception:
                pass
        else:
            # Transient/permission failure (429/5xx/403): do not treat as "absent" and do not remove
            # the managed table — surface the real error to the placeholder fallback handler below.
            existing.raise_for_status()
        if not shortcut_current:
            resp = session.post(create_url, json=body)
            if resp.status_code not in (200, 201, 202):
                resp.raise_for_status()
        print(f"   ✅ Shortcut ready: {CENTRAL_DATASTORE_TABLE_NAME} -> own metadata_warehouse.{source_path}")
    except Exception as exc:
        print(f"   ⚠️  Shortcut unavailable ({exc}); creating empty placeholder table instead.")
        spark.createDataFrame([], CENTRAL_DATASTORE_SCHEMA).write.format("delta").mode("ignore").save(table_path)
        print(f"   ✅ Lakehouse table available: {CENTRAL_DATASTORE_TABLE_NAME}")

if metadata_lakehouse_id:
    central_datastore_table_path = (
        f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/"
        f"{metadata_lakehouse_id}/Tables/dbo/{CENTRAL_DATASTORE_TABLE_NAME}"
    )
    _ensure_central_datastore_object(metadata_lakehouse_id, warehouse_id_resolved, central_datastore_table_path)

# Notebooks (best-effort lookup — only override the ones that exist)
NOTEBOOK_VARIABLE_MAP = {
    "NB_Batch_Processing":               "notebook_id_batch_processing",
    "NB_Run_Exploratory_Data_Analysis":   "notebook_id_exploratory_data_analysis",
}

notebook_overrides = {}
for nb_display_name, var_name in NOTEBOOK_VARIABLE_MAP.items():
    nb_id = notebooks.get(nb_display_name)
    if nb_id:
        notebook_overrides[var_name] = nb_id
        print(f"   ✅ Notebook: {nb_display_name} → {nb_id}")
    else:
        print(f"   ⚠️  Notebook '{nb_display_name}' not found — skipping override")

# Spark Environment (look for ENV_Default first, then any env_ prefix, then fallback)
env_match = next(
    ((name, eid) for name, eid in environments.items()
     if name == "ENV_Default"),
    next(
        ((name, eid) for name, eid in environments.items()
         if name.lower().startswith("env_")),
        next(iter(environments.items()), None),
    ),
)
if env_match:
    env_name, env_id = env_match
    print(f"   ✅ Environment: {env_name} → {env_id}")
else:
    env_id = None
    print("   ⚠️  No Spark environment found — skipping override")

# Variable Library
vl_match = next(
    ((name, vid) for name, vid in var_libs.items()
     if name.lower() == VARIABLE_LIBRARY_NAME.lower()),
    None,
)
if not vl_match:
    raise RuntimeError(
        f"❌ Variable Library '{VARIABLE_LIBRARY_NAME}' not found. "
        f"Available: {list(var_libs.keys())}"
    )
vl_display_name, vl_id = vl_match
print(f"   ✅ Variable Library: {vl_display_name} → {vl_id}")

print("\n✅ Discovery complete!")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 2 — Update FeatureWorkspaceConfig Overrides
# 
# Reads the current Variable Library definition, then replaces **only** the
# `FeatureWorkspaceConfig` value set's `variableOverrides` array with the
# discovered artifact IDs. Base variables are **not modified**.

# CELL ********************

# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  Build the override list from discovered artifacts                         ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

overrides = []

def add_override(name, value):
    """Append a variable override entry."""
    if value:
        overrides.append({"name": name, "value": value})

# Metadata warehouse
add_override("default_workspace_id",          workspace_id)
add_override("default_warehouse_id",          warehouse_id_resolved)
add_override("default_warehouse_endpoint",    warehouse_endpoint)

# Logging — intentionally NOT overridden so logs go to the central workspace.
# The base variable values (pointing to the shared dev/prod warehouse) are kept as-is.

# Spark compute
add_override("spark_compute_workspace_id",    workspace_id)

# Pipeline invocation workspace. Feature workspace agents and direct notebook runs
# must target the feature workspace pipelines, not the central workspace pipelines.
add_override("pipeline_workspace_id",         workspace_id)

# Metadata files lakehouse
add_override("default_lakehouse_id",          metadata_lakehouse_id)

# Notebooks
for var_name, nb_id in notebook_overrides.items():
    add_override(var_name, nb_id)

# Spark environment
add_override("default_spark_environment_id",  env_id)

# Feature workspaces should execute the notebook definition for custom functions
# so branch edits are not shadowed by stale .py exports in the metadata lakehouse.
add_override("custom_function_load_mode",     "notebook")
add_override("is_feature_workspace",          "true")

print("📝 Variable overrides to write:")
print("─" * 60)
for o in overrides:
    print(f"   {o['name']:45s} = {o['value']}")
print("─" * 60)
print(f"   Total: {len(overrides)} override(s)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ╔══════════════════════════════════════════════════════════════════════════════╗
# ║  Read current Variable Library definition, update only the value set       ║
# ╚══════════════════════════════════════════════════════════════════════════════╝

print("📥 Reading current Variable Library definition...")

# ── Helper: poll Fabric long-running operation to completion ─────────────────
def poll_lro(response, label="operation", max_attempts=60):
    """
    Handle Fabric async (202) responses — poll until done, then fetch /result.
    Raises RuntimeError on failure or timeout.

    NOTE: The Fabric getDefinition endpoint almost always returns 202.
    If you see "NoneType is not subscriptable" on the response body, it
    means the LRO was not polled — make sure this function is called.
    """
    if response.status_code == 200:
        # Some Fabric APIs return 200 with an operation-status body instead
        # of the actual payload. Detect this and follow the LRO path.
        try:
            body = response.json()
            if body and "status" in body and "definition" not in body:
                # This is an operation-status response, not the definition
                print(f"   ℹ️  {label}: 200 response is an operation status, not a definition — following LRO")
                op_status = body.get("status", "").lower()
                if op_status == "failed":
                    error_info = body.get("error", body)
                    raise RuntimeError(f"{label} FAILED: {json.dumps(error_info, indent=2)}")
                # Try to get /result from the operation URL if available
                op_id = body.get("id")
                if op_id:
                    result_url = f"{BASE_URL}/operations/{op_id}/result"
                    result_resp = session.get(result_url)
                    if result_resp.status_code == 200:
                        return result_resp
        except (ValueError, KeyError):
            pass  # Not JSON or unexpected shape — return as-is
        return response  # Synchronous — definition already in body
    if response.status_code != 202:
        print(f"   ❌ {label}: unexpected status {response.status_code}")
        print(f"      Body: {response.text[:500]}")
        response.raise_for_status()

    poll_url = response.headers.get("Location")
    if not poll_url:
        raise RuntimeError(f"{label} returned 202 but no Location header! Headers: {dict(response.headers)}")

    print(f"   ⏳ {label}: polling for completion...")
    for attempt in range(1, max_attempts + 1):
        time.sleep(2)
        poll_resp = session.get(poll_url)
        if poll_resp.status_code == 200:
            # Check operation status in body (200 just means "operation finished", not necessarily "succeeded")
            try:
                op_body = poll_resp.json()
                op_status = op_body.get("status", "")
                if op_status.lower() == "failed":
                    error_info = op_body.get("error", op_body)
                    raise RuntimeError(f"{label} FAILED: {json.dumps(error_info, indent=2)}")
                print(f"   ✅ {label}: completed (poll #{attempt}, status={op_status or 'OK'})")
            except (ValueError, KeyError):
                print(f"   ✅ {label}: completed (poll #{attempt})")
            # Fetch the actual result payload
            result_url = f"{poll_url}/result"
            result_resp = session.get(result_url)
            result_resp.raise_for_status()
            return result_resp
        elif poll_resp.status_code == 202:
            if attempt % 5 == 0:
                print(f"      Still waiting... (poll #{attempt})")
            continue
        else:
            print(f"   ❌ {label}: poll returned {poll_resp.status_code}")
            print(f"      Body: {poll_resp.text[:500]}")
            poll_resp.raise_for_status()

    raise RuntimeError(f"{label} timed out after {max_attempts} polling attempts")

# ── Helper: poll an update/write LRO (no /result needed) ────────────────────
def poll_write_lro(response, label="write", max_attempts=60):
    """
    Poll a write operation (updateDefinition, PATCH) that returns 202.
    Unlike getDefinition, write operations don't have a /result endpoint.
    """
    if response.status_code == 200:
        return  # Synchronous success
    if response.status_code not in (200, 202):
        print(f"   ❌ {label}: unexpected status {response.status_code}")
        print(f"      Body: {response.text[:500]}")
        response.raise_for_status()
    if response.status_code != 202:
        return

    poll_url = response.headers.get("Location")
    if not poll_url:
        raise RuntimeError(f"{label} returned 202 but no Location header! Headers: {dict(response.headers)}")

    print(f"   ⏳ {label}: polling for completion...")
    for attempt in range(1, max_attempts + 1):
        time.sleep(2)
        poll_resp = session.get(poll_url)
        if poll_resp.status_code == 200:
            # Check if operation reports failure
            try:
                op_body = poll_resp.json()
                op_status = op_body.get("status", "")
                if op_status.lower() == "failed":
                    error_info = op_body.get("error", op_body)
                    raise RuntimeError(f"{label} FAILED: {json.dumps(error_info, indent=2)}")
                print(f"   ✅ {label}: completed (poll #{attempt}, status={op_status or 'OK'})")
            except (ValueError, KeyError):
                print(f"   ✅ {label}: completed (poll #{attempt})")
            return
        elif poll_resp.status_code == 202:
            if attempt % 5 == 0:
                print(f"      Still waiting... (poll #{attempt})")
            continue
        else:
            print(f"   ❌ {label}: poll returned {poll_resp.status_code}")
            print(f"      Body: {poll_resp.text[:500]}")
            poll_resp.raise_for_status()

    raise RuntimeError(f"{label} timed out after {max_attempts} polling attempts")

# ══════════════════════════════════════════════════════════════════════════════
# 2a. READ the current Variable Library definition
# ══════════════════════════════════════════════════════════════════════════════
get_def_url = f"{BASE_URL}/workspaces/{workspace_id}/VariableLibraries/{vl_id}/getDefinition"
resp = session.post(get_def_url)
resp = poll_lro(resp, label="getDefinition")

definition_body = resp.json()

# ── Defensive validation ─────────────────────────────────────────────────
if definition_body is None:
    raise RuntimeError(
        f"❌ getDefinition returned an empty body (HTTP {resp.status_code}). "
        f"Raw text: {resp.text[:500]}"
    )
if "definition" not in definition_body or definition_body["definition"] is None:
    raise RuntimeError(
        f"❌ getDefinition response missing 'definition' key. "
        f"This usually means the LRO was not fully resolved. "
        f"Keys present: {list(definition_body.keys())}. "
        f"Body preview: {json.dumps(definition_body, indent=2)[:500]}"
    )
if "parts" not in definition_body["definition"]:
    raise RuntimeError(
        f"❌ getDefinition 'definition' object has no 'parts'. "
        f"Keys: {list(definition_body['definition'].keys())}. "
        f"Body preview: {json.dumps(definition_body, indent=2)[:500]}"
    )

current_parts = definition_body["definition"]["parts"]

# Decode each part so we can inspect/modify
decoded = {}
for part in current_parts:
    raw = base64.b64decode(part["payload"]).decode("utf-8")
    decoded[part["path"]] = json.loads(raw)

print(f"   Parts found: {list(decoded.keys())}")

# ══════════════════════════════════════════════════════════════════════════════
# 2b. BUILD the new FeatureWorkspaceConfig value set
# ══════════════════════════════════════════════════════════════════════════════
value_set_path = f"valueSets/{VALUE_SET_NAME}.json"
new_value_set = {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/variableLibrary/definition/valueSet/1.0.0/schema.json",
    "name": VALUE_SET_NAME,
    "variableOverrides": overrides,
}

# Ensure FeatureWorkspaceConfig is in the activation order
settings = decoded.get("settings.json", {
    "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/variableLibrary/definition/settings/1.0.0/schema.json",
    "valueSetsOrder": [],
})
if VALUE_SET_NAME not in settings.get("valueSetsOrder", []):
    settings.setdefault("valueSetsOrder", []).append(VALUE_SET_NAME)
    print(f"   ➕ Added '{VALUE_SET_NAME}' to valueSetsOrder")

# ══════════════════════════════════════════════════════════════════════════════
# 2c. REASSEMBLE the definition parts
# ══════════════════════════════════════════════════════════════════════════════
def encode_part(path, obj):
    return {
        "path": path,
        "payload": base64.b64encode(json.dumps(obj, indent=2).encode("utf-8")).decode("utf-8"),
        "payloadType": "InlineBase64",
    }

new_parts = []
replaced_value_set = False
for part in current_parts:
    path = part["path"]
    if path == value_set_path:
        new_parts.append(encode_part(path, new_value_set))
        replaced_value_set = True
    elif path == "settings.json":
        new_parts.append(encode_part(path, settings))
    else:
        # Keep as-is (variables.json, .platform, etc.)
        new_parts.append(part)

# If value set file didn't exist yet, add it
if not replaced_value_set:
    new_parts.append(encode_part(value_set_path, new_value_set))
    print(f"   ➕ Value set part '{value_set_path}' did not exist — added new")
else:
    print(f"   🔄 Replaced existing value set part '{value_set_path}'")

print(f"   📦 Sending {len(new_parts)} definition part(s): {[p['path'] for p in new_parts]}")

# ══════════════════════════════════════════════════════════════════════════════
# 2d. PUSH the updated definition
# ══════════════════════════════════════════════════════════════════════════════
update_def_url = f"{BASE_URL}/workspaces/{workspace_id}/VariableLibraries/{vl_id}/updateDefinition"
update_body = {"definition": {"parts": new_parts}}

print("📤 Pushing updated Variable Library definition...")
resp = session.post(update_def_url, json=update_body)
print(f"   updateDefinition response: {resp.status_code}")
if resp.status_code not in (200, 202):
    print(f"   ❌ Response body: {resp.text[:1000]}")
    resp.raise_for_status()
poll_write_lro(resp, label="updateDefinition")

# ══════════════════════════════════════════════════════════════════════════════
# 2e. VERIFY the update actually persisted (re-read and compare)
# ══════════════════════════════════════════════════════════════════════════════
print("🔍 Verifying definition update persisted...")
time.sleep(3)  # Brief pause to let Fabric settle

resp = session.post(get_def_url)
resp = poll_lro(resp, label="getDefinition (verify)")

verify_body = resp.json()
if not verify_body or not verify_body.get("definition") or "parts" not in verify_body["definition"]:
    raise RuntimeError(
        f"❌ VERIFICATION FAILED: getDefinition returned unexpected structure. "
        f"Body preview: {json.dumps(verify_body, indent=2)[:500] if verify_body else resp.text[:500]}"
    )
verify_parts = verify_body["definition"]["parts"]
vs_part = next((p for p in verify_parts if p["path"] == value_set_path), None)

if not vs_part:
    all_paths = [p["path"] for p in verify_parts]
    raise RuntimeError(
        f"❌ VERIFICATION FAILED: '{value_set_path}' not found in updated definition!\n"
        f"   Available paths: {all_paths}\n"
        f"   The updateDefinition call did not persist the value set."
    )

vs_data = json.loads(base64.b64decode(vs_part["payload"]).decode("utf-8"))
written_overrides = {o["name"]: o["value"] for o in vs_data.get("variableOverrides", [])}
expected_overrides = {o["name"]: o["value"] for o in overrides}

if written_overrides != expected_overrides:
    print(f"   ❌ VERIFICATION FAILED: overrides don't match!")
    print(f"\n   EXPECTED ({len(expected_overrides)}):")
    for k, v in sorted(expected_overrides.items()):
        print(f"      {k:45s} = {v}")
    print(f"\n   ACTUAL ({len(written_overrides)}):")
    for k, v in sorted(written_overrides.items()):
        print(f"      {k:45s} = {v}")
    missing = set(expected_overrides) - set(written_overrides)
    extra = set(written_overrides) - set(expected_overrides)
    wrong = {k for k in expected_overrides if k in written_overrides and expected_overrides[k] != written_overrides[k]}
    if missing: print(f"\n   Missing: {missing}")
    if extra:   print(f"   Extra:   {extra}")
    if wrong:   print(f"   Wrong:   {wrong}")
    raise RuntimeError("Variable Library overrides were NOT written correctly!")

print(f"   ✅ VERIFIED: {len(written_overrides)} override(s) confirmed in definition")
for k, v in sorted(written_overrides.items()):
    print(f"      {k:45s} = {v[:20]}..." if len(v) > 20 else f"      {k:45s} = {v}")

# ══════════════════════════════════════════════════════════════════════════════
# 2f. ACTIVATE the value set via PATCH
# ══════════════════════════════════════════════════════════════════════════════
print(f"\n🔄 Activating value set '{VALUE_SET_NAME}'...")
patch_url = f"{BASE_URL}/workspaces/{workspace_id}/VariableLibraries/{vl_id}"
patch_body = {
    "properties": {
        "activeValueSetName": VALUE_SET_NAME,
    }
}
resp = session.patch(patch_url, json=patch_body)
print(f"   PATCH response: {resp.status_code}")
if resp.status_code not in (200, 202):
    print(f"   ❌ Response body: {resp.text[:1000]}")
    resp.raise_for_status()
poll_write_lro(resp, label="PATCH activation")

print(f"✅ Value set '{VALUE_SET_NAME}' is now ACTIVE")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 3 — Execute Datastore Configuration
# 
# Extracts the SQL content from the notebook definition and executes it directly
# against the warehouse SQL endpoint via pyodbc. Much faster than running the
# notebook as a job — no session spin-up required.

# CELL ********************

import pyodbc, struct, re

# ── Helper: connect to warehouse via pyodbc + AAD token ──────────────────────
def connect_to_warehouse():
    """Open a pyodbc connection to the metadata warehouse using the Fabric token."""
    token_bytes = fabric_token.encode("utf-16-le")
    token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
    conn = pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={warehouse_endpoint};"
        f"DATABASE={warehouse_display_name};"
        f"Encrypt=Yes;TrustServerCertificate=No;",
        attrs_before={1256: token_struct},
    )
    conn.autocommit = True
    return conn

# ── Helpers: robust SQL extraction from Fabric notebook payloads ─────────────
def _clean_sql_cell(cell_text):
    """
    Strip Fabric notebook infrastructure lines from a raw cell chunk,
    returning only genuine SQL.

    Removes:
    - '-- Fabric notebook source' header
    - '-- METADATA ****…' sections and their '-- META …' JSON content
    - '-- MARKDOWN ****…' sections
    - Stray '-- META' lines
    """
    # METADATA / MARKDOWN blocks always trail the SQL in a cell chunk;
    # strip from the first marker to end-of-string.
    cell_text = re.sub(r'\n--\s*METADATA\s*\*{4,}.*', '', cell_text, flags=re.DOTALL)
    cell_text = re.sub(r'\n--\s*MARKDOWN\s*\*{4,}.*', '', cell_text, flags=re.DOTALL)
    # Notebook file header (appears before the first CELL)
    cell_text = re.sub(r'^--\s*Fabric notebook source\s*$', '', cell_text, flags=re.MULTILINE)
    # Defensive: remove any stray META lines that survived
    cell_text = re.sub(r'^--\s*META\b.*$', '', cell_text, flags=re.MULTILINE)
    return cell_text.strip()


def _split_batches(sql_text):
    """
    Split SQL text on GO batch separators.

    pyodbc (and the Fabric SQL endpoint) do not understand GO — it is a
    client-side directive recognised only by SSMS / sqlcmd.  We split here
    so each fragment is a single executable batch.

    GO must appear on its own line (with optional surrounding whitespace).
    """
    return [
        batch.strip()
        for batch in re.split(r'^\s*GO\s*$', sql_text, flags=re.MULTILINE | re.IGNORECASE)
        if batch.strip()
    ]


def _is_executable_sql(sql_text):
    """
    Return True if *sql_text* contains at least one real SQL token —
    not just comments (-- or /* … */) and whitespace.
    """
    # Strip block comments  /* … */
    stripped = re.sub(r'/\*.*?\*/', '', sql_text, flags=re.DOTALL)
    # Strip line comments  -- …
    stripped = re.sub(r'--.*$', '', stripped, flags=re.MULTILINE)
    return bool(stripped.strip())


def _drain_sql_results(cursor):
    while cursor.nextset():
        pass


# ── Helper: extract SQL from notebook definition and execute ─────────────────
def _inject_before_transaction_commit(executable_cells, sql_to_inject):
    # Must accept every COMMIT form the metadata validator allows.
    commit_pattern = re.compile(r"(?im)^[ \t]*COMMIT(?:\s+TRAN(?:SACTION)?)?\s*;?[ \t]*$")
    matches = []
    for cell_index, batches in enumerate(executable_cells):
        for batch_index, batch in enumerate(batches):
            for match in commit_pattern.finditer(batch):
                matches.append((cell_index, batch_index, match.start()))

    if len(matches) != 1:
        raise RuntimeError(
            "Datastore SQL must contain exactly one COMMIT TRANSACTION so feature bindings "
            f"can be applied atomically; found {len(matches)}."
        )

    cell_index, batch_index, insertion_index = matches[0]
    updated_cells = [list(batches) for batches in executable_cells]
    batch = updated_cells[cell_index][batch_index]
    updated_cells[cell_index][batch_index] = (
        batch[:insertion_index]
        + sql_to_inject.rstrip()
        + "\n"
        + batch[insertion_index:]
    )
    return updated_cells, (cell_index, batch_index)


def execute_sql_notebook(notebook_name, cursor, before_commit_sql="", before_commit_parameters=()):
    """
    Fetch the notebook definition via Fabric API, decode the SQL content,
    clean out notebook infrastructure markers, split on GO batch separators,
    and execute each batch against the warehouse.
    Returns the number of cells executed.
    """
    # 1. Find notebook ID
    all_notebooks = list_items("Notebook")
    nb_id = all_notebooks.get(notebook_name)
    if not nb_id:
        raise RuntimeError(
            f"❌ Notebook '{notebook_name}' not found in workspace. "
            f"Available notebooks: {sorted(all_notebooks.keys())}"
        )

    # 2. Get notebook definition (async LRO)
    get_def_url = f"{BASE_URL}/workspaces/{workspace_id}/items/{nb_id}/getDefinition"
    resp = session.post(get_def_url)
    resp = poll_lro(resp, label=f"getDefinition({notebook_name})")

    parts = resp.json()["definition"]["parts"]
    sql_part = next(
        (p for p in parts if p["path"].endswith(".sql")),
        None,
    )
    if not sql_part:
        raise RuntimeError(
            f"❌ No SQL content found in notebook '{notebook_name}'. "
            f"Parts: {[p['path'] for p in parts]}"
        )

    sql_content = base64.b64decode(sql_part["payload"]).decode("utf-8")

    # 3. Split and validate every cell before executing any SQL.
    raw_cells = sql_content.split("-- CELL ********************")
    executable_cells = []
    for raw_cell in raw_cells:
        cell_sql = _clean_sql_cell(raw_cell)
        if not cell_sql:
            continue
        batches = [batch for batch in _split_batches(cell_sql) if _is_executable_sql(batch)]
        if batches:
            executable_cells.append(batches)

    injected_location = None
    if before_commit_sql:
        executable_cells, injected_location = _inject_before_transaction_commit(
            executable_cells,
            before_commit_sql,
        )

    for cell_index, batches in enumerate(executable_cells):
        for batch_index, batch in enumerate(batches):
            if injected_location == (cell_index, batch_index):
                cursor.execute(batch, *before_commit_parameters)
            else:
                cursor.execute(batch)
            _drain_sql_results(cursor)
        cell_number = cell_index + 1
        batch_note = f" ({len(batches)} batch{'es' if len(batches) > 1 else ''})" if len(batches) > 1 else ""
        print(f"      Cell {cell_number}{batch_note} ✅")

    return len(executable_cells)


def build_current_workspace_datastore_rebind():
    rebind_items = [
        (warehouse_display_name, warehouse_id_resolved, warehouse_endpoint, "Warehouse", True)
    ]
    if metadata_lakehouse:
        metadata_lakehouse_name, metadata_lakehouse_item_id = metadata_lakehouse
        rebind_items.append(
            (
                metadata_lakehouse_name,
                metadata_lakehouse_item_id,
                get_lakehouse_endpoint(metadata_lakehouse_item_id),
                "Lakehouse",
                True,
            )
        )
    # Shared data stores intentionally stay on their authored environment IDs.
    rebind_items.extend((name, item_id, None, "Notebook", False) for name, item_id in notebooks.items())
    rebind_items.extend((name, item_id, None, "Dataflow", False) for name, item_id in list_items("Dataflow").items())

    statements = []
    parameters = []
    for datastore_name, datastore_id, endpoint, datastore_type, required in rebind_items:
        if required:
            missing_row_message = (
                "Feature workspace datastore rebinding is missing required row "
                f"{datastore_name} ({datastore_type})."
            ).replace("'", "''")
            statements.append(
                f"""
            IF NOT EXISTS (
                SELECT 1
                FROM dbo.Datastore_Configuration
                WHERE LOWER(Datastore_Name) = LOWER(?)
                  AND LOWER(Datastore_Type) = LOWER(?)
            )
                THROW 50001, '{missing_row_message}', 1;
                """
            )
            parameters.extend((datastore_name, datastore_type))
        # T-SQL requires the statement before a THROW to be semicolon-terminated.
        statements.append(
            """
            UPDATE dbo.Datastore_Configuration
            SET Datastore_ID = ?,
                Workspace_ID = ?,
                Workspace_Name = ?,
                Endpoint = COALESCE(?, Endpoint)
            WHERE LOWER(Datastore_Name) = LOWER(?)
              AND LOWER(Datastore_Type) = LOWER(?);
            """
        )
        parameters.extend(
            (datastore_id, workspace_id, workspace_name, endpoint, datastore_name, datastore_type)
        )
    return "\n".join(statements), tuple(parameters), rebind_items


def count_current_workspace_datastore_rebinds(cursor, rebind_items):
    predicates = []
    parameters = [workspace_id]
    for datastore_name, datastore_id, _endpoint, datastore_type, _required in rebind_items:
        predicates.append(
            "(LOWER(Datastore_Name) = LOWER(?) AND LOWER(Datastore_Type) = LOWER(?) AND Datastore_ID = ?)"
        )
        parameters.extend((datastore_name, datastore_type, datastore_id))
    cursor.execute(
        "SELECT COUNT(*) FROM dbo.Datastore_Configuration "
        "WHERE Workspace_ID = ? AND (" + " OR ".join(predicates) + ")",
        *parameters,
    )
    return int(cursor.fetchone()[0])

# ── Execute the datastore notebook ───────────────────────────────────────────
print(f"📦 Executing datastore notebook: {DATASTORE_NOTEBOOK}")
print(f"   Connecting to {warehouse_endpoint} / {warehouse_display_name}...")

wh_conn = connect_to_warehouse()
wh_cursor = wh_conn.cursor()

try:
    rebind_sql, rebind_parameters, rebind_items = build_current_workspace_datastore_rebind()
    cells = execute_sql_notebook(
        DATASTORE_NOTEBOOK,
        wh_cursor,
        before_commit_sql=rebind_sql,
        before_commit_parameters=rebind_parameters,
    )
    rebound_count = count_current_workspace_datastore_rebinds(wh_cursor, rebind_items)
    print(f"Rebound {rebound_count} datastore rows to feature workspace {workspace_id}.")
    print(f"✅ Datastore configuration complete ({DATASTORE_NOTEBOOK}) — {cells} cell(s) executed")
except Exception as e:
    print(f"❌ Datastore notebook failed: {e}")
    wh_cursor.close()
    wh_conn.close()
    raise

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 4 — Execute Metadata SQL Notebooks
# 
# Runs each metadata notebook you specified in `METADATA_NOTEBOOKS` against
# the metadata warehouse. These contain the pipeline orchestration records
# (orchestration, primary config, advanced config).

# CELL ********************

def normalize_metadata_notebook_names(value):
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            value = json.loads(text)
        except json.JSONDecodeError:
            value = [name.strip() for name in text.split(",") if name.strip()]
        if isinstance(value, str):
            value = [value]
    if not isinstance(value, (list, tuple)):
        raise RuntimeError("METADATA_NOTEBOOKS must be a list or a JSON/comma-separated string.")
    return [str(name).strip() for name in value if str(name).strip()]

metadata_notebooks_to_execute = normalize_metadata_notebook_names(METADATA_NOTEBOOKS)

if not metadata_notebooks_to_execute:
    print("ℹ️  No metadata notebooks specified — skipping")
else:
    print(f"📋 Executing {len(metadata_notebooks_to_execute)} metadata notebook(s)...\n")
    
    results = []
    
    for nb_name in metadata_notebooks_to_execute:
        print(f"{'─' * 60}")
        print(f"📄 {nb_name}")
        
        try:
            cells = execute_sql_notebook(nb_name, wh_cursor)
            print(f"   ✅ {cells} cell(s) executed")
            results.append((nb_name, f"✅ {cells} cell(s)"))
        except Exception as e:
            print(f"   ❌ Failed: {e}")
            results.append((nb_name, f"❌ {e}"))
    
    # ── Summary ──────────────────────────────────────────────────────────────
    print(f"\n{'═' * 60}")
    print("📊 METADATA EXECUTION SUMMARY")
    print(f"{'═' * 60}")
    for name, status in results:
        print(f"   {status}  {name}")
    
    failed = sum(1 for _, s in results if s.startswith("❌"))
    if failed:
        wh_cursor.close()
        wh_conn.close()
        raise RuntimeError(f"{failed} metadata notebook(s) failed. Review the errors above.")
    else:
        print(f"\n✅ All {len(results)} notebook(s) executed successfully!")

# ── Clean up the warehouse connection ────────────────────────────────────────
wh_cursor.close()
wh_conn.close()
print("\n🔒 Warehouse connection closed")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Step 5 — Verification
# 
# Quick checks to confirm everything was set up correctly.

# CELL ********************

print("🔍 Running verification checks...\n")

checks_passed = 0
checks_total = 0

# ── Check 1: Variable Library overrides ──────────────────────────────────────
checks_total += 1
try:
    resp = session.post(
        f"{BASE_URL}/workspaces/{workspace_id}/VariableLibraries/{vl_id}/getDefinition",
    )
    resp = poll_lro(resp, label="getDefinition (verification)")
    parts = resp.json()["definition"]["parts"]
    vs_part = next(
        (p for p in parts if p["path"] == f"valueSets/{VALUE_SET_NAME}.json"),
        None,
    )
    if vs_part:
        vs_data = json.loads(base64.b64decode(vs_part["payload"]).decode("utf-8"))
        variable_overrides = vs_data.get("variableOverrides", [])
        override_count = len(variable_overrides)
        override_names = {override.get("name") for override in variable_overrides}
        if "pipeline_workspace_id" not in override_names:
            raise RuntimeError("FeatureWorkspaceConfig is missing pipeline_workspace_id override.")
        if "custom_function_load_mode" not in override_names:
            raise RuntimeError("FeatureWorkspaceConfig is missing custom_function_load_mode override.")
        print(f"   ✅ FeatureWorkspaceConfig: {override_count} override(s) active")
        checks_passed += 1
    else:
        print(f"   ❌ FeatureWorkspaceConfig value set not found in Variable Library")
except Exception as e:
    print(f"   ❌ Could not verify Variable Library: {e}")

# ── Check 2: Datastore_Configuration table ───────────────────────────────────
checks_total += 1
try:
    abfss_base = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{warehouse_id_resolved}/Tables/dbo"
    ds_df = spark.read.format("delta").load(f"{abfss_base}/Datastore_Configuration")
    ds_count = ds_df.count()
    print(f"   ✅ Datastore_Configuration: {ds_count} record(s)")
    checks_passed += 1
except Exception as e:
    print(f"   ❌ Could not read Datastore_Configuration: {e}")

# ── Check 3: Orchestration metadata ─────────────────────────────────────────
checks_total += 1
try:
    orch_df = spark.read.format("delta").load(f"{abfss_base}/Data_Pipeline_Metadata_Orchestration")
    orch_count = orch_df.count()
    print(f"   ✅ Orchestration metadata: {orch_count} record(s)")
    checks_passed += 1
except Exception as e:
    print(f"   ❌ Could not read orchestration metadata: {e}")

# ── Summary ──────────────────────────────────────────────────────────────────
print(f"\n{'═' * 60}")
if checks_passed == checks_total:
    print(f"🎉 ALL {checks_total} CHECKS PASSED — Feature workspace is ready!")
    print(f"   You can now run PL_01a_For_Each_Trigger_Step to test your pipeline.")
else:
    print(f"⚠️  {checks_passed}/{checks_total} checks passed — review issues above.")
print(f"{'═' * 60}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
