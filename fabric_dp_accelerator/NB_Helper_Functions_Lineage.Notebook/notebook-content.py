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

# # NB_Helper_Functions_Lineage - Data Pipeline Lineage Utilities
# 
# ## Overview
# This notebook provides all functions for building and analyzing data pipeline lineage:
# - **Graph Construction**: Building directed graphs from metadata tables
# - **Record Generation**: Creating lineage records with depth, path, and cross-trigger detection
# - **Narrative Generation**: Human-readable summaries for Copilot and Power BI
# - **Lineage Grouping**: Weakly connected components for "full scope" analysis
# - **Change Detection**: Fingerprinting and table-level diff for incremental updates
# 
# ### Integration Context
# This notebook is `%run` by `NB_Generate_Lineage` (the thin orchestrator) and by
# `NB_Integration_Tests` (for unit testing). It depends on `NB_Helper_Functions_1`
# and `NB_Helper_Functions_3` being loaded first (for `CUSTOM_FUNCTIONS_FOLDER`,
# `_get_notebook_code_cells`, etc.).
# 
# ### Unit Testing
# All functions in this module are designed for testability:
# - Pure functions with dict/list inputs and outputs (no Spark dependency in core logic)
# - `LineageNode` class uses simple string normalization
# - Graph functions accept pre-collected Python data structures
# - Narrative functions work on plain dicts
# 
# Test file: `integration_tests/notebooks/test_lineage.py`

# CELL ********************

# ===========================================================================================
# LINEAGE GRAPH HELPER FUNCTIONS
# ===========================================================================================
# These functions build, analyze, and generate lineage from metadata.
# Designed for independent testability — core logic uses plain Python data structures.
# ===========================================================================================

from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from threading import Lock
import uuid
import re
import hashlib
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set, Tuple
from scipy import sparse as scipy_sparse
from scipy.sparse import csgraph as scipy_csgraph
from pyspark.sql import functions as f
from pyspark.sql import Window
from pyspark.sql.types import BooleanType, IntegerType, StringType, StructField, StructType, TimestampType


class LineageNoPathError(Exception):
    """Raised when no directed path exists between two lineage nodes."""


class _LineageNodeView:
    def __init__(self, graph):
        self._graph = graph

    def __call__(self, data: bool = False):
        if data:
            return [(node_id, attributes) for node_id, attributes in self._graph._nodes.items()]
        return list(self._graph._nodes)

    def __iter__(self) -> Iterator[str]:
        return iter(self._graph._nodes)

    def __contains__(self, node_id: str) -> bool:
        return node_id in self._graph._nodes

    def __getitem__(self, node_id: str) -> Dict[str, Any]:
        return self._graph._nodes[node_id]

    def get(self, node_id: str, default=None):
        return self._graph._nodes.get(node_id, default)


class _LineageEdgeView:
    def __init__(self, graph):
        self._graph = graph

    def __call__(self, data: bool = False):
        if data:
            return [
                (source_id, target_id, attributes)
                for (source_id, target_id), attributes in self._graph._edges.items()
            ]
        return list(self._graph._edges)

    def __iter__(self) -> Iterator[Tuple[str, str]]:
        return iter(self._graph._edges)

    def __getitem__(self, edge: Tuple[str, str]) -> Dict[str, Any]:
        return self._graph._edges[edge]


class LineageGraph:
    """Minimal ordered directed graph used by lineage generation."""

    def __init__(self):
        self._nodes: Dict[str, Dict[str, Any]] = {}
        self._edges: Dict[Tuple[str, str], Dict[str, Any]] = {}
        self._successors: Dict[str, Dict[str, None]] = {}
        self._predecessors: Dict[str, Dict[str, None]] = {}
        self._sparse_structure_cache = None
        self.nodes = _LineageNodeView(self)
        self.edges = _LineageEdgeView(self)

    def __contains__(self, node_id: str) -> bool:
        return node_id in self._nodes

    def add_node(self, node_for_adding: str, **attributes) -> None:
        if node_for_adding not in self._nodes:
            self._nodes[node_for_adding] = {}
            self._successors[node_for_adding] = {}
            self._predecessors[node_for_adding] = {}
            self._sparse_structure_cache = None
        self._nodes[node_for_adding].update(attributes)

    def add_edge(self, u_of_edge: str, v_of_edge: str, **attributes) -> None:
        self.add_node(u_of_edge)
        self.add_node(v_of_edge)
        edge = (u_of_edge, v_of_edge)
        if edge not in self._edges:
            self._edges[edge] = {}
            self._successors[u_of_edge][v_of_edge] = None
            self._predecessors[v_of_edge][u_of_edge] = None
            self._sparse_structure_cache = None
        self._edges[edge].update(attributes)

    def add_edges_from(self, edges: Iterable[Tuple]) -> None:
        for edge in edges:
            if len(edge) == 2:
                self.add_edge(edge[0], edge[1])
            elif len(edge) == 3:
                self.add_edge(edge[0], edge[1], **edge[2])
            else:
                raise ValueError("Lineage edges must contain a source, target, and optional attribute dictionary")

    def predecessors(self, node_id: str) -> Iterator[str]:
        return iter(self._predecessors[node_id])

    def successors(self, node_id: str) -> Iterator[str]:
        return iter(self._successors[node_id])

    def in_degree(self, node_id: str) -> int:
        return len(self._predecessors[node_id])

    def out_degree(self, node_id: str) -> int:
        return len(self._successors[node_id])

    def __getitem__(self, node_id: str) -> Dict[str, Dict[str, Any]]:
        if node_id not in self._nodes:
            raise KeyError(node_id)
        return {
            target_id: self._edges[(node_id, target_id)]
            for target_id in self._successors[node_id]
        }

    def has_edge(self, source_id: str, target_id: str) -> bool:
        return (source_id, target_id) in self._edges

    def get_edge_data(self, source_id: str, target_id: str, default=None):
        return self._edges.get((source_id, target_id), default)

    def number_of_nodes(self) -> int:
        return len(self._nodes)

    def number_of_edges(self) -> int:
        return len(self._edges)

    def _sparse_structure(self):
        if self._sparse_structure_cache is None:
            node_ids = tuple(self._nodes)
            node_indexes = {node_id: index for index, node_id in enumerate(node_ids)}
            row_indexes = []
            column_indexes = []
            for source_id, target_id in self._edges:
                row_indexes.append(node_indexes[source_id])
                column_indexes.append(node_indexes[target_id])
            adjacency = scipy_sparse.csr_matrix(
                ([True] * len(row_indexes), (row_indexes, column_indexes)),
                shape=(len(node_ids), len(node_ids)),
                dtype=bool,
            )
            self._sparse_structure_cache = adjacency, node_ids, node_indexes
        return self._sparse_structure_cache

    def subgraph(self, node_ids: Iterable[str]):
        selected_nodes = set(node_ids)
        result = LineageGraph()
        for node_id, attributes in self._nodes.items():
            if node_id in selected_nodes:
                result.add_node(node_id, **attributes.copy())
        for (source_id, target_id), attributes in self._edges.items():
            if source_id in selected_nodes and target_id in selected_nodes:
                result.add_edge(source_id, target_id, **attributes.copy())
        return result

    def copy(self):
        return self.subgraph(self._nodes)


def lineage_single_source_shortest_paths(graph: LineageGraph, source_id: str) -> Dict[str, List[str]]:
    """Return one insertion-order-stable shortest path from source to each reachable node."""
    if source_id not in graph:
        raise KeyError(source_id)

    paths = {source_id: [source_id]}
    pending = deque([source_id])
    while pending:
        node_id = pending.popleft()
        for successor_id in graph.successors(node_id):
            if successor_id not in paths:
                paths[successor_id] = paths[node_id] + [successor_id]
                pending.append(successor_id)
    return paths


def lineage_shortest_path(graph: LineageGraph, source_id: str, target_id: str) -> List[str]:
    """Return the deterministic shortest directed path or raise LineageNoPathError."""
    if source_id not in graph or target_id not in graph:
        raise LineageNoPathError(f"No path from {source_id} to {target_id}")
    path = lineage_single_source_shortest_paths(graph, source_id).get(target_id)
    if path is None:
        raise LineageNoPathError(f"No path from {source_id} to {target_id}")
    return path


def _lineage_reachable(graph: LineageGraph, node_id: str, reverse: bool = False) -> Set[str]:
    if node_id not in graph:
        raise KeyError(node_id)

    adjacency, node_ids, node_indexes = graph._sparse_structure()
    traversal_graph = adjacency.transpose().tocsr() if reverse else adjacency
    traversal_order = scipy_csgraph.breadth_first_order(
        traversal_graph,
        node_indexes[node_id],
        directed=True,
        return_predecessors=False,
    )
    return {node_ids[int(index)] for index in traversal_order[1:]}


def lineage_ancestors(graph: LineageGraph, node_id: str) -> Set[str]:
    return _lineage_reachable(graph, node_id, reverse=True)


def lineage_descendants(graph: LineageGraph, node_id: str) -> Set[str]:
    return _lineage_reachable(graph, node_id)


def lineage_weakly_connected_components(graph: LineageGraph) -> List[List[str]]:
    """Return weak components in node insertion order."""
    adjacency, node_ids, _ = graph._sparse_structure()
    if not node_ids:
        return []

    _, labels = scipy_csgraph.connected_components(
        adjacency,
        directed=False,
        return_labels=True,
    )
    components_by_label = {}
    for node_id, label in zip(node_ids, labels):
        components_by_label.setdefault(int(label), []).append(node_id)
    return list(components_by_label.values())


def _lineage_topological_order(graph: LineageGraph) -> Optional[List[str]]:
    in_degrees = {node_id: graph.in_degree(node_id) for node_id in graph.nodes}
    pending = deque(node_id for node_id in graph.nodes if in_degrees[node_id] == 0)
    ordered_nodes = []
    while pending:
        node_id = pending.popleft()
        ordered_nodes.append(node_id)
        for successor_id in graph.successors(node_id):
            in_degrees[successor_id] -= 1
            if in_degrees[successor_id] == 0:
                pending.append(successor_id)
    return ordered_nodes if len(ordered_nodes) == graph.number_of_nodes() else None


def lineage_is_directed_acyclic_graph(graph: LineageGraph) -> bool:
    return _lineage_topological_order(graph) is not None


def lineage_dag_longest_path(graph: LineageGraph) -> List[str]:
    """Return the longest DAG path, resolving equal lengths by insertion order."""
    ordered_nodes = _lineage_topological_order(graph)
    if ordered_nodes is None:
        raise ValueError("The lineage graph contains a directed cycle")
    if not ordered_nodes:
        return []

    paths = {node_id: [node_id] for node_id in ordered_nodes}
    for node_id in ordered_nodes:
        for successor_id in graph.successors(node_id):
            candidate_path = paths[node_id] + [successor_id]
            if len(candidate_path) > len(paths[successor_id]):
                paths[successor_id] = candidate_path

    longest_path = []
    for node_id in ordered_nodes:
        if len(paths[node_id]) > len(longest_path):
            longest_path = paths[node_id]
    return longest_path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================================
# LINEAGE NODE CLASS
# ===========================================================================================

class LineageNode:
    """Represents a node in the lineage graph (source or target entity)"""
    
    def __init__(
        self,
        entity_name: str,
        datastore: str,
        medallion_layer: str,
        system_type: str = None,
        node_type: str = "entity"
    ):
        self.entity_name = entity_name
        self.datastore = datastore
        self.medallion_layer = medallion_layer
        self.system_type = system_type
        self.node_type = node_type
        
        # Build node_id: always use canonical form datastore.entity_name
        # CRITICAL: Normalize entity_name to strip the datastore prefix if present.
        # This prevents the same table appearing as both "dbo.orders" and "bronze.dbo.orders"
        # depending on whether it's a target (from orchestration) or source (from primary config).
        if datastore and entity_name:
            if entity_name.lower().startswith(f"{datastore.lower()}."):
                # entity_name includes the datastore prefix — strip it for consistency
                # e.g., "bronze.dbo.sales" → entity_name="dbo.sales", node_id="bronze.dbo.sales"
                self.entity_name = entity_name[len(datastore) + 1:]
                self.node_id = f"{datastore}.{self.entity_name}"
            else:
                # entity_name is just schema.table or table, prepend the datastore
                self.node_id = f"{datastore}.{entity_name}"
        else:
            self.node_id = entity_name or datastore or "unknown"

        # Canonicalize node identity to avoid case-sensitive splits (e.g.,
        # bronze.dbo.CREW_MANHOURS vs bronze.dbo.crew_manhours) across triggers.
        self.node_id = str(self.node_id).strip().lower()
    
    def __hash__(self):
        return hash(self.node_id)
    
    def __eq__(self, other):
        return isinstance(other, LineageNode) and self.node_id == other.node_id
    
    def to_dict(self) -> Dict:
        return {
            "entity_name": self.entity_name,
            "datastore": self.datastore,
            "medallion_layer": self.medallion_layer,
            "system_type": self.system_type,
            "node_type": self.node_type,
            "node_id": self.node_id
        }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================================
# MEDALLION LAYER AND DATASTORE RESOLUTION
# ===========================================================================================

def derive_medallion_layer(datastore: str) -> str:
    """Derive medallion layer from datastore name"""
    if not datastore:
        return "Unknown"
    datastore_lower = datastore.lower()
    if "bronze" in datastore_lower:
        return "Bronze"
    elif "silver" in datastore_lower:
        return "Silver"
    elif "gold" in datastore_lower:
        return "Gold"
    return "Unknown"


def resolve_table_datastore(table_name: str, G: LineageGraph = None) -> Tuple[Optional[str], Optional[str]]:
    """
    Resolve the datastore for a table reference that may be 1, 2, or 3 parts.
    
    For 3-part names (datastore.schema.table): returns (datastore, medallion_layer)
    For 2-part names (schema.table or datastore.table): checks if parts[0] is a known
      medallion layer (bronze/silver/gold). If not, tries resolving from the graph.
    For 1-part names: returns (None, "Unknown")
    
    Args:
        table_name: The table reference (e.g., "silver.dbo.customers" or "dbo.customers")
        G: Optional lineage graph to resolve 2-part ambiguous names against existing nodes
    
    Returns:
        Tuple of (datastore, medallion_layer)
    """
    if not table_name:
        return None, "Unknown"
    
    parts = table_name.split(".")
    
    if len(parts) == 3:
        # 3-part name: datastore.schema.table — unambiguous
        return parts[0], derive_medallion_layer(parts[0])
    
    if len(parts) == 2:
        # 2-part name: could be datastore.table OR schema.table
        candidate = parts[0]
        candidate_layer = derive_medallion_layer(candidate)
        if candidate_layer != "Unknown":
            # parts[0] is a recognized datastore (bronze, silver, gold)
            return candidate, candidate_layer
        
        # parts[0] is NOT a datastore (likely a schema like "dbo")
        # Try to resolve from existing graph nodes
        if G is not None:
            for existing_id in G.nodes():
                if existing_id.lower().endswith(f".{table_name.lower()}"):
                    resolved_datastore = existing_id.split(".")[0]
                    return resolved_datastore, derive_medallion_layer(resolved_datastore)
        
        # Could not resolve — return None
        return None, "Unknown"
    
    # 1-part name: just table name, no datastore info
    return None, "Unknown"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================================
# SOURCE AND TARGET DERIVATION FUNCTIONS
# ===========================================================================================

def derive_source_system_type(source_configs: List) -> str:
    """Derive source system type from pre-filtered source_details rows"""
    
    source_type = None
    has_table_name = False
    has_wildcard_path = False
    
    for row in source_configs:
        config_name = row.Configuration_Name
        config_value = row.Configuration_Value
        
        if config_name == "source":
            source_type = config_value
        elif config_name == "table_name":
            has_table_name = True
        elif config_name == "wildcard_folder_path":
            has_wildcard_path = True
    
    if source_type:
        return source_type
    elif has_wildcard_path:
        return "file"
    elif has_table_name:
        return "delta_table"
    
    return "unknown"


def derive_source_entity_and_datastore(source_configs: List) -> Tuple[str, str, str]:
    """Derive source entity, datastore, and medallion layer from pre-filtered source_details rows"""
    
    source_entity = None
    source_datastore = None
    schema_name = None
    table_name_val = None
    wildcard_path = None
    sftp_wildcard_folder_path = None
    sftp_wildcard_file_name = None
    staging_folder_path = None
    datastore_name = None
    source_type = None
    
    for row in source_configs:
        config_name = row.Configuration_Name
        config_value = row.Configuration_Value
        
        if config_name == "table_name":
            # For Delta tables, table_name is the full 3-part name (datastore.schema.table)
            # For external databases, table_name is just the source table name
            source_entity = config_value
            table_name_val = config_value  # Store the raw table name value
            parts = config_value.split(".") if config_value else []
            if len(parts) == 3:
                source_datastore = parts[0]
                source_entity = config_value
            elif len(parts) == 2:
                source_entity = config_value
        elif config_name == "schema_name":
            schema_name = config_value
        elif config_name == "source":
            source_type = config_value
        elif config_name == "wildcard_folder_path":
            wildcard_path = config_value
        elif config_name == "sftp_wildcard_folder_path":
            sftp_wildcard_folder_path = config_value
        elif config_name == "sftp_wildcard_file_name":
            sftp_wildcard_file_name = config_value
        elif config_name == "staging_folder_path":
            staging_folder_path = config_value
        elif config_name == "datastore_name":
            datastore_name = config_value

    source_type_lower = (source_type or "").strip().lower()

    # SFTP sources are file-based external ingestion and should surface folder/file paths in lineage.
    if source_type_lower == "sftp":
        folder_path = sftp_wildcard_folder_path or wildcard_path
        file_pattern = (sftp_wildcard_file_name or "").strip()
        if folder_path:
            normalized_folder = str(folder_path).rstrip("/")
            if file_pattern:
                source_entity = f"sftp:{normalized_folder}/{file_pattern}"
            else:
                source_entity = f"sftp:{normalized_folder}"
        else:
            source_entity = "sftp:unknown_path"

        source_datastore = datastore_name or "sftp"
        return source_entity, source_datastore, "External"
    
    # For external database sources, keep external source identity.
    if source_type and source_type not in ["delta_table", "file", "sftp"]:
        # Build the source entity name: schema_name.table_name (no prefix needed - source_type is captured separately)
        if schema_name and table_name_val:
            source_entity = f"{schema_name}.{table_name_val}"
        elif schema_name:
            source_entity = schema_name
        elif table_name_val:
            source_entity = table_name_val
        else:
            source_entity = "unknown"
        source_datastore = source_type
        return source_entity, source_datastore, "External"
    
    # For file sources
    if wildcard_path:
        source_entity = f"Files/{wildcard_path}"
        source_datastore = datastore_name or "bronze"
        return source_entity, source_datastore, derive_medallion_layer(source_datastore)
    
    # For delta table sources
    if source_entity:
        source_medallion = derive_medallion_layer(source_datastore) if source_datastore else "Unknown"
        return source_entity, source_datastore, source_medallion
    
    return None, None, None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================================
# EXTENDED SOURCE/TARGET DETAIL EXTRACTION
# ===========================================================================================

def extract_extended_source_details(source_configs: List, workspace_variables: dict) -> Dict:
    """
    Extract extended source details for lineage including connection_id and ABFSS path.
    
    Returns:
        Dict with keys: connection_id, workspace_name, abfss_path, is_external
    """
    # Parse all source config values
    config_values = {}
    for row in source_configs:
        config_values[row.Configuration_Name] = row.Configuration_Value
    
    source_type = config_values.get("source")
    datastore_name = config_values.get("datastore_name")
    table_name = config_values.get("table_name")
    wildcard_path = config_values.get("wildcard_folder_path")
    
    result = {
        "connection_id": None,
        "workspace_name": None,
        "abfss_path": None,
        "is_external": False
    }
    
    # External database sources - get connection_id from Datastore_Configuration
    if source_type and source_type not in ["delta_table", "file"]:
        result["is_external"] = True
        if datastore_name:
            connection_id_key = f"{datastore_name.lower()}_datastore_connection_id"
            result["connection_id"] = workspace_variables.get(connection_id_key)
        return result
    
    # Contract: datastore_name is expected to be present for internal sources.
    # If it is missing, the function returns partial details (no ABFSS path).
    
    # Internal Fabric sources - build ABFSS path
    if datastore_name:
        # Get workspace variables for this datastore
        workspace_id_key = f"{datastore_name.lower()}_datastore_workspace_id"
        datastore_id_key = f"{datastore_name.lower()}_datastore_id"
        workspace_name_key = f"{datastore_name.lower()}_datastore_workspace_name"
        
        workspace_id = workspace_variables.get(workspace_id_key)
        datastore_id = workspace_variables.get(datastore_id_key)
        result["workspace_name"] = workspace_variables.get(workspace_name_key)
        
        if workspace_id and datastore_id:
            if wildcard_path:
                # File source
                result["abfss_path"] = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{datastore_id}/Files/{wildcard_path}"
            elif table_name:
                # Table source - parse schema.table from 3-part name
                parts = table_name.split(".") if table_name else []
                if len(parts) == 3:
                    schema_name = parts[1].lower()
                    tbl_name = parts[2].lower()
                    result["abfss_path"] = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{datastore_id}/Tables/{schema_name}/{tbl_name}"
                elif len(parts) == 2:
                    schema_name = parts[0].lower()
                    tbl_name = parts[1].lower()
                    result["abfss_path"] = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{datastore_id}/Tables/{schema_name}/{tbl_name}"
    
    return result


def extract_target_details(orchestration_row, workspace_variables: dict) -> Dict:
    """
    Extract target details for lineage including workspace name and ABFSS path.
    
    Args:
        orchestration_row: Row (or dict-like) with Target_Datastore and Target_Entity attributes
        workspace_variables: Dict of workspace variables from Datastore_Configuration
    
    Returns:
        Dict with keys: workspace_name, abfss_path
    """
    target_datastore = orchestration_row.Target_Datastore.strip().lower()
    target_entity = orchestration_row.Target_Entity
    
    # Get workspace variables for target datastore
    workspace_id_key = f"{target_datastore}_datastore_workspace_id"
    datastore_id_key = f"{target_datastore}_datastore_id"
    workspace_name_key = f"{target_datastore}_datastore_workspace_name"
    
    workspace_id = workspace_variables.get(workspace_id_key)
    datastore_id = workspace_variables.get(datastore_id_key)
    workspace_name = workspace_variables.get(workspace_name_key)
    
    result = {
        "workspace_name": workspace_name,
        "abfss_path": None
    }
    
    if workspace_id and datastore_id and target_entity:
        if "/" in target_entity:
            # File target
            result["abfss_path"] = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{datastore_id}/Files/{target_entity}"
        else:
            # Table target - parse schema.table
            parts = target_entity.split(".")
            if len(parts) == 2:
                schema_name = parts[0].lower()
                tbl_name = parts[1].lower()
                result["abfss_path"] = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{datastore_id}/Tables/{schema_name}/{tbl_name}"
    
    return result


def infer_fabric_entity_type(entity_name: Optional[str]) -> Optional[str]:
    """
    Infer Fabric entity type label from entity naming pattern.

    Returns:
        "Fabric Lakehouse (Files)" for file-style entities,
        "Fabric Lakehouse (Tables)" for table-style entities,
        None when type cannot be inferred.
    """
    if not entity_name:
        return None

    value = str(entity_name).strip()
    if not value:
        return None

    value_lower = value.lower()
    if value_lower.startswith("files/") or "/" in value:
        return "Fabric Lakehouse (Files)"

    if "." in value:
        return "Fabric Lakehouse (Tables)"

    return None


def _normalize_datastore_name(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    normalized = str(value).strip().lower()
    if normalized.endswith(".lakehouse"):
        normalized = normalized[:-10]
    return normalized or None


def _canonicalize_node_id(node_id: Optional[str]) -> Optional[str]:
    if node_id is None:
        return None
    value = str(node_id).strip().lower()
    if not value:
        return None
    value = value.replace(".lakehouse.", ".")
    if value.endswith(".lakehouse"):
        value = value[:-10]
    return value


def _get_workspace_var(workspace_variables: dict, datastore_name: Optional[str], property_name: str) -> Optional[str]:
    candidates = []

    raw = str(datastore_name).strip().lower() if datastore_name else None
    if raw:
        candidates.append(raw)

    normalized = _normalize_datastore_name(datastore_name)
    if normalized and normalized not in candidates:
        candidates.append(normalized)

    for candidate in candidates:
        key = f"{candidate}_datastore_{property_name}"
        if key in workspace_variables:
            return workspace_variables.get(key)

    return None


def _build_source_abfss_path(source_datastore: Optional[str], source_entity: Optional[str], workspace_variables: dict) -> Optional[str]:
    if not source_datastore or not source_entity:
        return None

    workspace_id = _get_workspace_var(workspace_variables, source_datastore, "workspace_id")
    datastore_id = _get_workspace_var(workspace_variables, source_datastore, "id")
    if not workspace_id or not datastore_id:
        return None

    entity = str(source_entity).strip()
    if not entity:
        return None

    # File-style source
    if entity.lower().startswith("files/"):
        return f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{datastore_id}/{entity}"
    if ":" in entity:
        _, maybe_file_path = entity.split(":", 1)
        if maybe_file_path.lower().startswith("files/"):
            return f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{datastore_id}/{maybe_file_path}"

    # Table-style source: supports datastore.schema.table and schema.table
    parts = entity.split(".")
    if len(parts) == 3:
        _, schema_name, table_name = parts
        return f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{datastore_id}/Tables/{schema_name.lower()}/{table_name.lower()}"
    if len(parts) == 2:
        schema_name, table_name = parts
        return f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{datastore_id}/Tables/{schema_name.lower()}/{table_name.lower()}"

    return None


def backfill_source_location_details(source_details: Dict, source_node: Dict, source_type: Optional[str], workspace_variables: dict) -> Dict:
    """
    Fill missing Source_Workspace_Name and Source_ABFSS_Path for internal Fabric sources.

    This keeps enrichment in helper scope so behavior is unit-testable and orchestrator stays thin.
    """
    details = dict(source_details or {})
    source_type_value = str(source_type or "").lower()
    if "fabric lakehouse" not in source_type_value:
        return details

    source_datastore = source_node.get("datastore")
    source_entity = source_node.get("entity_name")

    if not details.get("workspace_name"):
        workspace_name = _get_workspace_var(workspace_variables, source_datastore, "workspace_name")
        if workspace_name:
            details["workspace_name"] = workspace_name

    if not details.get("abfss_path"):
        abfss_path = _build_source_abfss_path(source_datastore, source_entity, workspace_variables)
        if abfss_path:
            details["abfss_path"] = abfss_path

    return details


def build_source_details_from_node(source_node: Dict, workspace_variables: dict) -> Dict:
    """
    Build source details (workspace_name, abfss_path) directly from a source node's attributes.
    
    Used for custom function sources where the Table_ID's primary config doesn't have
    the source info (because custom functions can read from multiple tables).
    
    Args:
        source_node: Node attributes dict with entity_name, datastore, medallion_layer, etc.
        workspace_variables: Dict of workspace variables from Datastore_Configuration
    
    Returns:
        Dict with keys: connection_id, workspace_name, abfss_path, is_external
    """
    result = {
        "connection_id": None,
        "workspace_name": None,
        "abfss_path": None,
        "is_external": False
    }
    
    entity_name = source_node.get("entity_name", "")
    datastore = source_node.get("datastore", "")
    medallion_layer = source_node.get("medallion_layer", "")
    
    # External sources - get connection_id from Datastore_Configuration
    if medallion_layer == "External":
        result["is_external"] = True
        if datastore:
            connection_id_key = f"{datastore.lower()}_datastore_connection_id"
            result["connection_id"] = workspace_variables.get(connection_id_key)
        return result
    
    # Get datastore from entity_name if not set (e.g., "bronze.dbo.orders" -> "bronze")
    if not datastore and entity_name:
        parts = entity_name.split(".")
        if len(parts) >= 2:
            datastore = parts[0]
    
    if not datastore:
        return result
    
    # Get workspace variables for this datastore
    datastore_lower = datastore.lower()
    workspace_id_key = f"{datastore_lower}_datastore_workspace_id"
    datastore_id_key = f"{datastore_lower}_datastore_id"
    workspace_name_key = f"{datastore_lower}_datastore_workspace_name"
    
    workspace_id = workspace_variables.get(workspace_id_key)
    datastore_id = workspace_variables.get(datastore_id_key)
    result["workspace_name"] = workspace_variables.get(workspace_name_key)
    
    if not workspace_id or not datastore_id:
        return result
    
    # Parse entity_name to build ABFSS path
    # entity_name could be: "bronze.dbo.orders" (3-part) or "dbo.orders" (2-part) or "Files/path/file.csv"
    if "Files/" in entity_name or "/" in entity_name:
        # File source
        file_path = entity_name.split(":", 1)[-1] if ":" in entity_name else entity_name
        result["abfss_path"] = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{datastore_id}/{file_path}"
    else:
        # Table source
        parts = entity_name.split(".")
        if len(parts) == 3:
            # datastore.schema.table
            schema_name = parts[1].lower()
            tbl_name = parts[2].lower()
            result["abfss_path"] = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{datastore_id}/Tables/{schema_name}/{tbl_name}"
        elif len(parts) == 2:
            # schema.table
            schema_name = parts[0].lower()
            tbl_name = parts[1].lower()
            result["abfss_path"] = f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{datastore_id}/Tables/{schema_name}/{tbl_name}"
    
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================================
# TRANSFORMATION AND SOURCE EXTRACTION FUNCTIONS
# ===========================================================================================

def extract_join_sources(transform_configs: List) -> List[Dict]:
    """Extract join table sources from pre-filtered transform rows"""
    join_configs = [r for r in transform_configs if r.Configuration_Name == "join_data"]
    
    join_instances = {}
    for row in join_configs:
        instance = row.Configuration_Name_Instance_Number
        if instance not in join_instances:
            join_instances[instance] = {}
        join_instances[instance][row.Configuration_Attribute_Name] = row.Configuration_Attribute_Value
    
    joins = []
    for instance, attrs in join_instances.items():
        if "right_table_name" in attrs:
            joins.append({
                "instance": instance,
                "right_table_name": attrs.get("right_table_name"),
                "join_type": attrs.get("join_type", "inner"),
                "join_condition": attrs.get("join_condition")
            })
    
    return joins


def extract_dimension_surrogate_key_sources(transform_configs: List) -> List[Dict]:
    """Extract dimension table sources from pre-filtered transform rows"""
    dim_configs = [r for r in transform_configs if r.Configuration_Name == "attach_dimension_surrogate_key"]
    
    dim_instances = {}
    for row in dim_configs:
        instance = row.Configuration_Name_Instance_Number
        if instance not in dim_instances:
            dim_instances[instance] = {}
        dim_instances[instance][row.Configuration_Attribute_Name] = row.Configuration_Attribute_Value
    
    dimensions = []
    for instance, attrs in dim_instances.items():
        if "dimension_table_name" in attrs:
            dimensions.append({
                "instance": instance,
                "dimension_table_name": attrs.get("dimension_table_name"),
                "join_logic": attrs.get("dimension_table_join_logic"),
                "key_column": attrs.get("dimension_table_key_column_name"),
                "output_column": attrs.get("dimension_key_output_column_name"),
                "columns_to_add": attrs.get("dimension_columns_to_add_to_fact")
            })
    
    return dimensions


def extract_union_sources(transform_configs: List) -> List[Dict]:
    """Extract union table sources from pre-filtered transform rows"""
    union_configs = [r for r in transform_configs if r.Configuration_Name == "union_data"]
    
    union_instances = {}
    for row in union_configs:
        instance = row.Configuration_Name_Instance_Number
        if instance not in union_instances:
            union_instances[instance] = {}
        union_instances[instance][row.Configuration_Attribute_Name] = row.Configuration_Attribute_Value
    
    unions = []
    for instance, attrs in union_instances.items():
        if "union_tables" in attrs:
            # union_tables can be comma-separated list of table names
            union_tables_str = attrs.get("union_tables", "")
            table_names = [t.strip() for t in union_tables_str.split(",") if t.strip()]
            for table_name in table_names:
                unions.append({
                    "instance": instance,
                    "union_table_name": table_name,
                    "union_type": attrs.get("union_type", "by_name"),
                    "deduplicate": attrs.get("deduplicate", "false")
                })
    
    return unions


def extract_transformations(transform_configs: List) -> List[str]:
    """Extract list of transformation types applied to this table (pre-filtered transform rows)"""
    return list(set(
        r.Configuration_Name
        for r in transform_configs
        if getattr(r, "Configuration_Category", "data_transformation_steps") == "data_transformation_steps"
    ))


def find_root_sources(G: LineageGraph) -> List[str]:
    """Find all root nodes (external sources with no incoming edges)"""
    return [node for node in G.nodes() if G.in_degree(node) == 0]


def build_lineage_path_context(G: LineageGraph) -> Dict[str, Any]:
    """Precompute reusable path metadata for lineage depth calculation.

    The lineage record builder evaluates one result per graph edge. Recomputing root
    discovery and shortest paths inside that loop turns a linear edge walk into repeated
    graph traversals. This helper performs the root-to-node shortest path work once and
    keeps, for each node, the deepest reachable root path used by the existing lineage
    semantics.
    """
    root_sources = find_root_sources(G)
    longest_root_paths: Dict[str, List[str]] = {root: [root] for root in root_sources}

    for root in root_sources:
        for node_id, path in lineage_single_source_shortest_paths(G, root).items():
            existing_path = longest_root_paths.get(node_id)
            if existing_path is None or len(path) > len(existing_path):
                longest_root_paths[node_id] = path

    return {
        "root_sources": set(root_sources),
        "longest_root_paths": longest_root_paths,
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================================
# CUSTOM FUNCTION LINEAGE EXTRACTION
# ===========================================================================================

_CUSTOM_SOURCE_NOTEBOOK_CONFIGS = [
    ("custom_source_function", "custom_source_function_notebook", "custom_source_function_scope", "source", "custom_source_function"),
    ("custom_staging_function", "custom_staging_function_notebook", "custom_staging_function_scope", "staging", "custom_staging_function"),
    ("custom_table_ingestion_function", "custom_table_ingestion_function_notebook", "custom_table_ingestion_function_scope", "table", "custom_table_ingestion_function"),
    ("custom_file_ingestion_function", "custom_file_ingestion_function_notebook", "custom_file_ingestion_function_scope", "file", "custom_file_ingestion_function"),
]


def _split_csv_values(value: Optional[str]) -> List[str]:
    if not value:
        return []
    return [part.strip() for part in str(value).split(",") if part and part.strip()]


def _normalize_notebook_name(notebook_name: Optional[str]) -> Optional[str]:
    if not notebook_name:
        return None
    normalized = str(notebook_name).strip()
    return normalized or None


def extract_custom_ingestion_function_references(source_configs: List) -> List[Dict[str, Optional[str]]]:
    """
    Extract all source-side custom function notebook references from source_details rows.

    Covers custom source, custom staging, custom table ingestion, and custom file ingestion.
    """
    config_values = {
        row.Configuration_Name: row.Configuration_Value
        for row in source_configs
    }

    references = []
    for function_key, notebook_key, scope_key, function_type, relationship_type in _CUSTOM_SOURCE_NOTEBOOK_CONFIGS:
        function_name = config_values.get(function_key)
        notebook_name = _normalize_notebook_name(config_values.get(notebook_key))
        if function_name or notebook_name:
            references.append({
                "function_name": function_name,
                "notebook_name": notebook_name,
                "function_scope": str(config_values.get(scope_key) or "local").strip().lower(),
                "function_type": function_type,
                "relationship_type": relationship_type,
            })

    return references


def extract_custom_ingestion_function_info(source_configs: List) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Extract custom ingestion function and notebook info from pre-filtered source_details rows.
    
    This handles:
    - custom_source_function: For external source extraction returning a DataFrame
    - custom_staging_function: For external source staging that lands raw files
    - custom_table_ingestion_function: For custom SQL/table extraction from databases or Delta tables
    - custom_file_ingestion_function: For custom file parsing (XML, proprietary formats, etc.)
    
    Returns:
        Tuple of (function_name, notebook_name, function_type)
        Returns (None, None, None) if not configured
    """
    
    references = extract_custom_ingestion_function_references(source_configs)
    if references:
        first_reference = references[0]
        return (
            first_reference.get("function_name"),
            first_reference.get("notebook_name"),
            first_reference.get("function_type"),
        )

    return None, None, None


def extract_custom_transformation_function_references(transform_configs: List) -> List[Dict[str, Optional[str]]]:
    """Extract all custom_transformation_function notebook references from transform rows."""
    ctf_configs = [r for r in transform_configs if r.Configuration_Name == "custom_transformation_function"]
    if not ctf_configs:
        return []

    instances = {}
    for row in ctf_configs:
        instance = row.Configuration_Name_Instance_Number
        if instance not in instances:
            instances[instance] = {}
        instances[instance][row.Configuration_Attribute_Name] = row.Configuration_Attribute_Value

    references = []
    for instance in sorted(instances):
        attrs = instances[instance]
        function_names = _split_csv_values(attrs.get("functions_to_execute"))
        notebook_names = _split_csv_values(attrs.get("notebooks_to_run"))
        function_scope = str(attrs.get("function_scope") or "local").strip().lower()

        if not notebook_names:
            continue

        for index, notebook_name in enumerate(notebook_names):
            function_name = function_names[index] if index < len(function_names) else (function_names[0] if function_names else None)
            references.append({
                "function_name": function_name,
                "notebook_name": _normalize_notebook_name(notebook_name),
                "function_scope": function_scope,
                "function_type": "transformation",
                "relationship_type": "custom_transformation_function",
            })

    return references


def extract_custom_transformation_function_info(transform_configs: List) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract custom transformation function and notebook info from pre-filtered transform rows.
    
    This handles custom_transformation_function which transforms data AFTER ingestion.
    Configured in data_transformation_steps category in Advanced Configuration.
    
    Returns:
        Tuple of (function_name, notebook_name) or (None, None) if not configured
    """
    ctf_configs = [r for r in transform_configs if r.Configuration_Name == "custom_transformation_function"]

    if not ctf_configs:
        return None, None

    instances = {}
    for row in ctf_configs:
        instance = row.Configuration_Name_Instance_Number
        if instance not in instances:
            instances[instance] = {}
        instances[instance][row.Configuration_Attribute_Name] = row.Configuration_Attribute_Value

    if instances:
        first_instance = instances[min(instances.keys())]
        function_name = first_instance.get("functions_to_execute")
        notebook_name = first_instance.get("notebooks_to_run")
        return function_name, notebook_name

    return None, None


def extract_custom_validation_function_references(advanced_configs: List) -> List[Dict[str, Optional[str]]]:
    """Extract custom DQ and post-write reconciliation references for lineage attribution."""
    supported_configs = {
        ("data_quality", "custom_data_quality_function"),
        ("post_write_reconciliation", "custom_post_write_reconciliation_function"),
    }
    instances = {}
    for row in advanced_configs:
        config_key = (getattr(row, "Configuration_Category", ""), row.Configuration_Name)
        if config_key not in supported_configs:
            continue
        instance_key = (*config_key, row.Configuration_Name_Instance_Number)
        instances.setdefault(instance_key, {})[row.Configuration_Attribute_Name] = row.Configuration_Attribute_Value

    references = []
    for (category, configuration_name, _), attrs in sorted(instances.items()):
        references.append({
            "function_name": attrs.get("function_name"),
            "notebook_name": _normalize_notebook_name(attrs.get("notebook_name")),
            "function_scope": str(attrs.get("function_scope") or "local").strip().lower(),
            "function_type": "data_quality" if category == "data_quality" else "post_write_reconciliation",
            "relationship_type": configuration_name,
        })
    return references


def format_custom_function_attribution(reference: Dict[str, Optional[str]]) -> str:
    """Return stable, scope-aware custom-function attribution text."""
    relationship_type = reference.get("relationship_type") or "custom_function"
    function_name = reference.get("function_name") or "unknown"
    notebook_name = reference.get("notebook_name") or "unknown"
    function_scope = str(reference.get("function_scope") or "local").strip().lower()
    return f"{relationship_type}: {function_name}, notebook: {notebook_name}, scope: {function_scope}"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================================
# NOTEBOOK AND SQL PARSING FOR LINEAGE
# ===========================================================================================

# Cache for notebook parsing results to avoid re-fetching the same notebook definition.
# Key includes scope, load mode, and source because local and central notebooks can share a name.
_notebook_parse_cache: Dict[Tuple[str, str, str, str], List[Dict[str, str]]] = {}
_notebook_parse_cache_lock = Lock()


def clear_custom_function_parse_cache() -> None:
    """Clear notebook parse results at the start of a lineage run."""
    with _notebook_parse_cache_lock:
        _notebook_parse_cache.clear()


def _custom_function_parse_cache_key(notebook_name: str, function_scope: str, load_mode: str, source_id: str) -> Tuple[str, str, str, str]:
    return (
        str(function_scope or "local").strip().lower(),
        str(load_mode or "").strip().lower(),
        str(source_id or "").strip(),
        notebook_name,
    )


def _snapshot_custom_function_runtime() -> Dict[str, str]:
    return {
        "CUSTOM_FUNCTION_LOAD_MODE": str(globals().get("CUSTOM_FUNCTION_LOAD_MODE", "file") or "file"),
        "CUSTOM_FUNCTIONS_WORKSPACE_ID": str(globals().get("CUSTOM_FUNCTIONS_WORKSPACE_ID", "") or ""),
        "CUSTOM_FUNCTIONS_FILE_ABFSS_BASE_PATH": str(globals().get("CUSTOM_FUNCTIONS_FILE_ABFSS_BASE_PATH", "") or ""),
    }


def _set_lineage_custom_function_file_runtime(workspace_id: str, lakehouse_id: str) -> Dict[str, str]:
    state = _snapshot_custom_function_runtime()
    globals()["CUSTOM_FUNCTION_LOAD_MODE"] = "file"
    globals()["CUSTOM_FUNCTIONS_WORKSPACE_ID"] = str(workspace_id or "").strip()
    globals()["CUSTOM_FUNCTIONS_FILE_ABFSS_BASE_PATH"] = build_lakehouse_files_abfss_base(
        str(workspace_id or "").strip(),
        str(lakehouse_id or "").strip(),
        str(globals().get("CUSTOM_FUNCTIONS_FOLDER", "custom_functions") or "custom_functions"),
    )
    return state


def _restore_custom_function_runtime(state: Dict[str, str]) -> None:
    globals()["CUSTOM_FUNCTION_LOAD_MODE"] = state.get("CUSTOM_FUNCTION_LOAD_MODE", "file")
    globals()["CUSTOM_FUNCTIONS_WORKSPACE_ID"] = state.get("CUSTOM_FUNCTIONS_WORKSPACE_ID", "")
    globals()["CUSTOM_FUNCTIONS_FILE_ABFSS_BASE_PATH"] = state.get("CUSTOM_FUNCTIONS_FILE_ABFSS_BASE_PATH", "")


def _read_custom_function_file_source(
    notebook_name: str,
    file_base_path: str = "",
    folder_names=None,
) -> Tuple[str, str]:
    """Read the first existing custom-function file and return its source and path."""
    file_candidates = _get_custom_function_file_candidates(
        notebook_name,
        file_base_path,
        folder_names=folder_names,
    )
    for py_file_path, exists_path in file_candidates:
        try:
            file_exists = notebookutils.fs.exists(exists_path)
        except Exception:
            file_exists = False

        if file_exists:
            return read_text_file(py_file_path), py_file_path

    searched_paths = ', '.join(path for path, _ in file_candidates)
    raise FileNotFoundError(
        f"Custom function '{notebook_name}' was not found at [{searched_paths}]."
    )


def extract_tables_from_notebook(notebook_name: str, function_scope: str = "local") -> List[Dict[str, str]]:
    """
    Extract table references from a custom notebook.
    
    Uses the same custom function load mode as instantiate_notebook():
    - local + notebook: read the Fabric notebook definition through the API
    - local + file: read Files/custom_functions
    - central: read Files/central_custom_functions through the shortcut
    
    Parses the notebook code to find:
    - spark.sql() calls with SELECT/FROM clauses
    - spark.read.table() and spark.table() calls
    - spark.read.format("delta").load() with ABFSS paths
    - DeltaTable.forPath() and DeltaTable.forName() calls
    - Direct ABFSS path references in read operations
    
    Args:
        notebook_name: Name of the notebook (without .ipynb extension)
        function_scope: local (default) or central
    
    Returns:
        List of dicts with table info: [{"table_name": "...", "source_type": "table|path|sql"}]
    """
    normalized_notebook_name = _normalize_notebook_name(notebook_name)
    if not normalized_notebook_name:
        return []

    normalized_scope = str(function_scope or "local").strip().lower()
    if normalized_scope not in {"local", "central"}:
        raise ValueError("function_scope must be one of: local, central.")

    custom_function_load_mode = str(globals().get('CUSTOM_FUNCTION_LOAD_MODE', 'file') or 'file').strip().lower()
    custom_function_workspace_id = str(globals().get('CUSTOM_FUNCTIONS_WORKSPACE_ID', '') or '').strip()
    custom_function_file_base_path = str(globals().get('CUSTOM_FUNCTIONS_FILE_ABFSS_BASE_PATH', '') or '').strip().rstrip('/')
    effective_load_mode = 'file' if normalized_scope == 'central' else custom_function_load_mode
    custom_function_source_id = custom_function_file_base_path if effective_load_mode == 'file' and custom_function_file_base_path else custom_function_workspace_id
    cache_key = _custom_function_parse_cache_key(
        normalized_notebook_name,
        normalized_scope,
        effective_load_mode,
        custom_function_source_id,
    )

    # Return cached result if this notebook was already parsed
    with _notebook_parse_cache_lock:
        cached = _notebook_parse_cache.get(cache_key)
    if cached is not None:
        print(f"📖 Found {len(cached)} table references in notebook '{normalized_notebook_name}' (cached)")
        return cached

    tables_found = []
    
    try:
        # Same source selection as instantiate_notebook(), but read-only — no execution.
        code_strings = []
        if normalized_scope == 'central':
            code, loaded_file_path = _read_custom_function_file_source(
                normalized_notebook_name,
                custom_function_file_base_path,
                folder_names=(CENTRAL_CUSTOM_FUNCTIONS_FOLDER,),
            )
            code_strings = [code]
            print(f"Loaded code from '{loaded_file_path}' (central scope).")
        elif effective_load_mode == 'notebook':
            workspace_text = f" in workspace {custom_function_workspace_id}" if custom_function_workspace_id else ""
            print(f"Loading notebook definition for '{normalized_notebook_name}' (notebook mode{workspace_text}).")
            code_cells = _get_notebook_code_cells(normalized_notebook_name)
            for cell in code_cells:
                source = cell.get('source', [])
                code = ''.join(source) if isinstance(source, list) else source
                if code.strip():
                    code_strings.append(code)
        else:
            code, loaded_file_path = _read_custom_function_file_source(
                normalized_notebook_name,
                custom_function_file_base_path,
                folder_names=(CUSTOM_FUNCTIONS_FOLDER,),
            )
            code_strings = [code]
            print(f"Loaded code from '{loaded_file_path}' (file mode).")
        
        if not code_strings:
            print(f"⚠️ No code found in notebook '{normalized_notebook_name}'")
            with _notebook_parse_cache_lock:
                _notebook_parse_cache[cache_key] = []
            return []
        
        for code in code_strings:
            # Pattern 1a: spark.sql(f"...") — direct string literal in spark.sql()
            sql_pattern = r'spark\.sql\s*\(\s*f?["\'](.+?)["\']'
            sql_matches = re.findall(sql_pattern, code, re.DOTALL | re.IGNORECASE)
            
            # Pattern 1b: SQL strings assigned to variables (e.g., query = f"SELECT * FROM ...")
            # Catches cases like: sales_query = f"SELECT ..."; spark.sql(sales_query)
            sql_var_pattern = r'=\s*f?["\']\s*(SELECT\b.+?)["\']'
            sql_var_matches = re.findall(sql_var_pattern, code, re.DOTALL | re.IGNORECASE)
            
            for sql in sql_matches + sql_var_matches:
                # Extract table names from SQL SELECT statements
                table_refs = extract_tables_from_sql(sql)
                tables_found.extend(table_refs)
            
            # Pattern 2: spark.read.table("table_name") or spark.table("table_name")
            read_table_pattern = r'spark\.(?:read\.)?table\s*\(\s*[f]?["\']([^"\']+)["\']'
            read_matches = re.findall(read_table_pattern, code, re.IGNORECASE)
            for table in read_matches:
                tables_found.append({"table_name": table, "source_type": "table"})
            
            # Pattern 3: DeltaTable.forName(spark, "table_name")
            delta_name_pattern = r'DeltaTable\.forName\s*\(\s*spark\s*,\s*[f]?["\']([^"\']+)["\']'
            delta_name_matches = re.findall(delta_name_pattern, code, re.IGNORECASE)
            for table in delta_name_matches:
                tables_found.append({"table_name": table, "source_type": "table"})
            
            # Pattern 4: ABFSS paths - spark.read...load("abfss://...")
            abfss_pattern = r'\.load\s*\(\s*[f]?["\'](abfss://[^"\']+)["\']'
            abfss_matches = re.findall(abfss_pattern, code, re.IGNORECASE)
            for abfss_path in abfss_matches:
                table_name = extract_table_from_abfss_path(abfss_path)
                if table_name:
                    tables_found.append({"table_name": table_name, "source_type": "abfss_path", "full_path": abfss_path})
            
            # Pattern 5: DeltaTable.forPath(spark, "abfss://...")
            delta_path_pattern = r'DeltaTable\.forPath\s*\(\s*spark\s*,\s*[f]?["\'](abfss://[^"\']+)["\']'
            delta_path_matches = re.findall(delta_path_pattern, code, re.IGNORECASE)
            for abfss_path in delta_path_matches:
                table_name = extract_table_from_abfss_path(abfss_path)
                if table_name:
                    tables_found.append({"table_name": table_name, "source_type": "abfss_path", "full_path": abfss_path})
            
            # Pattern 6: Variable assignments with ABFSS paths
            abfss_var_pattern = r'["\']?(abfss://[^"\']+/Tables/[^"\']+)["\']?'
            abfss_var_matches = re.findall(abfss_var_pattern, code, re.IGNORECASE)
            for abfss_path in abfss_var_matches:
                table_name = extract_table_from_abfss_path(abfss_path)
                if table_name:
                    tables_found.append({"table_name": table_name, "source_type": "abfss_path", "full_path": abfss_path})
            
            # Pattern 7: Files path references (for file ingestion)
            files_pattern = r'["\']?(abfss://[^"\']+/Files/[^"\']+)["\']?'
            files_matches = re.findall(files_pattern, code, re.IGNORECASE)
            for files_path in files_matches:
                file_name = extract_file_from_abfss_path(files_path)
                if file_name:
                    tables_found.append({"table_name": file_name, "source_type": "file_path", "full_path": files_path})
        
        # Deduplicate
        seen = set()
        unique_tables = []
        for t in tables_found:
            key = t["table_name"]
            if key not in seen:
                seen.add(key)
                unique_tables.append(t)
        
        print(f"📖 Found {len(unique_tables)} table references in notebook '{normalized_notebook_name}'")
        with _notebook_parse_cache_lock:
            _notebook_parse_cache[cache_key] = unique_tables
        return unique_tables
        
    except Exception as e:
        print(f"⚠️ Could not parse notebook '{normalized_notebook_name}': {str(e)}")
        with _notebook_parse_cache_lock:
            _notebook_parse_cache[cache_key] = []
        return []


def extract_tables_from_notebooks_parallel(
    notebook_references: List,
    max_workers: Optional[int] = None
) -> Dict[Tuple[str, str], List[Dict[str, str]]]:
    """Parse custom notebooks concurrently and return results by (scope, name)."""
    unique_references = []
    seen_references = set()
    for reference in notebook_references:
        if isinstance(reference, dict):
            notebook_name = reference.get("notebook_name")
            function_scope = reference.get("function_scope", "local")
        else:
            notebook_name = reference
            function_scope = "local"
        normalized_name = _normalize_notebook_name(notebook_name)
        normalized_scope = str(function_scope or "local").strip().lower()
        reference_key = (normalized_scope, normalized_name)
        if normalized_name and reference_key not in seen_references:
            seen_references.add(reference_key)
            unique_references.append(reference_key)

    if not unique_references:
        return {}

    results = {}
    worker_count = min(max_workers or 8, len(unique_references))
    if worker_count <= 1:
        for function_scope, notebook_name in unique_references:
            results[(function_scope, notebook_name)] = extract_tables_from_notebook(
                notebook_name,
                function_scope=function_scope,
            )
        return results

    print(f"Parsing {len(unique_references)} custom notebooks in parallel with {worker_count} workers")
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        future_to_reference = {
            executor.submit(extract_tables_from_notebook, notebook_name, function_scope): (function_scope, notebook_name)
            for function_scope, notebook_name in unique_references
        }
        for future in as_completed(future_to_reference):
            reference_key = future_to_reference[future]
            try:
                results[reference_key] = future.result()
            except Exception as e:
                print(f"Warning: Could not parse notebook {reference_key} for lineage: {e}")
                results[reference_key] = []

    return results


def _resolve_custom_notebook_source_reference(table_ref: Dict[str, str], G: LineageGraph) -> Tuple[str, Optional[str], str, str]:
    """Resolve a parsed notebook source reference into entity/datastore/node type fields."""
    ref_table_name = table_ref["table_name"]
    source_type = table_ref.get("source_type", "unknown")
    normalized_ref = str(ref_table_name).lower()
    node_type = "file" if source_type in ("file", "file_path") or normalized_ref.startswith("files/") or ":files/" in normalized_ref else "entity"

    if node_type == "file":
        ref_datastore = None
        ref_entity = ref_table_name
        if ":" in ref_table_name:
            candidate_datastore, candidate_entity = ref_table_name.split(":", 1)
            if candidate_datastore and candidate_entity:
                ref_datastore = candidate_datastore
                ref_entity = candidate_entity
        else:
            normalized_entity = str(ref_entity).lower()
            for existing_node_id, node_attrs in G.nodes(data=True):
                existing_entity = str(node_attrs.get("entity_name", "")).lower()
                if existing_entity == normalized_entity or existing_node_id.endswith(f".{normalized_entity}"):
                    ref_datastore = node_attrs.get("datastore")
                    break

        ref_medallion = derive_medallion_layer(ref_datastore) if ref_datastore else "Unknown"
        return ref_entity, ref_datastore, ref_medallion, node_type

    ref_datastore, ref_medallion = resolve_table_datastore(ref_table_name, G)
    return ref_table_name, ref_datastore, ref_medallion, node_type


def _add_custom_notebook_lineage_edges(
    G: LineageGraph,
    target_node,
    table_id: int,
    trigger_name_val: str,
    notebook_reference: Dict[str, Optional[str]],
    notebook_tables_by_name: Dict[Tuple[str, str], List[Dict[str, str]]]
) -> None:
    notebook_name = notebook_reference.get("notebook_name")
    if not notebook_name:
        return

    function_scope = str(notebook_reference.get("function_scope") or "local").strip().lower()
    notebook_tables = notebook_tables_by_name.get((function_scope, notebook_name), [])
    if not notebook_tables:
        relationship_type = notebook_reference.get("relationship_type") or "custom_notebook"
        if relationship_type not in {"custom_source_function", "custom_staging_function"}:
            return

        function_name = notebook_reference.get("function_name") or "unknown"
        function_datastore = "central_custom_functions" if function_scope == "central" else "local_custom_functions"
        function_source_node = LineageNode(
            entity_name=f"{notebook_name}.{function_name}",
            datastore=function_datastore,
            medallion_layer="External",
            system_type=relationship_type,
            node_type="external_source"
        )

        G.add_node(function_source_node.node_id, **function_source_node.to_dict())
        G.add_node(target_node.node_id, **target_node.to_dict())
        G.add_edge(
            function_source_node.node_id,
            target_node.node_id,
            table_id=table_id,
            trigger_name=trigger_name_val,
            relationship_type=f"{relationship_type} ({function_name})",
            transformation_applied=format_custom_function_attribution(notebook_reference)
        )
        return

    relationship_type = notebook_reference.get("relationship_type") or "custom_notebook"
    function_name = notebook_reference.get("function_name") or "unknown"

    print(f"Adding lineage from {relationship_type} notebook: {notebook_name} (Table_ID: {table_id})")

    for table_ref in notebook_tables:
        ref_entity, ref_datastore, ref_medallion, node_type = _resolve_custom_notebook_source_reference(table_ref, G)
        custom_source_node = LineageNode(
            entity_name=ref_entity,
            datastore=ref_datastore,
            medallion_layer=ref_medallion,
            system_type=relationship_type,
            node_type=node_type
        )

        G.add_node(custom_source_node.node_id, **custom_source_node.to_dict())
        G.add_node(target_node.node_id, **target_node.to_dict())

        transformation_applied = format_custom_function_attribution(notebook_reference)
        if G.has_edge(custom_source_node.node_id, target_node.node_id):
            existing_edge = G[custom_source_node.node_id][target_node.node_id]
            existing_transformation = existing_edge.get("transformation_applied")
            if existing_transformation:
                if transformation_applied not in existing_transformation:
                    existing_edge["transformation_applied"] = f"{existing_transformation}; {transformation_applied}"
            else:
                existing_edge["transformation_applied"] = transformation_applied
            continue

        G.add_edge(
            custom_source_node.node_id,
            target_node.node_id,
            table_id=table_id,
            trigger_name=trigger_name_val,
            relationship_type=f"{relationship_type} ({function_name})",
            transformation_applied=transformation_applied
        )


def extract_table_from_abfss_path(abfss_path: str) -> Optional[str]:
    """
    Extract table name from an ABFSS path.
    
    Handles paths like:
    - abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse.Lakehouse/Tables/schema/table
    - abfss://...@onelake.../bronze.Lakehouse/Tables/dbo/customers
    
    Returns:
        Table name in format "datastore.schema.table" or None if not a Tables path
    """
    if "/Tables/" not in abfss_path:
        return None
    
    # Extract lakehouse name and table path
    # Pattern: /lakehouse_name.Lakehouse/Tables/schema/table
    match = re.search(r'/([^/]+)\.Lakehouse/Tables/(.+?)(?:\?|$)', abfss_path, re.IGNORECASE)
    if match:
        lakehouse_name = match.group(1)
        table_path = match.group(2).rstrip('/')
        # table_path might be "schema/table" or just "table"
        parts = table_path.split('/')
        if len(parts) >= 2:
            return f"{lakehouse_name}.{parts[0]}.{parts[1]}"
        elif len(parts) == 1:
            return f"{lakehouse_name}.dbo.{parts[0]}"

    # Fabric item-id path: /{lakehouse_item_id}/Tables/schema/table
    item_match = re.search(r'/([^/?]+)/Tables/(.+?)(?:\?|$)', abfss_path, re.IGNORECASE)
    if item_match:
        table_path = item_match.group(2).rstrip('/')
        parts = [part for part in table_path.split('/') if part]
        if len(parts) >= 2:
            return f"{parts[0]}.{parts[1]}"
        elif len(parts) == 1:
            return f"dbo.{parts[0]}"
    
    return None


def extract_file_from_abfss_path(abfss_path: str) -> Optional[str]:
    """
    Extract file reference from an ABFSS Files path.
    
    Handles paths like:
    - abfss://workspace@onelake.../bronze.Lakehouse/Files/folder/file.csv
    
    Returns:
        File path in format "Files/folder/file.csv" or None
    """
    if "/Files/" not in abfss_path:
        return None
    
    # Extract the Files portion
    match = re.search(r'/([^/]+)\.Lakehouse/(Files/.+?)(?:\?|$)', abfss_path, re.IGNORECASE)
    if match:
        lakehouse_name = match.group(1)
        file_path = match.group(2).rstrip('/')
        return f"{lakehouse_name}:{file_path}"

    # Fabric item-id path: /{lakehouse_item_id}/Files/path/to/file
    item_match = re.search(r'/([^/?]+)/(Files/.+?)(?:\?|$)', abfss_path, re.IGNORECASE)
    if item_match:
        return item_match.group(2).rstrip('/')
    
    return None


def extract_tables_from_sql(sql: str) -> List[Dict[str, str]]:
    """
    Extract table names from a SQL query string (READ operations only).
    
    Handles:
    - FROM clause: SELECT * FROM table_name
    - JOIN clauses: JOIN table_name ON ...
    
    Args:
        sql: SQL query string
    
    Returns:
        List of dicts with table info
    """
    tables = []
    
    # Preprocess: strip backtick-quoted f-string workspace variables
    # e.g., `{silver_workspace_name}`.Silver.dbo.table → Silver.dbo.table
    sql = re.sub(r'`\{[^}]+\}`\.', '', sql)
    # Strip remaining backtick-quoted identifiers: `name` → name
    sql = re.sub(r'`([^`]+)`', r'\1', sql)
    
    # Pattern for FROM and JOIN - captures 3-part names (datastore.schema.table)
    from_join_pattern = r'(?:FROM|JOIN)\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*){0,2})'
    matches = re.findall(from_join_pattern, sql, re.IGNORECASE)
    
    # SQL keywords to skip
    skip_keywords = {'SELECT', 'WHERE', 'AND', 'OR', 'ON', 'AS', 'SET', 'VALUES', 
                     'GROUP', 'ORDER', 'BY', 'HAVING', 'LIMIT', 'UNION', 'CASE', 
                     'WHEN', 'THEN', 'ELSE', 'END', 'NULL', 'TRUE', 'FALSE'}
    
    for table in matches:
        if table.upper() not in skip_keywords:
            tables.append({"table_name": table, "source_type": "sql"})
    
    return tables

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================================
# LINEAGE DEPTH AND PATH CALCULATION
# ===========================================================================================

def calculate_lineage_depth_and_path(
    G: LineageGraph,
    target_node_id: str,
    source_node_id: str,
    relationship_type: str = "direct",
    lineage_path_context: Optional[Dict[str, Any]] = None
) -> Tuple[int, str]:
    """
    Calculate the lineage depth and full path from any root source to the target.
    
    For 'direct' relationships: Shows full lineage chain from ultimate root to target.
    For secondary sources (joins, dimension lookups, unions): Shows simplified path
    since those sources have their own lineage records with full chains.
    
    Returns:
        depth: Number of hops from the earliest ancestor (root) to target
        path: Path string (full chain for direct, simplified for secondary sources)
    """
    lineage_path_context = lineage_path_context or build_lineage_path_context(G)
    root_sources = lineage_path_context.get("root_sources", set())
    longest_root_paths = lineage_path_context.get("longest_root_paths", {})
    
    # For secondary sources (joins, dimension lookups, unions), show simplified path
    # These sources have their own lineage records that show their full chain
    if relationship_type and relationship_type != "direct":
        # Calculate depth through this secondary source
        if source_node_id in root_sources:
            depth = 1
        else:
            path_to_source = longest_root_paths.get(source_node_id)
            depth = len(path_to_source) if path_to_source else 1
        
        # Show simplified path: just the immediate relationship
        simplified_path = f"{source_node_id} → {target_node_id}"
        return depth, simplified_path
    
    # For direct relationships, show full lineage chain
    # If the source IS a root source, depth is 1
    if source_node_id in root_sources:
        path = f"{source_node_id} → {target_node_id}"
        return 1, path
    
    # Find the deepest path from any root to the source, then add the target
    best_path_to_source = longest_root_paths.get(source_node_id)
    
    if best_path_to_source is None:
        # No path found from any root, this edge starts from source directly
        return 1, f"{source_node_id} → {target_node_id}"

    best_depth = len(best_path_to_source)
    best_path = " → ".join(best_path_to_source + [target_node_id])

    return best_depth, best_path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================================
# BUILD LINEAGE GRAPH
# ===========================================================================================

def build_lineage_graph(
    orch_records: List,
    primary_source_by_table: Dict[int, List],
    advanced_transform_by_table: Dict[int, List],
    extract_custom_transformation_function_lineage: bool = True
) -> LineageGraph:
    """
    Build a directed graph representing data lineage.
    
    This function accepts pre-collected Python data structures (not DataFrames)
    to enable unit testing without Spark.
    
    Args:
        orch_records: List of orchestration rows (each with Table_ID, Target_Datastore,
                      Target_Entity, Trigger_Name attributes)
        primary_source_by_table: Dict mapping Table_ID to list of source_details config rows
        advanced_transform_by_table: Dict mapping Table_ID to transformation, data quality,
                         and post-write reconciliation rows used by lineage
        extract_custom_transformation_function_lineage: Whether to parse custom notebooks for additional sources
    
    Returns:
        LineageGraph with nodes (entities) and edges (data flows)
    """
    G = LineageGraph()

    custom_notebook_refs_by_table = {}
    custom_notebook_references = []
    if extract_custom_transformation_function_lineage:
        for orch_row in orch_records:
            table_id = orch_row.Table_ID
            source_configs = primary_source_by_table.get(table_id, [])
            transform_configs = advanced_transform_by_table.get(table_id, [])
            notebook_references = (
                extract_custom_ingestion_function_references(source_configs)
                + extract_custom_transformation_function_references(transform_configs)
            )
            notebook_references = [ref for ref in notebook_references if ref.get("notebook_name")]
            custom_notebook_refs_by_table[table_id] = notebook_references
            custom_notebook_references.extend(notebook_references)

    notebook_tables_by_name = extract_tables_from_notebooks_parallel(custom_notebook_references) if custom_notebook_references else {}
    
    for orch_row in orch_records:
        table_id = orch_row.Table_ID
        target_datastore = orch_row.Target_Datastore
        target_entity = orch_row.Target_Entity
        trigger_name_val = orch_row.Trigger_Name
        
        # Get pre-collected configs for this table (pure Python, no Spark actions)
        source_configs = primary_source_by_table.get(table_id, [])
        transform_configs = advanced_transform_by_table.get(table_id, [])
        
        # Create target node
        target_medallion = derive_medallion_layer(target_datastore)
        target_node = LineageNode(
            entity_name=target_entity,
            datastore=target_datastore,
            medallion_layer=target_medallion,
            node_type="entity"
        )
        G.add_node(target_node.node_id, **target_node.to_dict())
        
        # Get source details
        source_entity, source_datastore, source_medallion = derive_source_entity_and_datastore(
            source_configs
        )
        source_system_type = derive_source_system_type(source_configs)
        
        if source_entity:
            source_node = LineageNode(
                entity_name=source_entity,
                datastore=source_datastore,
                medallion_layer=source_medallion,
                system_type=source_system_type,
                node_type="external_source" if source_medallion == "External" else "entity"
            )
            
            G.add_node(source_node.node_id, **source_node.to_dict())
            G.add_node(target_node.node_id, **target_node.to_dict())
            
            transformations = extract_transformations(transform_configs)
            transformation_parts = []
            if transformations:
                transformation_parts.append(", ".join(transformations))

            for ingestion_reference in extract_custom_ingestion_function_references(source_configs):
                transformation_parts.append(format_custom_function_attribution(ingestion_reference))

            for transformation_reference in extract_custom_transformation_function_references(transform_configs):
                transformation_parts.append(format_custom_function_attribution(transformation_reference))

            for validation_reference in extract_custom_validation_function_references(transform_configs):
                transformation_parts.append(format_custom_function_attribution(validation_reference))

            transformation_str = "; ".join(transformation_parts) if transformation_parts else None
            
            G.add_edge(
                source_node.node_id,
                target_node.node_id,
                table_id=table_id,
                trigger_name=trigger_name_val,
                relationship_type="direct",
                transformation_applied=transformation_str
            )
        
        # Add edges for join sources
        join_sources = extract_join_sources(transform_configs)
        for join_info in join_sources:
            right_table = join_info["right_table_name"]
            if right_table:
                join_datastore, join_medallion = resolve_table_datastore(right_table, G)
                
                join_node = LineageNode(
                    entity_name=right_table,
                    datastore=join_datastore,
                    medallion_layer=join_medallion,
                    node_type="entity"
                )
                
                G.add_node(join_node.node_id, **join_node.to_dict())
                G.add_edge(
                    join_node.node_id,
                    target_node.node_id,
                    table_id=table_id,
                    trigger_name=trigger_name_val,
                    relationship_type=f"join ({join_info['join_type']})",
                    transformation_applied=f"join_data: {join_info.get('join_condition', '')}"
                )
        
        # Add edges for dimension surrogate key lookups
        dim_sources = extract_dimension_surrogate_key_sources(transform_configs)
        for dim_info in dim_sources:
            dim_table = dim_info["dimension_table_name"]
            if dim_table:
                dim_datastore, dim_medallion = resolve_table_datastore(dim_table, G)
                
                dim_node = LineageNode(
                    entity_name=dim_table,
                    datastore=dim_datastore,
                    medallion_layer=dim_medallion,
                    node_type="dimension"
                )
                
                G.add_node(dim_node.node_id, **dim_node.to_dict())
                
                # Build transformation description
                transform_desc = f"attach_dimension_surrogate_key: {dim_info.get('key_column', 'surrogate_key')}"
                if dim_info.get('columns_to_add'):
                    transform_desc += f" + denormalize({dim_info['columns_to_add']})"
                
                G.add_edge(
                    dim_node.node_id,
                    target_node.node_id,
                    table_id=table_id,
                    trigger_name=trigger_name_val,
                    relationship_type="dimension_lookup",
                    transformation_applied=transform_desc
                )
        
        # Add edges for union sources
        union_sources = extract_union_sources(transform_configs)
        for union_info in union_sources:
            union_table = union_info["union_table_name"]
            if union_table:
                union_datastore, union_medallion = resolve_table_datastore(union_table, G)
                
                union_node = LineageNode(
                    entity_name=union_table,
                    datastore=union_datastore,
                    medallion_layer=union_medallion,
                    node_type="entity"
                )
                
                G.add_node(union_node.node_id, **union_node.to_dict())
                
                # Build transformation description
                transform_desc = f"union_data ({union_info.get('union_type', 'by_name')})"
                if union_info.get('deduplicate') == 'true':
                    transform_desc += " + deduplicate"
                
                G.add_edge(
                    union_node.node_id,
                    target_node.node_id,
                    table_id=table_id,
                    trigger_name=trigger_name_val,
                    relationship_type="union",
                    transformation_applied=transform_desc
                )
        
        # Extract lineage from custom notebooks (source-side and transformation functions)
        if extract_custom_transformation_function_lineage:
            for notebook_reference in custom_notebook_refs_by_table.get(table_id, []):
                _add_custom_notebook_lineage_edges(
                    G=G,
                    target_node=target_node,
                    table_id=table_id,
                    trigger_name_val=trigger_name_val,
                    notebook_reference=notebook_reference,
                    notebook_tables_by_name=notebook_tables_by_name,
                )
    
    return G


def prepare_graph_inputs(orchestration_df, primary_config_df, advanced_config_df, table_ids: List[int] = None) -> Tuple[List, Dict, Dict]:
    """
    Pre-collect Spark DataFrames into Python data structures for build_lineage_graph.
    
    This is the only function that touches Spark — it bridges the gap between
    Spark DataFrames and the pure-Python graph builder.
    
    Args:
        orchestration_df: Spark DataFrame with orchestration metadata
        primary_config_df: Spark DataFrame with primary configuration
        advanced_config_df: Spark DataFrame with advanced configuration
        table_ids: Optional list of specific Table_IDs to include
    
    Returns:
        Tuple of (orch_records, primary_source_by_table, advanced lineage config by table)
    """
    # Pre-collect DataFrames into Python dicts indexed by Table_ID
    _primary_source_by_table = {}
    for row in primary_config_df.collect():
        if row.Configuration_Category == "source_details":
            _primary_source_by_table.setdefault(row.Table_ID, []).append(row)
    
    _advanced_transform_by_table = {}
    for row in advanced_config_df.collect():
        if row.Configuration_Category in {
            "data_transformation_steps",
            "data_quality",
            "post_write_reconciliation",
        }:
            _advanced_transform_by_table.setdefault(row.Table_ID, []).append(row)
    
    if table_ids:
        orch_records = orchestration_df.filter(
            orchestration_df.Table_ID.isin(table_ids)
        ).collect()
    else:
        orch_records = orchestration_df.filter(
            orchestration_df.Ingestion_Active == 1
        ).collect()
    
    return orch_records, _primary_source_by_table, _advanced_transform_by_table

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================================
# LINEAGE RECORD GENERATION
# ===========================================================================================

def generate_lineage_records(
    G: LineageGraph,
    tables_with_logs: set,
    source_type_lookup: Dict[int, str],
    target_type_lookup: Dict[int, str],
    orch_lookup: Dict,
    primary_source_by_table: Dict[int, List],
    workspace_variables: dict,
    lineage_groups: Dict[str, Dict] = None,
    lineage_version: int = 1
) -> List[Dict]:
    """
    Generate lineage records by combining graph structure with execution log info.
    
    Creates ONE record per unique edge (structural lineage only).
    Only outputs edges where the target table has actual run data (no "Metadata Only" records).
    
    This function accepts pre-collected Python data structures (not DataFrames)
    to enable unit testing without Spark.
    
    Args:
        G: LineageGraph built by build_lineage_graph
        tables_with_logs: Set of Table_IDs that have actual processed log records
        source_type_lookup: Dict mapping Table_ID to Source_Type from logs
        target_type_lookup: Dict mapping Table_ID to Target_Type from logs
        orch_lookup: Dict mapping Table_ID to orchestration row
        primary_source_by_table: Dict mapping Table_ID to source config rows
        workspace_variables: Dict of workspace variables from Datastore_Configuration
        lineage_groups: Dict mapping node_id to group info (from compute_lineage_groups)
        lineage_version: Version number for lineage records
    
    Returns:
        List of lineage record dicts
    """
    lineage_records = []
    lineage_generated_at = datetime.utcnow()
    lineage_path_context = build_lineage_path_context(G)

    # Build a map of lineage node_id -> Table_ID for orchestration targets.
    # This enables Source_Table_ID population when a source node is also a modeled target table.
    node_target_table_id_lookup = {}
    for target_table_id, orch_row in (orch_lookup or {}).items():
        target_datastore = getattr(orch_row, "Target_Datastore", None)
        target_entity = getattr(orch_row, "Target_Entity", None)
        if not target_datastore or not target_entity:
            continue

        target_node_id = LineageNode(
            entity_name=target_entity,
            datastore=target_datastore,
            medallion_layer=derive_medallion_layer(target_datastore),
        ).node_id

        target_node_key = _canonicalize_node_id(target_node_id)
        existing = node_target_table_id_lookup.get(target_node_key)
        if existing is None or target_table_id < existing:
            node_target_table_id_lookup[target_node_key] = target_table_id
    
    # Build a map of node_id -> set(trigger_names) for cross-trigger detection
    node_trigger_map = {}
    for source_id, target_id, edge_data in G.edges(data=True):
        edge_trigger = edge_data.get("trigger_name")
        if edge_trigger:
            node_trigger_map.setdefault(target_id, set()).add(edge_trigger)
            node_trigger_map.setdefault(source_id, set()).add(edge_trigger)
    
    for source_id, target_id, edge_data in G.edges(data=True):
        edge_table_id = edge_data.get("table_id")
        target_node_key = _canonicalize_node_id(target_id)
        source_node_key = _canonicalize_node_id(source_id)
        target_table_id = edge_table_id if edge_table_id is not None else node_target_table_id_lookup.get(target_node_key)
        edge_trigger_name = edge_data.get("trigger_name")
        relationship_type = edge_data.get("relationship_type", "direct")
        
        # Skip edges without actual run data
        if target_table_id not in tables_with_logs:
            continue
        
        source_node = G.nodes[source_id]
        target_node = G.nodes[target_id]
        
        # Calculate lineage depth and path using graph traversal
        lineage_depth, lineage_path = calculate_lineage_depth_and_path(
            G,
            target_id,
            source_id,
            relationship_type,
            lineage_path_context = lineage_path_context
        )
        
        # Detect cross-trigger lineage by checking if path nodes belong to different triggers
        path_nodes = lineage_path.split(" → ") if lineage_path else []
        triggers_in_path = set()
        for node_id in path_nodes:
            if node_id in node_trigger_map:
                triggers_in_path.update(node_trigger_map[node_id])
        
        is_cross_trigger = len(triggers_in_path) > 1
        cross_trigger_info = ", ".join(sorted(triggers_in_path)) if is_cross_trigger else None
        
        # Extract extended source details
        if relationship_type != "direct":
            source_details = build_source_details_from_node(source_node, workspace_variables)
        else:
            source_details = extract_extended_source_details(primary_source_by_table.get(target_table_id, []), workspace_variables)

        source_details = backfill_source_location_details(
            source_details,
            source_node,
            source_type_lookup.get(target_table_id),
            workspace_variables,
        )
        
        # Extract target details
        target_details = {"workspace_name": None, "abfss_path": None}
        if target_table_id in orch_lookup:
            target_details = extract_target_details(orch_lookup[target_table_id], workspace_variables)
        
        lineage_id = str(uuid.uuid4())
        date_key = int(lineage_generated_at.strftime("%Y%m%d"))
        
        # Resolve lineage group for this edge's target node.
        # Prefer exact node_id lookup, then fall back to canonicalized key to handle
        # casing/.Lakehouse naming variants.
        target_group = (lineage_groups or {}).get(target_id)
        if target_group is None:
            target_group = (lineage_groups or {}).get(target_node_key, {})

        source_table_id = node_target_table_id_lookup.get(source_node_key)
        
        record = {
            "Lineage_ID": lineage_id,
            "Trigger_Name": edge_trigger_name,
            "Source_Table_ID": source_table_id,
            "Target_Table_ID": target_table_id,
            "Source_Entity": source_node.get("entity_name"),
            "Source_Datastore": source_node.get("datastore"),
            "Source_Medallion_Layer": source_node.get("medallion_layer"),
            "Source_Type": resolve_lineage_source_type(
                relationship_type,
                source_node,
                target_table_id,
                primary_source_by_table,
                source_type_lookup,
            ),
            "Source_Workspace_Name": source_details.get("workspace_name"),
            "Source_ABFSS_Path": source_details.get("abfss_path"),
            "Source_Connection_ID": source_details.get("connection_id"),
            "Target_Entity": target_node.get("entity_name"),
            "Target_Datastore": target_node.get("datastore"),
            "Target_Medallion_Layer": target_node.get("medallion_layer"),
            "Target_Type": infer_fabric_entity_type(target_node.get("entity_name")) or target_type_lookup.get(target_table_id),
            "Target_Workspace_Name": target_details.get("workspace_name"),
            "Target_ABFSS_Path": target_details.get("abfss_path"),
            "Relationship_Type": edge_data.get("relationship_type"),
            "Transformation_Applied": edge_data.get("transformation_applied"),
            "Lineage_Depth": lineage_depth,
            "Lineage_Path": lineage_path,
            "Is_Cross_Trigger": is_cross_trigger,
            "Cross_Trigger_Dependencies": cross_trigger_info,
            "Lineage_Group_ID": target_group.get("group_id"),
            "Lineage_Group_Table_Count": target_group.get("group_table_count", target_group.get("group_size")),
            "Lineage_Generated_At": lineage_generated_at,
            "Lineage_Version": lineage_version,
            "Date_Key": date_key
        }
        lineage_records.append(record)
    
    return lineage_records


def resolve_lineage_source_type(
    relationship_type: str,
    source_node: Dict,
    target_table_id: Optional[int],
    primary_source_by_table: Dict[int, List],
    source_type_lookup: Dict[int, str],
) -> Optional[str]:
    """
    Resolve Source_Type for lineage records with deterministic metadata-first logic.

    Priority:
    1) Direct edge: derive from source configuration
    2) Node hint: source_node.system_type
    3) Node semantics (External/SFTP or Fabric Files/Tables)
    4) Log fallback lookup
    """
    rel_type = str(relationship_type or "").strip().lower()

    def _normalize_source_type_label(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        raw_value = str(value).strip()
        if not raw_value:
            return None

        lowered = raw_value.lower()
        if lowered in ["delta_table", "delta table", "lakehouse_table", "lakehouse table"]:
            return "Fabric Lakehouse (Tables)"
        if lowered in ["file", "files", "lakehouse_file", "lakehouse files", "lakehouse_file(s)"]:
            return "Fabric Lakehouse (Files)"
        if lowered == "sftp":
            return "sftp"

        return raw_value

    if rel_type == "direct" and target_table_id is not None:
        config_type = derive_source_system_type(primary_source_by_table.get(target_table_id, []))
        if config_type and config_type != "unknown":
            return _normalize_source_type_label(config_type)

    system_type = str(source_node.get("system_type") or "").strip()
    if system_type and system_type.lower() != "unknown":
        return _normalize_source_type_label(system_type)

    source_entity = source_node.get("entity_name")
    source_datastore = str(source_node.get("datastore") or "").strip()
    source_layer = str(source_node.get("medallion_layer") or "").strip()

    if source_layer == "External":
        source_entity_lower = str(source_entity or "").strip().lower()
        source_datastore_lower = source_datastore.lower()
        if source_entity_lower.startswith("sftp:") or "sftp" in source_datastore_lower:
            return "sftp"
        return source_datastore or "external"

    inferred_type = infer_fabric_entity_type(source_entity)
    if inferred_type:
        return inferred_type

    return _normalize_source_type_label(source_type_lookup.get(target_table_id))


def validate_lineage_record_ids(records: List[Dict]) -> None:
    """
    Validate lineage ID population for records that are about to be persisted.

    Contract:
    - Target_Table_ID must always be populated.
    - Source_Table_ID may be null for true roots/external sources.
    """
    missing_target_table_id_records = [r for r in records if r.get("Target_Table_ID") is None]
    if missing_target_table_id_records:
        sample_missing = missing_target_table_id_records[0]
        raise ValueError(
            "Target_Table_ID is required but missing for lineage records. "
            f"Missing count: {len(missing_target_table_id_records)}. "
            f"Sample edge: {sample_missing.get('Source_Entity')} -> {sample_missing.get('Target_Entity')}"
        )


def prepare_log_lookups(logs_df, trigger_name_filter: str = None, execution_id_filter: str = None) -> Tuple[set, Dict, Dict]:
    """
    Pre-collect log DataFrame into Python data structures for generate_lineage_records.
    
    This is the only function that touches Spark for logs — it bridges the gap between
    Spark DataFrames and the pure-Python record generator.
    
    Args:
        logs_df: Spark DataFrame with pipeline logs (already filtered to Processed)
        trigger_name_filter: Optional trigger name to filter logs
        execution_id_filter: Optional execution ID to filter logs
    
    Returns:
        Tuple of (tables_with_logs, source_type_lookup, target_type_lookup)
    """
    filtered_logs = logs_df.filter(logs_df.Ingestion_Status == "Processed")
    if trigger_name_filter:
        filtered_logs = filtered_logs.filter(filtered_logs.Trigger_Name == trigger_name_filter)
    if execution_id_filter:
        filtered_logs = filtered_logs.filter(filtered_logs.Trigger_Execution_ID == execution_id_filter)
    
    log_records = filtered_logs.collect()
    
    tables_with_logs = set()
    source_type_lookup = {}
    target_type_lookup = {}
    for log in log_records:
        tables_with_logs.add(log.Table_ID)
        if hasattr(log, 'Source_Type') and log.Source_Type:
            source_type_lookup[log.Table_ID] = log.Source_Type
        if hasattr(log, 'Target_Type') and log.Target_Type:
            target_type_lookup[log.Table_ID] = log.Target_Type
    
    return tables_with_logs, source_type_lookup, target_type_lookup

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================================
# NARRATIVE GENERATION
# ===========================================================================================

def generate_lineage_summary(record: Dict) -> str:
    """
    Generate a human-readable summary for a lineage record.
    Focuses on structural data flow context (no execution-specific data).
    Makes the lineage easy to understand at a glance.
    """
    source = record.get("Source_Entity", "Unknown")
    target = record.get("Target_Entity", "Unknown")
    source_layer = record.get("Source_Medallion_Layer", "")
    target_layer = record.get("Target_Medallion_Layer", "")
    relationship = record.get("Relationship_Type", "direct")
    depth = record.get("Lineage_Depth", 1)
    transforms = record.get("Transformation_Applied")
    is_cross_trigger = record.get("Is_Cross_Trigger", False)
    
    # Build the flow description
    if source_layer == "External":
        flow_desc = f"Ingests from {source}"
    else:
        flow_desc = f"Reads from {source} ({source_layer})"
    
    flow_desc += f" → writes to {target} ({target_layer})"
    
    # Add relationship context
    if relationship == "direct":
        rel_desc = "as primary source"
    elif "join" in relationship.lower():
        rel_desc = f"via {relationship}"
    elif relationship == "dimension_lookup":
        rel_desc = "for dimension key lookup"
    elif relationship == "union":
        rel_desc = "as union source"
    elif "custom_transformation_function" in relationship.lower():
        rel_desc = "via custom transformation"
    elif "custom_table_ingestion_function" in relationship.lower() or "custom_file_ingestion_function" in relationship.lower():
        rel_desc = "via custom ingestion"
    else:
        rel_desc = f"({relationship})"
    
    # Add transformation info
    if transforms:
        transform_summary = transforms.split(",")[0].strip()
        if len(transforms.split(",")) > 1:
            transform_summary += f" +{len(transforms.split(','))-1} more"
        transform_str = f" • Transforms: {transform_summary}"
    else:
        transform_str = ""
    
    # Add depth context for multi-hop lineage
    if depth > 1:
        depth_str = f" • Depth {depth} (multi-hop)"
    else:
        depth_str = ""
    
    # Cross-trigger indicator
    cross_str = " • ⚠️ Cross-trigger" if is_cross_trigger else ""
    
    return f"{flow_desc} {rel_desc}{transform_str}{depth_str}{cross_str}"


def generate_target_lineage_narrative(records: List[Dict], target_entity: str) -> str:
    """
    Generate a consolidated, human-readable narrative for a target entity.
    Designed to be clear and natural for both humans and LLMs (Copilot).
    
    Example output:
    "gold.dbo.fact_sales is a Gold layer table that originates from Oracle SALES.ORDERS. 
     Data flows through 3 hops: Oracle → bronze.dbo.orders → silver.dbo.orders → gold.dbo.fact_sales.
     This table also joins with dim_customer (originating from Oracle CUSTOMERS) and dim_product 
     (originating from Oracle PRODUCTS) to enrich the data with dimension keys.
     Transformations applied: cleanse_data, derived_column, attach_dimension_surrogate_key."
    """
    # Filter records for this target
    target_records = [r for r in records if r.get("Target_Entity") == target_entity]
    
    if not target_records:
        return f"{target_entity}: No lineage data available."
    
    # Get target info from first record
    target_layer = target_records[0].get("Target_Medallion_Layer", "Unknown")
    target_datastore = target_records[0].get("Target_Datastore", "")
    
    # Separate primary (direct) sources from secondary sources
    primary_sources = []
    secondary_sources = []
    all_cross_triggers = set()
    all_transforms = set()
    max_depth = 0
    
    for r in target_records:
        rel_type = r.get("Relationship_Type", "direct")
        depth = r.get("Lineage_Depth", 1)
        max_depth = max(max_depth, depth)
        
        # Collect transforms
        transforms = r.get("Transformation_Applied", "")
        if transforms:
            for t in transforms.split(","):
                t_clean = t.split(":")[0].strip()
                if t_clean:
                    all_transforms.add(t_clean)
        
        if r.get("Is_Cross_Trigger"):
            cross_deps = r.get("Cross_Trigger_Dependencies", "")
            if cross_deps:
                for t in cross_deps.split(", "):
                    all_cross_triggers.add(t.strip())
        
        if rel_type == "direct":
            primary_sources.append(r)
        else:
            secondary_sources.append(r)
    
    # Build the narrative in natural language
    parts = []
    
    # --- OPENING: What is this table? ---
    full_target = f"{target_datastore}.{target_entity}" if target_datastore and not target_entity.startswith(target_datastore) else target_entity
    parts.append(f"{full_target} is a {target_layer} layer table.")
    
    # --- PRIMARY DATA FLOW: Where does the main data come from? ---
    if primary_sources:
        r = primary_sources[0]
        path = r.get("Lineage_Path", "")
        source = r.get("Source_Entity", "Unknown")
        source_layer = r.get("Source_Medallion_Layer", "")
        depth = r.get("Lineage_Depth", 1)
        
        # Find the ultimate origin (first node in path)
        if path:
            path_nodes = path.split(" → ")
            origin = path_nodes[0]
        else:
            origin = source
        
        # Determine the origin's medallion layer by checking the graph OR records
        origin_layer = None
        for rec in records:
            if rec.get("Source_Entity") == origin or (rec.get("Source_Datastore") and f"{rec.get('Source_Datastore')}.{rec.get('Source_Entity')}" == origin):
                origin_layer = rec.get("Source_Medallion_Layer")
                break
        if not origin_layer:
            origin_layer = derive_medallion_layer(origin.split(".")[0] if "." in origin else None)
        
        # Describe the origin based on its ACTUAL layer
        if origin_layer == "External" or source_layer == "External":
            origin_desc = f"originates from external source {origin}"
        elif origin_layer in ("Bronze", "Silver", "Gold"):
            origin_desc = f"traces back to {origin} ({origin_layer} layer); the original external source is not tracked in this lineage"
        else:
            origin_desc = f"sources data from {origin}"
        
        parts.append(f"It {origin_desc}.")
        
        # Describe the data flow path
        if depth > 1 and path:
            parts.append(f"Data flows through {depth} hops: {path}.")
        elif depth == 1:
            parts.append(f"Data flows directly from {source}.")
    
    # --- SECONDARY SOURCES: Joins, dimensions, unions ---
    if secondary_sources:
        join_sources = []
        dim_sources = []
        union_sources = []
        other_sources = []
        
        for r in secondary_sources:
            source = r.get("Source_Entity", "Unknown")
            rel_type = r.get("Relationship_Type", "")
            
            # Find the origin of this secondary source
            source_lineage = [rec for rec in records if rec.get("Target_Entity") == source and rec.get("Relationship_Type") == "direct"]
            if source_lineage:
                best = max(source_lineage, key=lambda x: x.get("Lineage_Depth", 1))
                origin_path = best.get("Lineage_Path", "")
                if origin_path:
                    sec_origin = origin_path.split(" → ")[0]
                else:
                    sec_origin = best.get("Source_Entity", "unknown origin")
            else:
                sec_origin = None
            
            source_info = {"name": source, "origin": sec_origin, "rel_type": rel_type}
            
            if "join" in rel_type.lower():
                join_sources.append(source_info)
            elif rel_type == "dimension_lookup":
                dim_sources.append(source_info)
            elif rel_type == "union":
                union_sources.append(source_info)
            else:
                other_sources.append(source_info)
        
        # Describe dimension lookups
        if dim_sources:
            if len(dim_sources) == 1:
                d = dim_sources[0]
                origin_text = f" (originating from {d['origin']})" if d['origin'] else ""
                parts.append(f"It enriches data by looking up keys from dimension table {d['name']}{origin_text}.")
            else:
                dim_descs = []
                for d in dim_sources:
                    origin_text = f" (from {d['origin']})" if d['origin'] else ""
                    dim_descs.append(f"{d['name']}{origin_text}")
                parts.append(f"It enriches data by looking up keys from dimension tables: {', '.join(dim_descs)}.")
        
        # Describe joins
        if join_sources:
            if len(join_sources) == 1:
                j = join_sources[0]
                origin_text = f" (originating from {j['origin']})" if j['origin'] else ""
                join_type = j['rel_type'].replace("join", "").replace("(", "").replace(")", "").strip() or "inner"
                parts.append(f"It joins with {j['name']}{origin_text} using a {join_type} join.")
            else:
                join_names = [j['name'] for j in join_sources]
                parts.append(f"It joins with tables: {', '.join(join_names)}.")
        
        # Describe unions
        if union_sources:
            union_names = [u['name'] for u in union_sources]
            parts.append(f"It unions data from: {', '.join(union_names)}.")
    
    # --- TRANSFORMATIONS ---
    if all_transforms:
        transforms_list = sorted(all_transforms)
        parts.append(f"Transformations applied: {', '.join(transforms_list)}.")
    
    # --- CROSS-TRIGGER WARNING ---
    if all_cross_triggers:
        parts.append(f"⚠️ This table spans multiple triggers: {', '.join(sorted(all_cross_triggers))}.")
    
    # --- DEPTH SUMMARY ---
    if max_depth > 2:
        parts.append(f"Total lineage depth: {max_depth} hops from origin.")
    
    return " ".join(parts)


def generate_source_narrative(source_entity: str, records: List[Dict], target_narrative_cache: Dict[str, str]) -> str:
    """
    Generate a narrative for a source entity.
    
    If the source is also a target in the lineage (intermediate table), reuses its
    target narrative. Otherwise generates a root/origin narrative.
    
    Args:
        source_entity: The source entity name
        records: All lineage records
        target_narrative_cache: Cache of target narratives (entity_name -> narrative)
    
    Returns:
        Human-readable narrative for the source entity
    """
    if source_entity in target_narrative_cache:
        return target_narrative_cache[source_entity]
    
    # This is a root/origin source (external system or file)
    source_records = [r for r in records if r.get("Source_Entity") == source_entity]
    if source_records:
        r = source_records[0]
        source_layer = r.get("Source_Medallion_Layer", "Unknown")
        source_type = r.get("Source_Type", "")
        if source_layer == "External":
            return f"{source_entity} is an external data source ({source_type or 'external system'}). It is the origin point for data flowing into this pipeline."
        else:
            return f"{source_entity} is a {source_layer} layer table. No upstream lineage found - this may be a root source or its lineage is not tracked."
    
    return f"{source_entity}: No lineage data available."

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================================
# CHANGE DETECTION (FINGERPRINTING)
# ===========================================================================================

def compute_lineage_fingerprint(records: List[Dict]) -> str:
    """
    Compute a fingerprint of the lineage structure to detect changes.
    Only considers the structural elements, not execution-specific data.
    """
    structural_data = []
    for r in records:
        structural_key = (
            r.get("Source_Entity", ""),
            r.get("Source_Datastore", ""),
            r.get("Source_Medallion_Layer", ""),
            r.get("Source_Workspace_Name", ""),
            r.get("Source_ABFSS_Path", ""),
            r.get("Source_Connection_ID", ""),
            r.get("Target_Entity", ""),
            r.get("Target_Datastore", ""),
            r.get("Target_Medallion_Layer", ""),
            r.get("Target_Workspace_Name", ""),
            r.get("Target_ABFSS_Path", ""),
            r.get("Relationship_Type", ""),
            r.get("Transformation_Applied", ""),
            r.get("Trigger_Name", ""),
            r.get("Source_Table_ID", ""),
            r.get("Target_Table_ID", ""),
            r.get("Lineage_Path", "")
        )
        structural_data.append(structural_key)
    
    structural_data.sort()
    fingerprint_str = str(structural_data)
    return hashlib.md5(fingerprint_str.encode()).hexdigest()


def compute_table_fingerprints(records: List[Dict]) -> Dict[int, str]:
    """
    Compute fingerprints for each target table to enable table-level change detection.
    
    Returns:
        Dict mapping Target_Table_ID to its lineage fingerprint
    """
    from collections import defaultdict
    
    records_by_table = defaultdict(list)
    for r in records:
        table_id = r.get("Target_Table_ID")
        if table_id is not None:
            records_by_table[table_id].append(r)
    
    table_fingerprints = {}
    for table_id, table_records in records_by_table.items():
        structural_data = []
        for r in table_records:
            structural_key = (
                r.get("Source_Entity", ""),
                r.get("Source_Datastore", ""),
                r.get("Source_Medallion_Layer", ""),
                r.get("Source_Workspace_Name", ""),
                r.get("Source_ABFSS_Path", ""),
                r.get("Source_Connection_ID", ""),
                r.get("Target_Entity", ""),
                r.get("Target_Datastore", ""),
                r.get("Target_Medallion_Layer", ""),
                r.get("Target_Workspace_Name", ""),
                r.get("Target_ABFSS_Path", ""),
                r.get("Relationship_Type", ""),
                r.get("Transformation_Applied", ""),
                r.get("Lineage_Path", "")
            )
            structural_data.append(structural_key)
        
        structural_data.sort()
        fingerprint_str = str(structural_data)
        table_fingerprints[table_id] = hashlib.md5(fingerprint_str.encode()).hexdigest()
    
    return table_fingerprints


def detect_table_level_changes(
    current_records: List[Dict],
    previous_records: List[Dict]
) -> Dict[str, List[int]]:
    """
    Detect which tables have changed, been added, or been removed.
    
    Returns:
        Dict with keys: 'changed', 'added', 'removed', 'unchanged'
        Each value is a list of Target_Table_IDs
    """
    current_fingerprints = compute_table_fingerprints(current_records)
    previous_fingerprints = compute_table_fingerprints(previous_records)
    
    current_tables = set(current_fingerprints.keys())
    previous_tables = set(previous_fingerprints.keys())
    
    added = list(current_tables - previous_tables)
    removed = list(previous_tables - current_tables)
    
    common_tables = current_tables & previous_tables
    changed = []
    unchanged = []
    
    for table_id in common_tables:
        if current_fingerprints[table_id] != previous_fingerprints[table_id]:
            changed.append(table_id)
        else:
            unchanged.append(table_id)
    
    return {
        'changed': sorted(changed),
        'added': sorted(added),
        'removed': sorted(removed),
        'unchanged': sorted(unchanged)
    }


def build_lineage_persistence_plan(
    current_records: List[Dict],
    previous_records: List[Dict],
    previous_version: int,
) -> Dict[str, Any]:
    """
    Compute versioning and append plan for lineage persistence.

    Args:
        current_records: Newly generated lineage records for current run.
        previous_records: Previous-version lineage records (dict format).
        previous_version: Latest persisted lineage version (0 means first run).

    Returns:
        Dict with keys:
            - lineage_changed (bool)
            - lineage_version (int)
            - table_changes (dict)
            - total_changes (int)
            - records_to_append (List[Dict])
    """
    if previous_version > 0:
        table_changes = detect_table_level_changes(current_records, previous_records or [])
        total_changes = len(table_changes['changed']) + len(table_changes['added']) + len(table_changes['removed'])

        if total_changes > 0:
            lineage_changed = True
            lineage_version = previous_version + 1
            changed_or_added_table_ids = set(table_changes['changed']) | set(table_changes['added'])
            records_to_append = [
                record for record in current_records
                if record.get("Target_Table_ID") in changed_or_added_table_ids
            ]
        else:
            lineage_changed = False
            lineage_version = previous_version
            records_to_append = []
    else:
        lineage_changed = True
        lineage_version = 1
        table_changes = {
            'changed': [],
            'added': [r.get('Target_Table_ID') for r in current_records],
            'removed': [],
            'unchanged': [],
        }
        total_changes = len(table_changes['added'])
        records_to_append = list(current_records)

    for record in records_to_append:
        record["Lineage_Version"] = lineage_version

    return {
        "lineage_changed": lineage_changed,
        "lineage_version": lineage_version,
        "table_changes": table_changes,
        "total_changes": total_changes,
        "records_to_append": records_to_append,
    }


def normalize_data_product_key(data_product_key: str) -> str:
    """Normalize a data product key for central observability filters."""
    return (data_product_key or "default").strip() or "default"


def data_product_key_filter(data_product_key: str):
    """Return a Spark filter expression that treats blank keys as default."""
    normalized_key = f.coalesce(
        f.when(
            f.col("Data_Product_Key").isNull() | (f.trim(f.col("Data_Product_Key")) == ""),
            f.lit(None),
        ).otherwise(f.trim(f.col("Data_Product_Key"))),
        f.lit("default"),
    )
    return normalized_key == normalize_data_product_key(data_product_key)


def is_expected_missing_lineage_table_error(err: Exception) -> bool:
    """Return True only for expected 'lineage table not found yet' conditions."""
    msg = str(err).lower()
    missing_markers = [
        "path does not exist",
        "not found",
        "cannot resolve",
        "does not exist",
        "no such file or directory",
    ]
    return any(marker in msg for marker in missing_markers)


def resolve_lineage_warehouse_context(
    data_product_key: str,
    metadata_warehouse_name: str,
    metadata_warehouse_id: str,
    metadata_warehouse_endpoint: str,
    metadata_workspace_id: str,
    metadata_lakehouse_id: str = "",
    metadata_lakehouse_workspace_id: str = "",
    central_metadata_warehouse_name: str = "",
    central_metadata_warehouse_id: str = "",
    central_metadata_warehouse_endpoint: str = "",
    central_metadata_workspace_id: str = "",
) -> Dict[str, str]:
    """Resolve product metadata and central metadata warehouse context for lineage."""
    normalized_key = normalize_data_product_key(data_product_key)
    resolved_central_metadata_warehouse_name = (central_metadata_warehouse_name or metadata_warehouse_name).strip() or metadata_warehouse_name
    resolved_central_metadata_warehouse_id = (central_metadata_warehouse_id or metadata_warehouse_id).strip() or metadata_warehouse_id
    resolved_central_metadata_endpoint = (central_metadata_warehouse_endpoint or metadata_warehouse_endpoint).strip() or metadata_warehouse_endpoint
    resolved_central_metadata_workspace_id = (central_metadata_workspace_id or metadata_workspace_id).strip() or metadata_workspace_id
    resolved_metadata_lakehouse_workspace_id = (metadata_lakehouse_workspace_id or metadata_workspace_id).strip() or metadata_workspace_id

    return {
        "data_product_key": normalized_key,
        "metadata_warehouse_name": metadata_warehouse_name,
        "metadata_warehouse_id": metadata_warehouse_id,
        "metadata_warehouse_endpoint": metadata_warehouse_endpoint,
        "metadata_workspace_id": metadata_workspace_id,
        "metadata_lakehouse_id": (metadata_lakehouse_id or "").strip(),
        "metadata_lakehouse_workspace_id": resolved_metadata_lakehouse_workspace_id,
        "central_metadata_warehouse_name": resolved_central_metadata_warehouse_name,
        "central_metadata_warehouse_id": resolved_central_metadata_warehouse_id,
        "central_metadata_warehouse_endpoint": resolved_central_metadata_endpoint,
        "central_metadata_workspace_id": resolved_central_metadata_workspace_id,
    }


def build_lineage_abfss_base(workspace_id: str, warehouse_id: str) -> str:
    """Build the Warehouse dbo table ABFSS base path used by Spark delta reads."""
    return f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{warehouse_id}/Tables/dbo"


def build_lakehouse_files_abfss_base(workspace_id: str, lakehouse_id: str, folder_path: str) -> str:
    """Build a Lakehouse Files ABFSS base path used by Spark/text reads."""
    normalized_folder = str(folder_path or "").strip("/")
    return f"abfss://{workspace_id}@onelake.dfs.fabric.microsoft.com/{lakehouse_id}/Files/{normalized_folder}"


def resolve_source_data_product_key(record, workspace_variables, workspace_to_product_key):
    """Best-effort attribution of a source artifact to its owning data product.

    Returns the product key when the source datastore's workspace maps to a known
    product context; otherwise None (external/shared sources stay unattributed).
    """
    if not workspace_to_product_key:
        return None
    source_datastore = str(record.get("Source_Datastore") or "").strip().lower()
    if not source_datastore:
        return None
    src_ws_id = workspace_variables.get(f"{source_datastore}_datastore_workspace_id")
    src_ws_name = workspace_variables.get(f"{source_datastore}_datastore_workspace_name")
    for candidate in (src_ws_id, src_ws_name):
        if candidate and str(candidate) in workspace_to_product_key:
            return workspace_to_product_key[str(candidate)]
    return None


def generate_lineage_for_all_products(
    spark,
    notebookutils,
    central_metadata_warehouse_name: str,
    central_metadata_warehouse_id: str,
    central_metadata_warehouse_endpoint: str,
    central_metadata_workspace_id: str = "",
    recency_days: int = 30,
) -> Dict[str, Any]:
    """Fan out lineage generation across all products observed at runtime.

    Enumerates the latest non-feature ``Trigger_Execution_Context`` row per
    ``Data_Product_Key`` within ``recency_days`` from the central warehouse, then
    generates lineage for each product with per-product isolation. Decommissioned
    products age out of the window naturally.
    """
    central_workspace = central_metadata_workspace_id or notebookutils.runtime.context.get("currentWorkspaceId")
    central_abfss = build_lineage_abfss_base(central_workspace, central_metadata_warehouse_id)
    spark.conf.set(
        f"spark.datawarehouse.{central_metadata_warehouse_name}.sqlendpoint",
        f"{central_metadata_warehouse_endpoint},1433",
    )
    print(f"\U0001F310 Enumerating product contexts from {central_metadata_warehouse_name}.Trigger_Execution_Context (last {recency_days}d, non-feature)...")
    try:
        tec_df = spark.read.format("delta").load(f"{central_abfss}/Trigger_Execution_Context")
    except Exception as err:
        if is_expected_missing_lineage_table_error(err):
            print("\u2139\uFE0F Trigger_Execution_Context not found \u2014 no runtime product contexts to enumerate. Nothing to do.")
            return {"data_product_key": "all", "products": [], "product_count": 0, "rows_to_append": 0}
        raise

    feature_flag = f.coalesce(f.col("Is_Feature_Workspace"), f.lit(False))
    filtered = tec_df.filter(
        (feature_flag == f.lit(False))
        & f.col("Data_Product_Key").isNotNull()
        & (f.trim(f.col("Data_Product_Key")) != "")
        & (f.col("Execution_Start_Time") >= f.date_sub(f.current_timestamp(), int(recency_days)))
    )
    window = Window.partitionBy("Data_Product_Key").orderBy(f.col("Execution_Start_Time").desc())
    latest = (
        filtered.withColumn("_rn", f.row_number().over(window))
        .filter(f.col("_rn") == 1)
        .drop("_rn")
        .collect()
    )

    workspace_to_product_key: Dict[str, str] = {}
    for ctx in latest:
        key = str(ctx["Data_Product_Key"]).strip()
        for ws_value in (ctx["Metadata_Workspace_ID"], ctx["Metadata_Workspace_Name"]):
            if ws_value:
                workspace_to_product_key.setdefault(str(ws_value), key)

    product_names = sorted({str(c["Data_Product_Key"]).strip() for c in latest})
    print(f"   Found {len(latest)} product context(s): {', '.join(product_names) or '(none)'}")

    products: List[Dict[str, Any]] = []
    total_rows = 0
    for ctx in latest:
        product_key = str(ctx["Data_Product_Key"]).strip()
        print(f"\n\u2500\u2500\u2500\u2500 Lineage for product '{product_key}' \u2500\u2500\u2500\u2500")
        try:
            result = generate_and_persist_lineage(
                spark=spark,
                notebookutils=notebookutils,
                data_product_key=product_key,
                metadata_warehouse_name="metadata_warehouse",
                metadata_warehouse_id=ctx["Metadata_Warehouse_ID"],
                metadata_warehouse_endpoint=ctx["Metadata_Warehouse_Endpoint"],
                metadata_workspace_id=ctx["Metadata_Workspace_ID"] or "",
                metadata_lakehouse_id=ctx["Metadata_Lakehouse_ID"] or "",
                metadata_lakehouse_workspace_id=ctx["Metadata_Workspace_ID"] or "",
                central_metadata_warehouse_name=central_metadata_warehouse_name,
                central_metadata_warehouse_id=central_metadata_warehouse_id,
                central_metadata_warehouse_endpoint=central_metadata_warehouse_endpoint,
                central_metadata_workspace_id=central_metadata_workspace_id,
                exit_on_no_logs=False,
                workspace_to_product_key=workspace_to_product_key,
            )
            total_rows += int(result.get("rows_to_append") or 0)
            products.append(result)
        except Exception as err:  # noqa: BLE001 \u2014 per-product isolation
            print(f"\u26A0\uFE0F Lineage generation failed for product '{product_key}': {err}")
            products.append({"data_product_key": product_key, "error": str(err), "rows_to_append": 0})

    print(f"\n\u2705 Cross-product lineage complete: {len(products)} product(s), {total_rows} total rows appended")
    return {
        "data_product_key": "all",
        "product_count": len(products),
        "products": products,
        "rows_to_append": total_rows,
    }


def generate_and_persist_lineage(
    spark,
    notebookutils,
    data_product_key: str,
    metadata_warehouse_name: str,
    metadata_warehouse_id: str,
    metadata_warehouse_endpoint: str,
    metadata_workspace_id: str = "",
    metadata_lakehouse_id: str = "",
    metadata_lakehouse_workspace_id: str = "",
    central_metadata_warehouse_name: str = "",
    central_metadata_warehouse_id: str = "",
    central_metadata_warehouse_endpoint: str = "",
    central_metadata_workspace_id: str = "",
    exit_on_no_logs: bool = True,
    workspace_to_product_key: Optional[Dict[str, str]] = None,
    recency_days: int = 30,
) -> Dict[str, Any]:
    """Generate lineage for one data product context, or for all products.

    Product metadata is read from the supplied metadata warehouse/lakehouse context,
    while logs and Data_Pipeline_Lineage remain in the central metadata warehouse.

    When ``data_product_key`` is ``"all"``, the central ``Trigger_Execution_Context``
    table is enumerated for the latest non-feature context per product (within
    ``recency_days``) and lineage is generated for each, with per-product isolation.
    """
    clear_custom_function_parse_cache()

    if str(data_product_key or "").strip().lower() in {"all", "*", "__all__"}:
        return generate_lineage_for_all_products(
            spark=spark,
            notebookutils=notebookutils,
            central_metadata_warehouse_name=(central_metadata_warehouse_name or metadata_warehouse_name),
            central_metadata_warehouse_id=(central_metadata_warehouse_id or metadata_warehouse_id),
            central_metadata_warehouse_endpoint=(central_metadata_warehouse_endpoint or metadata_warehouse_endpoint),
            central_metadata_workspace_id=(central_metadata_workspace_id or metadata_workspace_id),
            recency_days=recency_days,
        )
    context = resolve_lineage_warehouse_context(
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

    normalized_key = context["data_product_key"]
    print(f"📖 Reading product metadata from {context['metadata_warehouse_name']} for data_product_key={normalized_key}...")

    spark.conf.set(
        f"spark.datawarehouse.{context['metadata_warehouse_name']}.sqlendpoint",
        f"{context['metadata_warehouse_endpoint']},1433",
    )
    if (
        context["central_metadata_warehouse_name"] != context["metadata_warehouse_name"]
        or context["central_metadata_warehouse_endpoint"] != context["metadata_warehouse_endpoint"]
    ):
        spark.conf.set(
            f"spark.datawarehouse.{context['central_metadata_warehouse_name']}.sqlendpoint",
            f"{context['central_metadata_warehouse_endpoint']},1433",
        )

    metadata_workspace = context["metadata_workspace_id"] or notebookutils.runtime.context.get("currentWorkspaceId")
    central_metadata_workspace = context["central_metadata_workspace_id"] or notebookutils.runtime.context.get("currentWorkspaceId")
    metadata_abfss_base = build_lineage_abfss_base(metadata_workspace, context["metadata_warehouse_id"])
    central_metadata_abfss_base = build_lineage_abfss_base(central_metadata_workspace, context["central_metadata_warehouse_id"])
    print(f"   Metadata ABFSS base: {metadata_abfss_base}")
    print(f"   Central metadata ABFSS base: {central_metadata_abfss_base}")

    orchestration_df = spark.read.format("delta").load(f"{metadata_abfss_base}/Data_Pipeline_Metadata_Orchestration")
    primary_config_df = spark.read.format("delta").load(f"{metadata_abfss_base}/Data_Pipeline_Metadata_Primary_Configuration")
    advanced_config_df = spark.read.format("delta").load(f"{metadata_abfss_base}/Data_Pipeline_Metadata_Advanced_Configuration")

    try:
        existing_lineage_df = spark.read.format("delta").load(f"{central_metadata_abfss_base}/Data_Pipeline_Lineage")
        existing_lineage_df = existing_lineage_df.filter(data_product_key_filter(normalized_key))
        last_lineage_time = existing_lineage_df.selectExpr("MAX(Lineage_Generated_At) as last_run").collect()[0].last_run
        print(f"   Last lineage run: {last_lineage_time}")
    except Exception as err:
        if is_expected_missing_lineage_table_error(err):
            existing_lineage_df = None
            last_lineage_time = None
            print("   No existing lineage table found — first run")
        else:
            raise RuntimeError(
                f"Unexpected error while reading lineage table for last run timestamp at "
                f"{central_metadata_abfss_base}/Data_Pipeline_Lineage."
            ) from err

    logs_df = spark.read.format("delta").load(f"{central_metadata_abfss_base}/Data_Pipeline_Logs")
    logs_df = logs_df.filter(f.col("Ingestion_Status") == "Processed")
    logs_df = logs_df.filter(data_product_key_filter(normalized_key))
    if last_lineage_time:
        logs_df = logs_df.filter(f.col("Ingestion_Start_Time") >= last_lineage_time)
        print(f"   Filtered logs to records since {last_lineage_time}")

    log_count = logs_df.count()
    if log_count == 0:
        print("ℹ️ No new processed log records since last lineage run — nothing to process. Exiting notebook.")
        if exit_on_no_logs:
            notebookutils.notebook.exit("No new logs")
        return {
            "data_product_key": normalized_key,
            "previous_version": 0,
            "lineage_version": 0,
            "rows_to_append": 0,
            "lineage_changed": False,
            "table_changes": {"changed": [], "added": [], "removed": [], "unchanged": []},
        }

    print("📖 Loading datastore configuration...")
    datastore_config_df = spark.read.format("delta").load(f"{metadata_abfss_base}/Datastore_Configuration")
    datastore_rows = []
    if central_metadata_abfss_base != metadata_abfss_base:
        datastore_rows.extend(spark.read.format("delta").load(f"{central_metadata_abfss_base}/Datastore_Configuration").collect())
    datastore_rows.extend(datastore_config_df.collect())

    workspace_variables = {}
    for row in datastore_rows:
        ds_name = row.Datastore_Name.strip().lower()
        workspace_variables[f"{ds_name}_datastore_id"] = row.Datastore_ID
        workspace_variables[f"{ds_name}_datastore_workspace_id"] = row.Workspace_ID
        workspace_variables[f"{ds_name}_datastore_workspace_name"] = row.Workspace_Name
        if row.Endpoint:
            workspace_variables[f"{ds_name}_datastore_endpoint"] = row.Endpoint
        if row.Connection_ID:
            workspace_variables[f"{ds_name}_datastore_connection_id"] = row.Connection_ID

    print(f"   - Loaded {len(datastore_rows)} datastores from Datastore_Configuration table")
    print(f"   - Orchestration records: {orchestration_df.count()}")
    print(f"   - Primary config records: {primary_config_df.count()}")
    print(f"   - Advanced config records: {advanced_config_df.count()}")
    print(f"   - Log records: {log_count}")

    print("🔨 Building full lineage graph...")
    orch_records, primary_source_by_table, advanced_transform_by_table = prepare_graph_inputs(
        orchestration_df,
        primary_config_df,
        advanced_config_df,
        table_ids=None,
    )
    custom_function_runtime_state = None
    if context.get("metadata_lakehouse_id"):
        custom_function_runtime_state = _set_lineage_custom_function_file_runtime(
            context.get("metadata_lakehouse_workspace_id") or metadata_workspace,
            context["metadata_lakehouse_id"],
        )
    try:
        lineage_graph = build_lineage_graph(orch_records, primary_source_by_table, advanced_transform_by_table)
    finally:
        if custom_function_runtime_state is not None:
            _restore_custom_function_runtime(custom_function_runtime_state)
    lineage_groups = compute_lineage_groups(lineage_graph)

    tables_with_logs, source_type_lookup, target_type_lookup = prepare_log_lookups(logs_df)
    orch_lookup = {row.Table_ID: row for row in orchestration_df.collect()}
    lineage_records = generate_lineage_records(
        lineage_graph,
        tables_with_logs,
        source_type_lookup,
        target_type_lookup,
        orch_lookup,
        primary_source_by_table,
        workspace_variables,
        lineage_groups=lineage_groups,
        lineage_version=0,
    )
    print(f"📊 Generated {len(lineage_records)} lineage records")

    product_workspace_to_key = dict(workspace_to_product_key or {})
    for ws_value in (context.get("metadata_workspace_id"), metadata_workspace):
        if ws_value:
            product_workspace_to_key.setdefault(str(ws_value), normalized_key)
    for ds_key, ds_value in workspace_variables.items():
        if ds_value and (ds_key.endswith("_datastore_workspace_id") or ds_key.endswith("_datastore_workspace_name")):
            product_workspace_to_key.setdefault(str(ds_value), normalized_key)

    for record in lineage_records:
        record["Data_Product_Key"] = normalized_key
        record["Target_Data_Product_Key"] = normalized_key
        record["Source_Data_Product_Key"] = resolve_source_data_product_key(
            record, workspace_variables, product_workspace_to_key
        )
        record["Lineage_Summary"] = generate_lineage_summary(record)

    target_narrative_cache = {
        target: generate_target_lineage_narrative(lineage_records, target)
        for target in set(r.get("Target_Entity") for r in lineage_records)
    }
    source_narrative_cache = {
        source: generate_source_narrative(source, lineage_records, target_narrative_cache)
        for source in set(r.get("Source_Entity") for r in lineage_records)
    }
    group_narrative_cache = {}
    for group_id in set(r.get("Lineage_Group_ID") for r in lineage_records if r.get("Lineage_Group_ID")):
        sample_group = next((g for g in lineage_groups.values() if g["group_id"] == group_id), None)
        if sample_group:
            group_narrative_cache[group_id] = generate_lineage_group_narrative(sample_group, lineage_graph, lineage_records)

    for record in lineage_records:
        record["Target_Lineage_Narrative"] = target_narrative_cache.get(record.get("Target_Entity"), "")
        record["Source_Lineage_Narrative"] = source_narrative_cache.get(record.get("Source_Entity"), "")
        record["Lineage_Group_Narrative"] = group_narrative_cache.get(record.get("Lineage_Group_ID"), "")

    if existing_lineage_df is None:
        try:
            existing_lineage_df = spark.read.format("delta").load(f"{central_metadata_abfss_base}/Data_Pipeline_Lineage")
            existing_lineage_df = existing_lineage_df.filter(data_product_key_filter(normalized_key))
        except Exception as err:
            if is_expected_missing_lineage_table_error(err):
                existing_lineage_df = None
            else:
                raise RuntimeError(
                    f"Unexpected error while re-reading lineage table for version comparison at "
                    f"{central_metadata_abfss_base}/Data_Pipeline_Lineage."
                ) from err

    if existing_lineage_df is not None:
        max_version_row = existing_lineage_df.selectExpr("MAX(Lineage_Version) as max_version").collect()[0]
        previous_version = max_version_row.max_version or 0
    else:
        previous_version = 0

    previous_records_dict = []
    if previous_version > 0:
        previous_records = existing_lineage_df.filter(existing_lineage_df.Lineage_Version == previous_version).collect()
        previous_records_dict = [row.asDict() for row in previous_records]

    persistence_plan = build_lineage_persistence_plan(lineage_records, previous_records_dict, previous_version)
    records_to_append = persistence_plan["records_to_append"]
    validate_lineage_record_ids(records_to_append)

    print("\n📌 Lineage Change Summary:")
    print(f"   - Previous version: {previous_version}")
    print(f"   - Current version: {persistence_plan['lineage_version']}")
    print(f"   - Rows to append: {len(records_to_append)}")

    lineage_schema = StructType([
        StructField("Lineage_ID", StringType(), False),
        StructField("Data_Product_Key", StringType(), True),
        StructField("Source_Data_Product_Key", StringType(), True),
        StructField("Target_Data_Product_Key", StringType(), True),
        StructField("Trigger_Name", StringType(), True),
        StructField("Source_Table_ID", IntegerType(), True),
        StructField("Target_Table_ID", IntegerType(), True),
        StructField("Source_Entity", StringType(), True),
        StructField("Source_Datastore", StringType(), True),
        StructField("Source_Medallion_Layer", StringType(), True),
        StructField("Source_Type", StringType(), True),
        StructField("Source_Workspace_Name", StringType(), True),
        StructField("Source_ABFSS_Path", StringType(), True),
        StructField("Source_Connection_ID", StringType(), True),
        StructField("Target_Entity", StringType(), True),
        StructField("Target_Datastore", StringType(), True),
        StructField("Target_Medallion_Layer", StringType(), True),
        StructField("Target_Type", StringType(), True),
        StructField("Target_Workspace_Name", StringType(), True),
        StructField("Target_ABFSS_Path", StringType(), True),
        StructField("Relationship_Type", StringType(), True),
        StructField("Transformation_Applied", StringType(), True),
        StructField("Lineage_Depth", IntegerType(), True),
        StructField("Lineage_Path", StringType(), True),
        StructField("Is_Cross_Trigger", BooleanType(), True),
        StructField("Cross_Trigger_Dependencies", StringType(), True),
        StructField("Lineage_Group_ID", StringType(), True),
        StructField("Lineage_Group_Table_Count", IntegerType(), True),
        StructField("Lineage_Group_Narrative", StringType(), True),
        StructField("Lineage_Summary", StringType(), True),
        StructField("Source_Lineage_Narrative", StringType(), True),
        StructField("Target_Lineage_Narrative", StringType(), True),
        StructField("Lineage_Generated_At", TimestampType(), True),
        StructField("Lineage_Version", IntegerType(), True),
        StructField("Date_Key", IntegerType(), True),
    ])

    rows_to_append = len(records_to_append)
    if rows_to_append > 0:
        print("💾 Persisting lineage records to Data_Pipeline_Lineage table...")
        spark.createDataFrame(records_to_append, lineage_schema).write.mode("append").synapsesql(
            f"{context['central_metadata_warehouse_name']}.dbo.Data_Pipeline_Lineage"
        )
        print(f"✅ Persisted {rows_to_append} lineage records (Version {persistence_plan['lineage_version']})")
    elif persistence_plan["lineage_changed"] and persistence_plan["table_changes"].get("removed"):
        print("ℹ️ Lineage changes detected only as removed tables; no new/changed table records to append.")
    else:
        print("ℹ️ Skipping persistence (no changed/added records to append)")

    return {
        "data_product_key": normalized_key,
        "previous_version": previous_version,
        "lineage_version": persistence_plan["lineage_version"],
        "rows_to_append": rows_to_append,
        "lineage_changed": persistence_plan["lineage_changed"],
        "table_changes": persistence_plan["table_changes"],
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ===========================================================================================
# LINEAGE GROUPING (WEAKLY CONNECTED COMPONENTS)
# ===========================================================================================

def compute_lineage_groups(G: LineageGraph) -> Dict[str, Dict]:
    """
    Compute lineage groups using weakly connected components of the graph.
    
    Each group contains all tables that are transitively related — if data flows
    from A → B → C, and D joins into C, then A, B, C, D are all in the same group.
    
    This lets you answer: "What's the full scope of tables related to this one?"
    
    Returns:
        Dict mapping node_id to group info:
        {
            "group_id": "LG_0001",
            "group_size": 12,
            "group_table_count": 10,
            "group_roots": ["oracle.SALES.ORDERS", "oracle.CUSTOMERS"],
            "group_leaves": ["gold.dbo.fact_sales", "gold.dbo.dim_customer"]
        }
    """
    groups = {}

    def _is_table_node(node_data: Dict) -> bool:
        entity_name = str(node_data.get("entity_name") or "").strip()
        if not entity_name:
            return False

        entity_lower = entity_name.lower()
        if entity_lower.startswith("sftp:"):
            return False
        if entity_lower.startswith("files/") or "/" in entity_name:
            return False

        return "." in entity_name
    
    for i, component in enumerate(sorted(lineage_weakly_connected_components(G), key=len, reverse=True), 1):
        subgraph = G.subgraph(component)
        
        roots = sorted([n for n in subgraph.nodes() if subgraph.in_degree(n) == 0])
        leaves = sorted([n for n in subgraph.nodes() if subgraph.out_degree(n) == 0])
        group_table_count = sum(1 for node_id in component if _is_table_node(subgraph.nodes[node_id]))
        
        group_id = f"LG_{i:04d}"
        
        for node_id in component:
            groups[node_id] = {
                "group_id": group_id,
                "group_size": len(component),
                "group_table_count": group_table_count,
                "group_roots": roots,
                "group_leaves": leaves
            }
    
    return groups


def generate_lineage_group_narrative(
    group_info: Dict,
    G: LineageGraph,
    records: List[Dict]
) -> str:
    """
    Generate a comprehensive narrative describing an entire lineage group.
    
    This gives the "full scope" view: all sources, intermediate tables, targets,
    and how they connect — answering "what's the complete data flow for this pipeline?"
    
    Args:
        group_info: Dict with group_id, group_roots, group_leaves, group_size
        G: The full lineage graph
        records: All lineage records (to extract trigger and transformation info)
    
    Returns:
        Human-readable narrative describing the full group
    """
    group_id = group_info["group_id"]
    roots = group_info["group_roots"]
    leaves = group_info["group_leaves"]
    size = group_info["group_size"]
    
    parts = []
    
    # Opening: What is this group?
    parts.append(f"Lineage group {group_id} contains {size} connected entities.")
    
    # Describe the origin(s)
    if roots:
        external_roots = []
        internal_roots = []
        for r in roots:
            node_data = G.nodes.get(r, {})
            if node_data.get("medallion_layer") == "External":
                external_roots.append(r)
            else:
                internal_roots.append(r)
        
        if external_roots:
            parts.append(f"Data originates from external source(s): {', '.join(external_roots)}.")
        if internal_roots:
            parts.append(f"Internal root source(s): {', '.join(internal_roots)}.")
    
    # Describe the final outputs
    if leaves:
        leaf_layers = set()
        for leaf in leaves:
            node_data = G.nodes.get(leaf, {})
            leaf_layers.add(node_data.get("medallion_layer", "Unknown"))
        layers_str = "/".join(sorted(leaf_layers))
        parts.append(f"Final output(s) ({layers_str} layer): {', '.join(leaves)}.")
    
    # Count layers from the actual component
    reachable = set()
    for root in roots:
        if root in G:
            reachable.update(lineage_descendants(G, root))
            reachable.add(root)
    for leaf in leaves:
        if leaf in G:
            reachable.update(lineage_ancestors(G, leaf))
            reachable.add(leaf)
    
    layer_counts = {"External": 0, "Bronze": 0, "Silver": 0, "Gold": 0, "Unknown": 0}
    for node_id in reachable:
        layer = G.nodes[node_id].get("medallion_layer", "Unknown")
        layer_counts[layer] = layer_counts.get(layer, 0) + 1
    
    flow_parts = []
    for layer in ["External", "Bronze", "Silver", "Gold"]:
        if layer_counts.get(layer, 0) > 0:
            flow_parts.append(f"{layer_counts[layer]} {layer}")
    if flow_parts:
        parts.append(f"Flow: {' → '.join(flow_parts)} entities.")
    
    # Identify triggers involved
    # Prefer precise matching by datastore.entity where possible; fall back to entity-only.
    group_records = []
    for r in records:
        matched = False
        for n in reachable:
            if n not in G:
                continue
            node_data = G.nodes[n]
            node_entity = node_data.get("entity_name")
            node_datastore = node_data.get("datastore")
            full_node_entity = f"{node_datastore}.{node_entity}" if node_datastore and node_entity else None

            source_entity = r.get("Source_Entity")
            source_datastore = r.get("Source_Datastore")
            target_entity = r.get("Target_Entity")
            target_datastore = r.get("Target_Datastore")

            full_source = f"{source_datastore}.{source_entity}" if source_datastore and source_entity else None
            full_target = f"{target_datastore}.{target_entity}" if target_datastore and target_entity else None

            if full_node_entity and (full_source == full_node_entity or full_target == full_node_entity):
                matched = True
                break
            if source_entity == node_entity or target_entity == node_entity:
                matched = True
                break

        if matched:
            group_records.append(r)
    triggers = set(r.get("Trigger_Name") for r in group_records if r.get("Trigger_Name"))
    if triggers:
        parts.append(f"Trigger(s): {', '.join(sorted(triggers))}.")
    
    # Show the longest path (critical path)
    if roots and leaves and reachable:
        component_graph = G.subgraph(reachable).copy()
        longest_path = []

        if lineage_is_directed_acyclic_graph(component_graph):
            longest_path = lineage_dag_longest_path(component_graph)
        else:
            # Fallback for non-DAG graphs: approximate via longest among root->leaf shortest paths
            for root in roots:
                for leaf in leaves:
                    try:
                        path = lineage_shortest_path(component_graph, root, leaf)
                        if len(path) > len(longest_path):
                            longest_path = path
                    except LineageNoPathError:
                        continue

        if longest_path:
            parts.append(f"Longest data path ({len(longest_path)-1} hops): {' → '.join(longest_path)}.")
    
    return " ".join(parts)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("✅ NB_Helper_Functions_Lineage loaded successfully")
print("   Classes: LineageNode")
print("   Graph: build_lineage_graph, prepare_graph_inputs, compute_lineage_groups")
print("   Records: generate_lineage_records, prepare_log_lookups")
print("   Narratives: generate_lineage_summary, generate_target_lineage_narrative, generate_source_narrative, generate_lineage_group_narrative")
print("   Change Detection: compute_lineage_fingerprint, compute_table_fingerprints, detect_table_level_changes")
print("   Parsing: extract_tables_from_notebook, extract_tables_from_sql, extract_table_from_abfss_path")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
