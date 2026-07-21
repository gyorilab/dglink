"""
Assembles a knowledge graph by parsing the NCI General Commons (GC) Portal
https://datacommons.cancer.gov/repository/general-commons
"""

from dglink import NodeSet, EdgeSet, write_graph
from dglink.portals.nci.gc import NciGeneralCommonsClient
from dglink.portals.nci.gc.constants import NODE_ATTRIBUTES, EDGE_ATTRIBUTES
from dglink.portals.nci.gc.utils import (
    get_metadata_graph,
    download_tabular_files,
    get_tabular_iterator,
)
from dglink.core.tabular_data import get_tabular_data

RESOURCE_PATH = "nci_gc_graph"  ## where the assembled GC graph is written

if __name__ == "__main__":
    node_set = NodeSet(attributes=NODE_ATTRIBUTES)
    edge_set = EdgeSet(attributes=EDGE_ATTRIBUTES)
    client = NciGeneralCommonsClient(
        gen3_credential_file="nci_general_commons_credentials"
    )

    ## structural / metadata subgraph (programs, publications, investigators, diagnoses)
    get_metadata_graph(client, node_set=node_set, edge_set=edge_set)

    ## download open-access tabular files (skips already-cached files)
    download_tabular_files(client)

    ## ground the tabular data and attach entities to their study nodes
    study_ids, tabular_iterator = get_tabular_iterator()
    get_tabular_data(
        group_identifiers=study_ids,
        node_set=node_set,
        edge_set=edge_set,
        tabular_iterator=tabular_iterator,
        quality_check_method="heuristic",
    )

    write_graph(node_set=node_set, edge_set=edge_set, resource_path=RESOURCE_PATH)
