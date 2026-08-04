"""
Assembles a knowledge graph by parsing the NCI Proteomic Data Commons (PDC) Portal
https://pdc.cancer.gov/pdc/
"""

from dglink import NodeSet, EdgeSet, write_graph
from dglink.portals.nci.pdc import NciProteomicCommonsClient
from dglink.portals.nci.pdc.constants import NODE_ATTRIBUTES, EDGE_ATTRIBUTES
from dglink.portals.nci.pdc.utils import (
    get_metadata_graph,
    download_tabular_files,
    get_tabular_iterator,
)
from dglink.core.tabular_data import get_tabular_data

RESOURCE_PATH = "nci_pdc_graph"


## determines if want to keep biological samples, which creates a large number of nodes ##
INCLUDE_BIOSPECIMEN = 0
EX_STUDIES = [
    "b91a12b7-f3a0-11ea-b1fd-0aad30af8a83",
    "57b7e39e-d0bd-4aa9-8d46-62a854905797",
    "7b6e6ed7-1401-48c8-a43f-9e65fd2a5bb3",
]


if __name__ == "__main__":
    node_set = NodeSet(attributes=NODE_ATTRIBUTES)
    edge_set = EdgeSet(attributes=EDGE_ATTRIBUTES)
    client = NciProteomicCommonsClient()

    ## structural / metadata subgraph
    ## (program -> project -> study -> case -> sample -> aliquot, plus disease nodes)
    get_metadata_graph(
        client,
        node_set=node_set,
        edge_set=edge_set,
        include_biospecimen=INCLUDE_BIOSPECIMEN,
    )
    # ## download open-access tabular files (skips already-cached files)
    download_tabular_files(client=client, study_list=EX_STUDIES)

    # ## ground the tabular data and attach the extracted entities to their study nodes
    study_ids, tabular_iterator = get_tabular_iterator(study_list=EX_STUDIES)
    get_tabular_data(
        group_identifiers=study_ids,
        node_set=node_set,
        edge_set=edge_set,
        tabular_iterator=tabular_iterator,
        quality_check_method="heuristic",
    )

    write_graph(node_set=node_set, edge_set=edge_set, resource_path=RESOURCE_PATH)
