"""
Assembles a knowledge graph by parsing the NCI genomics Data Commons (GDC) Portal https://portal.gdc.cancer.gov/
"""

from dglink import NodeSet, EdgeSet, write_graph
from dglink.portals.nci.gdc import (
    NODE_ATTRIBUTES,
    EDGE_ATTRIBUTES,
    get_case_hierarchy,
    download_tabular_files,
)
from dglink.portals.nci.gdc.utils import get_tabular_iterator, MANIFEST_PATH
from dglink.core.tabular_data import get_tabular_data

import polars as pl

batch_size = 500  ## number of cases to process at one

RESOURCE_PATH = "nci_gdc_graph"  ## where the assembled GC graph is written


N_CASES = 150
SEED = 10082

## determines if want to keep biological samples, which creates a large number of nodes ##
INCLUDE_BIOSPECIMEN = 0
## set case list if want specific cases ##
case_ids = None

from dglink import load_graph

node_set, edge_set = load_graph(resource_path=RESOURCE_PATH)
if __name__ == "__main__":
    # set up a new node and edge set
    node_set = NodeSet(attributes=NODE_ATTRIBUTES)
    edge_set = EdgeSet(attributes=EDGE_ATTRIBUTES)
    ## load all meta data ##
    get_case_hierarchy(
        node_set=node_set,
        edge_set=edge_set,
        batch_length=batch_size,
        include_biospecimen=INCLUDE_BIOSPECIMEN,
    )
    # ## sample some number of cases from those that are open with tabular data at random if do not chose a list ##
    if not case_ids:
        files_df = pl.read_csv(MANIFEST_PATH, separator="\t")
        case_ids = (
            files_df.filter(
                pl.col("file_access").eq("open") & pl.col("file_data_format").eq("TSV")
            )
            .group_by("case_id")
            .first()
            .sample(N_CASES, seed=SEED)["case_id"]
            .to_list()
        )
    download_tabular_files(case_list=case_ids, verbose=True)
    case_to_files = get_tabular_iterator(case_list=case_ids)
    reports = get_tabular_data(
        group_identifiers=case_ids,
        node_set=node_set,
        edge_set=edge_set,
        tabular_iterator=case_to_files,
        quality_check_method="heuristic",
    )
    write_graph(node_set=node_set, edge_set=edge_set, resource_path=RESOURCE_PATH)
