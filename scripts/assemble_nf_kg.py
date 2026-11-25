"""
Assemble the NF Data Portal knowledge graph.

This is a complete restructure of the assemble NF KG method.
"""

from dglink import NodeSet, EdgeSet, get_projects, write_graph, load_graph
from dglink.core.tabular_data import get_tabular_data
from dglink.portals.nf_data_portal import get_tabular_iterator, get_wikis, get_publications, get_tools, get_meta, get_fastq_metadata
from dglink.portals.nf_data_portal.constants import NODE_ATTRIBUTES, EDGE_ATTRIBUTES, NF_STUDIES_BASE_URL, WIKI_FIELDS, NF_LABEL_TO_BIOLINK
from dglink.portals.nf_data_portal.utils import get_all_nf_studies
from dglink.core.timing import TIMER
from dglink.core.constants import REPORT_PATH

import os

import logging
logger = logging.getLogger(__name__)

QC_METHOD = 'hierarchal_selector' 
# LLM used by the column selector (passed through get_tabular_data to the selector)
LLM_PROVIDER = "anthropic"
LLM_MODEL = "global.anthropic.claude-haiku-5-5"
# QC_METHOD = None

# RESUME_FROM_TABULAR: load the graph checkpointed after the tabular stage
# (dglink/resources/graph/{nodes,edges}.tsv) and skip the projects + tabular stages,
# so the LLM column selection is not redone.
RESUME_FROM_TABULAR = True
# RUN_FASTQ: FASTQ metadata stage is muted for now (the 2026-11-13 full run died here or in wikis).
RUN_FASTQ = False

if __name__ == "__main__":
    if RESUME_FROM_TABULAR:
        # Load the tabular checkpoint
        with TIMER.stage("load_checkpoint"):
            node_set, edge_set = load_graph(
                node_attributes=NODE_ATTRIBUTES,
                edge_attributes=EDGE_ATTRIBUTES,
                resource_path="dglink/resources/graph/",
                node_name="nodes.tsv",
                edge_name="edges.tsv",
            )
        print(f"resumed from checkpoint: {len(node_set)} nodes, {len(edge_set)} edges")
    else:
        # Make a new graph
        node_set = NodeSet(attributes=NODE_ATTRIBUTES)
        edge_set = EdgeSet(attributes=EDGE_ATTRIBUTES)
    # Get a full list of studies
    print("getting studies and all that...")
    with TIMER.stage("api.list_studies"):
        project_ids = get_all_nf_studies()
    # project_ids = [
    #     'syn11374354', 
    #     # 'syn25881328',
    # # "syn2343195",  # Large project
    # # "syn5562324",  # Small project
    # # "syn27761862",  # Small project
    # # "syn4939874",  # Large project
    # # "syn4939876",  # Locked
    # # "syn4939906",  # Small
    # # "syn4939916",  # Locked
    # # "syn7217928",  # Large
    # # "syn8016635",  # Small
    # # "syn11638893",  # Locked
    # # "syn11817821",  # Large
    # # "syn21641813",  # Locked
    # # "syn21642027",  # Locked
    # # "syn21650493",  # Large
    # # "syn21984813",  # Large
    # # "syn23639889",  # Locked
    # # "syn51133914",  # Locked
    # # "syn52740594",  # Large
    # ]
    if not RESUME_FROM_TABULAR:
        # Add projects to KG as nodes
        with TIMER.stage("projects"):
            node_set, edge_set = get_projects(
                project_ids=project_ids,
                node_set=node_set,
                edge_set=edge_set,
                studies_base_url=NF_STUDIES_BASE_URL,
                write_set=True,
                label=NF_LABEL_TO_BIOLINK["Project"],
                raw_label="Project",
            )

        # Process the tabular data
        # File listing + download happen eagerly in get_tabular_iterator (timed as
        # "crawl_file_listing" and "download"); "tabular" is processing only.
        with TIMER.stage("tabular.fetch_files"):
            tabular_iterator = get_tabular_iterator(project_list=project_ids)
        with TIMER.stage("tabular"):
            reports = get_tabular_data(
                group_identifiers=project_ids,
                node_set=node_set,
                edge_set = edge_set,
                tabular_iterator=tabular_iterator,
                quality_check_method=QC_METHOD,
                provider=LLM_PROVIDER,
                model=LLM_MODEL,
            )
    # Add metadata from the FastQ sequencing files
    if RUN_FASTQ:
        with TIMER.stage("fastq_metadata"):
            node_set, edge_set = get_fastq_metadata(project_list=project_ids, node_set=node_set, edge_set=edge_set, write_set=True)
    # Load in wikis
    with TIMER.stage("wikis"):
        node_set, edge_set = get_wikis(
            node_set=node_set,
            edge_set=edge_set,
            project_ids=project_ids,
            write_set=True,
        )
    # Load in publications
    with TIMER.stage("publications"):
        node_set, edge_set = get_publications(
            node_set=node_set, edge_set=edge_set, project_ids=project_ids, write_set=True
        )
    # Load in tools
    with TIMER.stage("tools"):
        node_set, edge_set = get_tools(
            node_set=node_set, edge_set=edge_set, project_ids=project_ids, write_set=True
        )
    # Load in metadata
    with TIMER.stage("metadata"):
        node_set, edge_set = get_meta(
            project_ids=project_ids,
            node_set=node_set,
            edge_set=edge_set,
            write_set=True,
        )
    # Write the graph
    with TIMER.stage("write_graph"):
        write_graph(node_set=node_set, edge_set=edge_set)
    TIMER.print_summary()
    # A resumed run has no tabular timings, so don't let it pass as the full-run report
    timing_name = "timing_nf_resume.json" if RESUME_FROM_TABULAR else "timing_nf.json"
    TIMER.write_report(
        os.path.join(REPORT_PATH, timing_name),
        n_projects=len(project_ids),
        n_nodes=len(node_set),
        n_edges=len(edge_set),
        qc_method=QC_METHOD,
        resumed_from_tabular=RESUME_FROM_TABULAR,
        ran_fastq=RUN_FASTQ,
    )
