"""
Extract VCF data from the NF Data Portal's Synapse projects.

Finds the VCF files of each project, downloads them from Synapse and reads
them with dglink.core.vcf_data, taking each file's study from its Synapse
annotations.
"""

from dglink.core.constants import VCF_FILE_TYPES, RESOURCE_PATH, REPORT_PATH
from dglink.core.nodes import NodeSet
from dglink.core.edges import EdgeSet
from dglink.core.utils import write_graph
from dglink.core.vcf_data import extract_variants, extract_vcf_metadata
from .constants import syn
from .utils import get_project_files
import os
import logging
import tqdm
import polars as pl

logger = logging.getLogger(__name__)


def parse_vcf_file(
    file_id: str,
    node_set: NodeSet,
    edge_set: EdgeSet,
    project_id: str,
    process_variants: bool = True,
) -> tuple[NodeSet, EdgeSet, dict]:
    """Parse a single VCF file and extract its information into the graph.

    Downloads the VCF file from Synapse and extracts both variants and
    metadata.

    Parameters
    ----------
    file_id :
        Synapse file ID (e.g., 'syn12345678').
    node_set :
        Existing set of nodes to update.
    edge_set :
        Existing set of edges to update.
    project_id :
        Synapse project ID containing the file.
    process_variants :
        If True, extract variant information. If False, only extract metadata.

    Returns
    -------
    node_set : NodeSet
        The updated node set.
    edge_set : EdgeSet
        The updated edge set.
    status : dict
        Status dict containing project_id, file_id, and the able_to_process
        flag.
    """
    able_to_process = True
    try:
        obj = syn.get(file_id)
        file_path = obj.path
    except:
        file_path = None
    if file_path is None:
        able_to_process = False
    else:
        # The Synapse id and study annotation of the downloaded file
        synapse_id = obj.get("id", "")
        study_id = obj.get("studyId", ["study_id_missing"])[0]
        # Extract the variants
        if process_variants:
            node_set, edge_set, able_to_process = extract_variants(
                path=file_path,
                file_id=synapse_id,
                node_set=node_set,
                edge_set=edge_set,
            )
        # Extract the meta data
        node_set, edge_set = extract_vcf_metadata(
            path=file_path,
            file_id=synapse_id,
            study_id=study_id,
            node_set=node_set,
            edge_set=edge_set,
        )
    return (
        node_set,
        edge_set,
        {
            "project_id": project_id,
            "file_id": file_id,
            "able_to_process": able_to_process,
        },
    )


def get_vcf_data(
    project_ids: list,
    node_set: NodeSet,
    edge_set: EdgeSet,
    write_set: bool = False,
    process_compressed_files: bool = True,
    process_variants: bool = True,
    write_intermediate: bool = True,
    write_reports: bool = True,
) -> tuple[NodeSet, EdgeSet, list[pl.DataFrame]]:
    """Process VCF files from Synapse projects and build a knowledge graph.

    Main orchestration function that discovers VCF files in specified
    projects, processes them to extract variants and metadata, and constructs
    a knowledge graph. Supports both compressed (.vcf.gz) and uncompressed
    (.vcf) files.

    Parameters
    ----------
    project_ids :
        List of Synapse project IDs to process.
    node_set :
        Existing set of nodes to update.
    edge_set :
        Existing set of edges to update.
    write_set :
        If True, write final knowledge graph to disk.
    process_compressed_files :
        If True, process .vcf.gz files. If False, skip them.
    process_variants :
        If True, extract variant data. If False, only extract metadata.
    write_intermediate :
        If True, write graph after each project.
    write_reports :
        If True, generate TSV reports of processing status.

    Returns
    -------
    node_set : NodeSet
        The updated node set.
    edge_set : EdgeSet
        The updated edge set.
    reports : list[pl.DataFrame]
        List of processing report DataFrames.

    Notes
    -----
    Intermediate graphs and reports are written to RESOURCE_PATH/artifacts and
    REPORT_PATH.
    """
    logger.info(f"Adding tabular experimental data for "
                f"{len(project_ids)} projects")
    process_files = []
    i = 0
    vcf_formats = (
        VCF_FILE_TYPES
        if process_compressed_files
        else [x for x in VCF_FILE_TYPES if not x.endswith("gz")]
    )
    for project_id in tqdm.tqdm(project_ids):
        i = i + 1
        project_files = get_project_files(
            project_syn_id=project_id, file_types=vcf_formats, as_list=True
        )

        logger.info(
            f"adding VCF experimental data project {project_id}\n\
                    This is project {i} out of {len(project_ids)+1} \n\
                    There are {len(project_files)} total files to parse."
        )
        i = i + 1
        for file_id in tqdm.tqdm(project_files):
            node_set, edge_set, able_to_process = parse_vcf_file(
                file_id=file_id,
                node_set=node_set,
                edge_set=edge_set,
                process_variants=process_variants,
                project_id=project_id,
            )
            process_files.append(able_to_process)
        if write_intermediate:
            write_graph(
                node_set=node_set,
                edge_set=edge_set,
                source_filter=True,
                strict=True,
                source_name=["vcf_data", "experimental_data"],
                resource_path=os.path.join(RESOURCE_PATH, "artifacts"),
            )

    # Write a sub-graph with just vcf experimental data
    if write_set:
        write_graph(
            node_set=node_set,
            edge_set=edge_set,
            source_filter=True,
            strict=True,
            source_name=["vcf_data", "experimental_data"],
            resource_path=os.path.join(RESOURCE_PATH, "artifacts"),
        )
    processed_df = pl.from_dicts(process_files)
    if write_reports:
        os.makedirs(REPORT_PATH, exist_ok=True)
        processed_df.write_csv(
            os.path.join(REPORT_PATH, "vcf_file_report.tsv"), separator="\t"
        )

    return node_set, edge_set, [processed_df]
