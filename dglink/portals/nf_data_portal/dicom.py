"""
Extract DICOM series from the NF Data Portal's Synapse projects.

Finds the DICOM files of each project, identifies their series from Synapse
annotations, downloads one file per series and reads it with
dglink.core.dicom_data.
"""

from dglink.core.constants import RESOURCE_PATH, REPORT_PATH
from dglink.core.dicom_data import process_dicom
from dglink.core.nodes import NodeSet
from dglink.core.edges import EdgeSet
from dglink.core.utils import write_graph
from .constants import syn
from .utils import get_project_files
import os
import logging
import tqdm

import polars as pl

logger = logging.getLogger(__name__)


def process_synapse_dicom(
    file_id: str,
    node_set: NodeSet,
    edge_set: EdgeSet,
    dicom_identifiers: set,
    project_granularity: bool = False,
) -> tuple[NodeSet, EdgeSet, set, dict]:
    """Process a single DICOM file from Synapse into the knowledge graph.

    Uses the file's Synapse annotations to identify its series, and skips files
    whose series has already been processed. Supports processing at either
    series-level or project-level granularity.

    Parameters
    ----------
    file_id :
        Synapse file ID for the DICOM file (e.g., 'syn12345678').
    node_set :
        Existing set of nodes to update.
    edge_set :
        Existing set of edges to update.
    dicom_identifiers :
        Set of already-processed series/project identifiers to avoid
        duplicates.
    project_granularity :
        If True, process one DICOM per project. If False, process all unique
        series (identified by studyId, assay, specimenID, individualID,
        timepoint).

    Returns
    -------
    node_set : NodeSet
        The updated node set.
    edge_set : EdgeSet
        The updated edge set.
    dicom_identifiers : set
        The updated set of processed identifiers.
    status : dict
        Status dict containing project_id, file_id, and the able_to_process
        flag.
    """
    able_to_process = True
    annotations = syn.get_annotations(file_id)
    project_id = annotations.get("studyId", ["project_id_missing"])[0]
    # Try to process all DICOM series
    if not project_granularity:
        # Annotations do not have series identifier so using all this
        # info as a proxy
        assay = annotations.get("assay", ["assay_missing"])[0]
        specimen_id = annotations.get("specimenID", ["specimenID_missing"])[0]
        individual_id = annotations.get("individualID",
                                        ["individualID_missing"])[0]
        experimental_timepoint = annotations.get(
            "experimentalTimepoint", ["experimentalTimepoint_missing"]
        )[0]
        series_identifier = (
            project_id,
            assay,
            specimen_id,
            individual_id,
            experimental_timepoint,
        )
    # Try to process one series per study
    else:
        series_identifier = project_id
    if series_identifier in dicom_identifiers:
        able_to_process = False
    else:
        dicom_identifiers.add(series_identifier)
        try:
            obj = syn.get(file_id)
            obj_path = obj.path
        except:
            able_to_process = False
            obj_path = None
        if obj_path is not None:
            node_set, edge_set = process_dicom(
                path=obj_path,
                file_id=file_id,
                group_identifier=project_id,
                node_set=node_set,
                edge_set=edge_set,
            )
    return (
        node_set,
        edge_set,
        dicom_identifiers,
        {
            "project_id": project_id,
            "file_id": file_id,
            "able_to_process": able_to_process,
        },
    )


def get_dicom_data(
    project_ids: list,
    node_set: NodeSet,
    edge_set: EdgeSet,
    write_set: bool = False,
    project_granularity: bool = False,
    write_intermediate: bool = True,
    write_reports: bool = True,
) -> tuple[NodeSet, EdgeSet, list[pl.DataFrame]]:
    """Process DICOM files from Synapse projects and build a knowledge graph.

    Main orchestration function that discovers DICOM files (.dcm) in specified
    projects, extracts medical imaging metadata and grounded biomedical
    entities, and constructs a knowledge graph. Supports both series-level and
    project-level processing granularity.

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
    project_granularity :
        If True, process one DICOM per project. If False, process all unique
        series per project.
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
    Uses Gilda for entity grounding of unstructured DICOM text fields to
    biomedical ontology terms. Intermediate graphs and reports are written to
    RESOURCE_PATH/artifacts and REPORT_PATH respectively.

    Examples
    --------
    >>> # Process all series from projects
    >>> nodes, edges, reports = get_dicom_data(
    ...     project_ids=['syn12345', 'syn67890'],
    ...     node_set=NodeSet(),
    ...     edge_set=EdgeSet(),
    ...     project_granularity=False
    ... )
    >>>
    >>> # Process one DICOM per project for quick overview
    >>> nodes, edges, reports = get_dicom_data(
    ...     project_ids=['syn12345'],
    ...     node_set=NodeSet(),
    ...     edge_set=EdgeSet(),
    ...     project_granularity=True
    ... )
    """
    logger.info(f"Adding tabular experimental data for "
                f"{len(project_ids)} projects")
    process_files = []
    dicom_identifiers = set()
    i = 0
    for project_id in tqdm.tqdm(project_ids):
        i = i + 1
        project_files = get_project_files(
            project_syn_id=project_id, file_types=[".dcm"], as_list=True
        )

        logger.info(
            f"adding DCM experimental data project {project_id}\n\
                    This is project {i} out of {len(project_ids)+1} \n\
                    There are {len(project_files)} total files to parse."
        )
        i = i + 1
        for file_id in tqdm.tqdm(project_files):
            node_set, edge_set, dicom_identifiers, able_to_process = (
                process_synapse_dicom(
                    file_id=file_id,
                    node_set=node_set,
                    edge_set=edge_set,
                    dicom_identifiers=dicom_identifiers,
                    project_granularity=project_granularity,
                )
            )
            process_files.append(able_to_process)
        if write_intermediate:
            write_graph(
                node_set=node_set,
                edge_set=edge_set,
                source_filter=True,
                strict=True,
                source_name=["dicom_data", "experimental_data"],
                resource_path=os.path.join(RESOURCE_PATH, "artifacts"),
            )

    # Write a sub-graph with just dicom experimental data
    if write_set:
        write_graph(
            node_set=node_set,
            edge_set=edge_set,
            source_filter=True,
            strict=True,
            source_name=["dicom_data", "experimental_data"],
            resource_path=os.path.join(RESOURCE_PATH, "artifacts"),
        )
    processed_df = pl.from_dicts(process_files)
    if write_reports:
        os.makedirs(REPORT_PATH, exist_ok=True)
        processed_df.write_csv(
            os.path.join(REPORT_PATH, "dicom_file_report.tsv"), separator="\t"
        )

    return node_set, edge_set, [processed_df]
