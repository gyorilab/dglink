from .nci_proteomic_data_commons_client import NciProteomicCommonsClient
from .constants import (
    NCI_PDC_CACHE_DIR,
    NCI_TABULAR_FILE_TYPES,
    PDC_LABEL_TO_BIOLINK,
    PDC_EDGE_TO_BIOLINK,
    PDC_CURIE_PREFIX,
    pdc_curie,
)
from dglink import NodeSet, EdgeSet
from dglink.core.grounding import ground_term

import polars as pl

import re
from typing import Iterator
import logging

logger = logging.getLogger(__name__)

lazzy_get = lambda d, x: f"{d.get(x, f'{x}_missing')}"


def _disease_curie(name: str) -> str:
    """Mint a stable `pdc:` CURIE for a disease/site name (names are not UUIDs)."""
    slug = re.sub(r"[^a-z0-9]+", "_", name.strip().lower()).strip("_")
    return pdc_curie(f"disease_{slug}")


def _add_disease(
    node_set: NodeSet,
    edge_set: EdgeSet,
    parent_curie: str,
    disease_type: str,
    primary_site: str = "",
    source: str = "structural_information",
):
    """Add a grounded biolink:Disease *concept* node for `disease_type` and link
    `parent` -> disease. The disease name is grounded to an ontology curie (shared with
    GC/PDC so the concept merges across portals); the node holds only identity, while
    disease_type / primary_site ride on the parent -> disease edge."""
    if not disease_type:
        return
    name, disease_curie, iri = ground_term(disease_type)
    grounded = disease_curie is not None
    if not disease_curie:
        ## grounding failed -> portal-local slug id (won't merge across portals)
        disease_curie = _disease_curie(disease_type)
        name = disease_type
    node_set.update_nodes(
        {
            "curie:ID": disease_curie,
            ":LABEL": PDC_LABEL_TO_BIOLINK["disease"],
            "raw_label": "disease",
            "name": name,
            "iri": iri,
            "grounded:boolean": "true" if grounded else "false",
            "source:string[]": source,
        }
    )
    edge_set.update_edges(
        {
            ":START_ID": parent_curie,
            ":END_ID": disease_curie,
            ":TYPE": PDC_EDGE_TO_BIOLINK["Associated_Disease"],
            "raw_type": "Associated_Disease",
            "source:string[]": source,
            "disease_type": disease_type,
            "primary_site": primary_site,
        }
    )


def get_program_hierarchy(
    client: NciProteomicCommonsClient, node_set: NodeSet, edge_set: EdgeSet
) -> list[str]:
    """Build the program -> project -> study structural subgraph.

    Every node carries a valid Biolink category in :LABEL (with the PDC-native
    type kept in raw_label) and a `pdc:`-prefixed CURIE id. Study-level disease
    types are emitted as biolink:Disease nodes linked to the study.

    Returns the list of study UUIDs discovered, so the caller can pull each
    study's biospecimen hierarchy without re-fetching the program tree.
    """
    study_ids: list[str] = []
    for program in client.get_program_hierarchy():
        program_id = program.get("program_id")
        if not program_id:
            continue
        program_curie = pdc_curie(program_id)
        node_set.update_nodes(
            {
                "curie:ID": program_curie,
                ":LABEL": PDC_LABEL_TO_BIOLINK["program"],
                "raw_label": "program",
                "name": lazzy_get(program, "name"),
                "source:string[]": "structural_information",
            }
        )
        for project in program.get("projects", []):
            project_id = project.get("project_id")
            if not project_id:
                continue
            project_curie = pdc_curie(project_id)
            node_set.update_nodes(
                {
                    "curie:ID": project_curie,
                    ":LABEL": PDC_LABEL_TO_BIOLINK["project"],
                    "raw_label": "project",
                    "name": lazzy_get(project, "name"),
                    "source:string[]": "structural_information",
                }
            )
            edge_set.update_edges(
                {
                    ":START_ID": program_curie,
                    ":END_ID": project_curie,
                    ":TYPE": PDC_EDGE_TO_BIOLINK["Has_Project"],
                    "raw_type": "Has_Project",
                    "source:string[]": "structural_information",
                }
            )
            for study in project.get("studies", []):
                study_id = study.get("study_id")
                if not study_id:
                    continue
                study_ids.append(study_id)
                study_curie = pdc_curie(study_id)
                primary_sites = study.get("primary_sites") or []
                node_set.update_nodes(
                    {
                        "curie:ID": study_curie,
                        ":LABEL": PDC_LABEL_TO_BIOLINK["study"],
                        "raw_label": "study",
                        "name": lazzy_get(study, "study_name"),
                        "submitter_id:string[]": lazzy_get(study, "study_submitter_id"),
                        "analytical_fraction": lazzy_get(study, "analytical_fraction"),
                        "experiment_type": lazzy_get(study, "experiment_type"),
                        "acquisition_type": lazzy_get(study, "acquisition_type"),
                        "primary_site": ";".join(primary_sites),
                        "source:string[]": "structural_information",
                    }
                )
                edge_set.update_edges(
                    {
                        ":START_ID": project_curie,
                        ":END_ID": study_curie,
                        ":TYPE": PDC_EDGE_TO_BIOLINK["Has_Study"],
                        "raw_type": "Has_Study",
                        "source:string[]": "structural_information",
                    }
                )
                primary_site = primary_sites[0] if primary_sites else ""
                for disease_type in study.get("disease_types") or []:
                    _add_disease(
                        node_set,
                        edge_set,
                        parent_curie=study_curie,
                        disease_type=disease_type,
                        primary_site=primary_site,
                    )
    return study_ids


def get_biospecimen_hierarchy(
    client: NciProteomicCommonsClient,
    node_set: NodeSet,
    edge_set: EdgeSet,
    study_ids: list[str],
    include_biospecimen: bool = True,
):
    """Build the study -> case -> sample -> aliquot biospecimen subgraph.

    For each study we pull `biospecimenPerStudy` and emit: the case (linked to its
    study via has_part), the sample (derives_from the case) and the aliquot
    (derives_from the sample). Disease/site/sample-type metadata is attached to the
    relevant nodes; the human-readable submitter ids are kept as aliases, not nodes.

    When `include_biospecimen` is False the sample and aliquot (biolink:MaterialSample)
    nodes/edges are skipped; the case nodes and their disease links are still emitted,
    since those are the meaningful clinical metadata (and the extraction anchor).
    """
    for study_id in study_ids:
        study_curie = pdc_curie(study_id)
        for row in client.get_study_biospecimen(study_id):
            case_id = row.get("case_id")
            sample_id = row.get("sample_id")
            aliquot_id = row.get("aliquot_id")
            disease_type = row.get("disease_type") or ""
            primary_site = row.get("primary_site") or ""

            if case_id:
                case_curie = pdc_curie(case_id)
                node_set.update_nodes(
                    {
                        "curie:ID": case_curie,
                        ":LABEL": PDC_LABEL_TO_BIOLINK["case"],
                        "raw_label": "case",
                        "submitter_id:string[]": lazzy_get(row, "case_submitter_id"),
                        "disease_type": disease_type,
                        "primary_site": primary_site,
                        "taxon": lazzy_get(row, "taxon"),
                        "status": lazzy_get(row, "case_status"),
                        "source:string[]": "structural_information",
                    }
                )
                edge_set.update_edges(
                    {
                        ":START_ID": study_curie,
                        ":END_ID": case_curie,
                        ":TYPE": PDC_EDGE_TO_BIOLINK["Has_Case"],
                        "raw_type": "Has_Case",
                        "source:string[]": "structural_information",
                    }
                )
                _add_disease(
                    node_set,
                    edge_set,
                    parent_curie=case_curie,
                    disease_type=disease_type,
                    primary_site=primary_site,
                )

            if sample_id and include_biospecimen:
                sample_curie = pdc_curie(sample_id)
                node_set.update_nodes(
                    {
                        "curie:ID": sample_curie,
                        ":LABEL": PDC_LABEL_TO_BIOLINK["sample"],
                        "raw_label": "sample",
                        "submitter_id:string[]": lazzy_get(row, "sample_submitter_id"),
                        "sample_type": lazzy_get(row, "sample_type"),
                        "primary_site": primary_site,
                        "pool": lazzy_get(row, "pool"),
                        "status": lazzy_get(row, "sample_status"),
                        "source:string[]": "structural_information",
                    }
                )
                if case_id:
                    edge_set.update_edges(
                        {
                            ":START_ID": sample_curie,
                            ":END_ID": pdc_curie(case_id),
                            ":TYPE": PDC_EDGE_TO_BIOLINK["Derived_From_Case"],
                            "raw_type": "Derived_From_Case",
                            "source:string[]": "structural_information",
                        }
                    )

            if aliquot_id and include_biospecimen:
                aliquot_curie = pdc_curie(aliquot_id)
                node_set.update_nodes(
                    {
                        "curie:ID": aliquot_curie,
                        ":LABEL": PDC_LABEL_TO_BIOLINK["aliquot"],
                        "raw_label": "aliquot",
                        "submitter_id:string[]": lazzy_get(row, "aliquot_submitter_id"),
                        "status": lazzy_get(row, "aliquot_status"),
                        "source:string[]": "structural_information",
                    }
                )
                if sample_id:
                    edge_set.update_edges(
                        {
                            ":START_ID": aliquot_curie,
                            ":END_ID": pdc_curie(sample_id),
                            ":TYPE": PDC_EDGE_TO_BIOLINK["Derived_From_Sample"],
                            "raw_type": "Derived_From_Sample",
                            "source:string[]": "structural_information",
                        }
                    )


def get_metadata_graph(
    client: NciProteomicCommonsClient,
    node_set: NodeSet,
    edge_set: EdgeSet,
    include_biospecimen: bool = True,
):
    """Build the full PDC structural/metadata subgraph (program -> project -> study
    -> case -> sample -> aliquot, plus disease nodes) into the node/edge sets.
    Analogous to GDC's get_case_hierarchy and GC's get_metadata_graph.

    Set `include_biospecimen=False` to omit the sample/aliquot (biolink:MaterialSample)
    scaffolding while keeping the program/project/study tree, the cases and all disease
    links. No content is extracted from specimen nodes, so this is a lossless prune for
    the extraction / cross-portal-integration story.
    """
    study_ids = get_program_hierarchy(client, node_set, edge_set)
    get_biospecimen_hierarchy(
        client, node_set, edge_set, study_ids, include_biospecimen=include_biospecimen
    )
    return node_set, edge_set


def download_tabular_files(client: NciProteomicCommonsClient, study_list: list[str]):
    """Download tabular files for the given studies into the PDC cache.

    Ensures each study's files are recorded in the study -> file manifest
    (study_to_files.tsv) via client.get_study_files (cached, so re-runs are cheap),
    selects the tabular ones, and hands them to client.download_files, which skips
    any file already present on disk.
    """
    file_ids = []
    for study_id in study_list:
        study_files = client.get_study_files(study_id, page_size=50)
        file_ids = [
            f["file_id"]
            for f in study_files
            if f.get("file_format") in NCI_TABULAR_FILE_TYPES
        ]
        ## for now downloading files of each case independently want to batch this later. ##
        client.download_files(file_ids)
        # file_ids += [
        #     f["file_id"]
        #     for f in study_files
        #     if f.get("file_format") in NCI_TABULAR_FILE_TYPES
        # ]
    # client.download_files(file_ids)


def get_tabular_iterator(study_list: list[str] | None = None) -> tuple[list, Iterator]:
    """Return (group_ids, iterator) over downloaded tabular files, grouped by study.

    The group id is the pdc: CURIE of the study so grounded-entity edges attach to the
    study node. The iterator yields (study_curie, file_paths, file_ids) tuples, matching
    the shape get_tabular_data expects. Only downloaded files (non-null manifest `path`)
    of a tabular format are included.
    """
    manifest = NCI_PDC_CACHE_DIR.joinpath("study_to_files.tsv")
    files_df = pl.read_csv(manifest, separator="\t").cast({"study_id": pl.String})
    files_df = files_df.filter(
        pl.col("file_format").is_in(NCI_TABULAR_FILE_TYPES)
        & pl.col("path").is_not_null()
    )
    if study_list is not None:
        files_df = files_df.filter(pl.col("study_id").is_in(study_list))
    study_files = (
        files_df.select(["study_id", "path", "file_id"])
        .rename({"path": "file_paths", "file_id": "file_ids"})
        .group_by("study_id", maintain_order=True)
        .agg([pl.col("file_paths"), pl.col("file_ids")])
        .with_columns(
            study_curie=pl.format("{}:{}", pl.lit(PDC_CURIE_PREFIX), pl.col("study_id"))
        )
        .select(["study_curie", "file_paths", "file_ids"])
    )
    group_ids = study_files["study_curie"].to_list()
    return group_ids, study_files.iter_rows()
