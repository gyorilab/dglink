from dglink.portals.nci.gc import NciGeneralCommonsClient
from dglink.portals.nci.gc.constants import (
    GC_LABEL_TO_BIOLINK,
    GC_EDGE_TO_BIOLINK,
    gc_curie,
    NCI_GC_CURIE_PREFIX,
    NCI_GC_CACHE_DIR,
    NCI_TABULAR_FILE_TYPES,
)
from dglink import NodeSet, EdgeSet

import os
from typing import Iterator
import polars as pl

lazzy_get = lambda d, x: f"{d.get(x, f'{x}_missing')}"


def get_program_hierarchy(
    client: NciGeneralCommonsClient, node_set: NodeSet, edge_set: EdgeSet
):
    """parse the hierachy associated with a program"""
    program_details = client.get_program_details()
    for program in program_details:
        program_id = program.get("program")
        if not program_id:
            continue
        program_curie = gc_curie(program_id)
        node_set.update_nodes(
            {
                "curie:ID": program_curie,
                ":LABEL": GC_LABEL_TO_BIOLINK["Program"],
                "raw_label": "Program",
                "name": lazzy_get(program, "program_name"),
                "program_description": lazzy_get(program, "program_short_description"),
                "num_participants": lazzy_get(program, "num_participants"),
                "num_files": lazzy_get(program, "num_files"),
                "num_samples": lazzy_get(program, "num_samples"),
                "num_disease_sites": lazzy_get(program, "num_disease_sites"),
                "program_url": lazzy_get(program, "program_url"),
                "source:string[]": "structural_information",
            }
        )
        for study in program.get("studies", []):
            accession = study.get("accession")
            if not accession:
                continue
            study_curie = gc_curie(accession)
            node_set.update_nodes(
                {
                    "curie:ID": study_curie,
                    ":LABEL": GC_LABEL_TO_BIOLINK["Study"],
                    "raw_label": "Study",
                    "name": lazzy_get(study, "study_name"),
                    "study_access": lazzy_get(study, "study_access"),
                    "study_version": lazzy_get(study, "study_version"),
                    "study_data_types": lazzy_get(study, "study_data_types"),
                    "short_description": lazzy_get(study, "short_description"),
                    "num_participants": lazzy_get(study, "num_participants"),
                    "num_samples": lazzy_get(study, "num_samples"),
                    "num_files": lazzy_get(study, "num_files"),
                    "source:string[]": "structural_information",
                }
            )
            edge_set.update_edges(
                {
                    ":START_ID": program_curie,
                    ":END_ID": study_curie,
                    ":TYPE": GC_EDGE_TO_BIOLINK["Has_Study"],
                    "raw_type": "Has_Study",
                    "source:string[]": "structural_information",
                }
            )

        for participant_group in program.get("study_participants", []):
            group = participant_group.get("group")
            subjects = lazzy_get(participant_group, "subjects")
            if not group:
                continue

            group_curie = gc_curie(group)
            node_set.update_nodes(
                {
                    "curie:ID": group_curie,
                    ":LABEL": GC_LABEL_TO_BIOLINK["StudyParticipantGroup"],
                    "raw_label": "StudyParticipantGroup",
                    "name": group,
                    "subject_count": subjects,
                    "source:string[]": "metadata",
                }
            )

            edge_set.update_edges(
                {
                    ":START_ID": program_curie,
                    ":END_ID": group_curie,
                    ":TYPE": GC_EDGE_TO_BIOLINK["Has_Participant_Group"],
                    "raw_type": "Has_Participant_Group",
                    "source:string[]": "metadata",
                }
            )
    return node_set, edge_set


def get_publications(
    client: NciGeneralCommonsClient, node_set: NodeSet, edge_set: EdgeSet
):
    """Add publications (biolink:Publication) and publication -> study edges.

    Emitted as publication --biolink:mentions--> study (reverses the GC-native
    Published direction to match mentions semantics).
    """
    for publication in client.get_publications():
        doi = lazzy_get(publication, "DOI_or_Pub_ID")
        if not doi:
            continue
        doi_curie = gc_curie(doi)
        node_set.update_nodes(
            {
                "curie:ID": doi_curie,
                ":LABEL": GC_LABEL_TO_BIOLINK["Publication"],
                "raw_label": "Publication",
                "name": lazzy_get(publication, "Publication_Title"),
                "publication_type": lazzy_get(publication, "Publication_Type"),
                "publication_status": lazzy_get(publication, "Publication_Status"),
                "doi": doi,
                "source:string[]": "metadata",
            }
        )
        for phs in publication.get(
            "phs_accessions", [lazzy_get(publication, "phs_accession")]
        ):
            edge_set.update_edges(
                {
                    ":START_ID": doi_curie,
                    ":END_ID": gc_curie(phs),
                    ":TYPE": GC_EDGE_TO_BIOLINK["Published"],
                    "raw_type": "Published",
                    "source:string[]": "metadata",
                }
            )
    return node_set, edge_set


def get_investigators(
    client: NciGeneralCommonsClient, node_set: NodeSet, edge_set: EdgeSet
):
    """Add investigators (biolink:Agent) and investigator -> study edges (contributes_to)."""
    for investigator in client.get_investigators():
        inv_id = investigator.get("investigator_id")
        if not inv_id:
            continue
        ## build a display name — fall back to primary_investigator_name if first/last absent
        first = investigator.get("first_name", "")
        last = investigator.get("last_name", "")
        display_name = f"{first} {last}".strip() or lazzy_get(
            investigator, "primary_investigator_name"
        )
        inv_curie = gc_curie(inv_id)
        node_set.update_nodes(
            {
                "curie:ID": inv_curie,
                ":LABEL": GC_LABEL_TO_BIOLINK["Investigator"],
                "raw_label": "Investigator",
                "name": display_name,
                "email": lazzy_get(investigator, "email"),
                "role_or_affiliation": lazzy_get(investigator, "role_or_affiliation"),
                "title": lazzy_get(investigator, "title"),
                "source:string[]": "metadata",
            }
        )
        for phs in investigator.get(
            "phs_accessions", [lazzy_get(investigator, "phs_accession")]
        ):
            edge_set.update_edges(
                {
                    ":START_ID": inv_curie,
                    ":END_ID": gc_curie(phs),
                    ":TYPE": GC_EDGE_TO_BIOLINK["Leads_Study"],
                    "raw_type": "Leads_Study",
                    "role": lazzy_get(investigator, "role_or_affiliation"),
                    "source:string[]": "metadata",
                }
            )
    return node_set, edge_set


def get_diagnoses(
    client: NciGeneralCommonsClient, node_set: NodeSet, edge_set: EdgeSet
):
    """Add diagnoses (biolink:Disease) and study -> diagnosis edges (associated_with)."""
    for diagnosis in client.get_diagnoses(only_open=True):
        diag_id = diagnosis.get("diagnosis_id")
        if not diag_id:
            continue
        diag_curie = gc_curie(diag_id)
        node_set.update_nodes(
            {
                "curie:ID": diag_curie,
                ":LABEL": GC_LABEL_TO_BIOLINK["Diagnosis"],
                "raw_label": "Diagnosis",
                "name": lazzy_get(diagnosis, "primary_diagnosis"),
                "disease_type": lazzy_get(diagnosis, "disease_type"),
                "primary_site": lazzy_get(diagnosis, "primary_site"),
                "tissue_or_organ_of_origin": lazzy_get(
                    diagnosis, "tissue_or_organ_of_origin"
                ),
                "site_of_resection_or_biopsy": lazzy_get(
                    diagnosis, "site_of_resection_or_biopsy"
                ),
                "tumor_grade": lazzy_get(diagnosis, "tumor_grade"),
                "tumor_stage_clinical_m": lazzy_get(
                    diagnosis, "tumor_stage_clinical_m"
                ),
                "tumor_stage_clinical_n": lazzy_get(
                    diagnosis, "tumor_stage_clinical_n"
                ),
                "tumor_stage_clinical_t": lazzy_get(
                    diagnosis, "tumor_stage_clinical_t"
                ),
                "morphology": lazzy_get(diagnosis, "morphology"),
                "vital_status": lazzy_get(diagnosis, "vital_status"),
                "age_at_diagnosis": lazzy_get(diagnosis, "age_at_diagnosis"),
                "incidence_type": lazzy_get(diagnosis, "incidence_type"),
                "progression_or_recurrence": lazzy_get(
                    diagnosis, "progression_or_recurrence"
                ),
                "last_known_disease_status": lazzy_get(
                    diagnosis, "last_known_disease_status"
                ),
                "days_to_recurrence": lazzy_get(diagnosis, "days_to_recurrence"),
                "days_to_last_followup": lazzy_get(diagnosis, "days_to_last_followup"),
                "days_to_last_known_disease_status": lazzy_get(
                    diagnosis, "days_to_last_known_disease_status"
                ),
                "study_diagnosis_id": lazzy_get(diagnosis, "study_diagnosis_id"),
                "crdc_id": lazzy_get(diagnosis, "crdc_id"),
                "source:string[]": "clinical",
            }
        )
        ## Prefer attaching the diagnosis to its participant (a biolink:Case) so GC has
        ## case-level granularity like GDC/PDC; also link the study to the participant.
        ## Fall back to a study -> diagnosis edge when no participant id is present.
        participant_id = diagnosis.get("participant_id")
        phs = diagnosis.get("phs_accession")
        if participant_id:
            participant_curie = gc_curie(participant_id)
            node_set.update_nodes(
                {
                    "curie:ID": participant_curie,
                    ":LABEL": GC_LABEL_TO_BIOLINK["Participant"],
                    "raw_label": "Participant",
                    "source:string[]": "structural_information",
                }
            )
            edge_set.update_edges(
                {
                    ":START_ID": participant_curie,
                    ":END_ID": diag_curie,
                    ":TYPE": GC_EDGE_TO_BIOLINK["Participant_Has_Diagnosis"],
                    "raw_type": "Participant_Has_Diagnosis",
                    "source:string[]": "clinical",
                }
            )
            if phs:
                edge_set.update_edges(
                    {
                        ":START_ID": gc_curie(phs),
                        ":END_ID": participant_curie,
                        ":TYPE": GC_EDGE_TO_BIOLINK["Study_Has_Participant"],
                        "raw_type": "Study_Has_Participant",
                        "source:string[]": "structural_information",
                    }
                )
        elif phs:
            edge_set.update_edges(
                {
                    ":START_ID": gc_curie(phs),
                    ":END_ID": diag_curie,
                    ":TYPE": GC_EDGE_TO_BIOLINK["Study_Has_Diagnosis"],
                    "raw_type": "Study_Has_Diagnosis",
                    "source:string[]": "clinical",
                }
            )
    return node_set, edge_set


def get_metadata_graph(
    client: NciGeneralCommonsClient, node_set: NodeSet, edge_set: EdgeSet
):
    """Build the full NCI GC structural/metadata subgraph (programs, publications,
    investigators, diagnoses) into the node/edge sets. Analogous to GDC's get_case_hierarchy.
    """
    get_program_hierarchy(client, node_set, edge_set)
    get_publications(client, node_set, edge_set)
    get_investigators(client, node_set, edge_set)
    get_diagnoses(client, node_set, edge_set)
    return node_set, edge_set


def download_tabular_files(client: NciGeneralCommonsClient, step_size: int = 250):
    """Download open-access tabular files for all studies into the GC cache.

    Populates the study_to_files.tsv manifest (via client.get_study_files) and
    downloads any not-yet-cached files (client.download_files skips files whose
    path is already set), so re-runs are cheap.
    """
    records = []
    for study in client.get_all_studies():
        if study.get("study_access") != "Open":
            continue
        phs = study.get("phs_accession")
        if not phs:
            continue
        records += client.get_study_files(phs, page_size=1000)
    if not records:
        return
    files_df = pl.from_dicts(records).filter(
        pl.col("file_type").is_in(NCI_TABULAR_FILE_TYPES)
    )
    file_ids = files_df["file_id"].to_list()
    save_directory = os.path.join(NCI_GC_CACHE_DIR, "files")
    for i in range(0, len(file_ids), step_size):
        client.download_files(
            file_ids=file_ids[i : i + step_size], save_directory=save_directory
        )


def get_tabular_iterator(study_list: list | None = None) -> tuple[list, Iterator]:
    """Return (group_ids, iterator) over downloaded tabular files, grouped by study.

    The group id is the gc: CURIE of the study (phs_accession) so grounded-entity
    edges attach to the study node. The iterator yields (study_curie, file_paths,
    file_ids) tuples, matching the shape get_tabular_data expects.
    """
    manifest = os.path.join(NCI_GC_CACHE_DIR, "study_to_files.tsv")
    files_df = pl.read_csv(manifest, separator="\t").cast({"phs_accession": pl.String})
    files_df = files_df.filter(
        pl.col("file_type").is_in(NCI_TABULAR_FILE_TYPES) & pl.col("path").is_not_null()
    )
    if study_list is not None:
        files_df = files_df.filter(pl.col("phs_accession").is_in(study_list))
    study_files = (
        files_df.select(["phs_accession", "path", "file_id"])
        .rename({"path": "file_paths", "file_id": "file_ids"})
        .group_by("phs_accession", maintain_order=True)
        .agg([pl.col("file_paths"), pl.col("file_ids")])
        .with_columns(
            study_curie=pl.format(
                "{}:{}", pl.lit(NCI_GC_CURIE_PREFIX), pl.col("phs_accession")
            )
        )
        .select(["study_curie", "file_paths", "file_ids"])
    )
    group_ids = study_files["study_curie"].to_list()
    return group_ids, study_files.iter_rows()
