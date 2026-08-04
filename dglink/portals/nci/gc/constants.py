import os

## module store info ##
home_dir: str = os.getenv("HOME") or "/"
NCI_GC_CACHE_DIR = os.path.join(
    home_dir,
    ".data",
    "nci_general_commons",
)
## API endpoints
NCI_GQL_ENDPOINT = "https://general.datacommons.cancer.gov/v1/graphql/"
NCI_GEN3_ENDPOINT = "https://nci-crdc.datacommons.io"

## portal specific tabular file detentions ##
NCI_TABULAR_FILE_TYPES = ["CSV", "XLS", "XLSX", "ODS", "TXT"]


NODE_ATTRIBUTES = [
    ## core fields - all nodes should have ths other fields are optional
    "curie:ID",
    ":LABEL",
    "name",
    "iri",
    "grounded:boolean",  ## true if name/curie came from ontology grounding, false if raw text
    "source:string[]",
    "raw_label",  ## holds original label in case we use pascalify to make node labels look nice ##
    ## Program fields
    "program_description",
    "num_participants",
    "num_files",
    "num_disease_sites",
    "num_samples",
    "program_url",
    ## study fields
    "study_access",
    "study_version",
    "study_data_types",
    "short_description",
    ## StudyParticipantGroup
    "subject_count",
    ## publication fields ##
    "publication_type",
    "publication_status",
    "doi",
    ## investigator fields ##
    "email",
    "role_or_affiliation",
    "title",
    ## participant (biolink:Case) patient-course fields — the diagnosis-event
    ## qualifiers (stage, morphology, site, age, ...) live on the edge, see EDGE_ATTRIBUTES
    "vital_status",
    "last_known_disease_status",
    "days_to_last_followup",
    "days_to_last_known_disease_status",
    ## NB: tabular provenance (raw_texts / columns / file_id) is NOT a node attribute —
    ## it rides on the group -> entity edge (see EDGE_ATTRIBUTES) so it stays unambiguous
    ## once portals are merged onto a shared entity node.
]
EDGE_ATTRIBUTES = [
    ## core fields - all edges should have ths other fields are optional
    ":START_ID",
    ":END_ID",
    ":TYPE",
    "source:string[]",
    "raw_type",
    ## investigator -> study
    "role",
    ## diagnosis-event qualifiers carried on the participant/study -> disease edge
    ## (per-patient facts; kept off the shared grounded Disease concept node)
    "diagnosis_id",
    "disease_type",
    "primary_site",
    "tissue_or_organ_of_origin",
    "site_of_resection_or_biopsy",
    "tumor_grade",
    "tumor_stage_clinical_m",
    "tumor_stage_clinical_n",
    "tumor_stage_clinical_t",
    "morphology",
    "age_at_diagnosis",
    "incidence_type",
    "progression_or_recurrence",
    "days_to_recurrence",
    "study_diagnosis_id",
    "crdc_id",
    ## tabular provenance — which file / column / raw text an extracted entity was
    ## found in (moved here from the node so it stays per-portal after merge)
    "raw_texts:string[]",
    "columns:string[]",
    "file_id:string[]",
]

## --- Biolink mapping ------------------------------------------------------
## NCI GC ids (program names, phs accessions, DOIs, investigator/diagnosis ids)
## are not uniform CURIEs; Biolink requires CURIE node ids, so we mint a single
## `ncigc:` prefix and apply it to every node id and edge endpoint consistently.
NCI_GC_CURIE_PREFIX = "ncigc"
GC_LABEL_TO_BIOLINK = {
    "Program": "biolink:Study",
    "Study": "biolink:Study",
    "StudyParticipantGroup": "biolink:StudyPopulation",
    "Publication": "biolink:Publication",
    "Investigator": "biolink:Agent",
    "Diagnosis": "biolink:Disease",
    "Participant": "biolink:Case",
}
GC_DEFAULT_BIOLINK_CATEGORY = "biolink:NamedThing"

## GC-native edge type -> Biolink predicate. The native relation is kept in
## `raw_type`. Note: "Published" is emitted as publication -> study (a publication
## mentions the study), which reverses the GC-native start/end.
GC_EDGE_TO_BIOLINK = {
    "Has_Study": "biolink:has_part",
    "Has_Participant_Group": "biolink:has_part",
    "Leads_Study": "biolink:contributes_to",
    "Study_Has_Diagnosis": "biolink:associated_with",
    "Study_Has_Participant": "biolink:has_part",
    "Participant_Has_Diagnosis": "biolink:associated_with",
    "Published": "biolink:mentions",
}


def gc_curie(value: str) -> str:
    """Prefix a raw NCI GC id as an `ncigc:` CURIE (Biolink requires CURIE ids)."""
    return f"{NCI_GC_CURIE_PREFIX}:{value}"
