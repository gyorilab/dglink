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
    ## diagnosis fields ##
    "disease_type",
    "primary_site",
    "tissue_or_organ_of_origin",
    "site_of_resection_or_biopsy",
    "tumor_grade",
    "tumor_stage_clinical_m",
    "tumor_stage_clinical_n",
    "tumor_stage_clinical_t",
    "morphology",
    "vital_status",
    "age_at_diagnosis",
    "incidence_type",
    "progression_or_recurrence",
    "last_known_disease_status",
    "crdc_id",
    ## tabular data fields ##
    "raw_texts:string[]",
    "columns:string[]",
    "file_id:string[]",
]
EDGE_ATTRIBUTES = [
    ## core fields - all edges should have ths other fields are optional
    ":START_ID",
    ":END_ID",
    ":TYPE",
    "source:string[]",
    "raw_type",
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
    "Published": "biolink:mentions",
}


def gc_curie(value: str) -> str:
    """Prefix a raw NCI GC id as an `ncigc:` CURIE (Biolink requires CURIE ids)."""
    return f"{NCI_GC_CURIE_PREFIX}:{value}"
