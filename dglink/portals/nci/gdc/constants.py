"""
Constants specific to the NCI genomic Data Commons portal
"""

from pystow import module

NCI_GDC_CACHE_DIR = module("nci_genomic_data_commons").base

DATA_ENDPNT = "https://api.gdc.cancer.gov/data"
CASES_ENDPNT = "https://api.gdc.cancer.gov/cases/"
NODE_ATTRIBUTES = [
    ## core fields - all nodes should have ths other fields are optional
    "curie:ID",
    ":LABEL",
    "raw_label",  ## Golds GDC-native fields not conforming to BioLinK (e.g. "aliquot")
    "name",
    "iri",
    "grounded:boolean",  ## true if name/curie came from ontology grounding, false if raw text
    "source:string[]",
    ## NB: tabular provenance (raw_texts / columns / file_id) is NOT a node attribute — it
    ## rides on the group -> entity edge (see EDGE_ATTRIBUTES) to stay per-portal after merge
    "submitter_id:string[]",  ## human-readable GDC submitter barcode(s); an alias, not a node
    ## sample (specimen) fields
    "tumor_descriptor",
    "specimen_type",
    "sample_type",
    "tissue_type",
    "preservation_method",
    ## case demographic fields (expanded `demographic` object)
    "gender",
    "race",
    "ethnicity",
    "vital_status",
    "age_at_index",
    "days_to_death",
    "days_to_birth",
    ## project -> biolink:Study fields (expanded `project` object)
    "program",
    "disease_type",
    "primary_site",
    ## NB: per-diagnosis fields (primary_diagnosis, morphology, stage, ...) are NOT node
    ## attributes — they ride on the case -> disease edge (see EDGE_ATTRIBUTES). The Disease
    ## node itself is a grounded concept (name / curie / iri only).
]
EDGE_ATTRIBUTES = [
    ## core fields - all edges should have ths other fields are optional
    ":START_ID",
    ":END_ID",
    ":TYPE",
    "raw_type",  ## GDC-native relation (e.g. "has_aliquot") preserved alongside the Biolink predicate
    "source:string[]",
    ## diagnosis-event qualifiers carried on the case -> disease edge (per-patient;
    ## kept off the shared grounded Disease concept node)
    "diagnosis_id",
    "primary_diagnosis",
    "morphology",
    "tissue_or_organ_of_origin",
    "site_of_resection_or_biopsy",
    "tumor_grade",
    "ajcc_pathologic_stage",
    "classification_of_tumor",
    "prior_malignancy",
    "age_at_diagnosis",
    "progression_or_recurrence",
    "last_known_disease_status",
    ## tabular provenance — which file / column / raw text an extracted entity was found
    ## in (moved here from the node so it stays per-portal after merge)
    "raw_texts:string[]",
    "columns:string[]",
    "file_id:string[]",
    ## predicted similar project fields
    "jacquard_score",
    "score_cutoff",
    "intersection_score",
    "union_score",
    "shared_edges:string[]",
    "head_only_edges:string[]",
    "tail_only_edges:string[]",
    "edge_weights:string[]",
]

## fields to take from files for csv
FILE_FIELDS = [
    "access",
    "data_category",
    "data_format",
    "data_type",
    "experimental_strategy",
    "file_name",
    "file_size",
]


## Prefix GDC UUIDs with gdc to conform to biolink curies
GDC_CURIE_PREFIX = "ncigdc"

## GDC-native entity type -> concrete Biolink category (verified against Biolink
## 4.2.2). The specimen subdivisions have no dedicated Biolink class, so they all
## map to biolink:MaterialSample; the native distinction is kept in `raw_label`.
GDC_LABEL_TO_BIOLINK = {
    "case": "biolink:Case",
    "sample": "biolink:MaterialSample",
    "aliquot": "biolink:MaterialSample",
    "analyte": "biolink:MaterialSample",
    "portion": "biolink:MaterialSample",
    "slide": "biolink:MaterialSample",
    "diagnosis": "biolink:Disease",
}
GDC_DEFAULT_BIOLINK_CATEGORY = "biolink:NamedThing"


GDC_RELATION = {
    "sample": ("biolink:derives_from", "child_to_case"),
    "aliquot": ("biolink:derives_from", "child_to_case"),
    "analyte": ("biolink:derives_from", "child_to_case"),
    "portion": ("biolink:derives_from", "child_to_case"),
    "slide": ("biolink:derives_from", "child_to_case"),
    "diagnosis": ("biolink:associated_with", "case_to_child"),
}
