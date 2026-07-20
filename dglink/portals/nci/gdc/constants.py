"""
Constants specific to the NCI genomics Data Commons portal
"""

import os

DATA_ENDPNT = "https://api.gdc.cancer.gov/data"
CASES_ENDPNT = "https://api.gdc.cancer.gov/cases/"
NODE_ATTRIBUTES = [
    ## core fields - all nodes should have ths other fields are optional
    "curie:ID",
    ":LABEL",
    "raw_label",  ## Golds GDC-native fields not conforming to BioLinK (e.g. "aliquot")
    "name",
    "iri",
    "source:string[]",
    "raw_texts:string[]",
    "columns:string[]",
    "file_id:string[]",
    "submitter_id:string[]",  ## human-readable GDC submitter barcode(s); an alias, not a node
    "tumor_descriptor",
    "specimen_type",
    "sample_type",
    "tissue_type",
    "preservation_method",
]
EDGE_ATTRIBUTES = [
    ## core fields - all edges should have ths other fields are optional
    ":START_ID",
    ":END_ID",
    ":TYPE",
    "raw_type",  ## GDC-native relation (e.g. "has_aliquot") preserved alongside the Biolink predicate
    "source:string[]",
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
home_dir: str = os.getenv("HOME") or "/"
GDC_CACHE_DIR = os.path.join(
    home_dir,
    ".data",
    "gdc",
)

## Prefix GDC UUIDs with gdc to conform to biolink curies
GDC_CURIE_PREFIX = "gdc"

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
