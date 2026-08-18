from pystow import module

## module store info ##
NCI_PDC_CACHE_DIR = module("nci_proteomic_data_commons").base

## API endpoints
PDC_GQL_ENDPOINT = "https://pdc.cancer.gov/graphql"
PDC_SWAGGER_ENDPOINT = ""

## portal specific tabular file CHOICES ##
NCI_TABULAR_FILE_TYPES = ["csv", "xls", "xlsx", "ods", "txt", "tsv"]


NODE_ATTRIBUTES = [
    ## core fields - all nodes should have these, the rest are optional
    "curie:ID",
    ":LABEL",
    "raw_label",  ## PDC-native type (e.g. "aliquot") kept alongside the Biolink category
    "name",
    "iri",
    "grounded:boolean",  ## true if name/curie came from ontology grounding, false if raw text
    "source:string[]",
    "submitter_id:string[]",  ## human-readable PDC submitter id(s); an alias, not a node
    ## study fields
    "analytical_fraction",
    "experiment_type",
    "acquisition_type",
    ## disease / site metadata (carried on study, case, sample, disease nodes)
    "disease_type",
    "primary_site",
    ## biospecimen fields
    "sample_type",
    "taxon",
    "pool",
    "status",
    ## NB: tabular provenance (raw_texts / columns / file_id) is NOT a node attribute — it
    ## rides on the group -> entity edge (see EDGE_ATTRIBUTES) to stay per-portal after merge
]
EDGE_ATTRIBUTES = [
    ## core fields - all edges should have these, the rest are optional
    ":START_ID",
    ":END_ID",
    ":TYPE",
    "raw_type",  ## PDC-native relation preserved alongside the Biolink predicate
    "source:string[]",
    ## disease qualifiers carried on the parent -> disease edge (kept off the grounded
    ## Disease concept node so it merges across portals)
    "disease_type",
    "primary_site",
    ## tabular provenance — which file / column / raw text an extracted entity was found in
    "raw_texts:string[]",
    "columns:string[]",
    "file_id:string[]",
]

## --- Biolink mapping ------------------------------------------------------
## PDC entity ids are UUIDs (study_id, case_id, sample_id, ...) and are not
## CURIEs; Biolink requires CURIE node ids, so we mint a single `pdc:` prefix and
## apply it to every node id and edge endpoint consistently. Disease/site names
## are not UUIDs, so they are slugged and given the same prefix.
PDC_CURIE_PREFIX = "ncipdc"

## PDC-native entity type -> concrete Biolink category (the native distinction is
## kept in `raw_label`). Program/Project/Study are all study-like groupings; the
## specimen subdivisions have no dedicated Biolink class so they map to
## biolink:MaterialSample.
PDC_LABEL_TO_BIOLINK = {
    "program": "biolink:Study",
    "project": "biolink:Study",
    "study": "biolink:Study",
    "case": "biolink:Case",
    "sample": "biolink:MaterialSample",
    "aliquot": "biolink:MaterialSample",
    "disease": "biolink:Disease",
}
PDC_DEFAULT_BIOLINK_CATEGORY = "biolink:NamedThing"

## PDC-native edge type -> Biolink predicate. The native relation is kept in
## `raw_type`. `derives_from` edges point from the child specimen up to its parent
## (aliquot -> sample -> case), matching GDC's specimen hierarchy convention.
PDC_EDGE_TO_BIOLINK = {
    "Has_Project": "biolink:has_part",  ## program -> project
    "Has_Study": "biolink:has_part",  ## project -> study
    "Has_Case": "biolink:has_part",  ## study -> case
    "Derived_From_Case": "biolink:derives_from",  ## sample -> case
    "Derived_From_Sample": "biolink:derives_from",  ## aliquot -> sample
    "Associated_Disease": "biolink:associated_with",  ## study/case -> disease
}


def pdc_curie(value: str) -> str:
    """Prefix a raw PDC id (or slugged name) as a `pdc:` CURIE (Biolink requires CURIE ids)."""
    return f"{PDC_CURIE_PREFIX}:{value}"
