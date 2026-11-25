import os

from dglink.core.optional_deps import import_optional

synapseclient = import_optional("synapseclient", "synapse")

# --- Synapse ------------------------------------------------------------------
token = os.getenv("SYNAPSE_AUTHTOKEN")
if token:
    syn = synapseclient.login(authToken=token)
else:
    try:
        syn = synapseclient.login()
    except Exception as e:
        raise ValueError(
            "Can not authenticate to synapse, either set credentials in ~/.synapseConfig or set $SYNAPSE_AUTHTOKEN in your environment"
        ) from e

NF_STUDIES_BASE_URL = "https://nf.synapse.org/Explore/Studies/{study_id}/Details"
# Every NF study, with its name and summary (cached by utils.download_all_nf_studies)
NF_STUDIES_TABLE = "syn52694652"
NF_DATA_PORTAL_CACHE_DIR = os.path.join(
    os.getenv("HOME"),
    ".data",
    "nf_data_portal",
)
# Portal file view (every file's annotations); unlike the files materialized view
# (syn52702673) it has modelSystemName
NF_FILES_VIEW = "syn16858331"
# Publications: the portal's own table, and the NF tools db table of papers that used or
# developed a tool (most are not tied to any portal study)
PORTAL_PUBLICATIONS_TABLE = "syn16857542"
TOOLS_PUBLICATIONS_TABLE = "syn26486839"
# Flattened view of every tool (see nf-osi/nf-research-tools-schema MATERIALIZED_VIEWS.md)
NF_TOOLS_TABLE = "syn51730943"
# Tools db link tables (resourceId <-> publicationId), keyed by the edge raw_type
TOOL_PUBLICATION_TABLES = {
    "usage": "syn26486841",  # Publications that used the tool
    "development": "syn26486807",  # Publications that developed the tool
}

NF_CURIE_PREFIX = "nfdataportal"


# --- NF research tools: tools view columns -------------------------------------
# Every tools view column is in exactly one group below; get_tool_nodes warns about any
# other column, so upstream schema changes are noticed.

# Tools view column -> node attribute, for columns kept as plain tool properties
# (list columns are string[]). Fields named in the nf-research-tools-schema LinkML model,
# except the view's own renames (computationalTool*, organoid*, pdm*, clinicalAssessment*).
NF_TOOL_PROPERTIES = {
    # All tools
    "synonyms": "synonyms:string[]",
    # Raw values; grounded values are also edges (NF_TOOL_DISEASE_FIELDS)
    "geneticDisorder": "geneticDisorder:string[]",
    "manifestation": "manifestation:string[]",
    "description": "description",
    # "usageRequirements": "usageRequirements:string[]",
    # "howToAcquire": "howToAcquire",
    "availability": "availability",
    # Donor (cell lines)
    "sex": "sex",
    "race": "race",
    "age": "age",
    "organ": "organ",
    "tissue": "tissue:string[]",
    "cellLineCategory": "cellLineCategory",
    # Animal models
    "backgroundStrain": "backgroundStrain",
    "backgroundSubstrain": "backgroundSubstrain",
    "animalState": "animalState",
    # Genetic reagents (insertName raw text is kept; the grounded gene is an edge)
    "insertName": "insertName",
    "insertSpecies": "insertSpecies:string[]",
    "vectorType": "vectorType:string[]",
    "selectableMarker": "selectableMarker",
    "resistance": "resistance",
    # Antibodies (targetAntigen raw text keeps the epitope, e.g. "NF1 (aa 2760 C-term)")
    "targetAntigen": "targetAntigen",
    "reactiveSpecies": "reactiveSpecies:string[]",
    "hostOrganism": "hostOrganism",
    "conjugate": "conjugate",
    # Biobanks
    "biobankName": "biobankName",
    "biobankURL": "biobankURL",
    "specimenTissueType": "specimenTissueType:string[]",
    "specimenPreparationMethod": "specimenPreparationMethod:string[]",
    "specimenFormat": "specimenFormat:string[]",
    "specimenType": "specimenType:string[]",
    # Patient-derived models and organoids
    "modelType": "modelType",
    "pdmModelSystemType": "pdmModelSystemType",
    "pdmHostStrain": "pdmHostStrain",
    "engraftmentSite": "engraftmentSite",
    "organoidModelType": "organoidModelType",
    "organoidDerivationSource": "organoidDerivationSource",
    "organoidCellTypes": "organoidCellTypes:string[]",
    "cultureSystem": "cultureSystem",
    # Computational tools
    "computationalToolType": "computationalToolType",
    "computationalToolLanguage": "computationalToolLanguage:string[]",
    "computationalToolPlatformSupport": "computationalToolPlatformSupport",
    "softwareType": "softwareType",
    "licenseType": "licenseType",
    "downloadURL": "downloadURL",
    # Clinical assessment tools
    "clinicalAssessmentType": "clinicalAssessmentType",
    "clinicalAssessmentTargetPopulation": "clinicalAssessmentTargetPopulation",
    "clinicalAssessmentDiseaseSpecific": "clinicalAssessmentDiseaseSpecific",
    "availabilityStatus": "availabilityStatus",
}
# Tools view columns used for the node id / category / name
NF_TOOL_ID_COLUMNS = {"resourceId", "rrid", "resourceName", "resourceType"}
# Tools view columns that only become edges (grounded diseases, taxa, investigators)
NF_TOOL_EDGE_COLUMNS = {
    "tumorType", "species", "investigatorName", "institution",
    "orcid",  # Read by utils._investigator_orcids, which gives investigators their ORCID ids
}
# Tools view columns left out: AI-generated text, tools db bookkeeping, and portal UI
# summaries derived from the other columns
NF_TOOL_DROPPED_COLUMNS = {
    "investigatorSynapseId",  # A Synapse user id; the ORCID identifies the person
    "aiSummary", "dateAdded", "dateModified", "latestPublicationDate",
    "completenessCategory", "availabilityCategory", "criticalInfoCategory",
    "otherInfoCategory", "observationCategory",
}


# --- NF research tools: edges --------------------------------------------------
# Tools view fields grounded to diseases (via NF_GROUND_FIELD_CATEGORIES) and linked to the tool
NF_TOOL_DISEASE_FIELDS = ["geneticDisorder", "manifestation", "tumorType"]
# Tool types that are disease models, linked to their diseases with biolink:model_of;
# other tools (biobanks, clinical assessment and computational tools) use related_to_at_instance_level
NF_TOOL_MODEL_TYPES = {"Cell Line", "Animal Model", "Patient-Derived Model", "Organoid Protocol"}

# Tools view species (Latin names, plus the odd common name) -> NCBI taxon and its name.
# A fixed map: the vocabulary is small and gilda does not ground taxa reliably.
NF_SPECIES_TO_TAXON = {
    "Homo sapiens": ("ncbitaxon:9606", "Homo sapiens"),
    "Human": ("ncbitaxon:9606", "Homo sapiens"),
    "Mus musculus": ("ncbitaxon:10090", "Mus musculus"),
    "Mus musculus (humanized)": ("ncbitaxon:10090", "Mus musculus"),
    "Rattus norvegicus": ("ncbitaxon:10116", "Rattus norvegicus"),
    "Danio rerio": ("ncbitaxon:7955", "Danio rerio"),
    "Drosophila melanogaster": ("ncbitaxon:7227", "Drosophila melanogaster"),
    "Saccharomyces cerevisiae": ("ncbitaxon:4932", "Saccharomyces cerevisiae"),
    "Sus scrofa": ("ncbitaxon:9823", "Sus scrofa"),
    "Canis lupus familiaris": ("ncbitaxon:9615", "Canis lupus familiaris"),
    "Cricetulus griseus": ("ncbitaxon:10029", "Cricetulus griseus"),
    "Escherichia coli": ("ncbitaxon:562", "Escherichia coli"),
    "Xenopus laevis": ("ncbitaxon:8355", "Xenopus laevis"),
    "Rhesus macaque": ("ncbitaxon:9544", "Macaca mulatta"),
}
# NF_KG_SCHEMA subject for a tool's species edge, by tool category: cell lines are "things
# with taxon" (in_taxon); animal models are strains, i.e. taxa themselves (subclass_of);
# any other tool falls back to the "tool" subject
NF_TOOL_SPECIES_HEAD = {
    "biolink:CellLine": "tool_with_taxon",
    "biolink:OrganismTaxon": "tool_strain",
}

# Tools view gene fields -> the field giving the gene's species (None: human). Antibody
# targets are named for the human protein; plasmid inserts say whose gene they carry.
NF_TOOL_GENE_FIELDS = {
    "targetAntigen": None,
    "insertName": "insertSpecies",
}
# Human genes ground to HGNC (Gene), other species to UniProt (Protein)
NF_GENE_CATEGORIES = {"biolink:Gene", "biolink:Protein"}
NF_UNGROUNDED_GENE_CATEGORY = "biolink:Gene"

# Tools view fields linked to the tool as Agent nodes: the investigator who developed it
# and their institution (each a single value; institutions are not split on commas, which
# also separate department and university)
NF_TOOL_AGENT_FIELDS = ["investigatorName", "institution"]

# File annotations matched against tool names to link a study to the tools its data come
# from: modelSystemName is the curated field, specimen / individual ids are often named
# after the cell line or model too
NF_TOOL_STUDY_FIELDS = ["modelSystemName", "specimenID", "individualID"]


# --- node and edge attributes ----------------------------------------------------
NODE_ATTRIBUTES = [
    # Core fields - all nodes should have these, the other fields are optional
    "curie:ID",
    ":LABEL",
    "raw_label",  # NF Data Portal native type (e.g. "initiative") kept alongside the Biolink category
    "name",
    "iri",
    "grounded:boolean",
    "source:string[]",
    # Synapse project fields
    "study_url",
    "summary",  # Study summary from the NF studies table
    "studyStatus:string[]",
    "parentId:string[]",
    "dataStatus:string[]",
    "dataType:string[]",
    "grantDOI:string[]",
    "diseaseFocus:string[]",  # Raw portal values; grounded values are also edges
    "manifestation:string[]",
    # Tabular data fields
    "raw_texts:string[]",
    "columns:string[]",
    "file_id:string[]",
    # DICOM fields
    "PatientID",
    "AccessionNumber",
    "Modality",
    "PatientSex",
    "PatientAge",
    "SOPClassUID",
    "Manufacturer",
    # VCF fields
    "chrom",
    "pos",
    "ref",
    "alt",
    "genotype",
    "quality",
    # Publication fields
    "doi",
    "pmid",
    "year",
    "journal",
    # Tool fields
    "tool_type",
    *NF_TOOL_PROPERTIES.values(),
    # Author / investigator ORCIDs matched by name from the tools db (see utils.author_curie)
    "orcid_source",
    # Genomic fields
    "assay",
    "nf1Genotype",
    "nf2Genotype",
    "diagnosis",
    "tumorType",
]
# Tools share some attributes with studies (e.g. manifestation); keep the first of each
NODE_ATTRIBUTES = list(dict.fromkeys(NODE_ATTRIBUTES))

EDGE_ATTRIBUTES = [
    # Core fields - all edges should have these, the other fields are optional
    ":START_ID",
    ":END_ID",
    ":TYPE",
    "raw_type:string[]",  # NF Data Portal native relation preserved alongside the Biolink predicate
    "source:string[]",
    # Tabular provenance — which file / column / raw text a tabular-extracted entity was
    # found in (extract_df_graph now puts these on the group -> entity edge, not the node;
    # NF keeps them on nodes too for its non-tabular sources: fastq / meta / wiki / dicom)
    "raw_texts:string[]",
    "columns:string[]",
    "file_id:string[]",
    # Predicted similar project fields
    "jacquard_score",
    "score_cutoff",
    "intersection_score",
    "union_score",
    "shared_edges:string[]",
    "head_only_edges:string[]",
    "tail_only_edges:string[]",
    "edge_weights:string[]",
]


# --- portal fields ---------------------------------------------------------------
# Study metadata fields kept as Study attributes
PROJECT_ATTRS = [
    "studyStatus",
    "parentId",
    "dataStatus",
    "dataType",
    "grantDOI",
    "diseaseFocus",
    "manifestation",
]
# Study metadata fields that become nodes
METADATA_NODES = [
    "fundingAgency",
    "initiative",
    "institutions",
]
# Journal is a publication property, not a node: few papers share one, and has_publisher
# points at the publisher, not the journal
PUBLICATION_METADATA_NODES = ['author', 'fundingAgency']
WIKI_FIELDS = ["markdown", "title"]
# Fastq annotations
SPECIMEN_FIELDS = {"age": "PatientAge", "sex": "PatientSex"}
EXPERIMENT_FIELDS = ["assay", "nf1Genotype", "nf2Genotype", "diagnosis", "tumorType"]

# Placeholder values (compared lower-cased) are kept as attributes only, never as nodes,
# so they do not link unrelated studies / tools
NF_PLACEHOLDER_VALUES = {
    "multiple", "other", "unknown", "not applicable", "not reported",
    # Values that say there is no disease, or give a clinical status rather than a disease
    # (they would otherwise ground to e.g. "Genetic Diseases, Inborn" or HPO "Unaffected")
    "no known genetic disorder", "unaffected nerve", "clinically affected", "no symptoms",
    "mild or no symptoms", "clinically asymptomatic (self-reported)",
    "clinically asymptomatic but at risk",
}


# --- grounding -------------------------------------------------------------------
# Study / publication metadata fields that are grounded
GROUND_FIELDS = [
    "manifestation",
    "diseaseFocus",
]
# Biolink categories a grounding may have per field; anything else is treated as
# ungrounded. Any field listed here is grounded by utils.parse_dict_fields.
NF_GROUND_FIELD_CATEGORIES = {
    "diseaseFocus": {"biolink:Disease"},
    "manifestation": {
        "biolink:Disease",
        "biolink:PhenotypicFeature",
        "biolink:BiologicalProcess",
        "biolink:NamedThing",
    },
    # Tools db fields
    "geneticDisorder": {"biolink:Disease"},
    "tumorType": {"biolink:Disease"},
}
# Category for values that do not ground
NF_UNGROUNDED_CATEGORY = {
    "diseaseFocus": "biolink:Disease",
    "manifestation": "biolink:DiseaseOrPhenotypicFeature",
    "geneticDisorder": "biolink:Disease",
    "tumorType": "biolink:Disease",
}
# Partial groundings must cover at least this share of the value
NF_GROUNDING_MIN_SPAN_FRACTION = 0.5
# Wiki free text is noisy; categories dropped from wiki mentions (NamedThing is INDRA "other",
# mostly generic terms like "quality of life" or "methods")
NF_WIKI_EXCLUDED_CATEGORIES = {"biolink:NamedThing"}


# --- Biolink mapping -------------------------------------------------------------
# NF Data Portal native entity type -> concrete Biolink category (the native distinction is
# kept in `raw_label`).
NF_LABEL_TO_BIOLINK = {
    # Project related nodes
    "Project": "biolink:Study",
    # Metadata related nodes
    "initiative": "biolink:Study",
    "institutions": "biolink:Agent",
    "fundingAgency": "biolink:Agent",
    # Publication related nodes
    "publication": "biolink:Publication",
    "author": "biolink:Agent",
    # Tool related nodes
    "tool": "biolink:NamedThing",  # Refined per row by NF_TOOL_TYPE_TO_BIOLINK
    "investigatorName": "biolink:Agent",
    "institution": "biolink:Agent",
    # Modality specific fields
    "specimen": "biolink:MaterialSample",
    "sample": "biolink:MaterialSample",  # VCF samples
    "sequencing_experiment": "biolink:Dataset",
    "DICOM_series": "biolink:Dataset",
    "genetic_variant": "biolink:SequenceVariant",
    # VCF header nodes, to be folded into file properties
    "VCF_file_format": "biolink:NamedThing",
    "VCF_reference": "biolink:NamedThing",
    "VCF_command": "biolink:NamedThing",
}

# NF tools table `resourceType` -> Biolink category. Antibody, Genetic Reagent and
# Biobank have no Biolink class and fall back to NF_DEFAULT_BIOLINK_CATEGORY.
NF_TOOL_TYPE_TO_BIOLINK = {
    "Cell Line": "biolink:CellLine",
    # An animal model is a strain (strain nomenclature, IMSR / MMRRC RRIDs), and Biolink
    # OrganismTaxon "can also be used to represent strains or subspecies"
    "Animal Model": "biolink:OrganismTaxon",
}
NF_DEFAULT_BIOLINK_CATEGORY = "biolink:NamedThing"

# (subject, object) -> Biolink predicate. Subject / object are NF raw labels or field
# names, plus a few tool subjects picked by tool type / category: "tool_model"
# (NF_TOOL_MODEL_TYPES), "tool_with_taxon" and "tool_strain" (NF_TOOL_SPECIES_HEAD).
# The native field is kept in `raw_type`.
NF_KG_SCHEMA = {
    # Project level
    ("Project", "initiative"): "biolink:part_of",
    ("Project", "Project"): "biolink:related_to_at_instance_level",  # relatedStudies
    ("Project", "predicted_relatedStudies_GL"): "biolink:related_to_at_instance_level",
    ("Project", "institutions"): "biolink:related_to_at_instance_level",  # Contributor is Agent -> ICE
    ("Project", "fundingAgency"): "biolink:related_to_at_instance_level",
    ("Project", "diseaseFocus"): "biolink:related_to_at_instance_level",
    ("Project", "manifestation"): "biolink:related_to_at_instance_level",
    # A study whose files come from a tool (see NF_TOOL_STUDY_FIELDS); Study is not a
    # "biological process or activity", so has_participant / has_input do not apply
    ("Project", "tool"): "biolink:related_to_at_instance_level",
    # Wiki level: entities found in the study wiki, linked straight to the study
    ("Project", "wiki_mention"): "biolink:mentions",
    # Publication level
    ("Project", "publication"): "biolink:has_part",  # Publication studyId field
    ("publication", "author"): "biolink:has_author",
    ("publication", "diseaseFocus"): "biolink:related_to_at_instance_level",
    ("publication", "manifestation"): "biolink:related_to_at_instance_level",
    ("publication", "fundingAgency"): "biolink:related_to_at_instance_level",
    ("publication", "tool"): "biolink:mentions",  # Tools db Usage / Development tables
    # Tool level
    # Diseases: models are model_of their diseases, other tools only related to them
    ("tool_model", "geneticDisorder"): "biolink:model_of",
    ("tool_model", "manifestation"): "biolink:model_of",
    ("tool_model", "tumorType"): "biolink:model_of",
    ("tool", "geneticDisorder"): "biolink:related_to_at_instance_level",
    ("tool", "manifestation"): "biolink:related_to_at_instance_level",
    ("tool", "tumorType"): "biolink:related_to_at_instance_level",
    # Species: subclass_of formally needs "ontology class" ends, which OrganismTaxon lacks;
    # Biolink says a node can be read as an ontology class too
    ("tool_with_taxon", "species"): "biolink:in_taxon",
    ("tool_strain", "species"): "biolink:subclass_of",
    ("tool", "species"): "biolink:related_to_at_instance_level",
    # Genes an antibody targets / a plasmid carries (or, for shRNA, knocks down)
    ("tool", "targetAntigen"): "biolink:related_to_at_instance_level",
    ("tool", "insertName"): "biolink:related_to_at_instance_level",
    # The investigator who developed a tool, and their institution
    ("tool", "investigatorName"): "biolink:related_to_at_instance_level",
    ("tool", "institution"): "biolink:related_to_at_instance_level",
    # Not yet converted modules (fastq / dicom / vcf), carried over from the old flat map;
    # revisit each when its module is converted
    ("Project", "specimen"): "biolink:has_part",
    ("Project", "sample"): "biolink:has_part",  # VCF samples
    ("specimen", "sequencing_experiment"): "biolink:has_part",  # Direction to check in fastq.py
    ("specimen", "organism"): "biolink:related_to_at_instance_level",
    ("Project", "organism"): "biolink:related_to_at_instance_level",
    ("Project", "DICOM_series"): "biolink:has_part",
    ("sample", "genetic_variant"): "biolink:related_to_at_instance_level",  # has_sequence_variant needs a GenomicEntity subject
    # VCF header edges, to be folded into file properties
    ("sample", "VCF_file_format"): "biolink:related_to_at_instance_level",
    ("sample", "VCF_reference"): "biolink:related_to_at_instance_level",
    ("sample", "VCF_command"): "biolink:related_to_at_instance_level",
}

# Fields whose ungrounded nodes get their own id namespace instead of sharing their
# category's (so an author never merges with a funder or institution of the same name)
NF_OWN_NAMESPACE_FIELDS = {"author"}
# Fields naming people: ORCID when the tools db has one for the name, else an `author_` id,
# so a tool investigator and a publication author with the same full name are one node
NF_PERSON_FIELDS = {"author", "investigatorName"}
