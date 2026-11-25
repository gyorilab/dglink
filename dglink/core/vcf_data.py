"""
Extract knowledge graph information from VCF (Variant Call Format) files.

This module reads local VCF files to extract genetic variants, sample
information, and metadata into a knowledge graph structure.
"""

from .nodes import NodeSet
from .edges import EdgeSet
from .optional_deps import import_optional

vcf = import_optional("vcf", "vcf")
from bioregistry import normalize_curie, get_iri, parse_curie
import logging

logger = logging.getLogger(__name__)
data_source = set(["vcf_data", "experimental_data"])


def extract_variants(
    path: str, file_id: str, node_set: NodeSet, edge_set: EdgeSet
) -> tuple[NodeSet, EdgeSet, bool]:
    """Add the genetic variants of a VCF file to the knowledge graph.

    Parses VCF files to extract variant identifiers (dbSNP rs IDs) and
    creates:
    - Nodes for each genetic variant with normalized CURIEs and IRIs
    - Edges connecting samples to their variants

    Parameters
    ----------
    path :
        Local path of the VCF file, gzip-compressed if it ends in .gz.
    file_id :
        Identifier of the file, stored on the variant nodes.
    node_set :
        Existing set of nodes to update.
    edge_set :
        Existing set of edges to update.

    Returns
    -------
    node_set : NodeSet
        The updated node set.
    edge_set : EdgeSet
        The updated edge set.
    able_to_process : bool
        False if the file could not be parsed.

    Notes
    -----
    Only processes variants with dbSNP rs identifiers. A file that cannot be
    parsed is logged and reported as not processed.
    """
    compressed = path.endswith(".gz")
    read_cmd = "rb" if compressed else "r"
    able_to_process = True
    try:
        with open(path, mode=read_cmd) as f:
            vcf_reader = vcf.Reader(f, compressed=compressed)

            for record in vcf_reader:
                raw_id = record.ID
                if raw_id is not None and raw_id.startswith("rs"):
                    curie = normalize_curie(f"dbsnp:{raw_id}")
                    parsed_curie = parse_curie(curie)

                    # Create variant node
                    node_set.update_nodes(
                        {
                            "curie:ID": curie,
                            ":LABEL": "genetic_variant",
                            "iri": get_iri(
                                prefix=parsed_curie.prefix,
                                identifier=parsed_curie.identifier,
                            ),
                            "file_id:string[]": file_id,
                            "source:string[]": data_source,
                            "chrom": str(record.CHROM),
                            "pos": str(record.POS),
                            "ref": str(record.REF),
                            "alt": str(record.ALT),
                        }
                    )
                    # Only create edges for samples that have the variant
                    for sample in record.samples:
                        genotype = sample["GT"]  # e.g., '0/1', '1/1', '0/0'
                        # Only connect if variant is present (not 0/0)
                        if genotype and genotype not in ["0/0", "./.", "0|0", ".|."]:
                            edge_set.update_edges(
                                {
                                    ":START_ID": sample.sample,
                                    ":END_ID": curie,
                                    ":TYPE": "has_genetic_variant",
                                    "source:string[]": data_source,
                                    "genotype": str(genotype),
                                    "quality": str(record.QUAL),
                                }
                            )
    except Exception as e:
        logger.error(
            f"Error extracting variants from {file_id}, this file may be misformed: {e}"
        )
        able_to_process = False

    return node_set, edge_set, able_to_process


def extract_vcf_metadata(
    path: str, file_id: str, study_id: str, node_set: NodeSet, edge_set: EdgeSet
) -> tuple[NodeSet, EdgeSet]:
    """Extract metadata and sample information from VCF file headers.

    Parses VCF metadata to create knowledge graph nodes and relationships for:
    - File format specifications
    - Reference genome information
    - Processing commands (source and GATK command lines)
    - Sample information and relationships to study

    Creates edges connecting samples to their study, file format, reference
    genome, and processing commands.

    Parameters
    ----------
    path :
        Local path of the VCF file, gzip-compressed if it ends in .gz.
    file_id :
        Identifier of the file, stored on the metadata and sample nodes.
    study_id :
        Identifier of the study the samples belong to.
    node_set :
        Existing set of nodes to update.
    edge_set :
        Existing set of edges to update.

    Returns
    -------
    node_set : NodeSet
        The updated node set.
    edge_set : EdgeSet
        The updated edge set.
    """
    compressed = path.endswith(".gz")
    read_cmd = "rb" if compressed else "r"
    with open(path, mode=read_cmd) as f:
        compressed = path.endswith(".gz")
        vcf_reader = vcf.Reader(f, compressed=compressed)
    meta = vcf_reader.metadata
    vcf_format = meta.get("fileformat", "vcf_format_missing")
    reference = meta.get("reference", "reference_fasta_missing")
    vcf_cmnds = meta.get("source", ["vcf_command_missing"])
    node_set.update_nodes(
        {
            "curie:ID": vcf_format,
            ":LABEL": "VCF_file_format",
            "name": vcf_format,
            "file_id:string[]": file_id,
            "source:string[]": data_source,
        }
    )
    node_set.update_nodes(
        {
            "curie:ID": reference,
            ":LABEL": "VCF_reference",
            "name": reference,
            "file_id:string[]": file_id,
            "source:string[]": data_source,
        }
    )
    ## extract the sources
    for source in vcf_cmnds:
        node_set.update_nodes(
            {
                "curie:ID": source,
                ":LABEL": "VCF_command",
                "name": source,
                "file_id:string[]": file_id,
                "source:string[]": data_source,
            }
        )
    ## source and commands are similar so merge to one node type
    ## extract the commands
    cmnds = meta.get("GATKCommandLine", [])
    for cmnd in cmnds:
        cmnd_id = cmnd.get("ID", "missing_command")
        node_set.update_nodes(
            {
                "curie:ID": cmnd_id,
                ":LABEL": "VCF_command",
                "name": cmnd_id,
                "file_id:string[]": file_id,
                "source:string[]": data_source,
            }
        )
        vcf_cmnds.append(cmnd_id)
    ## go through all possible samples
    for sample in vcf_reader.samples:
        ## add sample as node
        node_set.update_nodes(
            {
                "curie:ID": sample,
                ":LABEL": "sample",
                "name": sample,
                "file_id:string[]": file_id,
                "source:string[]": data_source,
            }
        )
        ## add edge between sample and project
        edge_set.update_edges(
            {
                ":START_ID": study_id,
                ":END_ID": sample,
                ":TYPE": "has_sample",
                "source:string[]": data_source,
            }
        )
        ## add edges from each sample to the file format and reference
        edge_set.update_edges(
            {
                ":START_ID": sample,
                ":END_ID": vcf_format,
                ":TYPE": "has_vcf_format",
                "source:string[]": data_source,
            }
        )
        edge_set.update_edges(
            {
                ":START_ID": sample,
                ":END_ID": reference,
                ":TYPE": "has_vcf_reference",
                "source:string[]": data_source,
            }
        )
        ## add each of the command ids to the sample
        for vcf_cmnd in vcf_cmnds:
            edge_set.update_edges(
                {
                    ":START_ID": sample,
                    ":END_ID": vcf_cmnd,
                    ":TYPE": "has_vcf_command",
                    "source:string[]": data_source,
                }
            )
    return node_set, edge_set
