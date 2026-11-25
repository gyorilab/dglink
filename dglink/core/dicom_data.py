"""
Extract knowledge graph information from DICOM files.

This module reads the header of a local DICOM file into a DICOM series node
linked from the group (for example a study) the file belongs to, and grounds
unstructured header fields to ontology terms with Gilda.
"""

from .constants import UNSTRUCTURED_DICOM_FIELDS
from .nodes import NodeSet
from .edges import EdgeSet
from .optional_deps import import_optional

pydicom = import_optional("pydicom", "dicom")
from bioregistry import normalize_curie
from indra.ontology.bio import bio_ontology
from gilda import annotate


def process_dicom(path: str, file_id: str, group_identifier: str,
                  node_set: NodeSet,
                  edge_set: EdgeSet) -> tuple[NodeSet, EdgeSet]:
    """Add the series of a local DICOM file and its grounded fields to a graph.

    Parameters
    ----------
    path :
        Local path of the DICOM file.
    file_id :
        Identifier of the file, stored on the series and entity nodes.
    group_identifier :
        Identifier of the group the file belongs to, from which edges to the
        series and the grounded entities start.
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
    source = set(["dicom_data", "experimental_data"])
    header = pydicom.dcmread(path)
    node_set.update_nodes(
        {
            "curie:ID": header.get(
                "SeriesInstanceUID", "SeriesInstanceUID_Missing"
            ),
            ":LABEL": "DICOM_series",
            "name": header.get(
                "SeriesInstanceUID", "SeriesInstanceUID_Missing"
            ),
            "file_id:string[]": file_id,
            "source:string[]": source,
            "PatientID": header.get("PatientID", ""),
            "AccessionNumber": header.get("AccessionNumber", ""),
            "Modality": header.get("Modality", ""),
            "PatientSex": header.get("PatientSex", ""),
            "PatientAge": header.get("PatientAge", ""),
            "SOPClassUID": header.get("SOPClassUID", ""),
            "Manufacturer": header.get("Manufacturer", ""),
            "SeriesInstanceUID": header.get("SeriesInstanceUID", ""),
        }
    )
    edge_set.update_edges(
        {
            ":START_ID": group_identifier,
            ":END_ID": header.get(
                "SeriesInstanceUID", "SeriesInstanceUID_Missing"
            ),
            ":TYPE": f"has_dicom",
            "source:string[]": source,
        }
    )
    for dcm_field in UNSTRUCTURED_DICOM_FIELDS:
        res = header.get(str(dcm_field), None)
        ans = annotate(res)
        if ans:
            nsid = ans[0].matches[0].term
            node_set.update_nodes(
                {
                    "curie:ID": normalize_curie(f"{nsid.db}:{nsid.id}"),
                    ":LABEL": bio_ontology.get_type(nsid.db, nsid.id),
                    "name": nsid.entry_name,
                    "file_id:string[]": file_id,
                    "source:string[]": source,
                    "raw_texts:string[]": res,
                }
            )
            edge_set.update_edges(
                {
                    ":START_ID": group_identifier,
                    ":END_ID": normalize_curie(f"{nsid.db}:{nsid.id}"),
                    "source:string[]": source,
                }
            )
    return node_set, edge_set
