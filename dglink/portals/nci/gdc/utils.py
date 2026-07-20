from .constants import (
    CASES_ENDPNT,
    FILE_FIELDS,
    DATA_ENDPNT,
    GDC_CACHE_DIR,
    GDC_LABEL_TO_BIOLINK,
    GDC_DEFAULT_BIOLINK_CATEGORY,
    GDC_RELATION,
    GDC_CURIE_PREFIX,
)
from dglink import NodeSet, EdgeSet, write_graph
from dglink.core.constants import REPORT_PATH

import json
import tqdm
import requests
import polars as pl
import os
import re
from subprocess import run
from typing import Iterator


def connect_cases_to_files(
    hits, file_to_cases: pl.DataFrame | None = None
) -> pl.DataFrame:
    """
    connects all files from a case API call to their associated case id. Updates the data frame.
    """
    records = []
    for hit in hits:
        case_id = hit.get("case_id", "case_id_missing")
        case_files = hit.get("files", [])
        for case_file in case_files:
            file_record = {
                "file_id": case_file.get("file_id", "file_id_missing"),
                "case_id": case_id,
            }
            for field in FILE_FIELDS:
                file_record[f"file_{field}"] = case_file.get(
                    field, f"file_{field}_missing"
                )
            records.append(file_record)
    cases_df = pl.from_dicts(records)
    if file_to_cases is None:
        return cases_df
    if len(records) == 0:
        return file_to_cases
    ## stack files and ensure uniqueness
    return file_to_cases.vstack(cases_df).unique()


def gdc_curie(uuid: str) -> str:
    """Prefix a raw GDC UUID as a `gdc:` CURIE (Biolink requires CURIE node ids)."""
    return f"{GDC_CURIE_PREFIX}:{uuid}"


def get_sample_metadata(hits: list, node_set: NodeSet):
    """
    Manually pulls all metadata field for a sample from the cases end point. Could be better done with existing GDC graph.
    Samples are typed as biolink:MaterialSample with the GDC-native type in raw_label
    and the human-readable submitter barcode kept as an alias.
    """
    for hit in hits:
        for sample in hit.get("samples", []):
            node_set.update_nodes(
                {
                    "curie:ID": gdc_curie(sample.get("sample_id", "sample_id_missing")),
                    ":LABEL": GDC_LABEL_TO_BIOLINK["sample"],
                    "raw_label": "sample",
                    "submitter_id:string[]": sample.get("submitter_id", ""),
                    "tumor_descriptor": sample.get(
                        "tumor_descriptor", "tumor_descriptor_missing"
                    ),
                    "specimen_type": sample.get(
                        "specimen_type", "specimen_type_missing"
                    ),
                    "sample_type": sample.get("sample_type", "sample_type_missing"),
                    "tissue_type": sample.get("tissue_type", "tissue_type_missing"),
                    "preservation_method": sample.get(
                        "preservation_method", "preservation_method_missing"
                    ),
                }
            )


# TODO: Expand logic
def process_case_hierarchy(hits, node_set, edge_set):
    """Extract the case hierarchy and add it to the graph as a Biolink-conformant subgraph.

    Every node carries a valid Biolink category in :LABEL (with the GDC-native type kept
    in raw_label) and a `gdc:`-prefixed CURIE id. Edges use Biolink predicates (with the
    GDC-native relation kept in raw_type). The `submitter_*` barcode fields are alternate
    identifiers, not entities, so they never become nodes; the authoritative barcodes are
    attached as the submitter_id alias from the case scalar and the nested samples object
    (the top-level submitter_<type>_ids arrays are not positionally aligned with the UUIDs,
    so specimen barcodes below the sample level are not reattached).
    """
    for hit in hits:
        case_uuid = hit.get("id", "case_id_missing")
        case_id = gdc_curie(case_uuid)
        node_set.update_nodes(
            {
                "curie:ID": case_id,
                ":LABEL": GDC_LABEL_TO_BIOLINK["case"],
                "raw_label": "case",
                "submitter_id:string[]": hit.get("submitter_id", ""),
            }
        )
        for key in hit.keys():
            ## the case itself is handled above; submitter_* fields are aliases, not nodes
            if key in ("id", "case_id"):
                continue
            if key.endswith("_ids"):
                native_type = key.removesuffix("_ids")
                vals = hit.get(key, [])
            elif key.endswith("_id"):
                native_type = key.removesuffix("_id")
                vals = [hit.get(key, "")]
            else:
                continue
            if native_type.startswith("submitter"):
                continue
            ## only emit structural entities we have a Biolink mapping for
            if native_type not in GDC_RELATION:
                continue
            predicate, direction = GDC_RELATION[native_type]
            category = GDC_LABEL_TO_BIOLINK.get(
                native_type, GDC_DEFAULT_BIOLINK_CATEGORY
            )
            ## Note: the parallel submitter_<type>_ids arrays are NOT positionally
            ## aligned with the UUID arrays, so we do not attach barcodes here.
            ## Authoritative submitter barcodes come from the case scalar (above)
            ## and the nested samples object (get_sample_metadata).
            for val in vals:
                if not val:
                    continue
                node_id = gdc_curie(val)
                node_set.update_nodes(
                    {
                        "curie:ID": node_id,
                        ":LABEL": category,
                        "raw_label": native_type,
                    }
                )
                start_id, end_id = (
                    (node_id, case_id)
                    if direction == "child_to_case"
                    else (case_id, node_id)
                )
                edge_set.update_edges(
                    {
                        ":START_ID": start_id,
                        ":END_ID": end_id,
                        ":TYPE": predicate,
                        "raw_type": f"has_{native_type}",
                    }
                )


def max_call_size(end_point):
    """small helper function to get max size for an api call"""
    params = {"format": "JSON", "size": 0}
    response = requests.get(end_point, params=params)
    return response.json()["data"]["pagination"]["total"]


def get_case_hierarchy(
    node_set: NodeSet,
    edge_set: EdgeSet,
    case_list: list | None = None,
    number_cases_arg: int | None = None,
    batch_length: int = 500,
):
    """connect cases to all down stream objects. Saves files as a tsv in `dglink/resources/reports/cases_to_files.tsv` , and connects all meta objects (samples, studies, projects, etc.) to case in the graph."""
    ## Filter for a specific list of cases
    if case_list is not None:
        number_cases: int = len(case_list)
        filters = {
            "op": "and",
            "content": [
                {"op": "in", "content": {"field": "case_id", "value": case_list}}
            ],
        }
    ## if no set of cases specified either process the first `number_cases` cases or process all.
    else:
        number_cases = number_cases_arg or max_call_size(CASES_ENDPNT)
        filters = {}
    params: dict[str, str | int] = {
        "filters": json.dumps(filters),
        "format": "JSON",
        "expand": "files,samples",  ## get associated files and expand samples
    }
    cases_to_files_path = os.path.join(GDC_CACHE_DIR, "cases_to_files.tsv")
    ## load from cache if already exists
    if os.path.exists(cases_to_files_path):
        case_to_files = pl.read_csv(cases_to_files_path, separator="\t")
    else:
        case_to_files = None
    for x in tqdm.tqdm(range(0, number_cases, batch_length)):
        params["from"] = x
        params["size"] = min(batch_length, number_cases - x)
        response = requests.get(CASES_ENDPNT, params=params)
        json_resp = json.loads(response.content.decode("utf-8"))
        data = json_resp.get("data", dict())
        hits = data.get("hits", [dict()])
        case_to_files = connect_cases_to_files(hits=hits, file_to_cases=case_to_files)
        get_sample_metadata(hits, node_set=node_set)
        process_case_hierarchy(hits=hits, node_set=node_set, edge_set=edge_set)
        if x % (batch_length * 10) == 0:
            write_graph(node_set, edge_set)
    write_graph(node_set, edge_set)
    os.makedirs(GDC_CACHE_DIR, exist_ok=True)
    if isinstance(case_to_files, pl.DataFrame):
        case_to_files.write_csv(cases_to_files_path, separator="\t")


def download_tabular_files(case_list: list):
    """download tabular files associated with a case list and save in `~/.data/gdc/files`"""
    files_df = pl.read_csv(
        os.path.join(GDC_CACHE_DIR, "cases_to_files.tsv"), separator="\t"
    )
    files_dir = os.path.join(GDC_CACHE_DIR, "files")
    ## filter for available files and also those that are tsv
    to_load = (
        files_df.filter(
            pl.col("file_access").eq("open")
            & pl.col("file_data_format").eq("TSV")
            & pl.col("case_id").is_in(case_list)
        )["file_id"]
        .unique()
        .sort()
        .to_list()
    )
    ## only download files that are not already in the cache
    undownloaded_files = filter(
        lambda x: not os.path.exists(os.path.join(files_dir, x)), to_load
    )
    params = {"ids": list(undownloaded_files)}
    ## if there are no files to download exit
    if len(params["ids"]) < 1:
        return

    response = requests.post(
        DATA_ENDPNT,
        data=json.dumps(params),
        headers={"Content-Type": "application/json"},
    )

    response_head_cd = response.headers["Content-Disposition"]

    file_name = re.findall("filename=(.+)", response_head_cd)[0]

    os.makedirs(files_dir, exist_ok=True)

    archive_path = os.path.join(files_dir, file_name)

    with open(archive_path, "wb") as output_file:
        output_file.write(response.content)
    ## stop all files from going in separate folder
    unzip_cmd = [
        "tar",
        "-xvzf",
        archive_path,
        "-C",
        files_dir,
    ]
    run(unzip_cmd)
    ## quick and dirty stuff to make the downloads nicer to work with
    rm_cmd = ["rm", archive_path, os.path.join(files_dir, "MANIFEST.txt")]
    run(rm_cmd)


def get_tabular_iterator(case_list: list) -> Iterator:
    """
    returns a 2d iterator of case_ids and associated file_paths
    """
    files_df = pl.read_csv(
        os.path.join(GDC_CACHE_DIR, "cases_to_files.tsv"), separator="\t"
    )
    ## get only tabular files
    project_files = files_df.filter(
        pl.col("file_access").eq("open")
        & pl.col("file_data_format").eq("TSV")
        & pl.col("case_id").is_in(case_list)
    ).with_columns(
        file_paths=pl.format(  # Better than string concat
            "{}/files/{}/{}",
            pl.lit(GDC_CACHE_DIR),
            pl.col("file_id"),
            pl.col("file_file_name"),
        )
    )

    ## group by case; yield the gdc: CURIE (not the raw uuid) as the group id so
    case_files = (
        project_files.group_by("case_id", maintain_order=True)
        .agg([pl.col("file_paths"), pl.col("file_id")])
        .with_columns(
            case_curie=pl.format("{}:{}", pl.lit(GDC_CURIE_PREFIX), pl.col("case_id"))
        )
        .select(["case_curie", "file_paths", "file_id"])
    )
    return case_files.iter_rows()
