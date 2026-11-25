from .constants import syn, NF_DATA_PORTAL_CACHE_DIR, NF_CURIE_PREFIX, NF_PLACEHOLDER_VALUES, NF_LABEL_TO_BIOLINK, \
    GROUND_FIELDS, NF_GROUND_FIELD_CATEGORIES, NF_GROUNDING_MIN_SPAN_FRACTION, NF_UNGROUNDED_CATEGORY, \
    NF_KG_SCHEMA, NF_STUDIES_BASE_URL, NF_OWN_NAMESPACE_FIELDS, NF_TOOLS_TABLE, NF_PERSON_FIELDS, \
    NF_STUDIES_TABLE
from dglink.core.constants import TABULAR_FILE_TYPES, DGLINK_CACHE, REPORT_PATH
from dglink.core import _is_empty
from dglink import NodeSet, EdgeSet
from dglink.core.grounding import ground_entity, slugify
from dglink.core.timing import TIMER

from bioregistry import normalize_curie, get_bioregistry_iri


import polars as pl
import pandas
from synapseutils import walk
from synapseclient.models import File

import os
import re
from pathlib import Path
import asyncio
from typing import Iterator, Union, Tuple
from functools import lru_cache
import tqdm
import logging 

logger = logging.getLogger(__name__)

SYNAPSE_ID = re.compile(r"syn\d+")
SOURCE_TO_HEAD = {
    'metadata' : 'Project',
    'publications' : 'publication',
    'tools' : 'tool',
}


def download_all_nf_studies():
    """Save a list of all studies on the NF Data Portal."""
    os.makedirs(Path(DGLINK_CACHE), exist_ok=True)
    query = syn.tableQuery(f"SELECT * FROM {NF_STUDIES_TABLE}")
    df = query.asDataFrame()
    df.to_csv(f"{DGLINK_CACHE}/all_nf_studies.tsv", sep="\t", index=False)

def _nf_studies_path() -> str:
    """Return the path of the cached NF studies table, downloading it first."""
    nf_studies_path = f"{DGLINK_CACHE}/all_nf_studies.tsv"
    if not os.path.exists(nf_studies_path):
        logger.info("NF Data Portal studies list not found.")
        logger.info("Pulling NF Data Portal studies list")
        download_all_nf_studies()
        logger.info(f"NF Data Portal studies list saved to {nf_studies_path}")
    return nf_studies_path


def get_all_nf_studies():
    """Return the ids of all studies on the NF Data Portal.

    The cached list of all studies is downloaded first if it does not exist.
    """
    return pandas.read_csv(_nf_studies_path(), sep="\t")["studyId"].to_list()


@lru_cache(maxsize=1)
def _studies_table() -> dict:
    """Return the rows of the NF studies table by studyId.

    Names and summaries are not in the project metadata. The table is loaded
    on first use, from the same cache as get_all_nf_studies.
    """
    df = pandas.read_csv(_nf_studies_path(), sep="\t")
    return {row["studyId"]: row for row in df.to_dict("records")}


def safe_download(syn_id:str)->str|None:
    """Download a file from the NF Data Portal, returning None on failure."""
    try:
        with TIMER.stage("download"):
            entity = syn.get(syn_id, ifcollision="keep.local") # Keep local file if already downloaded ie do not re-download.
        try:
            TIMER.add("download", files=1, bytes=os.path.getsize(entity.path))
        except (OSError, TypeError):
            TIMER.add("download", files=1)
        return entity.path
    except Exception:
        TIMER.add("download", failed=1)
        logger.warning(f"Could not download file {syn_id}", exc_info=True)
        return None

def fetch_nf_tabular_files()->pl.DataFrame:
    """Return all tabular files from the NF Data Portal and save their list.

    Returns
    -------
    pl.DataFrame
        The tabular files of all projects, with the paths they were downloaded
        to.
    """
    output_path:str = os.path.join(
            NF_DATA_PORTAL_CACHE_DIR, 
            'tabular_file_paths.tsv'
        )
    if os.path.exists(output_path):
        logger.warning(
            f"Loading tabular file paths from cache {output_path}: no files will be "
            "downloaded, so download timing will be empty. Delete it for a cold run."
        )
        TIMER.add("download", from_path_cache=1)
        tabular_project_files = pl.read_csv(output_path, separator='\t')
    else:
        logger.info("fetching tabular files from the NF data portal...")
        # Get a full list of studies
        project_ids = get_all_nf_studies()
        # Crawl all projects
        with TIMER.stage("crawl_file_listing"):
            project_files = get_projects_files(project_ids=project_ids, )
        # Filter for tabular projects
        file_types_pattern = f"({'|'.join(re.escape(s) for s in TABULAR_FILE_TYPES)})$"
        tabular_project_files = project_files.filter(
            pl.col("file_name").str.contains(file_types_pattern)
        )
        tabular_project_files = tabular_project_files.with_columns(
            file_path=pl.col('file_syn_id').map_elements(
                safe_download,
                return_dtype=pl.String
            )
        )
        os.makedirs(
            NF_DATA_PORTAL_CACHE_DIR, 
            exist_ok=True
        )
        tabular_project_files.write_csv(
            output_path, 
            separator='\t'
        )
    return tabular_project_files

def get_tabular_iterator(project_list: list) -> Iterator:
    """Return a 2D iterator of project ids and their associated file paths."""
    # Get a list of tabular files and download if need be
    tabular_files = fetch_nf_tabular_files()
    # Filter for files that could be downloaded and format the col names
    tabular_files = tabular_files.filter(pl.col("file_path").is_not_null()).select(
        ['project_syn_id','file_path', 'file_syn_id', ]
    ).rename(
        {'file_syn_id':'file_id', 'file_path':'file_paths'}
    )
    # Group by project id.
    project_files = tabular_files.group_by("project_syn_id", maintain_order=True).agg(
    [pl.col("file_paths"), pl.col("file_id")]
    )
    # Filter for only desired project id.
    project_files = project_files.filter(pl.col('project_syn_id').is_in(project_list))
    return project_files.iter_rows()

def get_project_files(
    project_syn_id: str, file_types: list = None, as_list: bool = False
) -> Union[pl.DataFrame, list]:
    """Return all files of a Synapse project, optionally filtered by extension.

    Uses cached data if available, otherwise crawls the project and updates
    the cache.

    There is a table that has many of the files already aggregated from the
    NF Data Portal, but it seems to be missing a lot of files that can be
    pulled by just crawling everything.

    Parameters
    ----------
    project_syn_id :
        Synapse project ID (e.g., 'syn12345678').
    file_types :
        Optional list of file extensions to filter by (e.g., ['.vcf', '.bam']).
    as_list :
        If True, returns a list of file_syn_ids instead of a DataFrame.

    Returns
    -------
    :
        DataFrame with columns [project_syn_id, file_syn_id, file_name], or a
        list of file_syn_ids if as_list=True.

    Examples
    --------
    >>> # Get all files as DataFrame
    >>> files = get_project_files('syn12345678')
    >>>
    >>> # Get only VCF files as list of IDs
    >>> vcf_ids = get_project_files('syn12345678', file_types=['.vcf'],
    ...                             as_list=True)
    """
    known_files = load_known_files_df()
    # Check if we have already crawled the files for this data frame
    if len(known_files.filter(pl.col("project_syn_id").eq(project_syn_id))) > 0:
        logger.info(f"loading files for {project_syn_id} from cache")
    # If not crawl the files and save the results
    else:
        logger.info(
            f"Files from {project_syn_id} not found in cache, checking synapse..."
        )
        known_files = crawl_project_files(
            project_syn_id=project_syn_id, known_files=known_files
        )

    project_files = known_files.filter(pl.col("project_syn_id").eq(project_syn_id))
    if file_types is not None:
        file_types_pattern = f"({'|'.join(re.escape(s) for s in file_types)})$"
        project_files = project_files.filter(
            pl.col("file_name").str.contains(file_types_pattern)
        )
    if as_list:
        project_files = project_files["file_syn_id"].to_list()
    return project_files

def load_known_files_df() -> pl.DataFrame:
    """Load the cached registry of files from previously crawled projects.

    Returns
    -------
    pl.DataFrame
        DataFrame with columns: project_syn_id, file_syn_id, file_name. An
        empty DataFrame with this schema if the cache file doesn't exist.
    """
    os.makedirs(REPORT_PATH, exist_ok=True)
    df_path = os.path.join(REPORT_PATH, "project_files.tsv")
    file_df_schema = pl.Schema(
        [("project_syn_id", pl.String), ("file_syn_id", pl.String), ("file_name", pl.String)]
    )
    if os.path.exists(df_path):
        return pl.read_csv(df_path, schema=file_df_schema, separator="\t")
    return pl.DataFrame(schema=file_df_schema)


def get_projects_files(project_ids: list) -> pl.DataFrame:
    """Return all files for multiple Synapse projects.

    Efficiently retrieves files for multiple projects by using cached data when
    available and only crawling uncached projects.

    Parameters
    ----------
    project_ids :
        List of Synapse project IDs (e.g., ['syn12345678', 'syn87654321']).

    Returns
    -------
    :
        DataFrame containing files from all specified projects with columns:
        [project_syn_id, file_syn_id, file_name].

    Notes
    -----
    Cache is automatically updated with any newly crawled projects.
    """
    known_files = load_known_files_df()
    for project_syn_id in tqdm.tqdm(project_ids):
        # Check if we have already crawled the files for this data frame
        if len(known_files.filter(pl.col("project_syn_id").eq(project_syn_id))) > 0:
            logger.info(f"loading files for {project_syn_id} from cache")
            continue
        # If not crawl the files and save the results
        else:
            logger.info(
                f"Files from {project_syn_id} not found in cache, checking synapse..."
            )
            known_files = crawl_project_files(
                project_syn_id=project_syn_id, known_files=known_files
            )

    return known_files.filter(pl.col("project_syn_id").is_in(project_ids))


def crawl_project_files(
    project_syn_id: str, known_files: pl.DataFrame = None
) -> pl.DataFrame:
    """Crawl a Synapse project to discover all files and update the cache.

    Parameters
    ----------
    project_syn_id :
        Synapse project ID (e.g., 'syn12345678').
    known_files :
        Existing file registry. If None, loads from cache.

    Returns
    -------
    :
        Updated DataFrame containing all known files including newly
        discovered ones. The cache file is automatically updated on disk.

    Notes
    -----
    Handles locked projects gracefully by logging a warning and continuing.
    """
    if known_files is None:
        known_files = load_known_files_df()
    found_files = []
    try:
        # Will throw and error if try to lead wiki of locked project
        _ = syn.getWiki(project_syn_id)
        file_name_iter = walk(
            syn=syn,
            synId=project_syn_id,
            includeTypes=[
                "file",
            ],
        )
    except Exception:
        logger.warning(
            f"Could not read files for project with id {project_syn_id}", exc_info=True
        )
        file_name_iter = [
            [
                "",
                "",
                [("", "")],
            ]
        ]  # Give just empty syn_id and file_name
    for _, _, filenames in file_name_iter:
        for filename, file_syn_id in filenames:
            found_files.append(
                {
                    "project_syn_id": project_syn_id,
                    "file_syn_id": file_syn_id,
                    "file_name": filename,
                }
            )

    found_files = pl.from_dicts(found_files, schema=known_files.schema)
    known_files = known_files.vstack(found_files)
    known_files.write_csv(
        os.path.join(REPORT_PATH, "project_files.tsv"), separator="\t"
    )
    return known_files


def get_file_annotations(file_ids:list)->dict[str, dict]:
    """Return the annotations of a list of Synapse files.

    Uses the Synapse file object to pull the file annotations.

    Parameters
    ----------
    file_ids :
        A list of file Synapse ids.

    Returns
    -------
    dict[str, dict]
        A dictionary mapping Synapse ids to a dictionary of their annotations.
    """
    logger.info("pulling annotations...")
    async def _safe_get(fid):
        try:
            return fid, await File(id=fid, download_file=False).get_async()
        except Exception as e:
            logger.warning(f"Skipping {fid}: {e}")
            return fid, None

    async def fetch_open_annotations(file_ids, batch_size=50):
        results = {}
        for i in tqdm.tqdm(range(0, len(file_ids), batch_size)):
            batch = file_ids[i:i+batch_size]
            tasks = [_safe_get(fid) for fid in batch]
            outcomes = await asyncio.gather(*tasks)
            
            for fid, entity in outcomes:
                if entity is not None:
                    results[fid] = entity.annotations
        return results
    return asyncio.run(fetch_open_annotations(file_ids))
def nf_curie(uuid: str) -> str:
    """Prefix a raw NF UUID as a `nfdataportal:` CURIE.

    Biolink requires CURIE node ids.
    """
    return f"{NF_CURIE_PREFIX}:{uuid}"

# Helper functions for parsing meta, publications and tools
def _is_placeholder(value: str) -> bool:
    return value.strip().lower() in NF_PLACEHOLDER_VALUES


def _unpack_dict_list(meta: dict, key: str) -> list:
    """Return the non-empty values of a dictionary metadata field.

    The metadata could be publication metadata as well. Values are returned as
    stripped strings, dropping leaked boolean annotations.
    """
    values = meta.get(key, "")
    values = values if isinstance(values, list) else [values]
    return [
        str(v).strip()
        for v in values
        if not _is_empty(v) and str(v).strip().lower() not in ("true", "false")
    ]

def meta_node_curie(key: str, entry: str) -> str:
    """Return the CURIE for an ungrounded metadata value.

    `key` is usually the Biolink category, so the same name under two fields
    of one category (e.g. a funder that is also an institution) is one node.
    Fields in NF_OWN_NAMESPACE_FIELDS pass their field name instead.
    """
    return nf_curie(f"{key.split(':')[-1].lower()}_{slugify(entry)}")

ORCID_SOURCE = "nf_tools_investigator_name_match"


@lru_cache(maxsize=1)
def _investigator_orcids() -> dict:
    """Return a mapping from slugified investigator names to ORCID CURIEs.

    The mapping comes from the NF tools db. Neither publications table has
    author ids, so a portal author whose full name matches an investigator gets
    that investigator's ORCID. Loaded on first use.
    """
    df = syn.tableQuery(f"SELECT investigatorName, orcid FROM {NF_TOOLS_TABLE}").asDataFrame()
    orcids = dict()
    for name, orcid in df.itertuples(index=False):
        # A single token (e.g. a surname) is too ambiguous to match on
        if _is_empty(name) or _is_empty(orcid) or len(str(name).split()) < 2:
            continue
        orcids[slugify(str(name))] = normalize_curie(f"orcid:{str(orcid).strip()}")
    return orcids


def author_curie(name: str) -> Tuple[str, str | None]:
    """Return the CURIE for a person and where its ORCID came from.

    A person is a publication author or tool investigator. Their CURIE is
    their ORCID when the tools db has one for that name, else an `author_` id.
    The source of the ORCID is None when there is no ORCID.
    """
    orcid = _investigator_orcids().get(slugify(name))
    if orcid:
        return orcid, ORCID_SOURCE
    return meta_node_curie("author", name), None


@lru_cache(maxsize=None)
def _ground(entry: str):
    # Tools repeat the same few values hundreds of times (e.g. "Neurofibromatosis type 1")
    return ground_entity(entry, min_span_fraction=NF_GROUNDING_MIN_SPAN_FRACTION)


def _process_grounded_field(entry:str, meta_curie:str, node_type:str):
    name, curie, iri, category = _ground(entry)
    if curie is not None and category not in NF_GROUND_FIELD_CATEGORIES[node_type]:
        logger.debug(
            f"{meta_curie}: {node_type} {entry!r} grounded to {curie} ({category}), treating as ungrounded"
        )
        curie = None
    grounded = curie is not None
    if not grounded:
        category = NF_UNGROUNDED_CATEGORY[node_type]
        name, iri = entry, None
        curie = meta_node_curie(category, entry)
    return name, curie, iri, category, "true" if grounded else "false"
def parse_dict_fields(meta_curie:str, meta:dict, node_types: list[str], source:str, node_set: NodeSet, edge_set : EdgeSet, head: str | None = None) -> Tuple[NodeSet, EdgeSet]:
    """Parse fields from dictionary metadata.

    The same procedure is used for publications and tools.

    Parameters
    ----------
    meta_curie :
        CURIE identifying the dictionary source, often a publication or
        project.
    meta :
        Dict with fields.
    node_types :
        A list of field names containing the nodes to extract.
    source :
        Where the information is being extracted from, often publications or
        metadata.
    node_set :
        Existing set of nodes to update.
    edge_set :
        Existing set of edges to update.
    head :
        NF_KG_SCHEMA subject name, if not the default for `source` (e.g.
        "tool_model").

    Returns
    -------
    node_set : NodeSet
        The updated node set.
    edge_set : EdgeSet
        The updated edge set.
    """
    head = head or SOURCE_TO_HEAD[source]
    # Prase simple non grounded node types
    for node_type in node_types:
        for entry in _unpack_dict_list(meta, node_type):
            if _is_placeholder(entry):
                continue
            orcid_source = None
            # Fields with a category allow-list are grounded
            if node_type in NF_GROUND_FIELD_CATEGORIES:
                name, curie, iri, category, grounded = _process_grounded_field(entry, meta_curie, node_type)
            else:
                category = NF_LABEL_TO_BIOLINK.get(node_type, None)
                if category is None:
                    raise ValueError(f"Node type {node_type} not recognized...")
                if node_type in NF_PERSON_FIELDS:
                    curie, orcid_source = author_curie(entry)
                else:
                    id_key = node_type if node_type in NF_OWN_NAMESPACE_FIELDS else category
                    curie = meta_node_curie(id_key, entry)
                iri = get_bioregistry_iri(*curie.split(":", 1)) if orcid_source else None
                grounded = None
                name = entry
            # fill_empty so a value seen again (e.g. a shared funder) never overwrites the existing node
            node_set.update_nodes(
                {
                    "curie:ID": curie,
                    ":LABEL": category,
                    "name": name,
                    "raw_label" : node_type,
                    "raw_texts:string[]": entry,
                    "source:string[]": source,
                    "iri": iri,
                    "grounded:boolean": grounded,
                    "orcid_source": orcid_source,
                },
                fill_empty=True,
            )
            edge_set.update_edges(
                {
                    ":START_ID": meta_curie,
                    ":END_ID": curie,
                    ":TYPE": NF_KG_SCHEMA[(head, node_type)],
                    "raw_type:string[]": node_type,
                    "source:string[]": source,
                }
            )
    return node_set, edge_set


def parse_related_studies(meta_curie:str, meta:dict, key:str, source:str, node_set: NodeSet, edge_set : EdgeSet ):
    """Parse related studies (for metadata) or StudyIDs for publications."""
    to_url = lambda x: NF_STUDIES_BASE_URL.replace("{study_id}", x)
    for entry in _unpack_dict_list(meta, key):
        curie = entry if SYNAPSE_ID.fullmatch(entry) else None
        if curie is None:
            logger.debug(f"{meta_curie}: skipping {key} value {entry!r}")
            continue
        # Add the related study as a project only if not present in other nodes
        if not curie in node_set.nodes:
            node_set.update_nodes(
                {
                    "curie:ID": curie,
                    ":LABEL": NF_LABEL_TO_BIOLINK.get("Project"),
                    "name" : get_project_name(curie),
                    "summary": get_project_summary(curie),
                    "raw_label": "Project",
                    "study_url": to_url(curie),
                    "source:string[]": source,
                }
            )
        if source == 'metadata':
            start = meta_curie
            end = curie
            edge_key = ("Project", "Project") 
        elif source == 'publications':
            start = curie 
            end = meta_curie
            edge_key =  ("Project", "publication")
        else:
            raise ValueError(f"bad source given {source}")
        edge_set.update_edges(
            {
                ":START_ID": start,
                ":END_ID": end,
                ":TYPE": NF_KG_SCHEMA[edge_key],
                "raw_type:string[]": key,
                "source:string[]": source,
            }
        )
    return node_set, edge_set

def get_project_name(study_id)->str:
    """Return the name of a project, or its CURIE if it has none."""
    name = _studies_table().get(study_id, {}).get("name")
    return study_id if _is_empty(name) else str(name)


def get_project_summary(study_id) -> str:
    """Return the study summary from the NF studies table, or "" if none."""
    summary = _studies_table().get(study_id, {}).get("summary")
    return "" if _is_empty(summary) else str(summary)