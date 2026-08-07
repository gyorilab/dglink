from fastapi import FastAPI, HTTPException
from neo4j import GraphDatabase
import os
import pygtrie
import pandas
from indra.databases import bioregistry_client
from urllib.parse import quote

app = FastAPI()


driver = GraphDatabase.driver(
    "bolt://neo-4j:7687",
    auth=(os.environ.get("NEO4J_USER", "neo4j"), os.environ.get("NEO4J_PASSWORD")),
)

## Methods for finding portal specific disease focus ##
NF_DISEASE_FOCUS_TYPE = "has_diseaseFocus"
BIOLINK_FOCUS_TYPES = ("biolink:related_to", "biolink:associated_with")
BIOLINK_STUDY_LABEL = "biolink:Study"
BIOLINK_DISEASE_LABEL = "biolink:Disease"
DISEASE_RAW_TEXT_COLUMN = "disease_type"

## Cap on rows returned per query ## s
RESULT_LIMIT = int(os.environ.get("SEMANTIC_SEARCH_RESULT_LIMIT", "1000"))


def load_prefix_sets(nodes_df, edges_df):
    """load the prefix sets of nodes and edges for auto-complete"""
    ## load node prefix set
    node_prefix_set = pygtrie.PrefixSet()
    ## add curie and name to node prefix set
    node_prefix_set = node_prefix_set | pygtrie.PrefixSet(
        nodes_df["curie:ID"].astype(str)
    )
    node_prefix_set = node_prefix_set | pygtrie.PrefixSet(nodes_df["name"].dropna())
    # load edge prefix set
    edge_prefix_set = pygtrie.PrefixSet()
    edges_df = pandas.read_csv(f"/app/resources/edges.tsv", sep="\t")
    edge_prefix_set = edge_prefix_set | pygtrie.PrefixSet(edges_df[":TYPE"])
    return node_prefix_set, edge_prefix_set


def load_mappings(nodes_df, edges_df):
    """get mapping from entity name to curie (and inverse) as well as a list of projects to their disease focus"""
    ## get name mappings
    names_mapping = {}
    for _, row in nodes_df.iterrows():
        curie = row["curie:ID"]
        names_mapping[curie] = curie
        names_mapping[row["name"]] = curie
    inverse_names_mapping = {names_mapping[key]: key for key in names_mapping}
    project_to_disease_focus = load_disease_focus(nodes_df, edges_df)
    return names_mapping, inverse_names_mapping, project_to_disease_focus


def get_no_context_indra_url(curie):
    get_indra_url = (
        lambda db, id: f"https://discovery.indra.bio/search/?agent_tuple=[%22{db}%22,%22{db}:{id}%22]"
    )
    split_curie = curie.split(":", maxsplit=1)
    if len(split_curie) < 2:
        return None
    db, id = bioregistry_client.get_ns_id_from_bioregistry_curie(curie)
    if id is None:
        return None
    id = id.split(":")[-1]
    return get_indra_url(db, id)


def get_url_with_context_indra_url(curie, project_curie):
    get_indra_url = (
        lambda db, id, mesh_id: f"https://discovery.indra.bio/search/?agent_tuple=[%22{db}%22,%22{db}:{id}%22]&mesh_tuple=[%22MESH%22,%22{mesh_id}%22]"
    )
    split_curie = curie.split(":", maxsplit=1)
    if len(split_curie) < 2:
        return None
    db, id = bioregistry_client.get_ns_id_from_bioregistry_curie(curie)
    if id is None:
        return None
    id = id.split(":")[-1]
    project_curie = project_curie.removesuffix(":Wiki")
    project_disease_focus = project_to_disease_focus.get(project_curie, "")

    if project_disease_focus == "":
        return None
    mesh_id = project_disease_focus[0].split(":", maxsplit=1)
    if len(mesh_id) < 2:
        return None
    return (
        get_indra_url(db, id, mesh_id=mesh_id[1]),
        project_disease_focus[1],
    )


def add_indra_url_no_context(record, object_whole, subject_whole):
    subject_curie = record.data()["subject"]
    subject_indra_url = get_no_context_indra_url(curie=subject_curie)
    if subject_indra_url is not None:
        subject_whole["Subject literature evidence"] = subject_indra_url
    object_curie = record.data()["object"]
    object_indra_url = get_no_context_indra_url(curie=object_curie)
    if object_indra_url is not None:
        object_whole["Object literature evidence"] = object_indra_url
    return subject_whole, object_whole


def add_indra_url_with_context(record, object_whole, subject_whole):
    subject_curie = record.data()["subject"]
    object_curie = record.data()["object"]
    if _has_disease_focus(subject_curie):
        context_url = get_url_with_context_indra_url(
            curie=object_curie, project_curie=subject_curie
        )
        if context_url is not None:
            object_whole[f"Object {context_url[1]} context literature evidence"] = (
                context_url[0]
            )
    elif _has_disease_focus(object_curie):
        context_url = get_url_with_context_indra_url(
            curie=subject_curie, project_curie=object_curie
        )
        if context_url is not None:
            subject_whole[f"Subject {context_url[1]} context literature evidence"] = (
                context_url[0]
            )
    return subject_whole, object_whole


## Bellow are a number of functions that help with making disease focus ##
## general between CRDC and NF Data portal the core idea is this lets you ##
## query Indra in the disease context of the study ##


def load_disease_focus(nodes_df, edges_df):
    """Map a project/study curie -> [mesh curie, display name] for INDRA disease context lookoup"""
    labels = dict(zip(nodes_df["curie:ID"], nodes_df[":LABEL"]))
    names = dict(zip(nodes_df["curie:ID"], nodes_df["name"]))
    project_to_disease_focus = {}

    ## NF data portal case ##
    nf_focus = edges_df[edges_df[":TYPE"] == NF_DISEASE_FOCUS_TYPE].drop_duplicates()
    for _, row in nf_focus.iterrows():
        project, disease = row.iloc[0], row.iloc[1]
        entry = project_to_disease_focus.setdefault(project, ["", "", ""])
        entry[0 if str(disease).startswith("mesh") else 1] = disease

    ## CRDC Case ##
    candidates = edges_df[edges_df[":TYPE"].isin(BIOLINK_FOCUS_TYPES)]
    is_focus = (candidates[":START_ID"].map(labels) == BIOLINK_STUDY_LABEL) & (
        candidates[":END_ID"].map(labels) == BIOLINK_DISEASE_LABEL
    )
    biolink_focus = candidates[is_focus]
    biolink_focus = biolink_focus[
        biolink_focus[":END_ID"].astype(str).str.startswith("mesh")
    ]
    ## find raw text of disease focus name if possible ##
    raw_column = (
        DISEASE_RAW_TEXT_COLUMN
        if DISEASE_RAW_TEXT_COLUMN in biolink_focus.columns
        else None
    )
    if raw_column:
        has_raw_text = biolink_focus[raw_column].notna() & (
            biolink_focus[raw_column].astype(str).str.strip() != ""
        )
        biolink_focus = biolink_focus.assign(_has_raw_text=has_raw_text).sort_values(
            "_has_raw_text", ascending=False, kind="stable"
        )
    ## a study can carry several diagnoses; keep the first mesh-grounded one ##
    biolink_focus = biolink_focus.drop_duplicates(subset=":START_ID")

    raw_texts = biolink_focus[raw_column] if raw_column else [None] * len(biolink_focus)
    for project, disease, raw_text in zip(
        biolink_focus[":START_ID"], biolink_focus[":END_ID"], raw_texts
    ):
        entry = project_to_disease_focus.setdefault(project, ["", "", ""])
        if not entry[0]:
            entry[0] = disease
            name = names.get(disease)
            if isinstance(name, str) and name:
                entry[1] = name
            if isinstance(raw_text, str) and raw_text.strip():
                entry[2] = raw_text.strip()
    return project_to_disease_focus


def relation_pattern(relation: str = None) -> str:
    """Cypher fragment binding `r`, optionally constrained to one relationship type.

    A relationship type cannot be parameterized -- `[r:$type]` is not valid Cypher -- so it
    has to be interpolated. Two consequences, both of which bit us:

      * Biolink types contain a colon, and `[r:biolink:related_to]` is a syntax error, so
        the type must be backtick-quoted. Every relation search against the CRDC graph
        failed with CypherSyntaxError until this was added.
      * Interpolation is an injection sink, and this endpoint is published on a public port
        by compose.crdc.yaml. The type is therefore accepted only if it actually occurs in
        the graph, which is also what autocomplete offers.
    """
    if not relation:
        return "r"
    if relation not in EDGE_TYPES:
        raise HTTPException(
            status_code=400, detail=f"unknown relation type: {relation!r}"
        )
    ## `relation` is now known to be one of the graph's own type strings, so it cannot carry
    ## a stray backtick, but escape anyway so the quoting stays correct if the allow-list is
    ## ever loosened.
    return "r:`{}`".format(relation.replace("`", "``"))


def get_disease_focus(curie):
    """The [mesh curie, name, raw text] focus entry for a project/study, or None."""
    if not isinstance(curie, str):
        return None
    return project_to_disease_focus.get(curie.removesuffix(":Wiki"))


def _has_disease_focus(curie) -> bool:
    """Helper function checking if a curie has a disease focus"""
    return get_disease_focus(curie) is not None


## king of jank helper function bellow ##
def add_disease_focus(record, object_whole, subject_whole):
    """Report a project/study's disease focus as its own fields on that node.

    Kept separate from the INDRA context links, which only appear when the *other* entity in
    the triple grounds to a bioregistry id. The focus is a property of the study itself, so it
    should show up either way. The raw text is the diagnosis string the disease was grounded
    from, reported alongside the grounding so a bad one is visible in the results -- e.g.
    "Early Onset Gastric Cancer" grounding to "Age of Onset".
    """
    for side, curie, attributes in (
        ("Subject", record.data()["subject"], subject_whole),
        ("Object", record.data()["object"], object_whole),
    ):
        focus = get_disease_focus(curie)
        if focus is None:
            continue
        name = focus[1]
        raw_text = focus[2] if len(focus) > 2 else ""
        ## a name only exists once the diagnosis grounded to an ontology term; the raw text is
        ## only present on graphs that carry the diagnosis column. Emit whichever we have.
        if name:
            attributes[f"{side} disease focus"] = name
        if raw_text:
            attributes[f"{side} disease focus raw text"] = raw_text
    return subject_whole, object_whole


@app.get("/query")
def query_dispatch(
    agent: str,
    relation: str = None,
    other_agent: str = None,
    query_type: str = "Subject",
):
    agent = (agent or "").split(", ", maxsplit=1)[-1]
    other_agent = (other_agent or "").split(", ", maxsplit=1)[-1]
    if agent in names_mapping:
        agent = names_mapping[agent]
    if other_agent in names_mapping:
        other_agent = names_mapping[other_agent]
    if not agent and not relation:
        raise HTTPException(
            status_code=400, detail="provide an agent, a relation, or both"
        )
    if not agent:
        res = relation_search(relation=relation)
    elif query_type == "Subject":
        res = subject_search(agent=agent, relation=relation, other_agent=other_agent)
    elif query_type == "Object":
        res = object_search(agent=agent, relation=relation, other_agent=other_agent)
    else:
        subjects = subject_search(
            agent=agent, relation=relation, other_agent=other_agent
        )
        objects = object_search(agent=agent, relation=relation, other_agent=other_agent)
        res = subjects + objects
    return {"message": res}


def relation_search(relation: str = None):
    records, _, _ = driver.execute_query(
        f"""
        MATCH (p)-[{relation_pattern(relation)}]->(e)
        RETURN p.curie as subject, p as subject_whole, r as relation, properties(r) as whole_relation, e.curie as object, e as object_whole
        LIMIT $limit
        """,
        limit=RESULT_LIMIT,
        database_="neo4j",
    )
    res = []
    for record in records:
        object_whole = {
            f"Object {key}": record.data()["object_whole"][key]
            for key in record.data()["object_whole"]
        }
        subject_whole = {
            f"Subject {key}": record.data()["subject_whole"][key]
            for key in record.data()["subject_whole"]
        }
        relation_whole = {
            f"Relation {key}": record.data()["whole_relation"][key]
            for key in record.data()["whole_relation"]
        }
        subject_whole, object_whole = add_indra_url_no_context(
            record=record, object_whole=object_whole, subject_whole=subject_whole
        )
        subject_whole, object_whole = add_indra_url_with_context(
            record=record, object_whole=object_whole, subject_whole=subject_whole
        )
        subject_whole, object_whole = add_disease_focus(
            record=record, object_whole=object_whole, subject_whole=subject_whole
        )
        del subject_whole["Subject curie"]
        del object_whole["Object curie"]
        res.append(
            (
                f"Subject identifier : {record.data()['subject']}",
                f"subject attributes : {subject_whole}",
                f"Relation : {record.data()['relation'][1]}",
                f"Relation attributes : {relation_whole}",
                f"Object identifier : {record.data()['object']}",
                f"object attributes : {object_whole}",
            )
        )
    return res


def subject_search(
    agent: str = "syn52740594", relation: str = None, other_agent: str = None
):
    other_agent_query = "AND e.curie = $other_agent" if other_agent else ""
    records, _, _ = driver.execute_query(
        f"""
        MATCH (p)-[{relation_pattern(relation)}]->(e)
        WHERE p.curie = $agent {other_agent_query}
        RETURN p.curie as subject, p as subject_whole, r as relation, properties(r) as whole_relation, e.curie as object, e as object_whole
        LIMIT $limit
        """,
        agent=agent,
        other_agent=other_agent,
        limit=RESULT_LIMIT,
        database_="neo4j",
    )
    res = []
    for record in records:
        object_whole = {
            f"Object {key}": record.data()["object_whole"][key]
            for key in record.data()["object_whole"]
        }
        subject_whole = {
            f"Subject {key}": record.data()["subject_whole"][key]
            for key in record.data()["subject_whole"]
        }
        relation_whole = {
            f"Relation {key}": record.data()["whole_relation"][key]
            for key in record.data()["whole_relation"]
        }

        del subject_whole["Subject curie"]
        del object_whole["Object curie"]
        subject_whole, object_whole = add_indra_url_no_context(
            record=record, object_whole=object_whole, subject_whole=subject_whole
        )
        subject_whole, object_whole = add_indra_url_with_context(
            record=record, object_whole=object_whole, subject_whole=subject_whole
        )
        subject_whole, object_whole = add_disease_focus(
            record=record, object_whole=object_whole, subject_whole=subject_whole
        )
        res.append(
            (
                f"Subject identifier : {record.data()['subject']}",
                f"subject attributes : {subject_whole}",
                f"Relation : {record.data()['relation'][1]}",
                f"Relation attributes : {relation_whole}",
                f"Object identifier : {record.data()['object']}",
                f"object attributes : {object_whole}",
            )
        )
    return res


def object_search(
    agent: str = "syn52740594", relation: str = None, other_agent: str = None
):
    other_agent_query = "AND p.curie = $other_agent" if other_agent else ""
    records, _, _ = driver.execute_query(
        f"""
        MATCH (p)-[{relation_pattern(relation)}]->(e)
        WHERE e.curie = $agent {other_agent_query}
        RETURN p.curie as subject, p as subject_whole, r as relation, properties(r) as whole_relation, e.curie as object, e as object_whole
        LIMIT $limit
        """,
        agent=agent,
        other_agent=other_agent,
        limit=RESULT_LIMIT,
        database_="neo4j",
    )
    res = []
    for record in records:
        object_whole = {
            f"Object {key}": record.data()["object_whole"][key]
            for key in record.data()["object_whole"]
        }
        subject_whole = {
            f"Subject {key}": record.data()["subject_whole"][key]
            for key in record.data()["subject_whole"]
        }
        relation_whole = {
            f"Relation {key}": record.data()["whole_relation"][key]
            for key in record.data()["whole_relation"]
        }
        subject_whole, object_whole = add_indra_url_no_context(
            record=record, object_whole=object_whole, subject_whole=subject_whole
        )
        subject_whole, object_whole = add_indra_url_with_context(
            record=record, object_whole=object_whole, subject_whole=subject_whole
        )
        subject_whole, object_whole = add_disease_focus(
            record=record, object_whole=object_whole, subject_whole=subject_whole
        )
        del subject_whole["Subject curie"]
        del object_whole["Object curie"]
        res.append(
            (
                f"Subject identifier : {record.data()['subject']}",
                f"subject attributes : {subject_whole}",
                f"Relation : {record.data()['relation'][1]}",
                f"Object identifier : {record.data()['object']}",
                f"Relation attributes : {relation_whole}",
                f"object attributes : {object_whole}",
            )
        )
    return res


@app.get("/autoComplete")
def Autocomplete(query: str, completion_type: str, k: int = 100):
    if completion_type != "relation":
        res = ["".join(x) for x in node_prefix_set.iter(prefix=query)][:k]
        if len(res) > 0:
            ret = []
            for x in res:
                if x != names_mapping[x]:
                    ret.append(f"{x}, {names_mapping[x]}")
                elif x != inverse_names_mapping[x]:
                    ret.append(f"{x}, {inverse_names_mapping[x]}")
                else:
                    ret.append(x)
            res = ret
    else:
        res = ["".join(x) for x in edge_prefix_set.iter(prefix=query)][:k]
    return {"suggestions": res}


## read in the graph as data frame.
nodes_df = pandas.read_csv(f"/app/resources/nodes.tsv", sep="\t")
edges_df = pandas.read_csv(f"/app/resources/edges.tsv", sep="\t")
node_prefix_set, edge_prefix_set = load_prefix_sets(nodes_df, edges_df)
names_mapping, inverse_names_mapping, project_to_disease_focus = load_mappings(
    nodes_df, edges_df
)
## allow-list for the one Cypher fragment that cannot be parameterized (see relation_pattern)
EDGE_TYPES = frozenset(edges_df[":TYPE"].dropna().astype(str))


@app.get("/health")
def health():
    neo4j_connected = False
    try:
        driver.verify_connectivity()
        neo4j_connected = True
    except Exception:
        pass
    return {
        "status": "healthy",
        "neo4j_connected": neo4j_connected,
        "graph_loaded": len(names_mapping) > 0,
    }
