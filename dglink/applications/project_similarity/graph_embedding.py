"""Train an embedding model as a proxy for related study prediction."""

from pykeen.triples import TriplesFactory
from pykeen.triples.utils import load_triples
from pykeen.pipeline import pipeline
import torch
import numpy as np
from itertools import combinations
import gzip
import torch
from jacquard_sim import get_projects_to_edges, check_related_study_exists
import pandas as pd
import re

RESOURCE_DIR = "dglink/applications/project_similarity/resources"
all_project_ids = [
    "syn2343195",  # Large project
    "syn5562324",  # Small project
    "syn27761862",  # Small project
    "syn4939874",  # Large project
    "syn4939876",  # Locked
    "syn4939906",  # Small
    "syn4939916",  # Locked
    "syn7217928",  # Large
    "syn8016635",  # Small
    "syn11638893",  # Locked
    "syn11817821",  # Large
    "syn21641813",  # Locked
    "syn21642027",  # Locked
    "syn21650493",  # Large
    "syn21984813",  # Large
    "syn23639889",  # Locked
    "syn51133914",  # Locked
    "syn52740594",  # Large
]


def train_embedding_model(
    resource_dir=RESOURCE_DIR,
    model_name="TransE",
    epochs=100,
    save=True,
    save_path="dglink/applications/project_similarity/embedding_test",
):
    """Train a network embedding model with the PyKEEN pipeline.

    Returns
    -------
    model
        The trained model.
    entity_to_id : dict
        The mapping from entity to id used in training.
    """
    # Split the dataset
    # tf = TriplesFactory.from_path(edge_path)
    edge_path = f"{resource_dir}/non_related_projects_edges.tsv"
    triples = load_triples(path=edge_path, column_remapping=(0, 2, 1))
    tf = TriplesFactory.from_labeled_triples(triples[1:])  # Drop header
    training, testing = tf.split()
    # training, testing, validation = tf.split([0.8, 0.1, 0.1])
    # train the model
    result = pipeline(
        training=training,
        testing=testing,
        model=model_name,
        epochs=epochs,
    )
    if save:
        result.save_to_directory(save_path)
    return result.model, result.training.entity_to_id


def load_entity_to_id(save_path="dglink/applications/project_similarity/embedding_test"):
    """Read the entity to id mapping as a dictionary."""
    id_path = f"{save_path}/training_triples/entity_to_id.tsv.gz"
    entity_to_id = {}
    with gzip.open(id_path, "rt", encoding="utf-8") as f:
        for line in f.readlines()[2:]:
            # import ipdb; ipdb.set_trace()
            id, term = line.strip().split("\t")
            entity_to_id[term] = int(id)
    return entity_to_id


def load_embedding_model(save_path="dglink/applications/project_similarity/embedding_test"):
    """Load the embedding model and the entity_to_id mapping."""
    model = torch.load(f"{save_path}/trained_model.pkl", weights_only=False)
    entity_to_id = load_entity_to_id(save_path=save_path)
    return model, entity_to_id


def id_to_embedding(entity_to_id, model, id_1, id_2):
    """Get a numpy vector of entity embeddings for two given ids."""

    entity_ids = [entity_to_id[id_1], entity_to_id[id_2]]
    entity_embeddings = model.entity_representations[0]
    return entity_embeddings(indices=torch.as_tensor(entity_ids)).detach().cpu().numpy()


def l2(a, b):
    """Return the L2 distance between two vectors."""

    return np.linalg.norm(a - b)


def cosine_dist(a, b):
    sim = np.sum(a * b) / (np.linalg.norm(a) * np.linalg.norm(b))
    return 1 - sim


if __name__ == "__main__":
    train = False
    model_name = "RotatE"
    # model_name = 'TransE'
    if train:
        model, entity_to_id = train_embedding_model(
            resource_dir=RESOURCE_DIR,
            model_name=model_name,
            epochs=250,
        )
    else:
        model, entity_to_id = load_embedding_model(
            save_path="dglink/applications/project_similarity/embedding_test"
        )
    res = []
    related_project_edges_df = pd.read_csv(
        f"{RESOURCE_DIR}/related_project_edges.tsv", sep="\t"
    )
    non_related_edges_df = pd.read_csv(
        f"{RESOURCE_DIR}/non_related_projects_edges.tsv", sep="\t"
    )
    x = 0
    # all_project_ids = filter(lambda x:re.match(r'^syn\d*$', x) is not None, entity_to_id.keys())
    _, all_project_ids = get_projects_to_edges(edges_df=non_related_edges_df)
    for id_1, id_2 in combinations(all_project_ids, 2):
        embed_1, embed_2 = id_to_embedding(
            entity_to_id=entity_to_id, model=model, id_1=id_1, id_2=id_2
        )
        distance = l2(embed_1, embed_2)
        cos = cosine_dist(embed_1, embed_2).real
        has_related = check_related_study_exists(
            edges_df=related_project_edges_df, id_1=id_1, id_2=id_2
        )
        res.append(
            {
                "study_1": id_1,
                "study_2": id_2,
                "l_2": distance,
                "cosine": cos,
                "has_related": has_related,
                "x": x,
            }
        )
        x = x + 1
    # Exploration stuff
    # Plotting
    distance_df = pd.DataFrame(res)
    write_df = distance_df[
        ["study_1", "study_2", "l_2", "cosine", "has_related"]
    ].sort_values(by=["l_2"])
    write_df.to_csv("study_embedding_by_distance.tsv", sep="\t", index=False)
