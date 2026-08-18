#!/bin/bash

set -eoxu pipefail

# The Neo4j Browser connects to bolt from the visitor's browser, using the address
# in this server's discovery document. That address defaults to port 7687, so when
# compose publishes bolt on a different host port the Browser must be told which one
# or it loads and then fails to connect. Only the port is set: the host half still
# follows the request's Host header, so this stays correct on any hostname.
# Deleting first keeps this idempotent across container restarts (a duplicate key in
# neo4j.conf is a startup error).
sed -i '/^dbms.connector.bolt.advertised_address=/d' /etc/neo4j/neo4j.conf
echo "dbms.connector.bolt.advertised_address=:${BOLT_ADVERTISED_PORT:-7687}" >> /etc/neo4j/neo4j.conf

echo "Starting database"
neo4j start

echo "Waiting for database"
until [ \
  "$(curl -s -w '%{http_code}' -o /dev/null "http://localhost:7474")" \
  -eq 200 ]
do
  sleep 5
done

neo4j status

# Index on all properties for improved lookup time.
echo "Creating property indexes"
cypher-shell -u neo4j -p "${NEO4J_PASSWORD:-password}" <<'CYPHER' || echo "WARNING: index creation failed; the graph still serves, but queries will full-scan"
CREATE INDEX idx_case_curie IF NOT EXISTS FOR (n:`biolink:Case`) ON (n.curie);
CREATE INDEX idx_case_name IF NOT EXISTS FOR (n:`biolink:Case`) ON (n.name);
CREATE INDEX idx_gene_curie IF NOT EXISTS FOR (n:`biolink:Gene`) ON (n.curie);
CREATE INDEX idx_gene_name IF NOT EXISTS FOR (n:`biolink:Gene`) ON (n.name);
CREATE INDEX idx_rnaproduct_curie IF NOT EXISTS FOR (n:`biolink:RNAProduct`) ON (n.curie);
CREATE INDEX idx_rnaproduct_name IF NOT EXISTS FOR (n:`biolink:RNAProduct`) ON (n.name);
CREATE INDEX idx_smallmolecule_curie IF NOT EXISTS FOR (n:`biolink:SmallMolecule`) ON (n.curie);
CREATE INDEX idx_smallmolecule_name IF NOT EXISTS FOR (n:`biolink:SmallMolecule`) ON (n.name);
CREATE INDEX idx_disease_curie IF NOT EXISTS FOR (n:`biolink:Disease`) ON (n.curie);
CREATE INDEX idx_disease_name IF NOT EXISTS FOR (n:`biolink:Disease`) ON (n.name);
CREATE INDEX idx_study_curie IF NOT EXISTS FOR (n:`biolink:Study`) ON (n.curie);
CREATE INDEX idx_study_name IF NOT EXISTS FOR (n:`biolink:Study`) ON (n.name);
CREATE INDEX idx_publication_curie IF NOT EXISTS FOR (n:`biolink:Publication`) ON (n.curie);
CREATE INDEX idx_publication_name IF NOT EXISTS FOR (n:`biolink:Publication`) ON (n.name);
CREATE INDEX idx_namedthing_curie IF NOT EXISTS FOR (n:`biolink:NamedThing`) ON (n.curie);
CREATE INDEX idx_namedthing_name IF NOT EXISTS FOR (n:`biolink:NamedThing`) ON (n.name);
CREATE INDEX idx_macromolecularcomplex_curie IF NOT EXISTS FOR (n:`biolink:MacromolecularComplex`) ON (n.curie);
CREATE INDEX idx_macromolecularcomplex_name IF NOT EXISTS FOR (n:`biolink:MacromolecularComplex`) ON (n.name);
CREATE INDEX idx_grossanatomicalstructure_curie IF NOT EXISTS FOR (n:`biolink:GrossAnatomicalStructure`) ON (n.curie);
CREATE INDEX idx_grossanatomicalstructure_name IF NOT EXISTS FOR (n:`biolink:GrossAnatomicalStructure`) ON (n.name);
CREATE INDEX idx_organismtaxon_curie IF NOT EXISTS FOR (n:`biolink:OrganismTaxon`) ON (n.curie);
CREATE INDEX idx_organismtaxon_name IF NOT EXISTS FOR (n:`biolink:OrganismTaxon`) ON (n.name);
CREATE INDEX idx_biologicalprocess_curie IF NOT EXISTS FOR (n:`biolink:BiologicalProcess`) ON (n.curie);
CREATE INDEX idx_biologicalprocess_name IF NOT EXISTS FOR (n:`biolink:BiologicalProcess`) ON (n.name);
CREATE INDEX idx_studypopulation_curie IF NOT EXISTS FOR (n:`biolink:StudyPopulation`) ON (n.curie);
CREATE INDEX idx_studypopulation_name IF NOT EXISTS FOR (n:`biolink:StudyPopulation`) ON (n.name);
CREATE INDEX idx_agent_curie IF NOT EXISTS FOR (n:`biolink:Agent`) ON (n.curie);
CREATE INDEX idx_agent_name IF NOT EXISTS FOR (n:`biolink:Agent`) ON (n.name);
CREATE INDEX idx_protein_curie IF NOT EXISTS FOR (n:`biolink:Protein`) ON (n.curie);
CREATE INDEX idx_protein_name IF NOT EXISTS FOR (n:`biolink:Protein`) ON (n.name);
CREATE INDEX idx_cellularcomponent_curie IF NOT EXISTS FOR (n:`biolink:CellularComponent`) ON (n.curie);
CREATE INDEX idx_cellularcomponent_name IF NOT EXISTS FOR (n:`biolink:CellularComponent`) ON (n.name);
CYPHER

# Block until the indexes are populated, so the first client query is already fast.
cypher-shell -u neo4j -p "${NEO4J_PASSWORD:-password}" "CALL db.awaitIndexes(600);" \
  || echo "WARNING: timed out waiting for indexes to come online"

neo4j stop

## read only neo4j 
echo "dbms.databases.default_to_read_only=true" >> /etc/neo4j/neo4j.conf

# Bind bolt and HTTP to all interfaces inside the container. Use the internal
# connector ports (7687, 7474)
for key_val in \
  "dbms.connector.bolt.listen_address=0.0.0.0:7687" \
  "dbms.connector.http.listen_address=0.0.0.0:7474"
do
  key="${key_val%%=*}"
  sed -i "/^${key}=/d" /etc/neo4j/neo4j.conf
  echo "${key_val}" >> /etc/neo4j/neo4j.conf
done

# Production (NEO4J_PRODUCTION=true): tell the Neo4j Browser and remote bolt
# clients the public hostname/port, and restrict CORS to the public HTTPS origin.
if [ "${NEO4J_PRODUCTION:-false}" = "true" ]; then
  bolt_host="${PUBLIC_HOST:-dglink.indra.bio}"
  bolt_port="${BOLT_ADVERTISED_PORT:-7676}"
  sed -i '/^dbms.connector.bolt.advertised_address=/d' /etc/neo4j/neo4j.conf
  echo "dbms.connector.bolt.advertised_address=${bolt_host}:${bolt_port}" >> /etc/neo4j/neo4j.conf

  cors_origin="${NEO4J_CORS_ALLOW_ORIGIN:-https://${PUBLIC_HOST:-dglink.indra.bio}}"
  sed -i '/^dbms.security.http_access_control_allow_origin=/d' /etc/neo4j/neo4j.conf
  echo "dbms.security.http_access_control_allow_origin=${cors_origin}" >> /etc/neo4j/neo4j.conf
fi

# Run `neo4j console` so Neo4j stays running in the foreground.
# Using `neo4j start` will put the process in the background which means this
# script will exit and the container will stop with it without any errors.
neo4j console
