from flask import Flask, render_template, request, jsonify
import requests
import ast
import os
app = Flask(__name__)

# BACKEND_URL = "http://semantic_search_backend:8010/"
SEMANTIC_SEARCH_BACKEND_URL = os.getenv('SEMANTIC_SEARCH_BACKEND_URL', 'http://semantic_search_backend:8010/')

# Shared DGLink navigation (see mcp/frontend/app.py). Defaults to the
# docker-compose localhost ports; override via env for other deployments.
NAV = {
    'overview': os.getenv('NAV_OVERVIEW_URL', 'http://localhost:5003/'),
    'chat': os.getenv('NAV_CHAT_URL', 'http://localhost:5005/'),
    'sequence': os.getenv('NAV_SEQUENCE_URL', 'http://localhost:5002/'),
    'query': os.getenv('NAV_QUERY_URL', 'http://localhost:5001/'),
}

# When false, the Sequence Search tab is hidden in the nav everywhere.
SHOW_SEQUENCE_SEARCH = os.getenv('SHOW_SEQUENCE_SEARCH', 'true').strip().lower() in ('1', 'true', 'yes', 'on')

# When false, the Chat Assistant tab (and every other reference to the MCP chat
# interface) is hidden in the nav everywhere. Disabled for deployments where the
# LLM-writes-Cypher chat interface should not be exposed.
SHOW_CHAT = os.getenv('SHOW_CHAT', 'true').strip().lower() in ('1', 'true', 'yes', 'on')

# Neo4j Browser web-view, linked from the nav on every page. Authentication is
# disabled for this deployment, so the URL opens with no login required.
NEO4J_BROWSER_URL = os.getenv('NEO4J_BROWSER_URL', 'http://localhost:7474')
def process_results(raw_results):
    processed = []
    for row in raw_results:
        processed_row = []
        for ent in row:
            try:
                load = ast.literal_eval(ent.split(":", 1)[1].strip())
                for key in load:
                    val = load[key]
                    if (
                        (key.split(" ")[-1] == "iri")
                        or (key.split(" ")[-1] == "study_url")
                        or (key.split(" ")[-1] == "evidence")
                    ):
                        processed_row.append(
                            {
                                "text": f"{val}",
                                "field": key,
                                "url": val,  # store the actual url
                            }
                        )
                    else:
                        processed_row.append(
                            {
                                "text": f"{key}: {val}",
                                "url": None,  # store the actual url
                            }
                        )
            except:
                processed_row.append({"text": ent, "url": None})  # store the actual url
        processed.append(processed_row)
    return processed


@app.route("/", methods=["GET", "POST"])
def index():
    result = None
    form_data = {
        "Agent": "",
        "Relation": "",
        "OtherAgent": "",
        "QueryType": "Subject",  # default selection
    }
    if request.method == "POST":
        form_data["Agent"] = request.form.get("Agent", "")
        form_data["Relation"] = request.form.get("Relation", "")
        form_data["OtherAgent"] = request.form.get("OtherAgent", "")
        form_data["QueryType"] = request.form.get("QueryType", "Subject")
        agent = request.form.get("Agent")
        relation = request.form.get("Relation")
        other_agent = request.form.get("OtherAgent")
        query_type = request.form.get("QueryType")
        response = requests.get(
            f"{SEMANTIC_SEARCH_BACKEND_URL}/query",
            params={
                "agent": agent,
                "relation": relation,
                "other_agent": other_agent,
                "query_type": query_type,
            },
        )
        data = response.json()
        raw_result = data["message"]
        result = process_results(raw_results=raw_result)

    return render_template(
        "index.html", result=result, form_data=form_data, nav=NAV, active="query",
        show_sequence=SHOW_SEQUENCE_SEARCH, show_chat=SHOW_CHAT,
        neo4j_browser_url=NEO4J_BROWSER_URL,
    )


@app.route("/autocomplete")
def autocomplete():
    query = request.args.get("query", "")
    completion_type = request.args.get("inputId", "").lower()
    response = requests.get(
        f"{SEMANTIC_SEARCH_BACKEND_URL}/autoComplete",
        params={
            "query": query,
            "completion_type": completion_type,
        },
    )
    data = response.json()
    return data


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
