from flask import Flask, render_template, request, jsonify, Response
import requests
import json
import os
import logging
logger = logging.getLogger()

app = Flask(__name__)

# Backend API configuration - use environment variable for Docker, fallback to localhost
BACKEND_URL = os.getenv('GENOMIC_INDEX_QUERY_BACKEND_URL', 'http://localhost:8002')

# Shared DGLink navigation (see mcp/frontend/app.py). Defaults to the
# docker-compose localhost ports; override via env for other deployments.
NAV = {
    'overview': os.getenv('NAV_OVERVIEW_URL', 'http://localhost:5003/'),
    'chat': os.getenv('NAV_CHAT_URL', 'http://localhost:5000/'),
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


@app.route('/')
def index():
    return render_template('index.html', nav=NAV, active='sequence', show_sequence=SHOW_SEQUENCE_SEARCH,
                           show_chat=SHOW_CHAT, neo4j_browser_url=NEO4J_BROWSER_URL)

@app.route('/chat', methods=['GET', 'POST'])
def chat():
    try:
        data = request.json
        user_message = data.get('message', '')
        model_provider = data.get('provider', 'anthropic')  # 'anthropic' or 'openai'
        
        # Forward request to your backend
        response = requests.post(
            f'{MCP_BACKEND_URL}/chat',
            json={
                'message': user_message,
                'provider': model_provider
            },
            stream=True,
            timeout=300
        )
        
        # Stream the response back to the client
        def generate():
            for chunk in response.iter_content(chunk_size=1024, decode_unicode=True):
                if chunk:
                    yield chunk
        
        return Response(generate(), mimetype='text/plain')
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/health', methods=['GET'])
def get_health():
    """Get health status from backend"""
    try:
        response = requests.get(f'{BACKEND_URL}/health')
        return jsonify(response.json())
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route("/query", methods=["POST" ,"GET"])
def query():
    data = request.get_json(force=True)
 
    raw_input = data.get("sequence", "").strip()
    k = int(data.get("k", 31))
    threshold = float(data.get("threshold", 0.8))
    index = data.get("index", "mantis")
 
    if not raw_input:
        return jsonify({"error": "No sequence provided."}), 400
 
    resp = requests.post(
        f'{BACKEND_URL}/query',
        params = {
            'query_input' : raw_input,
            'k' : k, 
            'threshold' : threshold, 
            'index' : index
        }
    )
    resp.raise_for_status()

    try:
        return jsonify(resp.json()), resp.status_code
    except requests.exceptions.ConnectionError:
        return jsonify({"error": "Could not reach index server."}), 502
    except requests.exceptions.Timeout:
        return jsonify({"error": "Index server timed out."}), 504
    except requests.exceptions.HTTPError as e:
        return jsonify({"error": str(e)}), resp.status_code

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002, debug=True)
