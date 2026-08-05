from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import asyncio
import csv
import functools
import os
import pathlib
from contextlib import AsyncExitStack
import textwrap

# OpenAI imports
from agents import Agent, Runner, function_tool
from agents.mcp import MCPServerStdio

# Anthropic imports
from anthropic import Anthropic
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo-4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")
NEO4J_DATABASE = os.getenv("NEO4J_DATABASE", "neo4j")


# config variable
PORTAL_NAME = os.getenv("PORTAL_NAME", "NF Data Portal")
MAX_TURNS = int(os.getenv("MAX_TURNS", "50"))
ABBREVIATED_RESPONSE = os.getenv("ABBREVIATED_RESPONSE", "0")
USE_LOCAL_FILES = os.getenv("USE_LOCAL_FILES", "False").lower() in ("true", "1", "t")

abbreviated = bool(int(ABBREVIATED_RESPONSE))

# ---------------------------------------------------------------------------
# Local-file access tool
# When USE_LOCAL_FILES is on, the agent can resolve an entity's file_id (CURIE)
# to its downloaded source file and read the contents for extra context. Reads
# are sandboxed to DATA_ROOT so the agent can never escape the mounted volume.
# ---------------------------------------------------------------------------
DATA_ROOT = pathlib.Path("/.data").resolve()
FILE_INDEX_PATH = DATA_ROOT / "nci_file_index.tsv"
MAX_FILE_CHARS = 20000


@functools.lru_cache(maxsize=1)
def _load_file_index() -> dict:
    """Map file_id (CURIE) -> in-container path, from the mounted index tsv."""
    if not FILE_INDEX_PATH.exists():
        print(f"WARNING: file index not found at {FILE_INDEX_PATH}")
        return {}
    index = {}
    with open(FILE_INDEX_PATH, newline="") as f:
        for row in csv.DictReader(f, delimiter="\t"):
            path = row.get("docker_path") or row.get("path")
            if row.get("file_id") and path:
                index[row["file_id"]] = path
    return index


def read_entity_file(file_id: str, max_chars: int = MAX_FILE_CHARS) -> str:
    """Read the downloaded source file for a given entity.

    Args:
        file_id: The entity's file identifier (CURIE) as it appears in the graph.
        max_chars: Maximum number of characters to return from the file.
    """
    path = _load_file_index().get(file_id)
    if not path:
        return f"No file is indexed for entity '{file_id}'."
    resolved = pathlib.Path(path).resolve()
    if resolved != DATA_ROOT and DATA_ROOT not in resolved.parents:
        return "Access denied: file is outside the permitted data directory."
    if not resolved.exists():
        return f"File for '{file_id}' is indexed at {path} but is not present on disk."
    return resolved.read_text(errors="replace")[:max_chars]


# Anthropic tool schema for the same function (OpenAI uses the decorated version).
READ_ENTITY_FILE_SCHEMA = {
    "name": "read_entity_file",
    "description": (
        "Read the downloaded source file for an entity to get additional "
        "context. Pass the entity's file_id (CURIE) exactly as it appears in "
        "the graph."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "file_id": {
                "type": "string",
                "description": "The entity's file_id (CURIE) from the graph.",
            },
        },
        "required": ["file_id"],
    },
}

# Global variables
openai_mcp = None
openai_agent = None
anthropic_client = None
anthropic_session = None
anthropic_exit_stack = None


class ChatRequest(BaseModel):
    message: str
    provider: str = "openai"


def get_base_prompt():
    base_prompt = textwrap.dedent(f"""
        You are a helpful scientist with access to a Neo4j database of content containing biomedical entities, studies, and datasets from the {PORTAL_NAME}.
        Use the Neo4j database and your scientific knowledge to answer the users questions. 
        Try to answer the user query directly with out going to far off track
            """).strip()
    if abbreviated:
        base_prompt += textwrap.dedent(f"""
                    Rules:
                    - Answer the user's question directly and concisely.  
                    - Do not explain what you are about to do before doing it.
                    - Do not summarize or restate the question.
                    - Do not add closing remarks like "I hope this helps" or "Let me know if you need more."
                    - If the answer requires data from the graph, query it and report only the relevant results.
                    - If you cannot answer from the graph, say so briefly.

                """).strip()
    if USE_LOCAL_FILES:
        base_prompt += textwrap.dedent(f"""
                    Rules:
                    - If any additional context is needed for understanding the role of an entity in a project, call the `read_entity_file` tool with that entity's file_id (CURIE) from the graph to read its downloaded source file directly.
                """).strip()

    print(f"USING abbreviated={abbreviated} for responses!!!!!!")
    print(f"USING local files = {USE_LOCAL_FILES} for response!!!!!!")
    print(f"BASE prompt:\n{base_prompt}")
    return base_prompt


async def initialize_openai():
    """Initialize OpenAI agent with MCP"""
    global openai_mcp, openai_agent

    if not OPENAI_API_KEY:
        print("Warning: OPENAI_API_KEY not set, OpenAI agent disabled")
        return

    try:
        # Create the MCP server for OpenAI
        openai_mcp = MCPServerStdio(
            params={
                "command": "uvx",
                "args": ["--with", "fastmcp<2.3.0", "mcp-neo4j-cypher@latest"],
                "env": {
                    "NEO4J_URI": NEO4J_URI,
                    "NEO4J_USERNAME": NEO4J_USER,
                    "NEO4J_PASSWORD": NEO4J_PASSWORD,
                    "NEO4J_DATABASE": NEO4J_DATABASE,
                },
            },
            # uvx cold-starting the neo4j MCP server can take well over the
            # SDK default of 5s; give the handshake room so init doesn't time out.
            client_session_timeout_seconds=60,
        )

        # Connect to the server
        await openai_mcp.connect()
        print("OpenAI MCP server connected!")

        # Wait for initialization
        await asyncio.sleep(2)
        base_prompt = get_base_prompt()
        # Create OpenAI agent
        local_tools = [function_tool(read_entity_file)] if USE_LOCAL_FILES else []
        openai_agent = Agent(
            name="Assistant",
            model="gpt-5",
            instructions=base_prompt,
            mcp_servers=[openai_mcp],
            tools=local_tools,
        )
        print("OpenAI Agent (gpt-5-chat-latest) Ready!")
    except Exception as e:
        print(f"Error initializing OpenAI agent: {e}")


async def initialize_anthropic():
    """Initialize Anthropic client with MCP"""
    global anthropic_client, anthropic_session, anthropic_exit_stack

    if not ANTHROPIC_API_KEY:
        print("Warning: ANTHROPIC_API_KEY not set, Anthropic client disabled")
        return

    try:
        anthropic_client = Anthropic(api_key=ANTHROPIC_API_KEY)
        anthropic_exit_stack = AsyncExitStack()

        # Set up MCP connection for Anthropic
        server_params = StdioServerParameters(
            command="uvx",
            args=["--with", "fastmcp<2.3.0", "mcp-neo4j-cypher@latest"],
            env={
                "NEO4J_URI": NEO4J_URI,
                "NEO4J_USERNAME": NEO4J_USER,
                "NEO4J_PASSWORD": NEO4J_PASSWORD,
                "NEO4J_DATABASE": NEO4J_DATABASE,
            },
        )

        stdio_transport = await anthropic_exit_stack.enter_async_context(
            stdio_client(server_params)
        )
        stdio, write = stdio_transport
        anthropic_session = await anthropic_exit_stack.enter_async_context(
            ClientSession(stdio, write)
        )

        await anthropic_session.initialize()

        # List available tools
        response = await anthropic_session.list_tools()
        tools = response.tools
        print(f"Anthropic MCP connected with tools: {[tool.name for tool in tools]}")
        print("Anthropic Client (Claude Sonnet 4) Ready!")
    except Exception as e:
        print(f"Error initializing Anthropic client: {e}")


async def initialize():
    """Initialize both OpenAI and Anthropic integrations"""
    print("=" * 60)
    print("Initializing MCP integrations...")
    print("=" * 60)

    # Initialize both in parallel
    await asyncio.gather(
        initialize_openai(), initialize_anthropic(), return_exceptions=True
    )

    print("=" * 60)
    print("Neo4j Query Assistant Ready!")
    print("=" * 60)


async def stream_anthropic_query(query: str):
    """Process and stream a query using Anthropic Claude with MCP tools"""
    if not anthropic_client or not anthropic_session:
        raise HTTPException(
            status_code=503,
            detail="Anthropic client not available. Check ANTHROPIC_API_KEY.",
        )

    print("Starting check")
    ## General response format ##
    messages = [
        {"role": "assistant", "content": get_base_prompt()},
        {"role": "user", "content": query},
    ]

    # Get available tools
    response = await anthropic_session.list_tools()
    available_tools = [
        {
            "name": tool.name,
            "description": tool.description,
            "input_schema": tool.inputSchema,
        }
        for tool in response.tools
    ]
    if USE_LOCAL_FILES:
        available_tools.append(READ_ENTITY_FILE_SCHEMA)

    # Tool calling loop
    while True:
        tool_used = False

        # Stream the response
        with anthropic_client.messages.stream(
            model="claude-opus-4-8",
            max_tokens=4000,
            messages=messages,
            tools=available_tools,
        ) as stream:
            assistant_content = []

            for event in stream:
                if event.type == "content_block_start":
                    if event.content_block.type == "text":
                        # Text block started, ready to stream
                        pass

                elif event.type == "content_block_delta":
                    if event.delta.type == "text_delta":
                        # Stream text as it arrives
                        yield event.delta.text

                elif event.type == "content_block_stop":
                    # Block finished
                    pass

            # Get the final message after streaming
            final_message = stream.get_final_message()

            # Check for tool use
            for content in final_message.content:
                assistant_content.append(content)

                if content.type == "tool_use":
                    tool_used = True

                    # Log Cypher queries to stdout
                    # if "cypher" in content.name.lower():
                    print(f"\n{'='*60}")
                    print(f"[CYPHER QUERY] Tool: {content.name}")
                    if "query" in content.input:
                        print(f"Query:\n{content.input['query']}")
                    if "params" in content.input and content.input["params"]:
                        print(f"Parameters: {content.input['params']}")
                    print(f"{'='*60}\n")

                    # Route to the local-file tool, or fall through to MCP.
                    if content.name == "read_entity_file":
                        tool_result_content = read_entity_file(**content.input)
                    else:
                        result = await anthropic_session.call_tool(
                            content.name, content.input
                        )
                        tool_result_content = result.content

                    # Append assistant message with tool_use
                    messages.append({"role": "assistant", "content": assistant_content})

                    # Append tool_result as user message
                    messages.append(
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "tool_result",
                                    "tool_use_id": content.id,
                                    "content": tool_result_content,
                                }
                            ],
                        }
                    )

                    break  # Exit the for loop to make another API call

        if not tool_used:
            break  # No more tools needed, exit while loop
    print("EXITING POST")


async def cleanup():
    """Cleanup on shutdown"""
    global openai_mcp, anthropic_exit_stack

    # Cleanup OpenAI MCP
    try:
        if openai_mcp:
            if hasattr(openai_mcp, "cleanup"):
                await openai_mcp.cleanup()
            elif hasattr(openai_mcp, "disconnect"):
                await openai_mcp.disconnect()
    except Exception as e:
        print(f"Error cleaning up OpenAI MCP: {e}")

    # Cleanup Anthropic MCP
    try:
        if anthropic_exit_stack:
            await anthropic_exit_stack.aclose()
    except Exception as e:
        print(f"Error cleaning up Anthropic MCP: {e}")


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    await initialize()


@app.on_event("shutdown")
async def shutdown_event():
    await cleanup()


@app.post("/chat")
async def chat(request: ChatRequest):
    """Handle chat requests - routes to appropriate provider"""
    try:
        user_query = request.message.strip()

        if not user_query:
            raise HTTPException(status_code=400, detail="Message cannot be empty")

        # Route based on provider
        if request.provider == "anthropic":
            print(f"\nProcessing with Claude: {user_query}")

            # Stream the response back
            async def generate_anthropic():
                async for chunk in stream_anthropic_query(user_query):
                    yield chunk

            return StreamingResponse(generate_anthropic(), media_type="text/plain")

        else:  # OpenAI
            if not openai_agent:
                raise HTTPException(
                    status_code=503,
                    detail="OpenAI agent not available. Check OPENAI_API_KEY.",
                )

            print(f"\nProcessing with GPT: {user_query}")

            # Stream the response back
            async def generate_openai():
                result = await Runner.run(openai_agent, user_query, max_turns=MAX_TURNS)
                # Simulate streaming by yielding in chunks
                text = result.final_output
                chunk_size = 50
                for i in range(0, len(text), chunk_size):
                    yield text[i : i + chunk_size]
                    await asyncio.sleep(0.01)  # Small delay for streaming effect

            return StreamingResponse(generate_openai(), media_type="text/plain")

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "openai_agent_ready": openai_agent is not None,
        "anthropic_client_ready": anthropic_client is not None
        and anthropic_session is not None,
        "openai_mcp_connected": openai_mcp is not None,
        "anthropic_mcp_connected": anthropic_session is not None,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8009)
