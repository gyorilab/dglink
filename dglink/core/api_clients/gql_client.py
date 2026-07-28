"""
Helper client for interfacing with GraphQL APIs
"""

from gql import Client, GraphQLRequest, gql
from gql.transport.requests import RequestsHTTPTransport

import logging

logger = logging.getLogger(__name__)


class GqlClient:
    def __init__(
        self,
        endpoint,
        fetch_schema_from_transport: bool = True,
    ):
        self.endpoint = endpoint
        self.transport = RequestsHTTPTransport(
            url=endpoint,
            verify=True,
        )
        self.client = Client(
            transport=self.transport,
            execute_timeout=30,
            fetch_schema_from_transport=fetch_schema_from_transport,
        )

    def execute(
        self, query: str | GraphQLRequest, variable_values: dict, max_retries: int = 3
    ):
        """Run a query with some set of parameters"""
        if not isinstance(query, GraphQLRequest):
            try:
                query = gql(query)
            except TypeError as e:
                raise TypeError(f"Invalid query: {e}") from e
        for attempt in range(max_retries):
            try:
                return self.client.execute(query, variable_values=variable_values)
            except Exception as e:
                logger.warning(f"Query attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    raise

    def batch_execute(
        self,
        query: str | GraphQLRequest,
        variable_values: dict,
        page_size: int = 1000,
        key: str = None,
    ):
        """Run a query with some set of parameters"""
        results = []
        variable_values["offset"] = 0
        variable_values["first"] = page_size
        while True:
            batch = self.execute(query, variable_values)
            result_key = key or list(batch.keys())[0]
            batch_res = batch[result_key]
            results.extend(batch_res)
            if len(batch_res) < page_size:
                break
            variable_values["offset"] += page_size
        return results
