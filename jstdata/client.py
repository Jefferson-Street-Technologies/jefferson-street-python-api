import os
from typing import List, Optional, Dict, Any
import requests
from dataclasses import dataclass
from enum import Enum
from collections import namedtuple

EntityType = namedtuple("EntityType", ["name", "slug", "classification"])

class ApiKeyNotSetError(Exception):
    pass

class InvalidApiKeyError(Exception):
    pass

class InvalidInputError(Exception):
    pass

class JeffersonStreetClient:
    # For testing purposes
    base_url = os.getenv("JEFFERSON_STREET_SERVER") or "https://api.jeffersonst.io"

    def __init__(self, api_key: Optional[str] = None):
        self._api_key = api_key
        self.session = requests.Session()
        self.session.params = {"api-key": api_key}

        self._api_key_is_valid = False

    @property
    def api_key(self):
        return self._api_key

    def _validate_api_key(self, api_key) -> None:
        if api_key is None:
            raise ApiKeyNotSetError("API key is not set")

        url = f"{self.base_url}/heartbeat"
        heartbeat = self.session.get(url).json()
        if heartbeat["status"] != "ok":
            raise InvalidApiKeyError("Invalid API key")
        self._api_key_is_valid = True
        return

    def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if not self._api_key_is_valid:
            self._validate_api_key(self.api_key)

        url = f"{self.base_url}/{endpoint}"
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def get_metrics(
        self,
        metric: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
        order_by: str = "last_updated",
        sort_order: str = "desc",
    ) -> Dict[str, Any]:
        """Get available metrics.
        
        Args:
            metric: The metric's slug. If empty, all metrics are returned
            limit: Maximum number of records to return (default: 10000)
            offset: Number of records to skip (default: 0)
            order_by: Column to order by (default: "last_updated")
            sort_order: Sort order ("asc" or "desc", default: "desc")
            
        Returns:
            MetricResponse containing list of available metrics
        """
        response = self._make_request("metric", {"metric": metric, "limit": limit, "offset": offset, "order_by": order_by, "sort_order": sort_order})
        return response["records"]

    def get_metric_dimensions(self, metric: str) -> Dict[str, Any]:
        try:
            response = self._make_request(f"metric/{metric}/entities")
        except requests.exceptions.HTTPError as e:
            raise InvalidInputError(f"Invalid input: {e}")
        return response["records"]

    def get_entity_groups(self) -> Dict[str, Any]:
        """Get available entity types.
        
        Returns:
            Dictionary containing list of available entity types
        """
        try:
            response = self._make_request("entity/groups")
        except requests.exceptions.HTTPError as e:
            raise InvalidInputError(f"Invalid input: {e}")
        return response["records"]

    def get_entities(
        self,
        entity_group: Optional[str] = None,
        offset: int = 0,
        limit: int = 10000,
        sort_order: str = "asc"
    ) -> Dict[str, Any]:
        """Get available entities.
        Args:
            offset: Number of records to skip (default: 0)
            limit: Maximum number of records to return (default: 10000)
            sort_order: Sort order ("asc" or "desc", default: "asc")
            
        Returns:
            Dictionary containing list of available entities
        """
        response = self._make_request(f"entity/{entity_group}", {"offset": offset, "limit": limit, "sort_order": sort_order})
        return response['records']

    def get_entity_metrics(
        self,
        entity: Optional[str] = None,
        limit: int = 10000,
        offset: int = 0,
        sort_order: str = "asc"
    ) -> Dict[str, Any]:
        """Get metrics for a specific entity.
        
        Args:
            entity: The entity's slug
            limit: Maximum number of records to return (default: 10000)
            offset: Number of records to skip (default: 0)
            order_by: Column to order by (default: "last_updated")
            sort_order: Sort order ("asc" or "desc", default: "asc")
            
        Returns:
            MetricResponse containing list of metrics for the entity
        """
        response = self._make_request(f"entity/{entity}/metrics", {"limit": limit, "offset": offset, "sort_order": sort_order})
        return response['records']


    def query(
        self,
        by: str,
        id: str,
        start_date: str,
        end_date: str,
        limit: int = 100,
        offset: int = 0,
        order_by: str = "release_date",
        sort_order: str = "asc",
        entity_filter: Optional[list[str]] = None
    ) -> Dict[str, Any]:
        """Query by metric.
        
        Args:
            by: The type of query to perform (either "metric" or "entity")
            id: The metric or entity's id/slug
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            limit: Maximum number of records to return (default: 100)
            offset: Number of records to skip (default: 0)
            order_by: Column to order by (either "release_date" or "series_label", default: "release_date")
            sort_order: Sort order ("asc" or "desc", default: "desc")
            
        Returns:
            QueryResponse containing list of query results
        """
        if by not in ['metric', 'entity']:
            raise InvalidInputError(f"Invalid input: {by}")
        if order_by not in ['release_date', 'series_label']:
            raise InvalidInputError(f"Invalid input: {order_by}")
        if sort_order not in ['asc', 'desc']:
            raise InvalidInputError(f"Invalid input: {sort_order}")
        params = {"start_date": start_date, "end_date": end_date, "limit": limit, "offset": offset, 'sort_order': sort_order, 'order_by': order_by}
        if entity_filter is not None:
            params['entities'] = entity_filter
        response = self._make_request(f"query/{by}/{id}",params)
        return response['records']

    def search_for_entity(
        self,
        query: str,
        limit: int = 3,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Search for an entity.
        
        Args:
            query: The query to search for
            offset: Number of records to skip (default: 0)
            limit: Maximum number of records to return (default: 10000)
            sort_order: Sort order ("asc" or "desc", default: "asc")
            
        Returns:
            SearchResponse containing list of search results
        """
        response = self._make_request(f"search/entity", {"query": query, "offset": offset, "limit": limit})
        return response['records']
