import os
from typing import List, Optional, Dict, Any
import requests

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
        self.api_key = api_key
        self.session = requests.Session()
        self.session.params = {"api-key": api_key}

        try:
            self._validate_api_key(api_key)
        except Exception as e:
            raise e

    def _validate_api_key(self, api_key) -> None:
        if api_key is None:
            raise ApiKeyNotSetError("API key is not set")
        heartbeat = self._make_request("heartbeat")
        if heartbeat["status"] != "ok":
            raise InvalidApiKeyError("Invalid API key")
        return

    def _make_request(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base_url}/{endpoint}"
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def get_metrics(
        self,
        limit: int = 10000,
        offset: int = 0,
        order_by: str = "last_updated",
        sort_order: str = "desc",
    ) -> Dict[str, Any]:
        """Get available metrics.
        
        Args:
            limit: Maximum number of records to return (default: 10000)
            offset: Number of records to skip (default: 0)
            order_by: Column to order by (default: "last_updated")
            sort_order: Sort order ("asc" or "desc", default: "desc")
            
        Returns:
            MetricResponse containing list of available metrics
        """
        response = self._make_request("metric", {"limit": limit, "offset": offset, "order_by": order_by, "sort_order": sort_order})
        return response["records"]

    def get_metric_series(
        self,
        metric: str,
        limit: int = 10000,
        offset: int = 0,
        order_by: str = "last_updated",
        sort_order: str = "asc"
    ) -> Dict[str, Any]:
        """Get series for a specific metric.
        
        Args:
            metric: The metric's slug
            limit: Maximum number of records to return (default: 10000)
            offset: Number of records to skip (default: 0)
            order_by: Column to order by (default: "last_updated")
            sort_order: Sort order ("asc" or "desc", default: "asc")
            
        Returns:
            SeriesResponse containing list of series for the metric
        """
        try:
            response = self._make_request("metric/series", {"metric": metric, "limit": limit, "offset": offset, "order_by": order_by, "sort_order": sort_order})
        except requests.exceptions.HTTPError as e:
            raise InvalidInputError(f"Invalid input: {e}")
        return response["records"]

    def get_metric_observations(
        self,
        series: List[str],
        observation_type: str = "latest",
        limit: int = 10000,
        offset: int = 0,
        order_by: str = "id",
        sort_order: str = "asc",
        expanded: bool = False,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get observations for one or more series.
        
        Args:
            series: List of series IDs
            observation_type: Type of observations ("earliest" or "latest", default: "latest")
            limit: Maximum number of records to return (default: 10000)
            offset: Number of records to skip (default: 0)
            order_by: Column to order by (default: "id")
            sort_order: Sort order ("asc" or "desc", default: "asc")
            
        Returns:
            ObservationResponse containing list of observations
        """
        try:
            response = self._make_request("metric/series", {"metric": metric, "limit": limit, "offset": offset, "order_by": order_by, "sort_order": sort_order})
        except requests.exceptions.HTTPError as e:
            raise InvalidInputError(f"Invalid input: {e}")
        return response["records"]

    def get_tickers(
        self,
        offset: int = 0,
        limit: int = 10000,
        sort_order: str = "asc"
    ) -> Dict[str, Any]:
        """Get available tickers.
        
        Args:
            offset: Number of records to skip (default: 0)
            limit: Maximum number of records to return (default: 10000)
            sort_order: Sort order ("asc" or "desc", default: "asc")
            
        Returns:
            Dictionary containing list of available tickers
        """
        pass

    def get_financials(
        self,
        ticker: str,
        metrics: Optional[str] = None,
        start_date: str = "2020-01-01",
        end_date: str = "2025-05-13",
        limit: int = 10000,
        offset: int = 0
    ) -> Dict[str, Any]:
        """Get historical financials for a company.
        
        Args:
            ticker: The ticker symbol
            metrics: Comma-separated list of metrics (optional)
            start_date: Earliest filing date (YYYY-MM-DD, default: "2020-01-01")
            end_date: Latest filing date (YYYY-MM-DD, default: "2025-05-13")
            limit: Maximum number of records to return (default: 10000)
            offset: Number of records to skip (default: 0)
            
        Returns:
            Dictionary containing financial data
        """
        pass

    def get_prices(
        self,
        ticker: str,
        start_date: str,
        end_date: str,
        limit: int = 10000,
        offset: int = 0,
        sort_order: str = "asc"
    ) -> Dict[str, Any]:
        """Get historical prices for a ticker.
        
        Args:
            ticker: The ticker symbol
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            limit: Maximum number of records to return (default: 10000)
            offset: Number of records to skip (default: 0)
            sort_order: Sort order ("asc" or "desc", default: "asc")
            
        Returns:
            PriceResponse containing list of price records
        """
        pass

    def get_countries(self, limit=10000, offset=0, sort_order='asc'):
        try:
            response = self._make_request("reference/geo/countries", {"limit": limit, "offset": offset, "sort_order": sort_order})
        except requests.exceptions.HTTPError as e:
            raise InvalidInputError(f"Invalid input: {e}")
        return response["records"]
