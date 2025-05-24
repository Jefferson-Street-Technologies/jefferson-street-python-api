import os
from typing import List, Optional, Dict, Any, Union
from datetime import datetime
import requests
import json
import pandas as pd
from dataclasses import dataclass, asdict
from tabulate import tabulate

# For testing purposes
SERVER = os.getenv("JEFFERSON_STREET_SERVER") or "https://api.jeffersonst.io"

class ApiKeyNotSetError(Exception):
    pass

class InvalidApiKeyError(Exception):
    pass

def validate_api_key(api_key) -> None:
    if api_key is None:
        raise ApiKeyNotSetError("API key is not set")
    if False: # Call API to validate
        raise InvalidApiKeyError("Invalid API key")
    return

class BaseResponse:
    """Base class for all API responses with common conversion methods"""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert response to dictionary"""
        return asdict(self)
    
    def to_json(self, indent: int = 2) -> str:
        """Convert response to JSON string"""
        def datetime_handler(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
        
        return json.dumps(self.to_dict(), indent=indent, default=datetime_handler)
    
    def to_dataframe(self) -> pd.DataFrame:
        """Convert response to pandas DataFrame"""
        if hasattr(self, 'records'):
            return pd.DataFrame([asdict(record) for record in self.records])
        return pd.DataFrame([self.to_dict()])
    
    def to_table(self) -> str:
        """Convert response to formatted table string"""
        if hasattr(self, 'records'):
            return tabulate(self.to_dataframe(), headers='keys', tablefmt='grid')
        return tabulate(self.to_dataframe(), headers='keys', tablefmt='grid')

@dataclass
class MetricItem:
    slug: str
    name: str
    frequencies: List[str]
    units: List[str]
    last_updated: datetime

@dataclass
class MetricResponse(BaseResponse):
    records: List[MetricItem]
    limit: int
    offset: int

@dataclass
class SeriesDimensionItem:
    geography: Optional[str]
    secondary_geography: Optional[str]
    industry: Optional[str]
    commodity: Optional[str]

@dataclass
class SeriesDescriptionItem:
    id: str
    source: str
    frequency: str
    unit: str
    last_updated: datetime
    dimensions: SeriesDimensionItem

@dataclass
class SeriesResponse(BaseResponse):
    records: List[SeriesDescriptionItem]
    limit: int
    offset: int

@dataclass
class SeriesObservationItem:
    id: str
    observation_date: datetime
    release_date: datetime
    value: float

@dataclass
class ObservationResponse(BaseResponse):
    records: List[SeriesObservationItem]
    limit: int
    offset: int

@dataclass
class PriceItem:
    date: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int

@dataclass
class PriceResponse(BaseResponse):
    records: List[PriceItem]
    limit: int
    offset: int

class JeffersonStreetClient:
    def __init__(self, api_key: Optional[str] = None, base_url: str = SERVER):
        if api_key is None:
            from dotenv import load_dotenv
            load_dotenv()
            api_key = os.getenv("JEFFERSON_STREET_API_KEY")
        try:
            validate_api_key(api_key)
        except Exception as e:
            raise e

        self.api_key = api_key
        self.base_url = base_url
        self.session = requests.Session()
        self.session.params = {"api-key": api_key}

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
        sort_order: str = "desc"
    ) -> MetricResponse:
        """Get available metrics.
        
        Args:
            limit: Maximum number of records to return (default: 10000)
            offset: Number of records to skip (default: 0)
            order_by: Column to order by (default: "last_updated")
            sort_order: Sort order ("asc" or "desc", default: "desc")
            
        Returns:
            MetricResponse containing list of available metrics
        """
        return self._make_request("metric", {"limit": limit, "offset": offset, "order_by": order_by, "sort_order": sort_order})

    def get_metric_series(
        self,
        metric: str,
        limit: int = 10000,
        offset: int = 0,
        order_by: str = "last_updated",
        sort_order: str = "asc"
    ) -> SeriesResponse:
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
        pass

    def get_metric_observations(
        self,
        series: List[str],
        observation_type: str = "latest",
        limit: int = 10000,
        offset: int = 0,
        order_by: str = "id",
        sort_order: str = "asc"
    ) -> ObservationResponse:
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
        pass

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
    ) -> PriceResponse:
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
