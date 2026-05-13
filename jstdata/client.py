import hashlib
import json
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import requests

from .models import (
    Entity,
    EntityRelationship,
    Metric,
    Observation,
    Series,
)

APP_DIR = Path.home() / ".jstdata"


class ApiKeyNotSetError(Exception):
    pass


class InvalidApiKeyError(Exception):
    pass


class InvalidInputError(Exception):
    pass


@dataclass
class JSTDataClientConfig:
    api_key: Optional[str] = None
    base_url: str = "https://api.jeffersonst.io"

    def __post_init__(self):
        APP_DIR.mkdir(exist_ok=True)
        self._default_cache_file = APP_DIR / "cache.json"
        if not self._default_cache_file.exists():
            with open(self._default_cache_file, "w") as f:
                json.dump({}, f)
        else:
            with open(self._default_cache_file, "r") as f:
                cfg = json.load(f)
                self.api_key = cfg.get("api_key")
                self.base_url = cfg.get("base_url")

        self.config_file = self._default_cache_file

    def write(self, **kwargs) -> None:
        cfg = {
            "api_key": kwargs.get("api_key") or self.api_key,
            "base_url": kwargs.get("base_url") or self.base_url,
        }
        with open(self.config_file, "w") as f:
            json.dump(cfg, f)

    def read(self, **kwargs) -> Dict[str, str]:
        fp = kwargs.get("config_file") or self.config_file
        with open(fp, "r") as f:
            return json.load(f)


@dataclass
class JSTDataCache:
    endpoint: str
    params: Optional[Dict[str, Any]]

    def __post_init__(self):
        if self.params is None:
            self.params = {}

        # Normalize params for hashing
        sorted_items = sorted(
            [(k, str(v)) for k, v in self.params.items() if v is not None]
        )
        self.params = dict(sorted_items)

        self._cache_dir = APP_DIR / "cache"
        self._cache_dir.mkdir(exist_ok=True)

        json_data = json.dumps(
            {
                "endpoint": self.endpoint,
                "params": self.params,
            }
        )

        key = hashlib.sha256(json_data.encode()).hexdigest()
        self._cache_file = self._cache_dir / f"{key}.parquet"

    def read(self):
        if not self._cache_file.exists():
            return None
        return pd.read_parquet(self._cache_file)

    def write(self, df: pd.DataFrame):
        df.to_parquet(self._cache_file)


class JSTDataClient:
    def __init__(
        self, api_key: Optional[str] = None, base_url: str = "https://api.jeffersonst.io"
    ):
        """
        Initializes the JSTDataClient.

        Args:
            api_key: The API key for authenticating with the Jefferson Street REST API.
            base_url: The base URL of the API.
        """
        self._cfg = JSTDataClientConfig(api_key=api_key, base_url=base_url)

    @property
    def api_key(self):
        api_key = self._cfg.api_key
        if not api_key:
            cached_cfg = self._cfg.read()
            cached_api_key = cached_cfg.get("api_key")
            if not cached_api_key:
                raise ApiKeyNotSetError("API key is not set")
            return cached_api_key
        return api_key

    @property
    def base_url(self):
        cached_cfg = self._cfg.read()
        cached_base_url = cached_cfg.get("base_url")
        return cached_base_url or self._cfg.base_url

    def make_request(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        enable_cache: bool = False,
    ) -> Dict[str, Any]:
        """
        Low-level method to make a request to the API.
        """
        if endpoint[0] != "/":
            endpoint = f"/{endpoint}"

        url = f"{self.base_url}{endpoint}"

        # Caching logic could be more sophisticated, but keeping it simple for now
        if enable_cache:
            cache = JSTDataCache(endpoint, params)
            cached_df = cache.read()
            if cached_df is not None:
                return {"records": cached_df.to_dict("records")}

        api_key = self.api_key
        with requests.Session() as session:
            session.params = {"api-key": api_key}
            response = session.get(url, params=params)

        if response.status_code == 403:
            raise InvalidApiKeyError("Invalid API key")
        response.raise_for_status()

        data = response.json()
        if enable_cache and "records" in data:
            cache.write(pd.DataFrame(data["records"]))

        return data

    # --- Metrics ---

    def list_metrics(
        self, limit: int = 100, offset: int = 0, sort_order: str = "asc"
    ) -> List[Metric]:
        """List all available metrics."""
        data = self.make_request(
            "metric", {"limit": limit, "offset": offset, "sort_order": sort_order}
        )
        return [Metric.from_dict(m) for m in data["records"]]

    def get_metric(self, metric_id: str) -> Metric:
        """Get details for a specific metric."""
        data = self.make_request(f"metric/{metric_id}")
        return Metric.from_dict(data)

    def get_metric_series(
        self, metric_id: str, limit: int = 100, offset: int = 0
    ) -> List[Series]:
        """Get all series associated with a metric."""
        data = self.make_request(f"metric/{metric_id}/series", {"limit": limit, "offset": offset})
        return [Series.from_dict(s) for s in data["records"]]

    # --- Series ---

    def list_series(
        self, limit: int = 100, offset: int = 0, sort_order: str = "asc"
    ) -> List[Series]:
        """List all available series."""
        data = self.make_request(
            "series", {"limit": limit, "offset": offset, "sort_order": sort_order}
        )
        return [Series.from_dict(s) for s in data["records"]]

    def get_series(self, series_id: str) -> Series:
        """Get details for a specific series."""
        data = self.make_request(f"series/{series_id}")
        return Series.from_dict(data)

    # --- Entities ---

    def get_entity(self, entity_id: str) -> Entity:
        """Get details for a specific entity."""
        data = self.make_request(f"entity/{entity_id}")
        return Entity.from_dict(data)

    def get_entity_series(
        self, entity_id: str, limit: int = 100, offset: int = 0
    ) -> List[Series]:
        """Get all series associated with an entity."""
        data = self.make_request(f"entity/{entity_id}/series", {"limit": limit, "offset": offset})
        return [Series.from_dict(s) for s in data["records"]]

    def get_entity_relations(
        self, entity_id: str, limit: int = 100, offset: int = 0
    ) -> List[EntityRelationship]:
        """Get relationships for an entity (the graph view)."""
        data = self.make_request(
            f"entity/{entity_id}/relations", {"limit": limit, "offset": offset}
        )
        return [EntityRelationship.from_dict(r) for r in data["records"]]

    # --- Search ---

    def search_entities(
        self, query: str, limit: int = 5, offset: int = 0
    ) -> List[Entity]:
        """Search for entities."""
        data = self.make_request(
            "search/entities", {"query": query, "limit": limit, "offset": offset}
        )
        return [Entity.from_dict(e) for e in data["records"]]

    def search_metrics(
        self, query: str, limit: int = 5, offset: int = 0
    ) -> List[Metric]:
        """Search for metrics."""
        data = self.make_request(
            "search/metrics", {"query": query, "limit": limit, "offset": offset}
        )
        return [Metric.from_dict(m) for m in data["records"]]

    def search_series(
        self, query: str, limit: int = 5, offset: int = 0
    ) -> List[Series]:
        """Search for series."""
        data = self.make_request(
            "search/series", {"query": query, "limit": limit, "offset": offset}
        )
        return [Series.from_dict(s) for s in data["records"]]

    # --- Query ---

    def query(
        self,
        metric: Optional[Union[str, List[str]]] = None,
        entity: Optional[Union[str, List[str]]] = None,
        series: Optional[Union[str, List[str]]] = None,
        frequency: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 1000,
        offset: int = 0,
    ) -> List[Observation]:
        """
        Query for observations. This is the main data extraction method.
        """
        params = {
            "metric": metric,
            "entity": entity,
            "series": series,
            "frequency": frequency,
            "start_date": start_date,
            "end_date": end_date,
            "limit": limit,
            "offset": offset,
        }
        # Filter out None values
        params = {k: v for k, v in params.items() if v is not None}

        data = self.make_request("query", params)
        return [Observation.from_dict(r) for r in data["records"]]

    def query_df(self, **kwargs) -> pd.DataFrame:
        """
        Convenience method that returns the query results as a single flattened pandas DataFrame.
        Excellent for notebook usage.
        """
        observations = self.query(**kwargs)
        if not observations:
            return pd.DataFrame()

        df = pd.DataFrame([asdict(o) for o in observations])
        df["observation_timestamp"] = pd.to_datetime(df["observation_timestamp"])
        df["release_timestamp"] = pd.to_datetime(df["release_timestamp"])
        return df
