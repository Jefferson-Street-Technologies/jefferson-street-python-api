import os, json, dataclasses, hashlib
from pathlib import Path
from typing import Any, Dict, Optional, Union, List
import requests
from collections import namedtuple
import pandas as pd

EntityType = namedtuple("EntityType", ["name", "slug", "classification"])
APP_DIR = Path.home() / ".jstdata"

class ApiKeyNotSetError(Exception):
    pass


class InvalidApiKeyError(Exception):
    pass


class InvalidInputError(Exception):
    pass


@dataclasses.dataclass
class JSTDataClientConfig:
    api_key: Optional[str] = None
    base_url: str = "https://api.jeffersonst.io"

    def __post_init__(self):
        APP_DIR.mkdir(exist_ok=True)
        self._default_cache_file = APP_DIR / "cache.json"
        if not self._default_cache_file.exists():
            with open(self._default_cache_file, "w") as f:
                json.dump({}, f)

        self.config_file = self._default_cache_file

    def write(self, **kwargs) -> None:
        fp = kwargs.get("config_file") or self.config_file
        cfg = {
            "api_key": kwargs.get("api_key") or self.api_key,
            "base_url": kwargs.get("base_url") or self.base_url
        }
        with open(self.config_file, "w") as f:
            json.dump(cfg, f)

    def read(self, **kwargs) -> Dict[str, str]:
        fp = kwargs.get("config_file") or self.config_file
        with open(fp, "r") as f:
            return json.load(f)

@dataclasses.dataclass
class JSTDataCache:
    endpoint: str
    params: Optional[Dict[str, Union[str,int]]]

    def __post_init__(self):
        if self.params is None:
            self.params = {}

        sorted_items = sorted(self.params.items())
        sorted_items = sorted(sorted_items, key=lambda x: x[0])
        self.params = dict(sorted_items)

        self._cache_dir = APP_DIR / "cache"
        self._cache_dir.mkdir(exist_ok=True)

        json_data = json.dumps({
            "endpoint": self.endpoint,
            "params": self.params,
        })

        key = hashlib.sha256(json_data.encode()).hexdigest()

        self._cache_file = self._cache_dir / f"{key}.parquet"

    def read(self):
        if not self._cache_file.exists():
            return None

        print('read cache')
        return json.loads(pd.read_parquet(self._cache_file).to_json())

    def write(self, records: List[dict]):
        print('write cache')
        pd.DataFrame(records).to_parquet(self._cache_file)

class JSTDataClient:
    def __init__(
            self,
            api_key: Optional[str] = None,
            base_url: str = "https://api.jeffersonst.io"
    ):
        """
        Initializes the JeffersonStreetClient with an API key.

        Args:
            api_key: The API key for authenticating with the Jefferson Street REST API.
        """

        self._cfg = JSTDataClientConfig(api_key=api_key, base_url=base_url)
        self._endpoints = None
        
    @property
    def api_key(self):
        """
        Returns the API key used by the client.
        """
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
        return self._cfg.base_url or cached_base_url

    def cache_config(self, **kwargs):
        return self._cfg.write(**kwargs)

    def make_request(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None, enable_cache: bool = False
    ) -> Dict[str, Any]:
        """
        Makes a GET request to the specified API endpoint.

        Args:
            endpoint: The API endpoint to call.
            params: A dictionary of query parameters to send with the request.

        Returns:
            A dictionary containing the JSON response from the API.

        Raises:
            ApiKeyNotSetError: If the API key is not set.
            InvalidApiKeyError: If the API key is invalid.
            requests.exceptions.RequestException: For network-related errors or unsuccessful HTTP responses.
        """
        # if first character isn't a slash, add it
        if endpoint[0] != "/":
            endpoint = f"/{endpoint}"

        url = f"{self.base_url}{endpoint}"

        cache = JSTDataCache(endpoint, params)
        if enable_cache:
            cached_records = cache.read()
            if cached_records:
                return {
                    'limit': params.get("limit"),
                    'offset': params.get("offset"),
                    "records": cached_records
                }

        api_key = self.api_key
        with requests.Session() as session:
            session.params = {"api-key": api_key}
            response = session.get(url, params=params)

        if response.status_code == 403:
            raise InvalidApiKeyError("Invalid API key")

        response.raise_for_status()
        if enable_cache:
            cache.write(response.json().get("records"))
        return response.json()

    def get_endpoints(self):
        if not self._endpoints:
            openapi = self.make_request("openapi.json")
            self._endpoints = list(openapi["paths"].keys())
        return self._endpoints

