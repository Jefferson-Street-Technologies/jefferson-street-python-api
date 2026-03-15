import pytest
import requests
import requests_mock

from jstdata.client import (
    ApiKeyNotSetError,
    InvalidApiKeyError,
    InvalidInputError,
    JSTDataClient
)


@pytest.fixture
def mock_url():
    """Fixture for the base URL of the Jefferson Street API."""
    return "https://api.jeffersonst.io"

@pytest.fixture(autouse=True)
def no_local_config(monkeypatch):
    r = lambda x: {}
    w = lambda x: None
    monkeypatch.setattr( "jstdata.client.JSTDataClientConfig.read", r)
    monkeypatch.setattr( "jstdata.client.JSTDataClientConfig.write", w)


@pytest.fixture
def client(mock_url):
    """Fixture for JSTDataClient with a dummy API key."""
    cli = JSTDataClient(api_key="test_api_key", base_url=mock_url)
    return cli


def test_client_init_no_api_key():
    """Test that ApiKeyNotSetError is raised when no API key is provided."""
    with pytest.raises(ApiKeyNotSetError):
        client = JSTDataClient(api_key=None)
        client.api_key


def test_validate_api_key_success(client, mock_url):
    """Test successful API key validation."""
    with requests_mock.Mocker() as m:
        m.get(f"{mock_url}/heartbeat", json={"status": "ok"})
        api_key = client.api_key


def test_validate_api_key_failure(client, mock_url):
    """Test failed API key validation due to invalid key or non-ok status."""
    with requests_mock.Mocker() as m:
        m.get(f"{mock_url}/heartbeat", status_code=403)
        with pytest.raises(InvalidApiKeyError):
            client.make_request("heartbeat")


def test_make_request_http_error(client, mock_url):
    """Test make_request handling of HTTP errors."""
    with requests_mock.Mocker() as m:
        m.get(f"{mock_url}/heartbeat", json={"status": "ok"})  # For initial validation
        m.get(f"{mock_url}/some-endpoint", status_code=404)
        with pytest.raises(requests.exceptions.HTTPError):
            client.make_request("some-endpoint")

