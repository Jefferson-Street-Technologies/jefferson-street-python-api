import pytest
import requests
import requests_mock
from datetime import datetime

from jstdata.client import (
    ApiKeyNotSetError,
    InvalidApiKeyError,
    JSTDataClient
)
from jstdata.models import Series, Entity, Metric, Observation


@pytest.fixture
def mock_url():
    """Fixture for the base URL of the Jefferson Street API."""
    return "https://api.jeffersonst.io"

@pytest.fixture(autouse=True)
def no_local_config(monkeypatch, tmp_path):
    # Set APP_DIR to a temp path to avoid touching user's home dir during tests
    monkeypatch.setattr("jstdata.client.APP_DIR", tmp_path)
    r = lambda *args, **kwargs: {"api_key": "test_api_key", "base_url": "https://api.jeffersonst.io"}
    w = lambda *args, **kwargs: None
    monkeypatch.setattr("jstdata.client.JSTDataClientConfig.read", r)
    monkeypatch.setattr("jstdata.client.JSTDataClientConfig.write", w)


@pytest.fixture
def client(mock_url):
    """Fixture for JSTDataClient with a dummy API key."""
    cli = JSTDataClient(api_key="test_api_key", base_url=mock_url)
    return cli


def test_client_init_no_api_key(monkeypatch, tmp_path):
    """Test that ApiKeyNotSetError is raised when no API key is provided."""
    monkeypatch.setattr("jstdata.client.APP_DIR", tmp_path)
    monkeypatch.setattr("jstdata.client.CONFIG_FILE", tmp_path / "config.json")
    # Ensure config read returns no api key
    monkeypatch.setattr("jstdata.client.JSTDataClientConfig.read", lambda *args, **kwargs: {})
    
    with pytest.raises(ApiKeyNotSetError):
        cli = JSTDataClient(api_key=None)
        _ = cli.api_key


def test_make_request_invalid_api_key(client, mock_url):
    """Test failed API key validation (403)."""
    with requests_mock.Mocker() as m:
        m.get(f"{mock_url}/some-endpoint", status_code=403)
        with pytest.raises(InvalidApiKeyError):
            client.make_request("some-endpoint")


def test_get_series(client, mock_url):
    """Test get_series method."""
    mock_data = {
        "id": "ABC123",
        "label": "Test Series",
        "frequency": "Monthly",
        "source": "Test Source",
        "units": "Test Units",
        "seasonal_adjustment": "Not Seasonally Adjusted",
        "last_updated": "2024-01-01 00:00:00",
        "metric_slug": "test-metric",
        "entities": [{"id": "usa", "label": "United States"}]
    }
    with requests_mock.Mocker() as m:
        m.get(f"{mock_url}/series/ABC123", json=mock_data)
        series = client.get_series("ABC123")
        assert isinstance(series, Series)
        assert series.id == "ABC123"
        assert series.entities[0].id == "usa"


def test_query(client, mock_url):
    """Test query method."""
    mock_data = {
        "records": [
            {
                "series_id": "ABC123",
                "observation_timestamp": "2024-01-01T00:00:00",
                "release_timestamp": "2024-01-01T00:00:00",
                "value": 100.0
            }
        ]
    }
    with requests_mock.Mocker() as m:
        m.get(f"{mock_url}/query", json=mock_data)
        results = client.query(metric="gdp", entity="usa")
        assert len(results) == 1
        assert isinstance(results[0], Observation)
        assert results[0].value == 100.0
        assert results[0].series_id == "ABC123"


def test_query_df(client, mock_url):
    """Test query_df method."""
    mock_data = {
        "records": [
            {
                "series_id": "ABC123",
                "observation_timestamp": "2024-01-01T00:00:00",
                "release_timestamp": "2024-01-01T00:00:00",
                "value": 100.0
            }
        ]
    }
    with requests_mock.Mocker() as m:
        m.get(f"{mock_url}/query", json=mock_data)
        df = client.query_df(metric="gdp")
        import pandas as pd
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert df.iloc[0]["value"] == 100.0
        assert df.iloc[0]["series_id"] == "ABC123"
