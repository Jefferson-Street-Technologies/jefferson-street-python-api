import pytest
import requests
import requests_mock

from jstdata.client import (
    ApiKeyNotSetError,
    InvalidApiKeyError,
    InvalidInputError,
    JeffersonStreetClient,
)


@pytest.fixture
def mock_url():
    """Fixture for the base URL of the Jefferson Street API."""
    return "https://api.jeffersonst.io"


@pytest.fixture
def client(mock_url):
    """Fixture for JeffersonStreetClient with a dummy API key."""
    cli = JeffersonStreetClient(api_key="test_api_key")
    cli.base_url = mock_url  # Ensure client uses the mocked URL
    return cli


def test_client_init_no_api_key():
    """Test that ApiKeyNotSetError is raised when no API key is provided."""
    with pytest.raises(ApiKeyNotSetError):
        JeffersonStreetClient(api_key=None)


def test_validate_api_key_success(client, mock_url):
    """Test successful API key validation."""
    with requests_mock.Mocker() as m:
        m.get(f"{mock_url}/heartbeat", json={"status": "ok"})
        client._validate_api_key(client.api_key)
        assert client._api_key_is_valid is True


def test_validate_api_key_failure(client, mock_url):
    """Test failed API key validation due to invalid key or non-ok status."""
    with requests_mock.Mocker() as m:
        m.get(f"{mock_url}/heartbeat", json={"status": "error"})
        with pytest.raises(InvalidApiKeyError):
            client._validate_api_key(client.api_key)
        assert client._api_key_is_valid is False


def test_make_request_http_error(client, mock_url):
    """Test _make_request handling of HTTP errors."""
    with requests_mock.Mocker() as m:
        m.get(f"{mock_url}/heartbeat", json={"status": "ok"})  # For initial validation
        m.get(f"{mock_url}/some-endpoint", status_code=404)
        with pytest.raises(requests.exceptions.HTTPError):
            client._make_request("some-endpoint")


def test_get_metrics_success(client, mock_url):
    """Test successful retrieval of metrics."""
    expected_metrics = [
        {"slug": "metric1", "name": "Metric One"},
        {"slug": "metric2", "name": "Metric Two"},
    ]
    with requests_mock.Mocker() as m:
        m.get(f"{mock_url}/heartbeat", json={"status": "ok"})  # For initial validation
        m.get(f"{mock_url}/metric", json={"records": expected_metrics})

        metrics = client.get_metrics()
        assert metrics == expected_metrics


def test_get_metric_dimensions_success(client, mock_url):
    """Test successful retrieval of metric dimensions."""
    metric_slug = "test_metric"
    expected_dimensions = [{"entity": "entity1"}, {"entity": "entity2"}]
    with requests_mock.Mocker() as m:
        m.get(f"{mock_url}/heartbeat", json={"status": "ok"})
        m.get(
            f"{mock_url}/metric/{metric_slug}/entities",
            json={"records": expected_dimensions},
        )
        dimensions = client.get_metric_dimensions(metric_slug)
        assert dimensions == expected_dimensions


def test_get_metric_dimensions_invalid_metric(client, mock_url):
    """Test get_metric_dimensions with an invalid metric slug."""
    metric_slug = "invalid_metric"
    with requests_mock.Mocker() as m:
        m.get(f"{mock_url}/heartbeat", json={"status": "ok"})
        m.get(f"{mock_url}/metric/{metric_slug}/entities", status_code=404)
        with pytest.raises(InvalidInputError):
            client.get_metric_dimensions(metric_slug)


def test_get_entity_groups_success(client, mock_url):
    """Test successful retrieval of entity groups."""
    expected_groups = [
        {"name": "Group A", "slug": "group_a"},
        {"name": "Group B", "slug": "group_b"},
    ]
    with requests_mock.Mocker() as m:
        m.get(f"{mock_url}/heartbeat", json={"status": "ok"})
        m.get(f"{mock_url}/entity/groups", json={"records": expected_groups})
        groups = client.get_entity_groups()
        assert groups == expected_groups


def test_get_entity_groups_http_error(client, mock_url):
    """Test get_entity_groups with an HTTP error."""
    with requests_mock.Mocker() as m:
        m.get(f"{mock_url}/heartbeat", json={"status": "ok"})
        m.get(f"{mock_url}/entity/groups", status_code=500)
        with pytest.raises(
            InvalidInputError
        ):  # The client wraps HTTPError in InvalidInputError
            client.get_entity_groups()


def test_get_entities_success(client, mock_url):
    """Test successful retrieval of entities within a group."""
    entity_group = "test_group"
    expected_entities = [{"id": 1, "name": "Entity 1"}, {"id": 2, "name": "Entity 2"}]
    with requests_mock.Mocker() as m:
        m.get(f"{mock_url}/heartbeat", json={"status": "ok"})
        m.get(f"{mock_url}/entity/{entity_group}", json={"records": expected_entities})
        entities = client.get_entities(entity_group)
        assert entities == expected_entities


def test_get_entity_metrics_success(client, mock_url):
    """Test successful retrieval of metrics for an entity."""
    entity_slug = "test_entity"
    expected_metrics = [{"slug": "metric_x"}, {"slug": "metric_y"}]
    with requests_mock.Mocker() as m:
        m.get(f"{mock_url}/heartbeat", json={"status": "ok"})
        m.get(
            f"{mock_url}/entity/{entity_slug}/metrics",
            json={"records": expected_metrics},
        )
        metrics = client.get_entity_metrics(entity_slug)
        assert metrics == expected_metrics


def test_query_metric_success(client, mock_url):
    """Test successful query by metric."""
    metric_slug = "gdp"
    expected_data = [{"date": "2023-01-01", "value": 100}]
    with requests_mock.Mocker() as m:
        m.get(f"{mock_url}/heartbeat", json={"status": "ok"})
        m.get(f"{mock_url}/query/metric/{metric_slug}", json={"records": expected_data})
        data = client.query("metric", metric_slug, "2023-01-01", "2023-12-31")
        assert data == expected_data


def test_query_entity_success(client, mock_url):
    """Test successful query by entity."""
    entity_slug = "usa"
    expected_data = [{"date": "2023-01-01", "value": 1000}]
    with requests_mock.Mocker() as m:
        m.get(f"{mock_url}/heartbeat", json={"status": "ok"})
        m.get(f"{mock_url}/query/entity/{entity_slug}", json={"records": expected_data})
        data = client.query("entity", entity_slug, "2023-01-01", "2023-12-31")
        assert data == expected_data


def test_query_invalid_by_param(client):
    """Test query with invalid 'by' parameter."""
    with pytest.raises(InvalidInputError):
        client.query("invalid_type", "some_id", "2023-01-01", "2023-12-31")


def test_query_invalid_order_by_param(client):
    """Test query with invalid 'order_by' parameter."""
    with pytest.raises(InvalidInputError):
        client.query(
            "metric", "some_id", "2023-01-01", "2023-12-31", order_by="invalid_order"
        )


def test_query_invalid_sort_order_param(client):
    """Test query with invalid 'sort_order' parameter."""
    with pytest.raises(InvalidInputError):
        client.query(
            "metric", "some_id", "2023-01-01", "2023-12-31", sort_order="invalid_sort"
        )


def test_search_for_entity_success(client, mock_url):
    """Test successful entity search."""
    search_query = "apple"
    expected_results = [{"type": "company", "name": "Apple Inc."}]
    with requests_mock.Mocker() as m:
        m.get(f"{mock_url}/heartbeat", json={"status": "ok"})
        m.get(f"{mock_url}/search/entity", json={"records": expected_results})
        results = client.search_for_entity(search_query)
        assert results == expected_results
