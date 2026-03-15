import os

import pytest
import requests_mock
from click.testing import CliRunner

from jstdata.cli import cli
from jstdata.client import JSTDataClient


@pytest.fixture
def runner():
    """Fixture for CliRunner."""
    return CliRunner()


@pytest.fixture
def mock_url():
    """Fixture for the base URL of the Jefferson Street API."""
    return 'https://api.some-site.io'


def test_cli_help(runner):
    """Test the main CLI help message."""
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "Usage: cli [OPTIONS] COMMAND [ARGS]..." in result.output
    assert "  metric  Commands for interacting with metrics." in result.output
    assert "  entity  Commands for interacting with entities." in result.output
    assert "  query   Commands for querying data." in result.output


def test_metric_ls_success(runner, mock_url, monkeypatch):
    """Test 'jstdata metric ls' command with a successful API response."""
    monkeypatch.setattr(
        "jstdata.cli.client", JSTDataClient(api_key="testing", base_url=mock_url)
    )
    expected_metrics = [
        {
            "slug": "metric1",
            "name": "Metric One",
            "frequency": "daily",
            "unit": "count",
            "last_updated": "2023-01-01",
        }
    ]

    with requests_mock.Mocker() as m:
        m.get(f"{mock_url}/heartbeat", json={"status": "ok"})
        m.get(f"{mock_url}/metric", json={"records": expected_metrics})

        result = runner.invoke(cli, ["metric", "ls"])
        assert result.exit_code == 0
        assert "metric1" in result.output
        # Verify the table format for condensed metrics
        assert "+------+" in result.output
        assert "| slug |" in result.output
        assert "+------+" in result.output


def test_metric_ls_expanded_success(runner, mock_url, monkeypatch):
    """Test 'jstdata metric ls --expanded' command with a successful API response."""
    monkeypatch.setattr(
        "jstdata.cli.client", JSTDataClient(api_key="testing", base_url=mock_url)
    )
    expected_metrics = [
        {
            "slug": "metric1",
            "name": "Metric One",
            "frequency": "daily",
            "unit": "count",
            "last_updated": "2023-01-01",
        }
    ]

    with requests_mock.Mocker() as m:
        m.get(f"{mock_url}/heartbeat", json={"status": "ok"})
        m.get(f"{mock_url}/metric", json={"records": expected_metrics})

        result = runner.invoke(cli, ["metric", "ls", "--expanded"])
        assert result.exit_code == 0
        assert "metric1" in result.output
        assert "Metric One" in result.output
        assert "daily" in result.output
        assert "count" in result.output
