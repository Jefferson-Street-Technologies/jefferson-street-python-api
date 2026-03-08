import os
import sys

import click

from .client import ApiKeyNotSetError, JeffersonStreetClient
from .utils import common_params, format_and_print

api_key = os.getenv("JEFFERSON_STREET_API_KEY")
if api_key is None:
    raise click.Abort(
        "Please set the JEFFERSON_STREET_API_KEY environment variable to a valid API key."
    )

client = JeffersonStreetClient(api_key)


@click.group()
def cli():
    """
    Jefferson Street CLI.
    """


@cli.group()
def metric():
    """
    Commands for interacting with metrics.
    """


@cli.group()
def entity():
    """
    Commands for interacting with entities.
    """


@cli.group()
def query():
    """
    Commands for querying data.
    """


@entity.command("groups")
@click.option(
    "--format",
    default="pretty",
    help="Output format. Valid formats are: json, csv, pretty.",
)
def list_entity_groups(format):
    """
    List all available entity groups.
    """
    entity_groups = client.get_entity_groups()
    format_and_print(entity_groups, format)


@entity.command("values")
@click.argument("entity_group", required=True)
@common_params
def list_entities(entity_group, limit, format, offset):
    """
    List entities within a specified entity group.
    """
    results = client.get_entities(entity_group, offset, limit)
    format_and_print(results, format)


@entity.command("metrics")
@click.argument("entity", required=True)
@common_params
def get_entity_links(entity, limit, format, offset):
    """
    Retrieve metrics associated with a specific entity.
    """
    results = client.get_entity_metrics(entity, limit, offset)
    format_and_print(results, format)


@metric.command("ls")
@common_params
@click.option(
    "--sort_order", default="desc", help="Sort order (asc or desc, default: desc)"
)
@click.option(
    "--order_by",
    default="last_updated",
    help="Column to order by (default: last_updated)",
)
@click.option("--expanded", is_flag=True, default=False, help="Return expanded metrics")
def list_metrics(limit, offset, sort_order, order_by, format, expanded):
    """
    List all available metrics.
    """
    metrics = client.get_metrics(None, limit, offset, order_by, sort_order)
    if expanded:
        format_and_print(metrics, format)
    else:
        condensed_metrics = [{"slug": m["slug"]} for m in metrics]
        format_and_print(condensed_metrics, format)


@metric.command("show")
@click.argument("metric", required=True)
@click.option(
    "--format",
    default="pretty",
    help="Output format. Valid formats are: json, csv, pretty.",
)
def show_metric(metric, format):
    """
    Display details for a specific metric.
    """
    metric = client.get_metrics(metric)
    format_and_print(metric, format)


@metric.command("entities")
@click.argument("metric", required=True)
@click.option(
    "--format",
    default="pretty",
    help="Output format. Valid formats are: json, csv, pretty.",
)
def show_metric_dimensions(metric, format):
    """
    Display dimensions for a specific metric.
    """
    dimensions = client.get_metric_dimensions(metric)
    format_and_print(dimensions, format)


@query.command("metric")
@common_params
@click.argument("metric", required=True, nargs=1)
@click.option(
    "--sort_order", default="desc", help="Sort order (asc or desc, default: desc)"
)
@click.option("--order_by", default="id", help="Column to order by (default: id)")
@click.option(
    "--start_date",
    default=None,
    help="Start period. Valid formats: YYYY-MM-DD, YYYY-MM, YYYY, unix timestamp",
)
@click.option(
    "--end_date",
    default=None,
    help="End period. Valid formats: YYYY-MM-DD, YYYY-MM, YYYY, unix timestamp",
)
def get_observations_by_metric(
    metric, sort_order, order_by, start_date, end_date, limit, offset, format
):
    """
    Retrieve observations for a specific metric.
    """
    observations = client.query("metric", metric, start_date, end_date, limit, offset)
    format_and_print(observations, format)


@query.command("entity")
@click.argument("entity", required=True, nargs=1)
@click.option(
    "--sort_order", default="desc", help="Sort order (asc or desc, default: desc)"
)
@click.option("--order_by", default="id", help="Column to order by (default: id)")
@click.option(
    "--start_date",
    default=None,
    help="Start period. Valid formats: YYYY-MM-DD, YYYY-MM, YYYY, unix timestamp",
)
@click.option(
    "--end_date",
    default=None,
    help="End period. Valid formats: YYYY-MM-DD, YYYY-MM, YYYY, unix timestamp",
)
@common_params
def get_observations_by_entity(
    entity, sort_order, order_by, start_date, end_date, limit, offset, format
):
    """
    Retrieve observations for a specific entity.
    """
    observations = client.query("entity", entity, start_date, end_date, limit, offset)
    format_and_print(observations, format)


@entity.command("search")
@click.option(
    "--limit", default=3, help="Maximum number of records to return (default: 10000)"
)
@click.option("--offset", default=0, help="Number of records to skip (default: 0)")
@click.option(
    "--format",
    default="pretty",
    help="Output format. Valid formats are: json, csv, pretty.",
)
@click.argument("query", required=True, nargs=1)
def search_for_entity(query, limit, offset, format):
    """
    Search for entities.
    """
    results = client.search_for_entity(query, limit, offset)
    format_and_print(results, format)


if __name__ == "__main__":
    try:
        cli()
    except ApiKeyNotSetError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
