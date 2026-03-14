import os
import sys
import click

from .client import ApiKeyNotSetError, JSTDataClient
from .utils import common_params, format_and_print

client = JSTDataClient()

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
    response = client.make_request("entity/groups")
    entity_groups = response["records"]
    format_and_print(entity_groups, format)


@entity.command("values")
@click.argument("entity_group", required=True)
@click.option(
    "--sort_order", type=click.Choice(["asc", "desc"]), default="desc", help="Sort order (asc or desc, default: desc)"
)
@common_params
def list_entities(entity_group, limit, format, offset, sort_order):
    """
    List entities within a specified entity group.
    """
    response = client.make_request(
        f"entity/{entity_group}",
        {"offset": offset, "limit": limit, "sort_order": sort_order},
    )
    results = response["records"]
    format_and_print(results, format)


@entity.command("metrics")
@click.argument("entity", required=True)
@click.option(
    "--sort_order", type=click.Choice(["asc", "desc"]), default="desc", help="Sort order (asc or desc, default: desc)"
)
@common_params
def get_entity_links(entity, limit, format, offset, sort_order):
    """
    Retrieve metrics associated with a specific entity.
    """
    response = client.make_request(
        f"entity/{entity}/metrics",
        {"limit": limit, "offset": offset, "sort_order": sort_order},
    )
    results = response["records"]
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
    response = client.make_request(
        "metric",
        {
            "metric": None,
            "limit": limit,
            "offset": offset,
            "order_by": order_by,
            "sort_order": sort_order,
        },
    )
    metrics = response["records"]
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
    response = client.make_request(
        "metric",
        {
            "metric": None,
            "limit": 1,
            "offset": 0
        },
    )
    metric = response["records"]
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
    response = client.make_request(f"metric/{metric}/entities")
    dimensions = response["records"]
    format_and_print(dimensions, format)


@query.command("metric")
@common_params
@click.argument("metric", required=True, nargs=1)
@click.option(
    "--sort_order", type=click.Choice(["asc", "desc"]), default="desc", help="Sort order (asc or desc, default: desc)"
)
@click.option("--order_by", type=click.Choice(["release_date", "series_label"]), default="release_date", help="Column to order by (default: release_date)")
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
@click.option(
    "--entity_filter",
    default=None,
    help="List of comma-separated entity slugs to filter by",
)
def get_observations_by_metric(
    metric, sort_order, start_date, end_date, limit, offset, format, entity_filter, order_by
):
    """
    Retrieve observations for a specific metric.
    """
    entity_filter_list = entity_filter.split(",") if entity_filter else None
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "limit": limit,
        "offset": offset,
        "sort_order": sort_order,
        "order_by": order_by
    }
    if entity_filter is not None:
        params["entities"] = entity_filter_list

    response = client.make_request(f"query/metric/{metric}", params)
    observations = response["records"]
    format_and_print(observations, format)


@query.command("entity")
@click.argument("entity", required=True, nargs=1)
@click.option(
    "--sort_order", type=click.Choice(["asc", "desc"]), default="desc", help="Sort order (asc or desc, default: desc)"
)
@click.option("--order_by", type=click.Choice(["release_date", "series_label"]), default="release_date", help="Column to order by (default: release_date)")
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
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "limit": limit,
        "offset": offset,
        "sort_order": sort_order,
        "order_by": order_by
    }

    response = client.make_request(f"query/entity/{entity}", params)
    observations = response["records"]
    format_and_print(observations, format)


@entity.command("search")
@click.option(
    "--limit", default=3, help="Maximum number of records to return (default: 3)"
)
@click.option("--offset", default=0, help="Number of records to skip (default: 0)")
@click.option(
    "--format",
    default="pretty",
    help="Output format. Valid formats are: json, csv, pretty.",
)
@click.argument("query", required=True, nargs=-1)
def search_for_entity(query, limit, offset, format):
    """
    Search for entities.
    """
    response = client.make_request(
        f"search/entity", {"query": query, "offset": offset, "limit": limit}
    )
    results = response["records"]
    format_and_print(results, format)


if __name__ == "__main__":
    try:
        cli()
    except ApiKeyNotSetError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
