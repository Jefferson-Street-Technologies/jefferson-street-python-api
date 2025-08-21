import os
import click
from .client import JeffersonStreetClient
from .utils import date_handler, format_and_print

api_key = os.getenv("JEFFERSON_STREET_API_KEY")
if api_key is None:
    raise Exception("Please set the JEFFERSON_STREET_API_KEY environment variable")

client = JeffersonStreetClient(api_key)

def common_params(f):
    f = click.option("--limit", default=10000, help="Maximum number of records to return (default: 10000)")(f)
    f = click.option("--offset", default=0, help="Number of records to skip (default: 0)")(f)
    f = click.option("--format", default="pretty", help="Output format. Valid formats are: json, csv, pretty.")(f)
    return f


@click.group()
def cli():
    pass

@cli.group("ls")
def show():
    pass

@cli.group()
def query():
    pass

@show.command("metrics")
@common_params
@click.option("--sort_order", default="desc", help="Sort order (asc or desc, default: desc)")
@click.option("--order_by", default="last_updated", help="Column to order by (default: last_updated)")
def list_metrics(limit, offset, sort_order, order_by, format):
    metrics = client.get_metrics(limit, offset, order_by, sort_order)
    format_and_print(metrics, format)

@show.command("series")
@common_params
@click.argument("metric", required=True)
@click.option("--sort_order", default="desc", help="Sort order (asc or desc, default: desc)")
@click.option("--order_by", default="last_updated", help="Column to order by (default: last_updated)")
def list_series(metric, limit, offset, sort_order, order_by, format):
    series = client.get_metric_series(metric, limit, offset, order_by, sort_order)
    format_and_print(series, format)

@query.command("series")
@common_params
@click.argument("series", required=True, nargs=-1)
@click.option("--observation_type", default="latest", help="Type of observations (earliest or latest, default: latest)")
@click.option("--sort_order", default="desc", help="Sort order (asc or desc, default: desc)")
@click.option("--order_by", default="id", help="Column to order by (default: id)")
@click.option("--start", default=None, help="Start period. Valid formats: YYYY-MM-DD, YYYY-MM, YYYY, unix timestamp")
@click.option("--end", default=None, help="End period. Valid formats: YYYY-MM-DD, YYYY-MM, YYYY, unix timestamp")
def get_observations(series, observation_type, limit, offset, order_by, sort_order, format, start=None, end=None):
    start_date = date_handler(start) if start else None
    end_date = date_handler(end) if end else None
    observations = client.get_metric_observations(series, observation_type, limit, offset, order_by, sort_order, False, start_date, end_date)
    format_and_print(observations, format)

@query.command("concept")
def search_by_concept():
    pass

@common_params
@show.command('countries')
def get_countries(format, limit, offset):
    response = client.get_countries(limit, offset)
    format_and_print(response, format)

if __name__ == "__main__":
    cli()
