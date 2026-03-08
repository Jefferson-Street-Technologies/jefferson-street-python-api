import json
from io import StringIO

import click
import pandas as pd
from tabulate import tabulate


def common_params(f):
    """
    Decorator to apply common CLI parameters: limit, offset, and format.
    """
    f = click.option(
        "--limit",
        default=100,
        help="Maximum number of records to return (default: 10000)",
    )(f)
    f = click.option(
        "--offset", default=0, help="Number of records to skip (default: 0)"
    )(f)
    f = click.option(
        "--format",
        default="pretty",
        help="Output format. Valid formats are: json, csv, pretty.",
    )(f)
    return f


def date_handler(date_str):
    if len(date_str) in (10, 11):  # YYYY-MM-DD or unix timestamp
        return date_str
    elif len(date_str) == 4:  # year
        return f"{date_str}-01-01"
    elif len(date_str) == 7:  # year-month
        return f"{date_str}-01"
    else:
        raise ValueError(f"Invalid date format: {date_str}")


def df_to_csv_string(df):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    return csv_buffer.getvalue()


def format_and_print(response_data, format):
    if format == "json":
        click.echo(json.dumps(response_data))
    elif format == "csv":
        click.echo(df_to_csv_string(pd.DataFrame(response_data)))
    elif format == "pretty":
        if len(response_data) == 1:
            response_data = [(k, v) for k, v in response_data[0].items()]
            click.echo(
                tabulate(response_data, headers=["key", "value"], tablefmt="pretty")
            )
            return
        click.echo(tabulate(response_data, headers="keys", tablefmt="pretty"))
