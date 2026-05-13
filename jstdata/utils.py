import json
from io import StringIO
from dataclasses import is_dataclass, asdict

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
        help="Maximum number of records to return (default: 100)",
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

def common_search_params(f):
    """
    Decorator to apply common CLI parameters: limit, offset, and format.
    Includes different defaults for limit
    """
    f = click.option(
        "--limit",
        default=5,
        help="Maximum number of records to return (default: 100)",
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



def df_to_csv_string(df):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    return csv_buffer.getvalue()


def format_and_print(response_data, format):
    # Convert dataclasses to dicts if necessary
    if isinstance(response_data, list):
        data = [asdict(r) if is_dataclass(r) else r for r in response_data]
    elif is_dataclass(response_data):
        data = asdict(response_data)
    else:
        data = response_data

    if format == "json":
        # Handle datetime serialization in JSON
        def default_serializer(obj):
            if hasattr(obj, "isoformat"):
                return obj.isoformat()
            raise TypeError(f"Object of type {type(obj)} is not JSON serializable")
        
        click.echo(json.dumps(data, indent=2, default=default_serializer))
    elif format == "csv":
        if isinstance(data, dict):
            data = [data]
        click.echo(df_to_csv_string(pd.DataFrame(data)))
    elif format == "pretty":
        if not data:
            click.echo("No records found.")
            return
            
        if isinstance(data, dict):
            # Single record: vertical table
            table_data = [(k, v) for k, v in data.items()]
            click.echo(
                tabulate(table_data, headers=["key", "value"], tablefmt="pretty")
            )
            return
            
        # List of records: horizontal table
        # Flatten nested structures (like 'entities' in Series) for better display
        flattened_data = []
        for item in data:
            flat_item = {}
            for k, v in item.items():
                if isinstance(v, list):
                    flat_item[k] = ", ".join([str(i.get("id", i)) if isinstance(i, dict) else str(i) for i in v])
                else:
                    flat_item[k] = v
            flattened_data.append(flat_item)
            
        click.echo(tabulate(flattened_data, headers="keys", tablefmt="pretty"))
