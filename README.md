# jstdata

A Python interface to the Jefferson Street REST API.

## Installation

```bash
pip install jstdata
```

## Usage

Set your API key as an environment variable:

```bash
export JEFFERSON_STREET_API_KEY="your_api_key_here"
```

Then you can use the CLI:

```bash
jst --help
```

Or use it as a Python library:

```python
from jstdata.client import JeffersonStreetClient

client = JeffersonStreetClient("your_api_key_here")
metrics = client.get_metrics()
print(metrics)
```
