from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal
from textual.widgets import Header, Footer, Input, ListItem, ListView, Static, Label
from textual import on
import asyncio

from .client import JSTDataClient
from .models import Series, Entity, Metric

class ResultItem(ListItem):
    def __init__(self, resource: any):
        super().__init__()
        self.resource = resource
        if isinstance(resource, Series):
            self.label = f"[blue]Series:[/blue] {resource.label} ({resource.id})"
        elif isinstance(resource, Entity):
            self.label = f"[green]Entity:[/green] {resource.label} ({resource.id})"
        elif isinstance(resource, Metric):
            self.label = f"[yellow]Metric:[/yellow] {resource.name} ({resource.id})"
        else:
            self.label = str(resource)

    def compose(self) -> ComposeResult:
        yield Label(self.label)

class JSTDataApp(App):
    """A Textual app for exploring Jefferson Street data."""

    CSS = """
    Screen {
        background: $surface;
    }

    #search-container {
        height: 5;
        margin: 1 1;
    }

    #search-status {
        margin-left: 2;
        height: 1;
    }

    #main-container {
        height: 1fr;
    }

    #results-list {
        width: 40%;
        height: 1fr;
        border: solid $primary;
        margin: 0 1;
    }

    #preview-pane {
        width: 60%;
        height: 1fr;
        border: solid $secondary;
        padding: 1 2;
        background: $surface-darken-1;
    }

    .metadata-label {
        color: $text-muted;
        text-style: bold;
    }
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("ctrl+c", "quit", "Quit"),
        ("c", "copy_id", "Copy ID"),
        ("p", "copy_python", "Copy Python Snippet"),
        ("j", "cursor_down", "Down"),
        ("k", "cursor_up", "Up"),
    ]

    def __init__(self, client: JSTDataClient):
        super().__init__()
        self.client = client
        self.search_task = None
        self._search_debounce_time = 0.3  # 300ms debounce

    def compose(self) -> ComposeResult:
        yield Header()
        yield Container(
            Input(placeholder="Search metrics, entities, or series...", id="search-input"),
            Static("", id="search-status"),
            id="search-container"
        )
        yield Horizontal(
            ListView(id="results-list"),
            Static(id="preview-pane", content="Select a result to see details..."),
            id="main-container"
        )
        yield Footer()

    def on_mount(self) -> None:
        """Focus search input on start."""
        self.query_one("#search-input").focus()

    @on(Input.Changed, "#search-input")
    def on_search(self, event: Input.Changed) -> None:
        """Handle real-time search with debouncing."""
        if self.search_task:
            self.search_task.cancel()
        
        status = self.query_one("#search-status")
        if len(event.value) < 2:
            self.query_one("#results-list").clear()
            status.update("")
            return

        status.update("[italic]Waiting...[/italic]")
        self.search_task = asyncio.create_task(self._debounced_search(event.value))

    async def _debounced_search(self, query: str) -> None:
        """Wait for debounce period before searching."""
        try:
            await asyncio.sleep(self._search_debounce_time)
            status = self.query_one("#search-status")
            status.update("[bold cyan]Searching...[/bold cyan]")
            await self.perform_search(query)
            status.update("")
        except asyncio.CancelledError:
            pass

    async def perform_search(self, query: str) -> None:
        try:
            # We search all three categories in parallel
            tasks = [
                asyncio.to_thread(self.client.search_metrics, query, limit=5),
                asyncio.to_thread(self.client.search_entities, query, limit=5),
                asyncio.to_thread(self.client.search_series, query, limit=5),
            ]
            results = await asyncio.gather(*tasks)
            
            # Combine and update list
            all_results = results[0] + results[1] + results[2]
            
            list_view = self.query_one("#results-list")
            list_view.clear()
            for r in all_results:
                list_view.append(ResultItem(r))
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.notify(f"Search error: {e}", severity="error")

    @on(Input.Submitted, "#search-input")
    def on_search_submitted(self) -> None:
        """Move focus to results list when Enter is pressed in search input."""
        self.query_one("#results-list").focus()

    def action_cursor_down(self) -> None:
        """Handle 'j' key for list navigation."""
        list_view = self.query_one("#results-list")
        list_view.action_cursor_down()

    def action_cursor_up(self) -> None:
        """Handle 'k' key for list navigation."""
        list_view = self.query_one("#results-list")
        list_view.action_cursor_up()

    @on(ListView.Selected, "#results-list")
    def on_item_selected(self, event: ListView.Selected) -> None:
        """Update preview pane and fetch drill-down info when an item is selected/entered."""
        resource = event.item.resource
        asyncio.create_task(self._update_preview(resource))

    async def _update_preview(self, resource: any) -> None:
        preview = self.query_one("#preview-pane")
        
        # Initial metadata view
        if isinstance(resource, Series):
            content = self._format_series_preview(resource)
            drill_down_title = "\n[bold cyan]Recent Observations (Last 10)[/bold cyan]\n"
            fetch_func = lambda: self.client.query(series=resource.id, limit=10)
            formatter = self._format_observations
        elif isinstance(resource, Entity):
            content = self._format_entity_preview(resource)
            drill_down_title = "\n[bold cyan]Entity Relations (First 10)[/bold cyan]\n"
            fetch_func = lambda: self.client.get_entity_relations(resource.id, limit=10)
            formatter = self._format_relations
        elif isinstance(resource, Metric):
            content = self._format_metric_preview(resource)
            drill_down_title = "\n[bold cyan]Associated Series (First 10)[/bold cyan]\n"
            fetch_func = lambda: self.client.get_metric_series(resource.id, limit=10)
            formatter = self._format_series_list
        else:
            preview.update(str(resource))
            return

        preview.update(content + "\n[italic]Fetching deep details...[/italic]")
        
        try:
            # Fetch drill-down data
            data = await asyncio.to_thread(fetch_func)
            drill_down_content = formatter(data)
            preview.update(content + drill_down_title + drill_down_content)
        except Exception as e:
            preview.update(content + f"\n[red]Error fetching details: {e}[/red]")

    def _format_series_preview(self, series: Series) -> str:
        entities = ", ".join([f"{e.label} ({e.id})" for e in series.entities])
        return f"""
[bold blue]Series Detail[/bold blue]
--------------------
[bold]ID:[/bold] {series.id}
[bold]Label:[/bold] {series.label}
[bold]Metric:[/bold] {series.metric_slug}
[bold]Freq:[/bold] {series.frequency}
[bold]Units:[/bold] {series.units}
[bold]Source:[/bold] {series.source}
[bold]Updated:[/bold] {series.last_updated.strftime('%Y-%m-%d %H:%M:%S')}

[bold]Entities:[/bold]
{entities}
"""

    def _format_entity_preview(self, entity: Entity) -> str:
        return f"""
[bold green]Entity Detail[/bold green]
--------------------
[bold]ID:[/bold] {entity.id}
[bold]Label:[/bold] {entity.label}
"""

    def _format_metric_preview(self, metric: Metric) -> str:
        return f"""
[bold yellow]Metric Detail[/bold yellow]
--------------------
[bold]ID:[/bold] {metric.id}
[bold]Name:[/bold] {metric.name}
"""

    def _format_observations(self, observations: list) -> str:
        if not observations: return "No observations found."
        lines = [f"{o.observation_timestamp.strftime('%Y-%m-%d')}: [bold]{o.value}[/bold]" for o in observations]
        return "\n".join(lines)

    def _format_relations(self, relations: list) -> str:
        if not relations: return "No relations found."
        lines = [f"-> {r.id} ([italic]{r.relationship}[/italic])" for r in relations]
        return "\n".join(lines)

    def _format_series_list(self, series_list: list) -> str:
        if not series_list: return "No associated series found."
        lines = [f"- {s.label} ([blue]{s.id}[/blue])" for s in series_list]
        return "\n".join(lines)

    def action_copy_id(self) -> None:
        """Copy current result ID to clipboard."""
        list_view = self.query_one("#results-list")
        if list_view.highlighted_child:
            resource = list_view.highlighted_child.resource
            import pyperclip
            try:
                pyperclip.copy(resource.id)
                self.notify(f"Copied ID: {resource.id}")
            except Exception:
                self.notify("Failed to copy to clipboard. Ensure 'pyperclip' is working.", severity="error")

    def action_copy_python(self) -> None:
        """Copy a Python snippet for the current result."""
        list_view = self.query_one("#results-list")
        if list_view.highlighted_child:
            resource = list_view.highlighted_child.resource
            import pyperclip
            
            snippet = ""
            if isinstance(resource, Series):
                snippet = f"df = client.query_df(series='{resource.id}')"
            elif isinstance(resource, Entity):
                snippet = f"series = client.get_entity_series('{resource.id}')"
            elif isinstance(resource, Metric):
                snippet = f"series = client.get_metric_series('{resource.id}')"
                
            try:
                pyperclip.copy(snippet)
                self.notify(f"Copied Python snippet to clipboard")
            except Exception:
                self.notify("Failed to copy to clipboard", severity="error")
