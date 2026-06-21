from textual.app import App, ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.widgets import Header, Footer, Input, ListItem, ListView, Static, Label, Button
from textual.screen import ModalScreen
from textual import on
import asyncio

from .client import JSTDataClient
from .models import Series, Entity, Metric

class InspectScreen(ModalScreen):
    """A modal for deep inspection of a resource."""
    
    DEFAULT_CSS = """
    InspectScreen {
        align: center middle;
    }

    #inspect-container {
        width: 80%;
        height: 80%;
        border: thick $primary;
        background: $surface;
        padding: 1 2;
    }

    #inspect-content {
        height: 1fr;
        overflow-y: scroll;
    }

    #inspect-footer {
        height: 3;
        align: center middle;
    }
    """

    def __init__(self, resource: any, client: JSTDataClient):
        super().__init__()
        self.resource = resource
        self.client = client

    def compose(self) -> ComposeResult:
        with Vertical(id="inspect-container"):
            yield Static(id="inspect-content")
            with Horizontal(id="inspect-footer"):
                yield Button("Close (Esc)", variant="error", id="close-btn")

    def on_mount(self) -> None:
        asyncio.create_task(self._fetch_details())

    @on(Button.Pressed, "#close-btn")
    def on_close(self) -> None:
        self.app.pop_screen()

    async def _fetch_details(self) -> None:
        content_widget = self.query_one("#inspect-content")
        
        if isinstance(self.resource, Series):
            title = "[bold blue]Series Detail[/bold blue]"
            fetch_func = lambda: self.client.query(series=self.resource.id, limit=10)
            formatter = self._format_observations
            drill_title = "Recent Observations"
        elif isinstance(self.resource, Entity):
            title = "[bold green]Entity Detail[/bold green]"
            fetch_func = lambda: self.client.get_entity_relations(self.resource.id, limit=10)
            formatter = self._format_relations
            drill_title = "Entity Relations"
        elif isinstance(self.resource, Metric):
            title = "[bold yellow]Metric Detail[/bold yellow]"
            fetch_func = lambda: self.client.get_metric_series(self.resource.id, limit=10)
            formatter = self._format_series_list
            drill_title = "Associated Series"
        else:
            content_widget.update(str(self.resource))
            return

        content_widget.update(f"{title}\n[italic]Fetching deep details...[/italic]")
        
        try:
            data = await asyncio.to_thread(fetch_func)
            drill_content = formatter(data)
            output = f"{title}\nID: {self.resource.id}\nLabel: {getattr(self.resource, 'label', getattr(self.resource, 'name', ''))}\n"
            output += f"\n[bold cyan]{drill_title}[/bold cyan]\n" + drill_content
            content_widget.update(output)
        except Exception as e:
            content_widget.update(f"[red]Error: {e}[/red]")

    def _format_observations(self, observations: list) -> str:
        if not observations: return "No observations found."
        return "\n".join([f"{o.observation_timestamp.strftime('%Y-%m-%d')}: [bold]{o.value}[/bold]" for o in observations])

    def _format_relations(self, relations: list) -> str:
        if not relations: return "No relations found."
        return "\n".join([f"-> {r.id} ([italic]{r.relationship}[/italic])" for r in relations])

    def _format_series_list(self, series_list: list) -> str:
        if not series_list: return "No associated series found."
        return "\n".join([f"- {s.label} ([blue]{s.id}[/blue])" for s in series_list])


class ResultsScreen(ModalScreen):
    """A modal for displaying query results."""
    
    DEFAULT_CSS = """
    ResultsScreen {
        align: center middle;
    }

    #results-container {
        width: 90%;
        height: 90%;
        border: thick $magenta;
        background: $surface;
        padding: 1 2;
    }

    #results-table {
        height: 1fr;
        overflow-y: scroll;
    }
    """

    def __init__(self, observations: list):
        super().__init__()
        self.observations = observations

    def compose(self) -> ComposeResult:
        with Vertical(id="results-container"):
            yield Static("[bold magenta]Query Results[/bold magenta]\n" + "-"*20)
            yield Static(id="results-table")
            yield Button("Close (Esc)", variant="error", id="close-btn")

    def on_mount(self) -> None:
        table = self.query_one("#results-table")
        lines = [f"[bold]{'Date':<15} {'Series ID':<15} {'Value':<10}[/bold]", "-" * 45]
        for o in self.observations:
            date_str = o.observation_timestamp.strftime('%Y-%m-%d')
            lines.append(f"{date_str:<15} {o.series_id:<15} {o.value:<10}")
        table.update("\n".join(lines))

    @on(Button.Pressed, "#close-btn")
    def on_close(self) -> None:
        self.app.pop_screen()


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
        width: 50%;
        height: 1fr;
        border: solid $primary;
        margin: 0 1;
    }

    #query-builder {
        width: 50%;
        height: 1fr;
        border: solid $accent;
        padding: 1 2;
        background: $surface-darken-1;
    }
    """

    BINDINGS = [
        ("ctrl+q", "quit", "Quit"),
        ("ctrl+c", "quit", "Quit"),
        ("q", "run_query", "Run Query"),
        ("x", "clear_query", "Clear Query"),
        ("i", "inspect", "Inspect"),
        ("c", "copy_id", "Copy ID"),
        ("p", "copy_python", "Copy Python Snippet"),
        ("j", "cursor_down", "Down"),
        ("k", "cursor_up", "Up"),
    ]

    def __init__(self, client: JSTDataClient):
        super().__init__()
        self.client = client
        self.search_task = None
        self._search_debounce_time = 0.3
        self.query_state = {"metrics": [], "entities": [], "series": []}

    def compose(self) -> ComposeResult:
        yield Header()
        yield Container(
            Input(placeholder="Search metrics, entities, or series...", id="search-input"),
            Static("", id="search-status"),
            id="search-container"
        )
        yield Horizontal(
            ListView(id="results-list"),
            Static(id="query-builder", content="Enter on a result to add to query..."),
            id="main-container"
        )
        yield Footer()

    def on_mount(self) -> None:
        self.query_one("#search-input").focus()

    @on(Input.Changed, "#search-input")
    def on_search(self, event: Input.Changed) -> None:
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
        try:
            await asyncio.sleep(self._search_debounce_time)
            await self.perform_search(query)
            self.query_one("#search-status").update("")
        except asyncio.CancelledError:
            pass

    async def perform_search(self, query: str) -> None:
        try:
            results = await asyncio.to_thread(self.client.search, query, limit=15)
            
            list_view = self.query_one("#results-list")
            list_view.clear()
            for r in results:
                list_view.append(ResultItem(r))
        except Exception as e:
            self.notify(f"Search error: {e}", severity="error")

    @on(Input.Submitted, "#search-input")
    def on_search_submitted(self) -> None:
        self.query_one("#results-list").focus()

    def action_cursor_down(self) -> None:
        self.query_one("#results-list").action_cursor_down()

    def action_cursor_up(self) -> None:
        self.query_one("#results-list").action_cursor_up()

    @on(ListView.Selected, "#results-list")
    def on_item_selected(self, event: ListView.Selected) -> None:
        resource = event.item.resource
        category = "metrics" if isinstance(resource, Metric) else "entities" if isinstance(resource, Entity) else "series" if isinstance(resource, Series) else None
        
        if category and resource.id not in [r.id for r in self.query_state[category]]:
            self.query_state[category].append(resource)
            self._update_query_builder()
            self.notify(f"Added to query")

    def action_inspect(self) -> None:
        list_view = self.query_one("#results-list")
        if list_view.highlighted_child:
            self.push_screen(InspectScreen(list_view.highlighted_child.resource, self.client))

    def action_run_query(self) -> None:
        asyncio.create_task(self._run_query())

    async def _run_query(self) -> None:
        m_ids = [m.id for m in self.query_state["metrics"]]
        e_ids = [e.id for e in self.query_state["entities"]]
        s_ids = [s.id for s in self.query_state["series"]]
        
        if not (m_ids or e_ids or s_ids):
            self.notify("Add items to query first!", severity="warning")
            return

        try:
            obs = await asyncio.to_thread(self.client.query, metric=m_ids or None, entity=e_ids or None, series=s_ids or None, limit=50)
            if obs:
                self.push_screen(ResultsScreen(obs))
            else:
                self.notify("No results found")
        except Exception as e:
            self.notify(f"Query error: {e}", severity="error")

    def action_clear_query(self) -> None:
        self.query_state = {"metrics": [], "entities": [], "series": []}
        self._update_query_builder()

    def _update_query_builder(self) -> None:
        builder = self.query_one("#query-builder")
        lines = ["[bold magenta]Query Builder[/bold magenta]", "-"*20]
        for cat in ["metrics", "entities", "series"]:
            if self.query_state[cat]:
                lines.append(f"\n[bold]{cat.title()}:[/bold]")
                lines.extend([f"- {getattr(i, 'label', getattr(i, 'name', ''))} ({i.id})" for i in self.query_state[cat]])
        builder.update("\n".join(lines) if any(self.query_state.values()) else "\n[italic]Enter on a result to add to query...[/italic]")

    def action_copy_id(self) -> None:
        list_view = self.query_one("#results-list")
        if list_view.highlighted_child:
            import pyperclip
            pyperclip.copy(list_view.highlighted_child.resource.id)
            self.notify("Copied ID")

    def action_copy_python(self) -> None:
        list_view = self.query_one("#results-list")
        if list_view.highlighted_child:
            import pyperclip
            r = list_view.highlighted_child.resource
            snippet = f"client.query_df(series='{r.id}')" if isinstance(r, Series) else f"client.get_entity_series('{r.id}')" if isinstance(r, Entity) else f"client.get_metric_series('{r.id}')"
            pyperclip.copy(snippet)
            self.notify("Copied Python snippet")
