"""Site descriptions: collect a read-only snapshot of a lab and summarize it for agents."""

from .collect_live import collect, manager_urls_from_context
from .summary import summarize

__all__ = ["collect", "manager_urls_from_context", "summarize"]
