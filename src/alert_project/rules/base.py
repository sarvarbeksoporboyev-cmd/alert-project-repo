"""Base interface for alert rules."""

from __future__ import annotations

from abc import ABC, abstractmethod

import pandas as pd

from ..models import Alert


class AlertRule(ABC):
    """Stateful alert rule that evaluates one normalized chunk at a time."""

    name: str

    @abstractmethod
    def process(self, events: pd.DataFrame) -> list[Alert]:
        """Process normalized events and return newly triggered alerts."""
