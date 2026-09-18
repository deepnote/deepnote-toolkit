"""Map Deepnote input blocks to native Streamlit widgets."""

from __future__ import annotations

import calendar
import re
from collections.abc import Iterable
from datetime import date, timedelta
from typing import Any

from .document import InputBlock

_RELATIVE_RANGE_MONTHS = {
    "pastMonth": 1,
    "past3months": 3,
    "past6months": 6,
    "pastYear": 12,
}


def render_inputs(
    inputs: Iterable[InputBlock], container: Any = None, *, key_prefix: str = "deepnote"
) -> dict[str, Any]:
    """Render input blocks and return API-ready values keyed by variable name.

    `container` may be `st`, `st.sidebar`, or a fake with the same widget methods for tests. When it
    is omitted, Streamlit is imported lazily so parsing and API clients work without the app extra.
    """

    if container is None:
        import streamlit as st  # type: ignore[import-not-found]

        container = st

    values: dict[str, Any] = {}
    for input_block in inputs:
        # One submitted value applies to every block sharing a variable name.
        if input_block.variable_name in values:
            continue
        label = input_block.label or input_block.variable_name.replace("_", " ").title()
        key = f"{key_prefix}:{input_block.variable_name}"
        values[input_block.variable_name] = _render_one(
            container, input_block, label, key
        )
    return values


def _render_one(container: Any, input_block: InputBlock, label: str, key: str) -> Any:
    if input_block.type == "input-checkbox":
        return container.checkbox(label, value=_as_bool(input_block.value), key=key)

    if input_block.type == "input-select":
        options = list(input_block.options)
        if input_block.multiple:
            raw_defaults = (
                input_block.value if isinstance(input_block.value, list) else []
            )
            defaults = [
                normalized
                for value in raw_defaults
                if (normalized := str(value)) in options
            ]
            return container.multiselect(label, options, default=defaults, key=key)
        index = (
            options.index(str(input_block.value))
            if str(input_block.value) in options
            else 0
        )
        return (
            container.selectbox(label, options, index=index, key=key) if options else ""
        )

    if input_block.type == "input-slider":
        minimum = input_block.min if input_block.min is not None else 0
        maximum = input_block.max if input_block.max is not None else 100
        step = input_block.step if input_block.step is not None else 1
        value = _as_number(input_block.value, minimum)
        if any(isinstance(number, float) for number in (minimum, maximum, value, step)):
            minimum, maximum, value, step = (
                float(number) for number in (minimum, maximum, value, step)
            )
        return container.slider(
            label, min_value=minimum, max_value=maximum, value=value, step=step, key=key
        )

    if input_block.type == "input-date":
        selected = _serialize_date(
            container.date_input(label, value=_as_date(input_block.value), key=key)
        )
        # Date blocks older than version 2 only parse a full timestamp.
        is_timestamp = isinstance(input_block.value, str) and "T" in input_block.value
        return f"{selected}T00:00:00.000Z" if selected and is_timestamp else selected

    if input_block.type == "input-date-range":
        selected = container.date_input(
            label, value=_as_date_range(input_block.value), key=key
        )
        if not isinstance(selected, (list, tuple)):
            selected = (selected, selected)
        serialized = [_serialize_date(value) for value in selected]
        if len(serialized) == 1:
            return serialized * 2
        return serialized if len(serialized) == 2 else ["", ""]

    if input_block.type == "input-textarea":
        return container.text_area(label, value=str(input_block.value or ""), key=key)

    return container.text_input(label, value=str(input_block.value or ""), key=key)


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).lower() in {"true", "1"}


def _as_number(value: Any, fallback: float | int) -> float | int:
    try:
        number = float(value)
        return (
            number
            if isinstance(fallback, float) or not number.is_integer()
            else int(number)
        )
    except (TypeError, ValueError):
        return fallback


def _as_date(value: Any) -> date | None:
    """Read a date or the date part of a timestamp; None leaves the widget empty."""

    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _as_date_range(value: Any) -> tuple[date, ...]:
    """Resolve an absolute or relative Deepnote range; () leaves the widget empty."""

    if isinstance(value, list):
        dates = tuple(_as_date(item) for item in value[:2])
        return dates if len(dates) == 2 and None not in dates else ()

    today = date.today()
    if match := re.fullmatch(r"past(\d+)days|customDays(\d+)", str(value)):
        return today - timedelta(days=int(match.group(1) or match.group(2))), today
    if months := _RELATIVE_RANGE_MONTHS.get(str(value)):
        year, month = divmod(today.year * 12 + today.month - 1 - months, 12)
        last_day = calendar.monthrange(year, month + 1)[1]
        return date(year, month + 1, min(today.day, last_day)), today
    return ()


def _serialize_date(value: Any) -> str:
    return value.isoformat() if isinstance(value, date) else ""
