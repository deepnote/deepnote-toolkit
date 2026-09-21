"""Map Deepnote input blocks to native Streamlit widgets."""

from __future__ import annotations

import calendar
import math
import re
from collections.abc import Iterable
from datetime import date, timedelta
from typing import Any

from deepnote_toolkit.notebooks.models import InputBlock

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

    `container` may be `st`, `st.sidebar`, or a fake with the same widget methods
    for tests. When it is omitted, Streamlit is imported lazily so parsing and API
    clients work without the app extra.
    """

    if container is None:
        import streamlit as st  # type: ignore[import-not-found]

        container = st

    inputs = tuple(inputs)
    names = [block.variable_name for block in inputs]
    if len(names) != len(set(names)):
        raise ValueError("Input variable names must be unique")
    values: dict[str, Any] = {}
    for input_block in inputs:
        label = input_block.label or input_block.variable_name.replace("_", " ").title()
        key = f"{key_prefix}:{input_block.variable_name}"
        value = _render_one(container, input_block, label, key)
        if value is not None:
            values[input_block.variable_name] = value
    return values


def _render_one(container: Any, input_block: InputBlock, label: str, key: str) -> Any:
    """Render one input block using its saved default and constraints."""
    if input_block.type == "input-checkbox":
        return container.checkbox(label, value=_as_bool(input_block.value), key=key)

    if input_block.type == "input-select":
        options = list(input_block.options)
        if input_block.multiple:
            value = input_block.value
            if not isinstance(value, list):
                value = [] if value is None else [value]
            defaults = [
                normalized for item in value if (normalized := str(item)) in options
            ]
            if len(defaults) != len(value):
                container.warning(
                    f"{label}: saved selections are no longer available. Review the selection before running."
                )
            return container.multiselect(label, options, default=defaults, key=key)
        index = (
            options.index(str(input_block.value))
            if input_block.value is not None and str(input_block.value) in options
            else None
        )
        return container.selectbox(label, options, index=index, key=key)

    if input_block.type == "input-slider":
        minimum = input_block.min if input_block.min is not None else 0
        maximum = input_block.max if input_block.max is not None else 100
        step = input_block.step if input_block.step is not None else 1
        value = _as_number(input_block.value, minimum)
        if (
            not all(math.isfinite(n) for n in (minimum, maximum, step, value))
            or minimum >= maximum
            or step <= 0
            or not minimum <= value <= maximum
        ):
            raise ValueError(
                f"{label}: slider needs finite ordered bounds, a positive step, and a default within its bounds"
            )
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
            return None
        serialized = [_serialize_date(value) for value in selected]
        if len(serialized) == 1:
            return None
        return serialized if len(serialized) == 2 else ["", ""]

    if input_block.type == "input-textarea":
        return container.text_area(
            label,
            value=str(input_block.value) if input_block.value is not None else "",
            key=key,
        )

    return container.text_input(
        label,
        value=str(input_block.value) if input_block.value is not None else "",
        key=key,
    )


def _as_bool(value: Any) -> bool:
    """Decode checkbox defaults without treating the text false as truthy."""
    if isinstance(value, bool):
        return value
    return str(value).lower() in {"true", "1"}


def _as_number(value: Any, fallback: float | int) -> float | int:
    """Decode numeric defaults while preserving fractional values."""
    if value is None:
        return fallback
    try:
        number = float(value)
        return (
            number
            if isinstance(fallback, float) or not number.is_integer()
            else int(number)
        )
    except (TypeError, ValueError) as error:
        raise ValueError("Slider default must be a number") from error


def _as_date(value: Any) -> date | None:
    """Read a date or the date part of a timestamp. None leaves the widget empty."""

    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def _as_date_range(value: Any) -> tuple[date, ...]:
    """Resolve an absolute or relative Deepnote range. () leaves the widget empty."""

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
    """Encode a chosen date, leaving an empty widget empty."""
    return value.isoformat() if isinstance(value, date) else ""
