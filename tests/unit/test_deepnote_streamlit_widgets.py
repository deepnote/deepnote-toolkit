from datetime import date, timedelta
from typing import Any

from deepnote_toolkit.streamlit import InputBlock, render_inputs


class FakeContainer:
    def checkbox(self, _label: str, **kwargs: Any) -> Any:
        return kwargs["value"]

    def multiselect(self, _label: str, _options: list[str], **kwargs: Any) -> Any:
        return kwargs["default"]

    def selectbox(self, _label: str, options: list[str], **kwargs: Any) -> Any:
        return options[kwargs["index"]]

    def slider(self, _label: str, **kwargs: Any) -> Any:
        return kwargs["value"]

    def date_input(self, _label: str, **kwargs: Any) -> Any:
        return kwargs["value"]

    def text_area(self, _label: str, **kwargs: Any) -> Any:
        return kwargs["value"]

    def text_input(self, _label: str, **kwargs: Any) -> Any:
        return kwargs["value"]


def test_render_inputs_maps_all_deepnote_input_types_to_api_values() -> None:
    inputs = [
        InputBlock("name", "input-text", "Ada"),
        InputBlock("notes", "input-textarea", "Hello"),
        InputBlock("enabled", "input-checkbox", True),
        InputBlock("region", "input-select", "Europe", options=("All", "Europe")),
        InputBlock(
            "regions",
            "input-select",
            ["Europe"],
            options=("All", "Europe"),
            multiple=True,
        ),
        InputBlock("limit", "input-slider", "20", min=10, max=100, step=10),
        InputBlock("as_of", "input-date", date(2026, 8, 17)),
        InputBlock("period", "input-date-range", [date(2026, 8, 1), date(2026, 8, 17)]),
    ]

    assert render_inputs(inputs, FakeContainer()) == {
        "name": "Ada",
        "notes": "Hello",
        "enabled": True,
        "region": "Europe",
        "regions": ["Europe"],
        "limit": 20,
        "as_of": "2026-08-17",
        "period": ["2026-08-01", "2026-08-17"],
    }


def test_incomplete_date_range_is_still_valid_for_runner_contract() -> None:
    class IncompleteDateContainer(FakeContainer):
        def date_input(self, _label: str, **_kwargs: Any) -> Any:
            return (date(2026, 8, 17),)

    values = render_inputs(
        [
            InputBlock(
                "period", "input-date-range", [date(2026, 8, 1), date(2026, 8, 17)]
            )
        ],
        IncompleteDateContainer(),
    )

    assert values == {"period": ["2026-08-17", "2026-08-17"]}


def test_slider_preserves_fractional_default_with_integer_bounds() -> None:
    class SliderContainer(FakeContainer):
        slider_kwargs: dict[str, Any]

        def slider(self, _label: str, **kwargs: Any) -> Any:
            self.slider_kwargs = kwargs
            return kwargs["value"]

    container = SliderContainer()
    values = render_inputs(
        [InputBlock("threshold", "input-slider", "20.5", min=10, max=30, step=0.5)],
        container,
    )

    assert values == {"threshold": 20.5}
    assert container.slider_kwargs == {
        "min_value": 10.0,
        "max_value": 30.0,
        "value": 20.5,
        "step": 0.5,
        "key": "deepnote:threshold",
    }


def test_multiselect_normalizes_and_filters_stale_defaults() -> None:
    values = render_inputs(
        [
            InputBlock(
                "regions",
                "input-select",
                [1, "Europe", "Missing"],
                options=("1", "Europe"),
                multiple=True,
            )
        ],
        FakeContainer(),
    )

    assert values == {"regions": ["1", "Europe"]}


def test_date_reads_timestamp_default_and_keeps_its_shape() -> None:
    defaults = []

    class RecordingContainer(FakeContainer):
        def date_input(self, _label: str, **kwargs: Any) -> Any:
            defaults.append(kwargs["value"])
            return date(2026, 8, 20)

    values = render_inputs(
        [
            InputBlock("legacy", "input-date", "2026-08-17T00:00:00.000Z"),
            InputBlock("current", "input-date", "2026-08-17"),
        ],
        RecordingContainer(),
    )

    assert defaults == [date(2026, 8, 17), date(2026, 8, 17)]
    assert values == {"legacy": "2026-08-20T00:00:00.000Z", "current": "2026-08-20"}


def test_empty_dates_stay_empty_instead_of_becoming_today() -> None:
    values = render_inputs(
        [
            InputBlock("as_of", "input-date", ""),
            InputBlock("period", "input-date-range", ["", ""]),
        ],
        FakeContainer(),
    )

    assert values == {"as_of": "", "period": ["", ""]}


def test_relative_date_ranges_resolve_to_concrete_dates() -> None:
    values = render_inputs(
        [
            InputBlock("week", "input-date-range", "past7days"),
            InputBlock("custom", "input-date-range", "customDays3"),
            InputBlock("year", "input-date-range", "pastYear"),
        ],
        FakeContainer(),
    )

    today = date.today()
    # Feb 29 has no counterpart a year earlier and clamps to Feb 28.
    year_ago_day = 28 if (today.month, today.day) == (2, 29) else today.day
    assert values == {
        "week": [(today - timedelta(days=7)).isoformat(), today.isoformat()],
        "custom": [(today - timedelta(days=3)).isoformat(), today.isoformat()],
        "year": [
            today.replace(year=today.year - 1, day=year_ago_day).isoformat(),
            today.isoformat(),
        ],
    }


def test_inputs_sharing_a_variable_name_render_once() -> None:
    values = render_inputs(
        [
            InputBlock("region", "input-text", "EU"),
            InputBlock("region", "input-text", "US"),
        ],
        FakeContainer(),
    )

    assert values == {"region": "EU"}
