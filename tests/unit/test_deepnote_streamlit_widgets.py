from datetime import date
from typing import TYPE_CHECKING, Any

import pytest

from deepnote_toolkit.notebooks import InputBlock
from deepnote_toolkit.streamlit import render_inputs

if TYPE_CHECKING:
    from streamlit.testing.v1 import AppTest


class FakeContainer:
    """Return widget defaults without starting Streamlit."""

    def warning(self, message: str) -> None:
        """Ignore warnings unless a test installs a recording callback."""
        pass

    def checkbox(self, _label: str, **kwargs: Any) -> Any:
        """Return the configured checkbox value."""
        return kwargs["value"]

    def multiselect(self, _label: str, _options: list[str], **kwargs: Any) -> Any:
        """Return the configured selection list."""
        return kwargs["default"]

    def selectbox(self, _label: str, options: list[str], **kwargs: Any) -> Any:
        """Return the selected option, preserving an empty selection."""
        return options[kwargs["index"]] if kwargs["index"] is not None else None

    def slider(self, _label: str, **kwargs: Any) -> Any:
        """Record or return the configured slider value."""
        return kwargs["value"]

    def date_input(self, _label: str, **kwargs: Any) -> Any:
        """Return the dates supplied by the test container."""
        return kwargs["value"]

    def text_area(self, _label: str, **kwargs: Any) -> Any:
        """Return the configured multiline text."""
        return kwargs["value"]

    def text_input(self, _label: str, **kwargs: Any) -> Any:
        """Return the configured text."""
        return kwargs["value"]


def test_render_inputs_maps_all_deepnote_input_types_to_api_values() -> None:
    """Convert each supported widget value to its API representation."""
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
    """Omit incomplete ranges until the user chooses both dates."""

    class IncompleteDateContainer(FakeContainer):
        """Simulate a user selecting only the start of a range."""

        def date_input(self, _label: str, **_kwargs: Any) -> Any:
            """Return the dates supplied by the test container."""
            return (date(2026, 8, 17),)

    values = render_inputs(
        [
            InputBlock(
                "period", "input-date-range", [date(2026, 8, 1), date(2026, 8, 17)]
            )
        ],
        IncompleteDateContainer(),
    )

    assert values == {}


def test_slider_preserves_fractional_default_with_integer_bounds() -> None:
    """Keep fractional slider defaults and consistent numeric argument types."""

    class SliderContainer(FakeContainer):
        """Record slider arguments for numeric consistency checks."""

        slider_kwargs: dict[str, Any]

        def slider(self, _label: str, **kwargs: Any) -> Any:
            """Record or return the configured slider value."""
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
    """Normalize saved selections and filter unavailable options."""
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
    """Preserve timestamp compatibility for older date blocks."""
    defaults = []

    class RecordingContainer(FakeContainer):
        """Record defaults and simulate a changed date."""

        def date_input(self, _label: str, **kwargs: Any) -> Any:
            """Return the dates supplied by the test container."""
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
    """Leave unspecified dates empty."""
    values = render_inputs(
        [
            InputBlock("as_of", "input-date", ""),
            InputBlock("period", "input-date-range", ["", ""]),
        ],
        FakeContainer(),
    )

    assert values == {"as_of": "", "period": ["", ""]}


@pytest.mark.parametrize("value", [["2026-08-01", ""], ["", "2026-08-17"]])
def test_saved_open_ended_date_range_keeps_its_chosen_endpoint(
    value: list[str],
) -> None:
    """Preserve the saved endpoint instead of clearing an open-ended range."""
    assert render_inputs(
        [InputBlock("period", "input-date-range", value)], FakeContainer()
    ) == {"period": value}


@pytest.mark.parametrize("value", [["2026-08-01", ""], ["", "2026-08-17"]])
def test_real_widgets_preserve_and_edit_open_ended_date_ranges(
    streamlit_app_test: "type[AppTest]", value: list[str]
) -> None:
    """Keep open endpoints visible and allow completing them in real widgets."""

    def app(value: list[str]) -> None:
        """Render a saved open-ended range inside a Streamlit script."""
        import streamlit as st

        from deepnote_toolkit.notebooks import InputBlock
        from deepnote_toolkit.streamlit import render_inputs

        st.session_state["values"] = render_inputs(
            [InputBlock("period", "input-date-range", value)]
        )

    at = streamlit_app_test.from_function(app, args=(value,)).run()
    assert not at.exception
    assert at.session_state["values"] == {"period": value}
    assert len(at.date_input) == 2
    missing = value.index("")
    chosen = date(2026, 8, 1 if missing == 0 else 17)
    at.date_input[missing].set_value(chosen).run()
    assert not at.exception
    assert at.session_state["values"] == {"period": ["2026-08-01", "2026-08-17"]}


class FrozenDate(date):
    """Keep relative date calculations deterministic."""

    @classmethod
    def today(cls) -> "FrozenDate":
        """Use a month end in a leap year."""
        return cls(2024, 3, 31)


def test_relative_date_ranges_resolve_to_concrete_dates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Resolve relative ranges and clamp dates at month boundaries."""
    monkeypatch.setattr("deepnote_toolkit.streamlit.widgets.date", FrozenDate)

    values = render_inputs(
        [
            InputBlock("week", "input-date-range", "past7days"),
            InputBlock("custom", "input-date-range", "customDays3"),
            InputBlock("month", "input-date-range", "pastMonth"),
            InputBlock("year", "input-date-range", "pastYear"),
        ],
        FakeContainer(),
    )

    # Mar 31 has no counterpart a month earlier and clamps to Feb 29.
    assert values == {
        "week": ["2024-03-24", "2024-03-31"],
        "custom": ["2024-03-28", "2024-03-31"],
        "month": ["2024-02-29", "2024-03-31"],
        "year": ["2023-03-31", "2024-03-31"],
    }


def test_render_inputs_runs_on_real_streamlit_widgets(
    streamlit_app_test: "type[AppTest]",
) -> None:
    """Exercise every widget family using Streamlit AppTest."""

    def app() -> None:
        """Render the test inputs inside a real Streamlit script."""
        import streamlit as st

        from deepnote_toolkit.notebooks import InputBlock
        from deepnote_toolkit.streamlit import render_inputs

        st.session_state["values"] = render_inputs(
            [
                InputBlock("name", "input-text", "Ada"),
                InputBlock("enabled", "input-checkbox", True),
                InputBlock("region", "input-select", "EU", options=("US", "EU")),
                InputBlock(
                    "regions",
                    "input-select",
                    ["EU"],
                    options=("US", "EU"),
                    multiple=True,
                ),
                InputBlock("limit", "input-slider", "20", min=0, max=100, step=5),
                InputBlock("day", "input-date", "2026-08-17"),
                InputBlock("no_day", "input-date", ""),
                InputBlock("span", "input-date-range", ["2026-08-01", "2026-08-17"]),
                InputBlock("no_span", "input-date-range", ["", ""]),
            ]
        )

    at = streamlit_app_test.from_function(app).run()

    assert not at.exception
    assert at.session_state["values"] == {
        "name": "Ada",
        "enabled": True,
        "region": "EU",
        "regions": ["EU"],
        "limit": 20,
        "day": "2026-08-17",
        "no_day": "",
        "span": ["2026-08-01", "2026-08-17"],
        "no_span": ["", ""],
    }


def test_duplicate_variable_names_are_rejected() -> None:
    """Reject duplicate variables before widgets overwrite their values."""
    with pytest.raises(ValueError, match="unique"):
        render_inputs(
            [
                InputBlock("region", "input-text", "EU"),
                InputBlock("region", "input-text", "US"),
            ],
            FakeContainer(),
        )


def test_empty_variable_name_is_rejected() -> None:
    """Do not render a hand-built input that cannot be submitted to the API."""
    with pytest.raises(ValueError, match="must not be empty"):
        render_inputs([InputBlock("", "input-text", "value")], FakeContainer())


def test_multiselect_treats_a_scalar_default_as_one_selection() -> None:
    """Normalize scalar and absent multiselect defaults."""
    values = render_inputs(
        [
            InputBlock(
                "regions", "input-select", "EU", options=("EU", "US"), multiple=True
            ),
            InputBlock(
                "empty", "input-select", None, options=("EU", "US"), multiple=True
            ),
        ],
        FakeContainer(),
    )

    assert values == {"regions": ["EU"], "empty": []}


@pytest.mark.parametrize("kind", ["input-text", "input-textarea", "input-file"])
@pytest.mark.parametrize("value,expected", [(0, "0"), (False, "False"), (None, "")])
def test_falsey_text_defaults_are_preserved(
    kind: str, value: Any, expected: str
) -> None:
    """Keep zero and false defaults visible in text widgets."""
    assert render_inputs([InputBlock("x", kind, value)], FakeContainer()) == {
        "x": expected
    }


@pytest.mark.parametrize("value", [None, "stale"])
def test_unselected_single_select_does_not_submit_first_option(
    value: str | None,
) -> None:
    """Do not submit an option that the user has not selected."""
    assert (
        render_inputs(
            [InputBlock("x", "input-select", value, options=("first", "second"))],
            FakeContainer(),
        )
        == {}
    )


def test_stale_multiselect_default_warns() -> None:
    """Make unavailable saved selections visible to the user."""
    warnings = []
    container = FakeContainer()
    container.warning = warnings.append
    values = render_inputs(
        [
            InputBlock(
                "x",
                "input-select",
                ["old", "current"],
                options=("current",),
                multiple=True,
            )
        ],
        container,
    )
    assert values == {"x": ["current"]}
    assert len(warnings) == 1


@pytest.mark.parametrize(
    "min_value,max_value,step",
    [(10, 0, 1), (0, 10, 0), (0, 10, -1), (0, float("inf"), 1)],
)
def test_invalid_slider_constraints_are_rejected(
    min_value: float, max_value: float, step: float
) -> None:
    with pytest.raises(ValueError, match="slider"):
        render_inputs(
            [
                InputBlock(
                    "x", "input-slider", 3, min=min_value, max=max_value, step=step
                )
            ],
            FakeContainer(),
        )


@pytest.mark.parametrize(
    "value,expected", [(11, 10), (-1, 0), ("bad", 0), (float("nan"), 0), (None, 0)]
)
def test_slider_default_outside_bounds_is_clamped_with_a_warning(
    value: Any, expected: int
) -> None:
    warnings = []
    container = FakeContainer()
    container.warning = warnings.append
    values = render_inputs(
        [InputBlock("x", "input-slider", value, min=0, max=10, step=1)], container
    )
    assert values == {"x": expected}
    assert len(warnings) == (0 if value is None else 1)


def test_real_widgets_keep_falsey_defaults_and_require_selection(
    streamlit_app_test: "type[AppTest]",
) -> None:
    """Verify user interactions preserve defaults and omit partial ranges."""

    def app() -> None:
        """Render the test inputs inside a real Streamlit script."""
        import streamlit as st

        from deepnote_toolkit.notebooks import InputBlock
        from deepnote_toolkit.streamlit import render_inputs

        st.session_state["values"] = render_inputs(
            [
                InputBlock("zero", "input-text", 0),
                InputBlock("false", "input-textarea", False),
                InputBlock("choice", "input-select", "stale", options=("A", "B")),
                InputBlock("period", "input-date-range", ["", ""]),
            ]
        )

    at = streamlit_app_test.from_function(app).run()
    assert not at.exception
    assert at.text_input[0].value == "0" and at.text_area[0].value == "False"
    assert "choice" not in at.session_state["values"]
    at.selectbox[0].select("B").run()
    assert at.session_state["values"]["choice"] == "B"
    at.date_input[0].set_value((date(2026, 8, 17),)).run()
    assert not at.exception
    assert "period" not in at.session_state["values"]


def test_missing_select_value_does_not_select_literal_none_option() -> None:
    """Distinguish a missing default from an option containing the word None."""
    assert (
        render_inputs(
            [InputBlock("x", "input-select", None, options=("None", "EU"))],
            FakeContainer(),
        )
        == {}
    )
