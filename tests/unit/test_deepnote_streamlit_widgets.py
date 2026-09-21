from datetime import date
from typing import Any

import pytest

from deepnote_toolkit.notebooks import InputBlock
from deepnote_toolkit.streamlit import render_inputs


class FakeContainer:
    def warning(self, message):
        pass

    def checkbox(self, _label: str, **kwargs: Any) -> Any:
        return kwargs["value"]

    def multiselect(self, _label: str, _options: list[str], **kwargs: Any) -> Any:
        return kwargs["default"]

    def selectbox(self, _label: str, options: list[str], **kwargs: Any) -> Any:
        return options[kwargs["index"]] if kwargs["index"] is not None else None

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

    assert values == {}


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


class FrozenDate(date):
    @classmethod
    def today(cls) -> "FrozenDate":
        return cls(2024, 3, 31)


def test_relative_date_ranges_resolve_to_concrete_dates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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


def test_render_inputs_runs_on_real_streamlit_widgets() -> None:
    pytest.importorskip("streamlit")
    from streamlit.testing.v1 import AppTest

    def app() -> None:
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

    at = AppTest.from_function(app).run()

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
    with pytest.raises(ValueError, match="unique"):
        render_inputs(
            [
                InputBlock("region", "input-text", "EU"),
                InputBlock("region", "input-text", "US"),
            ],
            FakeContainer(),
        )


def test_multiselect_treats_a_scalar_default_as_one_selection() -> None:
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
def test_falsey_text_defaults_are_preserved(kind, value, expected):
    assert render_inputs([InputBlock("x", kind, value)], FakeContainer()) == {
        "x": expected
    }


@pytest.mark.parametrize("value", [None, "stale"])
def test_unselected_single_select_does_not_submit_first_option(value):
    assert (
        render_inputs(
            [InputBlock("x", "input-select", value, options=("first", "second"))],
            FakeContainer(),
        )
        == {}
    )


def test_stale_multiselect_default_warns():
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
    "value,min_value,max_value,step",
    [
        (11, 0, 10, 1),
        (-1, 0, 10, 1),
        (3, 10, 0, 1),
        (3, 0, 10, 0),
        (3, 0, 10, -1),
        ("bad", 0, 10, 1),
        (float("nan"), 0, 10, 1),
        (3, 0, float("inf"), 1),
    ],
)
def test_invalid_slider_configuration_is_reported(value, min_value, max_value, step):
    with pytest.raises(ValueError, match="[Ss]lider"):
        render_inputs(
            [
                InputBlock(
                    "x", "input-slider", value, min=min_value, max=max_value, step=step
                )
            ],
            FakeContainer(),
        )


def test_real_widgets_keep_falsey_defaults_and_require_selection():
    pytest.importorskip("streamlit")
    from streamlit.testing.v1 import AppTest

    def app():
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

    at = AppTest.from_function(app).run()
    assert not at.exception
    assert at.text_input[0].value == "0" and at.text_area[0].value == "False"
    assert "choice" not in at.session_state["values"]
    at.selectbox[0].select("B").run()
    assert at.session_state["values"]["choice"] == "B"
    at.date_input[0].set_value((date(2026, 8, 17),)).run()
    assert not at.exception
    assert "period" not in at.session_state["values"]
