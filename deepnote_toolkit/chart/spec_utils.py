import re
from typing import Any, Dict, List, Optional

import deepnote_toolkit.ocelots as oc
from deepnote_toolkit.chart.types import (
    COUNT_FIELD_NAME,
    COUNT_PERCENTAGE_FIELD_PREFIX,
    DEFAULT_CHART_COLOR,
    ChartError,
)

# NOTE: attach_config_to_vega_lite_spec and attach_selection_parameters_to_vega_lite_spec (as well
# as functions they use) are also defined in web app (see addPreCompilationPropertiesToVegaLiteSpec function).
# While not ideal, this is required for backwards compatibility. Even though we attach these properties in toolkit,
# previously they were attached on frontend directly before rendering the chart.
# See addPreCompilationPropertiesToVegaLiteSpec for more context.
#
# If you're editing any of this code you likely need to edit it in the web app too.


def attach_config_to_vega_lite_spec(spec):
    # We attach most of runtime properties to outputted Vega spec on
    # the frontend directly before rendering (see `addRuntimeDataToVegaSpec`),
    # but some (simple) Vega-Lite config properties when compiled to Vega
    # are translated into complex and elaborate structures. Because of this
    # we attach them here and let Vega-Lite handle conversion instead of doing
    # it by hand on frontend

    spec["height"] = "container"
    spec["width"] = "container"

    spec["autosize"] = {"type": "fit"}

    if "config" not in spec:
        spec["config"] = {}

    spec["config"]["axisQuantitative"] = {"tickCount": 5}

    if "area" not in spec["config"]:
        spec["config"]["area"] = {}
    spec["config"]["area"]["line"] = True

    spec["config"]["mark"] = {"color": DEFAULT_CHART_COLOR}
    spec["config"]["customFormatTypes"] = True


def _is_multilayer_spec_v1(spec: Dict[str, Any]) -> bool:
    return (
        spec.get("mark") is None
        and spec.get("layer") is not None
        and spec.get("usermeta", {}).get("specSchemaVersion") is None
    )


def _is_multilayer_spec_v2(spec: Dict[str, Any]) -> bool:
    return (
        spec.get("mark") is None
        and spec.get("layer") is not None
        and spec.get("usermeta", {}).get("specSchemaVersion") == 2
    )


def _is_top_layer_spec(spec: Dict[str, Any]) -> bool:
    return (
        spec.get("mark") is not None
        and spec.get("layer") is None
        and spec.get("usermeta", {}).get("specSchemaVersion") is None
    )


def _is_data_layer(layer: Dict[str, Any]) -> bool:
    # If layer has nested layers, it's not a data layer
    if "layer" in layer:
        return False

    # Check if it's a text layer (text layers are not data layers)
    mark = layer.get("mark")
    if isinstance(mark, str) and mark == "text":
        return False

    if isinstance(mark, dict) and mark.get("type") == "text":
        return False

    return True


def _get_all_data_layers(spec: Dict[str, Any]) -> List[Dict[str, Any]]:
    # There are 3 possible structures for spec: top layer spec, multilayer v1, and multilayer v2
    # In top layer spec everything is stored directly in top level, but in multilayer specs
    # there is nested structure. In v1 it goes like this root -> parent layer(s) -> data layer + helper layers
    # In v2 it's root -> axis group(s) -> parent layer(s) -> data layer + helper layers

    if _is_top_layer_spec(spec):
        return [{"layer": spec, "axisGroup": "primary", "helperLayers": []}]

    if _is_multilayer_spec_v1(spec):
        # In spec v1, if dual axis option was enabled, each layer got its own axis (and that worked poorly with 3+ layers),
        # so we just assume that first layer uses primary axis and rest use secondary axis
        resolve_scale = spec.get("resolve", {}).get("scale", {})
        chart_has_dual_axis = (
            resolve_scale.get("x") == "independent"
            or resolve_scale.get("y") == "independent"
        )

        result = []
        for index, layer in enumerate(spec["layer"]):
            if _is_data_layer(layer):
                data_layer = layer
                helper_layers = []
            else:
                # It's a parent layer with nested layers
                nested_layers = layer.get("layer", [])
                if nested_layers:
                    data_layer = nested_layers[0]
                    helper_layers = nested_layers[1:]
                else:
                    continue  # Skip if no nested layers

            axis_group = (
                "primary"
                if index == 0
                else ("secondary" if chart_has_dual_axis else "primary")
            )
            result.append(
                {
                    "layer": data_layer,
                    "axisGroup": axis_group,
                    "helperLayers": helper_layers,
                }
            )
        return result

    # Multilayer spec v2
    # In spec v2 measure axis for layer is indicated by its parent group, first group -> primary axis,
    # second group -> secondary axis
    result = []
    for group_index, axis_group in enumerate(spec.get("layer", [])):
        for parent_layer in axis_group.get("layer", []):
            nested_layers = parent_layer.get("layer", [])
            if nested_layers:
                data_layer = nested_layers[0]
                helper_layers = nested_layers[1:]
                axis_group_type = "primary" if group_index == 0 else "secondary"
                result.append(
                    {
                        "layer": data_layer,
                        "axisGroup": axis_group_type,
                        "helperLayers": helper_layers,
                    }
                )
    return result


def _get_mark_type(layer: Dict[str, Any]) -> str:
    # Check if layer has mark directly, otherwise get from first nested layer
    if "mark" in layer:
        mark = layer["mark"]
    else:
        # Get mark from first nested layer
        nested_layers = layer.get("layer", [])
        if nested_layers:
            mark = nested_layers[0].get("mark")
        else:
            mark = None

    # Extract type from mark
    if isinstance(mark, str):
        return mark
    elif isinstance(mark, dict) and mark.get("type"):
        return mark["type"]
    else:
        return "bar"  # Default fallback


def _create_vega_legend_filter_name(encoding_type: str, param_name_suffix: str) -> str:
    return f"legend_{encoding_type}_{param_name_suffix}"


def _create_chart_params(
    param_name_suffix: str, layer: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    legend_selections = [
        {
            "name": _create_vega_legend_filter_name("size", param_name_suffix),
            "select": {
                "type": "point",
                "encodings": ["size"],
                "toggle": "true",
            },
            "bind": "legend",
        },
        {
            "name": _create_vega_legend_filter_name("color", param_name_suffix),
            "select": {
                "type": "point",
                "encodings": ["color"],
                "toggle": "true",
            },
            "bind": "legend",
        },
    ]

    # Note that we are setting interval selection only for first layer. Vega is behaving strangely when
    # more than one interval selection is set.
    is_first_layer = param_name_suffix == "0" or param_name_suffix == "0_0"

    data_selections = []
    if layer and is_first_layer and _is_data_layer(layer):
        data_selections = [
            {
                "name": "interval_selection",
                "select": {
                    "type": "interval",
                    "encodings": ["x", "y"],
                },
            },
        ]

    return legend_selections + data_selections


def _create_chart_layer_axis_opacity(
    param_name_suffix: str, mark_type: str, params: List[Dict[str, Any]]
) -> Dict[str, Any]:
    does_mark_support_variable_opacity = mark_type in ["point", "circle", "bar"]
    does_interval_selection_param_exist = any(
        param.get("name") == "interval_selection" for param in params
    )
    should_add_interval_selection_params = (
        does_mark_support_variable_opacity and does_interval_selection_param_exist
    )

    condition_test = {
        "and": [
            {"param": _create_vega_legend_filter_name("size", param_name_suffix)},
            {"param": _create_vega_legend_filter_name("color", param_name_suffix)},
        ]
    }

    if should_add_interval_selection_params:
        condition_test["and"].append({"param": "interval_selection"})

    return {
        "condition": {
            "test": condition_test,
            "value": 1,
        },
        "value": 0.2,
    }


def attach_selection_parameters_to_vega_lite_spec(spec):
    if _is_multilayer_spec_v2(spec) or _is_multilayer_spec_v1(spec):
        data_layers = _get_all_data_layers(spec)

        for top_level_layer_index, layer_info in enumerate(data_layers):
            layer = layer_info["layer"]
            helper_layers = layer_info.get("helperLayers", [])

            for leaf_layer_index, leaf_layer in enumerate(helper_layers):
                # the "params" field needs to be added to all leaf layers, not just data layers, because of
                # https://stackoverflow.com/q/75240154/2761695
                leaf_layer["params"] = _create_chart_params(
                    f"{top_level_layer_index}_{leaf_layer_index}", None
                )

            # Add params and opacity to the main layer
            param_name_suffix = str(top_level_layer_index)
            params = _create_chart_params(param_name_suffix, layer)
            layer["params"] = params

            mark_type = _get_mark_type(layer)

            # Ensure encoding exists
            if "encoding" not in layer:
                layer["encoding"] = {}

            layer["encoding"]["opacity"] = _create_chart_layer_axis_opacity(
                param_name_suffix, mark_type, params
            )

    elif _is_top_layer_spec(spec):
        params = _create_chart_params("0", spec)
        spec["params"] = params

        mark_type = _get_mark_type(spec)

        # Ensure encoding exists
        if "encoding" not in spec:
            spec["encoding"] = {}

        spec["encoding"]["opacity"] = _create_chart_layer_axis_opacity(
            "0", mark_type, params
        )

    else:
        # Handle case where spec doesn't match expected patterns
        raise ChartError(f"Unrecognized spec structure: {list(spec.keys())}")

    return spec


def _extract_encodings_from_vega_lite_spec_recursive(spec_or_layer):
    # Lookup vega-lite.types.ts in the main app for details about different versions of the spec
    encodings = []
    if "layer" in spec_or_layer:
        # Either multi-layer spec or one of grouping layers (VegaLiteAxisGroup / VegaLiteMultiLayerSpecV1TopLevelLayer)
        for layer in spec_or_layer["layer"]:
            encodings.extend(_extract_encodings_from_vega_lite_spec_recursive(layer))
    elif "encoding" in spec_or_layer:
        # Either VegaLiteTopLayerSpec or leaf layer
        encodings.extend(spec_or_layer["encoding"].values())

    return encodings


def _unescape_field_name(field: str) -> str:
    # "date\\.time" -> "date.time"
    return re.sub(r"\\(.)", r"\1", field)


def _get_used_fields_from_vega_lite_spec(vega_lite_spec):
    encodings = _extract_encodings_from_vega_lite_spec_recursive(vega_lite_spec)

    return set(
        _unescape_field_name(encoding["field"])
        for encoding in encodings
        if "field" in encoding and isinstance(encoding["field"], str)
    )


def verify_used_fields(oc_df: oc.DataFrame, vega_lite_spec: Any) -> None:
    allowed_fields = set(oc_df.column_names) | set([COUNT_FIELD_NAME])

    fields = _get_used_fields_from_vega_lite_spec(vega_lite_spec)

    unknown_fields = fields - allowed_fields
    invalid_fields = [
        field
        for field in unknown_fields
        if not field.startswith(COUNT_PERCENTAGE_FIELD_PREFIX)
    ]

    if invalid_fields:
        raise ChartError(
            f"The following columns were selected for the chart but are not present in the dataframe: {invalid_fields}"
        )
