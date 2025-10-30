from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional, Tuple

from typing_extensions import Self

import deepnote_toolkit.ocelots as oc
from deepnote_toolkit.ocelots.data_preview import DataPreview
from deepnote_toolkit.sql.query_preview import DeepnoteQueryPreview

OutputType = Literal["dataframe", "query_preview", "data_preview"]


class InvalidAttributesError(Exception):
    pass


@dataclass
class BrowseSpec:
    """This class prepresent parsed DataFrame browsing spec"""

    filters: List[oc.Filter]
    sort_by: List[Tuple[str, bool]]
    page_size: int
    page_index: int
    cell_formatting_rules: List[Dict[str, Any]]
    color_scale_column_names: List[str]

    @classmethod
    def from_json(cls, spec_str: Optional[str], column_names: Tuple[str, ...]) -> Self:
        """Create a BrowseSpec instance from a JSON string.

        Args:
            spec_str: JSON string containing the browsing specification
            column_names: Tuple of column names to validate against

        Returns:
            BrowseSpec: New BrowseSpec instance with parsed values
        """
        # NOTE: make sure to keep these default constants
        # in sync with their other incarnations in `deepnote`
        # repo. They have identical names on purpose.
        DEFAULT_COLUMN_FILTERS = []
        DEFAULT_COLUMN_SORT_BY = []
        DEFAULT_PAGE_SIZE = 10
        DEFAULT_PAGE_INDEX = 0

        import json

        spec = json.loads(spec_str or "{}")

        raw_filters = spec.get("filters", DEFAULT_COLUMN_FILTERS)
        raw_conditional_filters = spec.get("conditionalFilters", DEFAULT_COLUMN_FILTERS)
        filters = [
            oc.Filter.from_dict(f) for f in raw_filters + raw_conditional_filters
        ]

        raw_sort_by = spec.get("sortBy", DEFAULT_COLUMN_SORT_BY)
        sort_by = [cls._parse_sort_by(column_names, item) for item in raw_sort_by]
        sort_by = [sort for sort in sort_by if sort is not None]

        cell_formatting_rules = spec.get("cellFormattingRules", [])
        color_scale_column_names = cls._parse_color_scale_column_names(
            cell_formatting_rules, column_names
        )

        return cls(
            filters=filters,
            sort_by=sort_by,
            page_size=spec.get("pageSize", DEFAULT_PAGE_SIZE),
            page_index=spec.get("pageIndex", DEFAULT_PAGE_INDEX),
            cell_formatting_rules=cell_formatting_rules,
            color_scale_column_names=color_scale_column_names,
        )

    @staticmethod
    def _parse_sort_by(
        column_names: Tuple[str, ...], sort_by_spec: Dict[str, Any]
    ) -> Optional[Tuple[str, bool]]:
        """Parse given sortBy spec, return it as tuple (column_id, bool_is_asc).

        Args:
            column_names: Tuple of column names to validate against
            sort_by_spec: Dictionary containing sort specification with keys:
                         - id: Column name to sort by
                         - type: Sort type ("asc" or "desc")

        Returns:
            Tuple of (column_name, is_ascending) or None if column doesn't exist

        Raises:
            InvalidAttributesError: If required keys are missing or type is invalid
        """
        if (
            not sort_by_spec
            or sort_by_spec.get("id") is None
            or sort_by_spec.get("type") is None
        ):
            raise InvalidAttributesError("Invalid attributes given")

        column_id = sort_by_spec["id"]
        column_type = sort_by_spec["type"]

        # We are going to explicitly not raise an exception for non existent column
        # id to enable users reuse spec between different cell execution results
        if column_id not in column_names:
            return None

        if column_type == "asc":
            return column_id, True
        if column_type == "desc":
            return column_id, False
        else:
            raise InvalidAttributesError("Invalid sort by type given")

    @staticmethod
    def _parse_color_scale_column_names(
        cell_formatting_rules: List[Dict[str, Any]], column_names: Tuple[str, ...]
    ) -> List[str]:
        """Returns a list of column names that have a color scale formatting rule applied."""

        color_scale_column_names = set()

        for rule in cell_formatting_rules:
            if rule.get("type") != "colorScale":
                continue

            column_selection_mode = rule.get("columnSelectionMode")
            rule_column_names = rule.get("columnNames", [])

            if column_selection_mode == "all":
                # Return early with all column names
                return list(column_names)

            if column_selection_mode == "allExcept":
                column_names_to_include = set(column_names) - set(rule_column_names)
            elif column_selection_mode == "only":
                column_names_to_include = set(rule_column_names)
            else:
                continue  # If columnSelectionMode is None or unrecognized, skip this rule

            # Only add columns that actually exist
            valid_column_names = column_names_to_include.intersection(set(column_names))
            color_scale_column_names.update(valid_column_names)

        return list(color_scale_column_names)


@dataclass
class BrowseDfResult:
    processed_df: oc.DataFrame
    rows: List[Dict[str, Any]]
    row_count: int
    preview_row_count: int
    output_type: OutputType


def _normalize_output_type(x: Any) -> OutputType:
    """
    Returns the type of the dataframe ("query_preview" or "dataframe").
    Each type is rendered differently in the webapp.
    """
    if isinstance(x, oc.DataFrame):
        if x.native_type == "pandas" and isinstance(
            x.to_native(), DeepnoteQueryPreview
        ):
            return "query_preview"
        return "dataframe"

    if isinstance(x, DataPreview):
        return "data_preview"

    raise ValueError(f"Object of type {type(x)} is not valid output type")


def browse_df(oc_df: oc.DataFrame, spec: Optional[BrowseSpec]) -> BrowseDfResult:
    """
    Apply browsing spec to return slice of dataframe.
    """

    if oc_df.data_preview is None and oc_df.size() == 0:
        return BrowseDfResult(
            processed_df=oc_df,
            rows=[],
            row_count=0,
            preview_row_count=0,
            output_type=_normalize_output_type(oc_df),
        )

    processed_df = (
        oc_df.prepare_for_serialization().filter(*spec.filters).sort(spec.sort_by)
    )

    if oc_df.data_preview is not None:
        oc_df.data_preview.update_if_needed(filters=spec.filters, sort_by=spec.sort_by)
        data_preview_page = oc_df.data_preview.page(spec.page_index, spec.page_size)
        return BrowseDfResult(
            processed_df=processed_df,
            rows=data_preview_page,
            row_count=oc_df.data_preview.total_size,
            preview_row_count=len(oc_df.data_preview.data),
            output_type=_normalize_output_type(oc_df.data_preview),
        )
    else:
        rows = processed_df.paginate(spec.page_index, spec.page_size).to_records(
            mode="json"
        )
        row_count = processed_df.size()
        return BrowseDfResult(
            processed_df=processed_df,
            rows=rows,
            row_count=row_count,
            preview_row_count=row_count,
            output_type=_normalize_output_type(processed_df),
        )
