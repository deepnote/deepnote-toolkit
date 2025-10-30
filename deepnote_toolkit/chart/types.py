class ChartError(Exception):
    pass


# NOTE: This is also defined in libs/shared/src/constants.ts in webapp
VEGA_5_MIME_TYPE = "application/vnd.vega.v5+json"

# This limit is applied to post-aggregated dataset, to e.g. not accidentally produce
# scatterchart with too many points
CHART_ROW_LIMIT = 5_000

# NOTE: keep in sync with defaultChartColor in web app
DEFAULT_CHART_COLOR = "#2266D3"

# NOTE: COUNT_PERCENTAGE_FIELD_PREFIX and COUNT_FIELD_NAME are also defined in chart-config.constants.ts in
# the main app, keep the 2 in sync
COUNT_PERCENTAGE_FIELD_PREFIX = "Percentage of "
COUNT_FIELD_NAME = "COUNT(*)"
