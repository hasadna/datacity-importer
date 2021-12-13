from dgp.core.base_enricher import enrichments_flows
from dgp.config.consts import RESOURCE_NAME

from datacity_server.processors import MunicipalityNameToCodeEnricher, FilterEmptyFields


class FilterEmptyCodes(FilterEmptyFields):
    FIELDS_TO_CHECK = {
        'business-kind': None,
        'business-kind-internal': None,
    }


def flows(config, context):
    return enrichments_flows(
        config, context,
        MunicipalityNameToCodeEnricher,
        FilterEmptyCodes,
    )
