from dataflows import Flow, set_type

from dgp.core.base_enricher import ColumnTypeTester, ColumnReplacer, \
        DatapackageJoiner, enrichments_flows, BaseEnricher
from dgp.config.consts import RESOURCE_NAME

from datacity_server.processors import MunicipalityNameToCodeEnricher, \
    FilterEmptyFields, StreamingDuplicateRemover


class FilterEmptyCodes(FilterEmptyFields):
    FIELDS_TO_CHECK = {
        'property-code': None,
        'property-tax-code-id': None,
        'business-kind': None
    }


def flows(config, context):
    return enrichments_flows(
        config, context,
        MunicipalityNameToCodeEnricher,
        FilterEmptyCodes,
        StreamingDuplicateRemover,
    )
