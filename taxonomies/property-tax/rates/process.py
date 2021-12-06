from dataflows import Flow, add_computed_field, delete_fields, \
    printer, set_type

from dgp.core.base_enricher import ColumnTypeTester, ColumnReplacer, \
        DatapackageJoiner, enrichments_flows, BaseEnricher
from dgp.config.consts import RESOURCE_NAME

from datacity_server.processors import MunicipalityNameToCodeEnricher, \
    FilterEmptyFields, StreamingDuplicateRemover, FillInDefaults


class FilterEmptyCodes(FilterEmptyFields):
    FIELDS_TO_CHECK = {
        'property-tax-code-id': None,
    }


class DefaultFiller(FillInDefaults):
    DEFAULT_FIELD_VALUES = {
        'property-tax-code-zone-kind': '',
        'property-tax-code-zone-id': '',
        'property-tax-code-min-area': 0,
        'property-tax-code-max-area': 1e9
    }


def flows(config, context):
    return enrichments_flows(
        config, context,
        MunicipalityNameToCodeEnricher,
        FilterEmptyCodes,
        StreamingDuplicateRemover,
        DefaultFiller
    )
