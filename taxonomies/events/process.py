import re

from dataflows import Flow, add_computed_field, delete_fields, \
    printer, set_type

from dgp.core.base_enricher import ColumnTypeTester, ColumnReplacer, \
        DatapackageJoiner, enrichments_flows, BaseEnricher, DuplicateRemover
from dgp.config.consts import RESOURCE_NAME

from datacity_server.processors import MunicipalityNameToCodeEnricher, \
    FilterEmptyFields


class SelectLatestProcessEnricher(DuplicateRemover):

    ORDER_BY_KEY = '{process-publication-date}'


VALID_CODES = re.compile('[-.א-ת0-9/]+')


class FilterEmptyCodes(FilterEmptyFields):

    FIELDS_TO_CHECK = {
        'event-title': lambda v: isinstance(v, str) and v.strip()
    }


def flows(config, context):
    return enrichments_flows(
        config, context,
        FilterEmptyCodes,
        MunicipalityNameToCodeEnricher,
        # SelectLatestProcessEnricher,
    )
