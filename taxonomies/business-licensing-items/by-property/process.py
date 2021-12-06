from dataflows import Flow, set_type

from dgp.core.base_enricher import ColumnTypeTester, ColumnReplacer, \
        DatapackageJoiner, enrichments_flows, BaseEnricher
from dgp.config.consts import RESOURCE_NAME

from datacity_server.processors import MunicipalityNameToCodeEnricher, \
    FilterEmptyFields, StreamingDuplicateRemover


class FilterEmptyCodes(FilterEmptyFields):
    FIELDS_TO_CHECK = {
        'property-code': None
    }

class LicensingItemCodeFixer(BaseEnricher):
    def __init__(self, *args):
        super().__init__(*args)

    def test(self):
        return True

    def fix_code(self, code):
        if not code:
            return None
        code = str(code).strip()
        while len(code) < 8:
            code = '0' + code
        parts = []
        while len(code) > 0:
            parts.append(int(code[:2]))
            code = code[2:]
        return '.'.join(map(str, parts))

    def work(self):
        return Flow(
            set_type('property-code', type='string', transform=lambda v: self.fix_code(v), resources=RESOURCE_NAME),
        )

    def postflow(self):
        return Flow(self.work())


def flows(config, context):
    return enrichments_flows(
        config, context,
        MunicipalityNameToCodeEnricher,
        FilterEmptyCodes,
        LicensingItemCodeFixer,
        StreamingDuplicateRemover,
    )
