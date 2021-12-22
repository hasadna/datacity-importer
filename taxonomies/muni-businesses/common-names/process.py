import dataflows as DF

from dgp.core.base_enricher import enrichments_flows, BaseEnricher
from dgp.config.consts import RESOURCE_NAME

from datacity_server.processors import MunicipalityNameToCodeEnricher, FilterEmptyFields


class FilterEmptyCodes(FilterEmptyFields):
    FIELDS_TO_CHECK = {
        'business-kind': None,
        'business-kind-internal': None,
    }

class Enumerator(BaseEnricher):

    def test(self):
        return True

    def work(self):
        def func(rows):
            for i, row in enumerate(rows):
                row['business-kind-order'] = i
                yield row
        return func

    def postflow(self):
        return DF.Flow(
            DF.add_field('business-kind-order', 'integer', 0),
            self.work()
        )


def flows(config, context):
    return enrichments_flows(
        config, context,
        MunicipalityNameToCodeEnricher,
        FilterEmptyCodes,
        Enumerator,
    )
