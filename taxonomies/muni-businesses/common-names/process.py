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


class LicensingItemProcessor(BaseEnricher):

    def test(self):
        return True

    def work(self):
        def func(row):
            if row['business-kind-store']:
                row['business-licensing-item-id'].append('6.2')
        return func

    def postflow(self):
        return DF.Flow(
            DF.set_type('business-licensing-item-id', type='array', transform=lambda v: [x.strip() for x in v.split(',')] if v else []),
            self.work()
        )


def flows(config, context):
    return enrichments_flows(
        config, context,
        MunicipalityNameToCodeEnricher,
        FilterEmptyCodes,
        Enumerator,
        LicensingItemProcessor
    )
