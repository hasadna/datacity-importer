import os

from sqlalchemy import create_engine

from dataflows import Flow, conditional, add_field, append_to_primary_key

from dgp.core.base_enricher import enrichments_flows, BaseEnricher
from dgp.config.consts import CONFIG_TAXONOMY_CT
from dgp_server.publish_flow import publish_flow

engine = create_engine(os.environ['DATASETS_DATABASE_URL'])


class MissingColumnsAdder(BaseEnricher):

    def test(self):
        return True

    def no_such_field(self, field_name):
        def func(dp):
            return all(field_name != f.name for f in dp.resources[0].schema.fields)
        return func

    def postflow(self):
        return Flow(
            *[
                conditional(
                    self.no_such_field(ct['name']),
                    Flow(
                        add_field(ct['name'], ct['dataType'], '-' if ct.get('unique') else None),
                        append_to_primary_key(ct['name']) if ct.get('unique') else None
                    )
                )
                for ct in self.config.get(CONFIG_TAXONOMY_CT)
            ]
        )


class DBWriter(BaseEnricher):

    def test(self):
        return True

    def postflow(self):
        return publish_flow(self.config, engine, mode='append', fast=True)


def flows(config, context):
    return enrichments_flows(
        config, context,
        MissingColumnsAdder,
        DBWriter,
    )
