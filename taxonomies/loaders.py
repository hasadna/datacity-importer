from urllib.parse import urljoin

from dgp.core import BaseDataGenusProcessor, BaseAnalyzer, Validator, Required
from dgp.core.base_enricher import enrichments_flows, BaseEnricher
from dgp.config.consts import CONFIG_JSON_PROPERTY, CONFIG_HEADER_FIELDS,\
    RESOURCE_NAME, CONFIG_FORMAT, CONFIG_URL

from dataflows import Flow, add_field, delete_fields

import tabulator
import ijson
from pyquery import PyQuery as pq


CONFIG_DATACITY_EXTRA_HEADERS = 'datacity.extra_headers'


class SharepointJson(BaseAnalyzer):

    REQUIRES = Validator(
        Required(CONFIG_FORMAT)
    )

    ROOTS = [
        'Root.Items.Item'
    ]

    def run(self):

        if (self.config.get(CONFIG_FORMAT) == 'json' and
                not self.config.get(CONFIG_JSON_PROPERTY)):
            stream = self.context.stream
            source = stream._Stream__source
            loader = stream._Stream__loader
            for root in self.ROOTS:
                try:
                    obj = next(iter(ijson.items(loader.load(source),
                                    prefix=root + '.item')))
                    fields = obj.get('Fields', {}).get('Field', [])
                    extra_fields = []
                    for field in fields:
                        extra_fields.append(field['Caption'])
                    self.config.set(CONFIG_DATACITY_EXTRA_HEADERS, extra_fields)
                    self.config.set(CONFIG_JSON_PROPERTY, root)
                    break
                except StopIteration:
                    pass
        if self.config.get(CONFIG_DATACITY_EXTRA_HEADERS):
            extra_fields = self.config.get(CONFIG_DATACITY_EXTRA_HEADERS)
            headers = self.config.get(CONFIG_HEADER_FIELDS)
            headers = list(filter(lambda x: x != 'Fields', headers))
            headers.extend(extra_fields)
            self.config.set(CONFIG_HEADER_FIELDS, headers)

    def unfurl_fields(self):
        def func(row):
            if 'Fields' in row:
                for x in row['Fields']['Field']:
                    row[x['Caption']] = x['Value']
        return func

    def flow(self):
        if self.config.get(CONFIG_DATACITY_EXTRA_HEADERS):
            return Flow(
                *[
                    add_field(f, 'string', resources=RESOURCE_NAME)
                    for f in self.config.get(CONFIG_DATACITY_EXTRA_HEADERS)
                ],
                self.unfurl_fields(),
                # delete_fields(['Fields'])
            )


class HTMLTableURLExtractor(BaseAnalyzer):

    REQUIRES = Validator(
        Required(CONFIG_FORMAT)
    )

    URL_FIELD = 'extracted_url'

    def run(self):
        if self.config.get(CONFIG_FORMAT) == 'html':
            self.config.set('source.raw_html', True)
            headers = self.config.get(CONFIG_HEADER_FIELDS)
            headers.append(self.URL_FIELD)
            self.config.set(CONFIG_HEADER_FIELDS, headers)

    def clean_html_values(self):
        base = self.config.get(CONFIG_URL)
        def func(row):
            is_set = False
            for k, v in row.items():
                if isinstance(v, str):
                    if '<' in v:
                        row[k] = pq(v).text()
                        if not is_set:
                            anchors = pq('<div>' + v + '</div>').find('a')
                            if len(anchors) > 0:
                                url = anchors[0].attrib.get('href')
                                if url:
                                    url = urljoin(base, url)
                                    row[self.URL_FIELD] = url
                                    is_set = True

        return func

    def flow(self):
        if self.config.get('source.raw_html'):
            return Flow(
                add_field(self.URL_FIELD, 'string', resources=RESOURCE_NAME),
                self.clean_html_values(),
            )


def analyzers(*_):
    return [
        SharepointJson,
        HTMLTableURLExtractor,
    ]
