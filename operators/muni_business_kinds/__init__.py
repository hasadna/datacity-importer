import os
import dataflows as DF
from collections import Counter
from dataflows_ckan import dump_to_ckan

MUNICIPALITIES = [
    ('באר שבע', 'beer-sheva'),
]

def licensing_items(muni_name):
    QUERY = f'''
        with raw as (
            SELECT cn."business-kind" as business_kind,
                   pt."municipality-name" as municipality_name,
                split_part("business-licensing-item-id", '.', 1) || '.' ||
                split_part("business-licensing-item-id", '.', 2) || '.' ||
                split_part("business-licensing-item-id", '.', 3) as business_licensing_item_id
            FROM property_tax_business_kinds as pt
            JOIN business_licensing_items_by_property USING ("municipality-name", "property-code")
            JOIN business_kind_common_names as cn ON (pt."business-kind" = cn."business-kind-internal" and pt."municipality-name" = cn."municipality-name")
            where pt."municipality-name" = '{muni_name}'
            and "business-licensing-item-id" != '-'
        ),
        totals as (
            select business_kind, count(1) as total from raw group by 1
        ),
        counts as (
            select business_kind, business_licensing_item_id, count(1) as count from raw group by 1,2
        )
        select business_kind, business_licensing_item_id, (100.0*count)/total as pct
        from counts
        join totals using (business_kind)
        where (1000*count)/total >= 100
        order by 1, 3 desc
    '''
    DF.Flow(
        DF.load('env://DATASETS_DATABASE_URL', query=QUERY, name='business_kind_licensing_items'),
        # DF.join_with_self('business_kind_licensing', ['business_kind'], dict(business_kind=None, business_licensing_item_id=dict(aggregate='set'))),
        # DF.printer(),
        DF.dump_to_path('out/business_kind_licensing_items'),
    ).process()


def licensing_law():
    DF.Flow(
        DF.load('env://DATASETS_DATABASE_URL', table='business_licensing_law', name='business_licensing_law'),
        DF.add_field('rule', 'object', default=lambda r: dict((k, v) for k, v in r.items() if k != 'id')),
        DF.join_with_self('business_licensing_law', ['id'], dict(
            id=None, rules=dict(name='rule', aggregate='array')
        )),
        # DF.printer(),
        DF.dump_to_path('out/business_licensing_law'),
    ).process()

def combine_rules():
    RESOLUTION_OPTIONS = dict([
        ('agriculture', ('yes', 'attn', 'no')),
        ('environment', ('yes', 'attn', 'no')),
        ('fire-dept', ('yes', 'attn', 'no')),
        ('ordinance', ('yes', 'attn', 'no')),
        ('police', ('yes', 'attn', 'no')),
        ('sanitation', ('yes', 'attn', 'no')),
        ('spec', (False, True)),
        ('track', ('full', 'b', 'a', 'deposition')),
        ('firedpt_track', ('inspection', 'deposition')),
    ])
    def func(rows):
        for row in rows:
            rules = row['rules']
            if not rules:
                return
            all_rules = [rr for r in row['rules'] if r for rr in r if rr]
            areas = sorted(set(r['area'] for r in all_rules))
            agg_rule = dict()
            # new_rules = []
            prev_rule = dict()
            for area in areas:
                area_rules = [r for r in all_rules if r['area'] == area]
                for r in area_rules:
                    for k, v in r.items():
                        agg_rule.setdefault(k, set()).add(v)
                rule = dict()
                for key, options in RESOLUTION_OPTIONS.items():
                    v = agg_rule.get(key) or []
                    rule[key] = options[0]
                    for option in options:
                        if option in v:
                            rule[key] = option
                            break
                rule['area'] = area
                rule['duration'] = min((int(d) for d in agg_rule.get('duration') if d), default=None)
                rule['spec_links'] = list(l for l in agg_rule.get('spec_link') if l)
                rule['item_ids'] = ', '.join(l for l in agg_rule.get('orig_id') if l)
                if any(rule[k] != prev_rule.get(k) for k in RESOLUTION_OPTIONS.keys()):
                    yield {**row, 'rule': rule}
                    prev_rule = rule
    return DF.Flow(
        DF.add_field('rule', 'object'),
        func,
        DF.add_field('area', 'integer', default=lambda r: r['rule'].get('area')),
        DF.add_field('environment', 'string', default=lambda r: r['rule'].get('environment')),
        DF.add_field('police', 'string', default=lambda r: r['rule'].get('police')),
        DF.add_field('ordinance', 'string', default=lambda r: r['rule'].get('ordinance')),
        DF.add_field('sanitation', 'string', default=lambda r: r['rule'].get('sanitation')),
        DF.add_field('agriculture', 'string', default=lambda r: r['rule'].get('agriculture')),
        DF.add_field('fire-dept', 'string', default=lambda r: r['rule'].get('fire-dept')),
        DF.add_field('firedpt_track', 'string', default=lambda r: r['rule'].get('firedpt_track')),
        DF.add_field('track', 'string', default=lambda r: r['rule'].get('track')),
        DF.add_field('duration', 'integer', default=lambda r: r['rule'].get('duration')),
        DF.add_field('item_ids', 'string', default=lambda r: r['rule'].get('item_ids')),
        DF.add_field('spec', 'boolean', default=lambda r: r['rule'].get('spec')),
        DF.add_field('spec_links', 'array', default=lambda r: r['rule'].get('spec_links')),
        DF.delete_fields(['rules', 'rule']),
    )

def licensing(muni_name):
    licensing_items(muni_name)
    licensing_law()
    business_licensing_law = DF.Flow(DF.load('out/business_licensing_law/datapackage.json')).results()[0][0]
    business_licensing_law = dict((r['id'], r['rules']) for r in business_licensing_law)
    DF.Flow(
        DF.load('out/business_kind_licensing_items/datapackage.json'),
        DF.update_resource(-1, name='business_kind_licensing_rules'),
        DF.join_with_self('business_kind_licensing_rules', ['business_kind'], dict(business_kind=None, business_licensing_item_id=dict(aggregate='set'))),
        DF.add_field('rules', 'array', 
            default=lambda row: [
                r for r in [
                    business_licensing_law.get(id) or business_licensing_law.get(id[:-2]) for id in row['business_licensing_item_id']
                ] if r
            ]),
        DF.delete_fields(['business_licensing_item_id']),
        DF.filter_rows(lambda r: r['rules'] is not None and len(r['rules']) > 0),
        combine_rules(),
        # DF.printer(),
        DF.dump_to_path('out/business_kind_licensing_rules', format='json'),
    ).process()

def property_tax_rules(muni_name):
    QUERY = f'''
WITH raw AS
  (SELECT "municipality-name",
          "business-kind" AS "business-kind-internal",
          "property-tax-code-id"
   FROM property_tax_business_kinds),
bk AS
  (SELECT "municipality-name",
          "business-kind",
          "property-tax-code-id",
          count(1) AS COUNT
   FROM raw
   JOIN business_kind_common_names USING ("business-kind-internal",
                                          "municipality-name")
   GROUP BY 1, 2, 3),
totals AS
  (SELECT "municipality-name",
          "business-kind",
          count(1) as total
   FROM bk
   GROUP BY 1, 2)
SELECT "business-kind",
       "property-tax-code-id",
       "count",
       "property-tax-code-zone-kind",
       "property-tax-code-zone-id",
       "property-tax-code-min-area",
       "property-tax-code-max-area",
       "property-tax-code-rate",
       "total"
FROM bk
JOIN tax_code_rates AS r USING ("property-tax-code-id", "municipality-name")
JOIN totals USING ("municipality-name", "business-kind")
WHERE "municipality-name"='{muni_name}'
    '''
    rows = DF.Flow(
        DF.load('env://DATASETS_DATABASE_URL', query=QUERY, name='business_kind_licensing_items'),
        # DF.join_with_self('business_kind_licensing', ['business_kind'], dict(business_kind=None, business_licensing_item_id=dict(aggregate='set'))),
        # DF.printer(),
        DF.dump_to_path('out/business_kind_property_tax'),
    ).results()[0][0]
    all_kinds = sorted(set((-r['total'], r['business-kind']) for r in rows))
    all_kinds = [k[1] for k in all_kinds]
    for kind in all_kinds:
        kind_rows = [r for r in rows if r['business-kind'] == kind]
        zone_kinds = Counter(dict((r['property-tax-code-zone-kind'], r['count']) for r in kind_rows))
        zone_kind = zone_kinds.most_common(1)[0][0]
        zone_kind_rows = [r for r in kind_rows if r['property-tax-code-zone-kind'] == zone_kind]
        zone_ids = set(r['property-tax-code-zone-id'] for r in zone_kind_rows)
        zones = dict()
        for zone_id in zone_ids:
            zone_id_rows = [r for r in zone_kind_rows if r['property-tax-code-zone-id'] == zone_id]
            code_ids = Counter(dict((r['property-tax-code-id'], r['count']) for r in zone_id_rows))
            code_id = code_ids.most_common(1)[0][0]
            code_id_rows = [r for r in zone_id_rows if r['property-tax-code-id'] == code_id]
            zone_id = zone_id or ''
            rules = []
            areas = [(r['property-tax-code-min-area'], r['property-tax-code-max-area'], r['property-tax-code-rate']) for r in code_id_rows]
            areas = sorted(areas)
            max_paid = 0
            for area in areas:
                area = list(map(float, area))
                a = area[2]
                b = max_paid - a * area[0]
                max_paid = a * area[1] + b
                rule = dict(min_area=area[0], max_area=area[1], rate=area[2], a=a, b=b)
                rules.append(rule)
            zones[zone_id] = rules
        rec = dict(business_kind=kind, zone_kind=zone_kind, zone_rates=zones)
        yield rec

def property_tax(muni_name):
    DF.Flow(
        property_tax_rules(muni_name),
        DF.update_resource(-1, name='business_kind_property_tax_rules', path='business_kind_property_tax_rules.csv'),
        DF.dump_to_path('out/business_kind_property_tax_rules', format='json'),
    ).process()


def main(muni_name):
    licensing(muni_name)
    property_tax(muni_name)
    return DF.Flow(
        DF.load('out/business_kind_licensing_rules/datapackage.json'),
        DF.load('out/business_kind_property_tax_rules/datapackage.json'),
        DF.printer(),
        DF.duplicate('business_kind_licensing_rules', 'business_kind_licensing_rules_csv'),
        DF.duplicate('business_kind_property_tax_rules', 'business_kind_property_tax_rules_csv'),
        DF.update_resource('business_kind_licensing_rules_csv', path='business_kind_licensing_rules.csv'),
        DF.update_resource('business_kind_property_tax_rules_csv', path='business_kind_property_tax_rules.csv'),
    )
    

def operator(*args):
    for muni_name, muni_slug in MUNICIPALITIES:
        DF.Flow(
            main(muni_name),
            DF.update_package(name=f'{muni_slug}-business-kind-rules', title=f'מאפייני עסקים ב{muni_name}'),
            dump_to_ckan(
                os.environ['CKAN_HOST'],
                os.environ['CKAN_API_KEY'],
                'datacity',
                force_format=False,
            ),
        ).process()


if __name__ == '__main__':
    DF.Flow(
        main('באר שבע'),
        # DF.printer()
    ).process()