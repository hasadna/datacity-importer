import re
import requests
import dataflows as DF


def convert_simple(field):
    IGNORE_WORDS = [
        'למעט לעניין אריזתו',
        'לעניין תכשירים לשימוש וטרינרי בלבד למעט תרופות',
        'לעניין מסעדה המיועדת ל-200 איש ויותר או לעניין הגשת משקאות משכרים לצריכה במקום',
        'לעניין מזנון, בית קפה או בית אוכל אחר, המיועדים ל-200 איש ויותר או לעניין הגשת משקאות משכרים לצריכה במקום',
        '(לעניין פסדים בלבד)',
        'רק מקום שבו הכניסה בתשלום',
        '(לעניין מכירתם בלבד)',
        '''ת' – מקום המיועד ל-50 איש לכל היותר ששטחו עד 150 מ"ר לרבות סגירת חורף''',
        '''ת' מקום המיועד ל-50 איש לכל היותר ששטחו עד 150 מ"ר לרבות סגירת חורף''',
        '''ת' – אחסנה של חומרים שאינם חומרים מסוכנים במקום ששטחו עד 200 מ"ר וגובהו עד 3.70 מטר''',
        'עד 2 קומות ועד 16 מיטות סך הכול',
        'עד 6 חדרים סך הכול בכל יחידות האירוח',
        'בשטח פתוח',
        'מקום המיועד ל-50 איש לכל היותר',
    ]
    FIRE_DEPT_RULE = re.compile('ת.+עד ([\d,]+) מ.ר', re.MULTILINE)

    def func(row):
        v = row.get(field) or ''
        for word in IGNORE_WORDS:
            v = v.replace(word, '')
        if field == 'fire-dept':
            m = FIRE_DEPT_RULE.search(v)
            rules = [dict(area=0, firedpt_track='inspection')]
            if m:
                rules = [
                    dict(area=0, firedpt_track='deposition'),
                    dict(area=int(m.group(1).replace(',', '')), firedpt_track='inspection')
                ]
                v = FIRE_DEPT_RULE.sub('', v)
            v = v.strip()
            if v.endswith("ת'") or v.endswith("ת׳"):
                rules = [dict(area=0, firedpt_track='deposition')]
                v = v[:-2]
            cur = row['rules'] or list()
            row['rules'] = cur + rules
        v = v.strip()
        ret = ''
        if v == '+':
            ret = 'yes'
        elif v == '[+]':
            ret = 'attn'
        elif not v:
            ret = 'no'
        else:
            print('BAD VALUE for {}: {}'.format(field, v))
        row[field] = ret
    return func


def canonize_id():
    ID_RE = re.compile('([\d.]+)\s?([א-ת]*)')
    def func(row):
        row['orig_id'] = row['id']
        m = ID_RE.match(row['id'])
        assert m is not None
        parts = m.group(1).split('.')
        article = m.group(2)
        if article:
            ofs = 0
            if len(article) > 1:
                assert article[0] == 'י'
                ofs = 10
                article = article[1:]
            article = ord(article) - ord('א') + 1 + ofs
            article = str(article)
            parts.append(article)
        row['id'] = '.'.join(parts)

    return DF.Flow(
        DF.add_field('orig_id', 'string', ''),
        func,
    )

def tracks():
    REPLACEMENTS = {
        '''א' – מקום המיועד ל-50 איש לכל היותר ששטחו עד 150 מ"ר לרבות סגירת חורף; ב' – כל מקום שאינו בא בגדר האמור''':
            "ב'",
        '''ר"ת – אחסנה של חומרים שאינם חומרים מסוכנים במקום ששטחו עד 200 מ"ר וגובהו עד 3.70 מטר''': '',

    }
    TRACK_RE = re.compile('''^((א')|(ב')|(ר.ת))([– ]+(עד|מעל) (\d+) מ.ר[\s;]*)?''')
    def func(row):
        track = row.get('track') or ''
        track = track.strip()
        for k, v in REPLACEMENTS.items():
            track = track.replace(k, v)
        orig_track = track
        rules = []
        if track:
            prev_area = 0
            while len(track) > 0:
                track = track.strip()
                # print('TTT', track)
                m = TRACK_RE.match(track)
                if m is None:
                    print('bad track: {} <<{}>>'.format(track, orig_track))
                    break
                else:
                    # print('RRR', track, '->', '::'.join(map(str, m.groups())))
                    t = m.group(2) or m.group(3) or m.group(4)
                    t = {
                        'ר"ת': 'deposition',
                        "א'": 'a',
                        "ב'" : 'b',
                    }[t]
                    rel = m.group(6)
                    area = m.group(7)
                    if area and rel:
                        area = int(area)
                        if rel == 'עד':
                            prev_area, area = area, prev_area
                        else:
                            prev_area = area
                        rules.append(dict(area=area, track=t))
                    else:
                        rules.append(dict(area=0, track=t))
                    track = track[m.end():]
        else:
            rules.append(dict(area=0, track='full'))
        row['rules'] = (row.get('rules') or list()) + rules

    return func


def rules():
    def func(rows):
        for row in rows:
            areas = sorted(set(r['area'] for r in row['rules']))
            assert len(areas) > 0
            min_area = None if row['id'] not in ('6.2', '7.5') else 800
            if min_area is not None:
                areas = [min_area] + [a for a in areas if a > min_area]
                # yield {'id': row['id'], 'area': 0}
            rule = {}
            for area in areas:
                for r in row['rules']:
                    if r['area'] == area or (min_area and area <= min_area):
                        rule.update(r)
                _row = dict()
                _row.update(row)
                _row.update(rule)
                yield _row
    return DF.Flow(
        DF.add_field('area', 'integer', -1),
        DF.add_field('firedpt_track', 'string', ''),
        func,
        DF.delete_fields(['rules']),
    )

def scrape_specs():
    URL = 'https://www.gov.il/he/api/DynamicCollector'
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:95.0) Gecko/20100101 Firefox/95.0',
    }

    offset = 0
    results = []
    while True:
        BODY = {
            'DynamicTemplateID': '27223a53-573c-470f-a942-89af62748442',
            'From': offset,
            'QueryFilters': {
                'skip': {
                    'Query': offset,
                },
            }
        }
        r = requests.post(URL, json=BODY, headers=HEADERS).json()['Results']
        if not r or len(r) == 0:
            break
        results.extend(r)
        offset += len(r)
    print('got {} results'.format(len(results)))
    for r in results:
        data = r['Data']
        id_parts = list(map(int, data['order'].split('-')))
        if id_parts[-1] == 0:
            id_parts = id_parts[:-1]
        row = dict(
            id='.'.join(map(str, id_parts)),
            link=data['reform_link']['URL'],
        )
        yield row


def main():
    URL = 'https://www.nevo.co.il/law_html/law01/500_849.htm'
    LICENSING_ITEM_RE = re.compile('\d[.\d]+\s*[א-ת]?')
    headers = ['id', 'itemname', 'environment', 'police', 'ordinance', 'agriculture', 'sanitation', 'fire-dept', 'track', 'spec', 'duration', 'notes']
    specs = DF.Flow(
        scrape_specs(),
        DF.checkpoint('uniform-specs'),
    ).results()[0][0]
    spec_links = dict(
        (s['id'], s['link']) for s in specs
    )
    return DF.Flow(
        DF.load(URL, name='law', selector='table[border="1"]', format='html', headers=headers),
        DF.checkpoint('law'),
        DF.set_type('duration', type='string', transform=str),
        DF.filter_rows(lambda row: row.get('itemname') is not None),
        DF.filter_rows(lambda row: LICENSING_ITEM_RE.match(row['id']) is not None),
        DF.filter_rows(lambda row: row['id'] != '6.9 ב'),  # רוכלות בעסק הטעון רישוי לפי פרט אחר בתוספת זו
        DF.add_field('rules', 'array', None),
        convert_simple('environment'),
        convert_simple('police'),
        convert_simple('ordinance'),
        convert_simple('agriculture'),
        convert_simple('sanitation'),
        convert_simple('fire-dept'),
        canonize_id(),
        tracks(),
        rules(),
        DF.set_type('spec', type='boolean', transform=lambda _, row: row['id'] in spec_links),
        DF.add_field('spec_link', 'string', default=lambda row: spec_links.get(row['id'])),
        DF.delete_fields(['notes']),
        DF.update_resource(-1, name='business_licensing_law'),
    )

def operator(*args):
    DF.Flow(
        main(),
        DF.dump_to_sql(
            dict(
                business_licensing_law={
                    'resource-name': 'business_licensing_law',
                }
            ), engine='env://DATASETS_DATABASE_URL'
        )
    ).process()

if __name__ == '__main__':
    DF.Flow(
        main(),
        DF.filter_rows(lambda row: row['id'] in ('6.2', '7.5')),
        DF.printer()
    ).process()