
import geocoder
import requests
import pyproj
import os
import logging

from dataflows import Flow, add_field, load, select_fields, set_type
from dgp.core.base_enricher import BaseEnricher, ColumnTypeTester
from dgp.config.consts import RESOURCE_NAME


class AddressFixer(BaseEnricher):

    def test(self):
        return True

    def combine_addresses(self, row):
        addresses = [
            str(row[field])
            for field in
            ('address-' + suffix
             for suffix in ('full', 'street', 'house-number', 'city'))
            if field in row and row.get(field)
        ]
        if len(addresses) > 0:
            row['address-full'] = ' '.join(addresses)
        return row

    def address_fixer(self):
        def func(package):
            address_fields = [
                f['name'].replace('address-', '')
                for f in package.pkg.descriptor['resources'][-1]['schema']['fields']
                if f['name'].startswith('address-')
            ]
            if len(address_fields) > 0 and 'full' not in address_fields:
                package.pkg.descriptor['resources'][-1]['schema']['fields'].append(dict(
                    name='address-full',
                    columnType='address:full',
                    type='string'
                ))
            yield package.pkg
            for i, res in enumerate(package):
                if i == len(package.pkg.resources) - 1:
                    yield (self.combine_addresses(r) for r in res)
                else:
                    yield res
        return func

    def postflow(self):
        return Flow(
            self.address_fixer(),
        )


class GeoCoder(ColumnTypeTester):

    REQUIRED_COLUMN_TYPES = ['address:full']
    PROHIBITED_COLUMN_TYPES = ['location:lat', 'location:lon']

    def prepare_address(self, address):
        if address:
            address = address.strip()
        if address:
            for prefix in ('שד', 'רח', 'רחוב'):
                if address.startswith(prefix + ' '):
                    address = address[len(prefix)+1:]
                    break
            return address
        return None

    def build_cache(self):
        ret = {}
        connection_str = os.environ.get('DATASETS_DATABASE_URL')
        if connection_str:
            from sqlalchemy import create_engine
            from sqlalchemy.engine import Engine
            engine: Engine = create_engine(connection_str)
            tables = ['schools', 'muni_businesses']
            for table in tables:
                try:
                    result = engine.execute(f'select "location-lat", "location-lon", "address-full" from {table}')
                    for row in result:
                        address = row['address-full']
                        address = self.prepare_address(address)
                        lat, lng = row['location-lat'], row['location-lon']
                        if address and lat and lng:
                            ret[address] = (lat, lng)
                except:
                    logging.exception('Failed to fill cache from %s', table)
        logging.info('GEOCODE CACHE CONTAINS %d RECORDS', len(ret))
        return ret

    def build_school_cache(self):
        results, _, _ = Flow(
            load('https://datacity-source-files.fra1.digitaloceanspaces.com/69-AlQasum/Education/Schools.xlsx', headers=1,
                 infer_strategy=load.INFER_STRINGS, cast_strategy=load.CAST_TO_STRINGS),
            select_fields(['SEMEL_MOSA', 'X', 'Y']),
            set_type('SEMEL_MOSA', type='string'),
            set_type('X', type='number'),
            set_type('Y', type='number'),
        ).results()
        ret = dict(
            (x['SEMEL_MOSA'], (x['X'], x['Y']))
            for x in results[0]
        )
        return ret
   
    def geocode(self):
        session = requests.Session()
        cache = self.build_cache()
        school_cache = self.build_school_cache()
        api_key = os.environ.get('GOOGLE_MAPS_API_KEY')

        def func(rows):
            count = 0
            for row in rows:
                address = row.get('address-full')
                address = self.prepare_address(address)
                if address:
                    if address in cache:
                        result = cache[address]
                    else:
                        result = None
                        if count < 1000:
                            if api_key:
                                g = geocoder.google(address, session=session, key=api_key, language='he')
                            else:
                                g = geocoder.osm(address, session=session, url='https://geocode.datacity.org.il/', language='he')
                            if g.ok and g.lat and g.lng:
                                result = (g.lat, g.lng)
                            cache[address] = result
                            count += 1
                    if result:
                        row['location-lat'], row['location-lon'] = result
                
                semel = row.get('school-symbol')
                if semel and semel in school_cache:
                    row['location-lat-ilgrid'], row['location-lon-ilgrid'] = school_cache[semel]
                yield row
            logging.info('GEOCODED %d RECORDS', count)

        return func

    def conditional(self):
        return Flow(
            add_field('location-lat', 'number',
                      resources=RESOURCE_NAME, columnType='location:lat'),
            add_field('location-lon', 'number',
                      resources=RESOURCE_NAME, columnType='location:lon'),
            self.geocode()
        )


class GeoProjection(ColumnTypeTester):

    REQUIRED_COLUMN_TYPES = ['location:lat:ilgrid', 'location:lon:ilgrid']
    PROHIBITED_COLUMN_TYPES = ['location:lat', 'location:lon']
    CRS = '+ellps=GRS80 +k=1.00007 +lat_0=31.73439361111111 +lon_0=35.20451694444445 +no_defs +proj=tmerc +units=m +x_0=219529.584 +y_0=626907.39'

    def project(self):
        projector = pyproj.Proj(self.CRS)

        def func(row):
            lat, lon = row.get('location-lat-ilgrid'), row.get('location-lon-ilgrid')
            if lat and lon:
                lon, lat = projector(lon, lat, inverse=True)
                row['location-lat'] = lat
                row['location-lon'] = lon
        return func

    def conditional(self):
        return Flow(
            add_field('location-lat', 'number',
                      resources=RESOURCE_NAME, columnType='location:lat'),
            add_field('location-lon', 'number',
                      resources=RESOURCE_NAME, columnType='location:lon'),
            self.project()
        )
