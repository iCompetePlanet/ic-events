#! /usr/bin/env python
# -*- coding: utf-8 -*-

""" PostgreSQL Initialization Module

.. moduleauthor:: Timothy Helton <tim@icompleteplanet.com>
"""

import datetime as dt
import functools
import gzip
import os
import shutil

import numpy as np
import psycopg2


__version__ = '1.1.0'


def status():
    """Decorator: Provide status for method execution."""
    def decorator(func):
        @functools.wraps(func)
        def wrapped(*args, **kwargs):

            try:
                print('\nExecute: {}'.format(func.__name__))
                func(*args, **kwargs)
            finally:
                print('Complete: {}\n'.format(func.__name__))

        return wrapped
    return decorator


class PostgresInitialize:
    """Initialize the PostgreSQL data base.

    :Attributes:

        - **city**: *ndarray* all USA city names corresponding to every postal \
            code
        - **conn**: *psycopg2.extensions.connection* database connection object
        - **country**: *ndarray* two letter abbreviation and full name for \
            every country in the world
        - **csv_file**: *str* name of csv file containing location data
        - **csv_zipped**: *bool* if csv file is gzip compressed then True else \
            False (default: True)
        - **cur**: *psycopg2.extensions.cursor* database cursor object
        - **db_name**: *str* name of PostgreSQL database (default: iCP_events)
        - **db_pwd**: *str* password for PostgreSQL database user
        - **db_user**: *str* name of PostgreSQL database user
        - **latitude**: *ndarray* all USA latitude values corresponding to \
            every postal code
        - **location_data**: *ndarray* composite of all USA cities, states, \
            latitude and longitude values for every corresponding postal code
        - **longitude**: *ndarray* all USA longitude values corresponding to \
            every postal code
        - **state**: *ndarray* all USA states
        - **postal_code**: *ndarray* all USA postal codes
        - **time**: *list* times for a nominal day spaced every five minutes
    """
    def __init__(self):
        self.city = None
        self.conn = None
        self.country = None
        self.csv_file = None
        self.csv_zipped = True
        self.cur = None
        self.db_name = 'iCP_events'
        self.db_pwd = None
        self.db_user = None
        self.latitude = None
        self.location_data = None
        self.longitude = None
        self.state = None
        self.postal_code = None
        self.time = None

    def table_select(self, table_name, return_field='*', search_field=None,
                     search_value=None):
        """Return values from a Postgres table with a given search value.

        :param str table_name: name of table to search
        :param str return_field: field to return from table query (default: * \
            will return all table fields)
        :param str search_field: field to search for value in table \
            (default: None will return all table values)
        :returns: value corresponding to requested field
        :rtype: str
        """
        base_cmd = 'SELECT {} FROM {}'.format(return_field, table_name)

        if search_field:
            search_cmd = 'WHERE {}=%s'.format(search_field)
            cmd = '{} {};'.format(base_cmd, search_cmd)
            self.cur.execute(cmd, (search_value, ))
        else:
            self.cur.execute('{};'.format(base_cmd))

        return self.cur.fetchall()

    @staticmethod
    def table_insert(name, field_names):
        """Return command to add a record into a PostgreSQL database.

        :param str name: name of table to append
        :param field_names: names of fields
        :type: str or list
        :return: command to append a record to a table
        :rtype: str
        """
        if isinstance(field_names, str):
            field_names = [field_names]

        length = len(field_names)
        if length > 1:
            values = ','.join(['%s'] * length)
        else:
            values = '%s'

        return '''INSERT INTO {table_name} ({fields})
                  VALUES ({values});'''.format(table_name=name,
                                               fields=', '.join(field_names),
                                               values=values)

    def get_password(self):
        """Get database user password."""
        self.db_pwd = input('Enter database password: ')

    def get_user(self):
        """Get database user name."""
        self.db_user = input('Enter database user name: ')
        if not self.db_user:
            self.db_user = 'Tim'

    def initiate_db(self):
        """Populate the PostgreSQL database."""
        self.load_countries()
        self.load_locations()
        self.load_times()

        self.get_user()
        self.get_password()

        self.psql_connection()
        self.psql_tables()
        self.psql_city()
        self.psql_country()
        self.psql_latitude()
        self.psql_longitude()
        self.psql_postal_code()
        self.psql_state()
        self.psql_times()
        self.psql_address()
        self.conn.commit()

    @status()
    def load_countries(self):
        """Load countries from self.csv file."""
        self.csv_file = 'countries.csv'
        self.unzip()
        country = np.genfromtxt(self.csv_file, dtype=[('abbr', 'U2'),
                                                      ('name', 'U50')],
                                delimiter=', ', usecols=[0, 1])
        self.zip_file()
        self.country = country.view(np.recarray)

    @status()
    def load_locations(self):
        """Load location data from self.csv file."""
        self.csv_file = 'us_postal_codes.csv'
        self.unzip()
        loc = np.genfromtxt(self.csv_file, dtype=[('postal_code', 'U5'),
                                                  ('city', 'U30'),
                                                  ('state', 'U2'),
                                                  ('latitude', 'U9'),
                                                  ('longitude', 'U9')],
                            delimiter=',', skip_header=1,
                            usecols=[0, 1, 3, 5, 6])
        self.zip_file()

        self.location_data = loc.view(np.recarray)
        self.city = np.unique(self.location_data.city)
        self.state = np.unique(self.location_data.state)
        self.postal_code = np.unique(self.location_data.postal_code)
        self.latitude = np.unique(self.location_data.latitude)
        self.longitude = np.unique(self.location_data.longitude)

    @status()
    def load_times(self):
        """Create list of times for a day spaced five minutes apart"""
        day = dt.datetime(1, 1, 1)
        delta = dt.timedelta(minutes=5)
        day_time = [day + delta * x for x in range(288)]
        self.time = [x.time() for x in day_time]

    def psql_connection(self):
        """Connect to PostgreSQL database and establish a cursor."""
        self.conn = psycopg2.connect(database=self.db_name, user=self.db_user,
                                     password=self.db_pwd)
        self.cur = self.conn.cursor()

    @status()
    def psql_tables(self):
        """Create PostgreSQL tables."""
        def table_create(name, schema, serial=False, unique=None):
            """Return command to create a table in a PostgreSQL database.

            :param str name: name of table
            :param list schema: schema of table provided in name data type \
                pairs [(n_1, dt_1), (n_2, dt_2]
            :param bool serial: a serialized index will be created for the \
                table and used as the primary key if True
            :param list unique: field names that define a unique record for \
                the table
            :return: command to create a table
            :rtype: str
            """
            base_cmd = 'CREATE TABLE {name} ('.format(name=name)

            if serial:
                serial_cmd = 'id SERIAL UNIQUE NOT NULL PRIMARY KEY,'
            else:
                serial_cmd = ''

            schema_cmd = ', '.join(schema)

            if unique:
                unique_cmd = ',UNIQUE ({field})'.format(field=', '.join(unique))
            else:
                unique_cmd = ''

            return '{base}{serial}{schema}{unique});'.format(base=base_cmd,
                                                             serial=serial_cmd,
                                                             schema=schema_cmd,
                                                             unique=unique_cmd)

        def table_drop(name):
            """Return command to drop a table from a PostgreSQL database.

            :param str name: name of table to drop
            :returns: command to remove table from database
            :rtype: str
            """
            return 'DROP TABLE if EXISTS {name} CASCADE;'.format(name=name)

        tables = {'city': ['value TEXT'],
                  'country': ['value CHAR(2)', 'name TEXT'],
                  'event': ['value TEXT'],
                  'event_date': ['value DATE'],
                  'event_time': ['value TIME'],
                  'latitude': ['value DOUBLE PRECISION'],
                  'longitude': ['value DOUBLE PRECISION'],
                  'postal_code': ['value CHAR(5)'],
                  'state': ['value CHAR(2)'],
                  'url': ['value TEXT']}

        composite = [('event', 'INTEGER'),
                     ('city', 'INTEGER'),
                     ('state', 'INTEGER'),
                     ('postal_code', 'INTEGER'),
                     ('country', 'INTEGER'),
                     ('latitude', 'INTEGER'),
                     ('longitude', 'INTEGER'),
                     ('event_date', 'INTEGER'),
                     ('event_time', 'INTEGER'),
                     ('url', 'INTEGER')]

        # Serial Tables
        [self.cur.execute(table_drop(x)) for x in tables]
        [self.cur.execute(table_create(x, tables[x], serial=True))
         for x in tables]

        # Composite Tables
        [self.cur.execute(table_drop(x)) for x in ['address', 'events']]
        self.cur.execute(table_create('address',
                                      [' '.join(x) for x in composite
                                       if x[0] in ('city', 'state',
                                                   'postal_code', 'latitude',
                                                   'longitude')],
                                      unique= ['city', 'state', 'postal_code']))
        self.cur.execute(table_create('events',
                                      [' '.join(x) for x in composite],
                                      unique=['event', 'city', 'postal_code',
                                              'event_date']))

    @status()
    def psql_address(self):
        """Populate composite PostreSQL address table."""
        def idx_sub(name, translate):
            """Replace record array strings with database index values.

            :param str name: name of field to modify
            :param list translate: list of tuples to define replace values \
                with new values (new, replace)
            """
            nonlocal addresses
            field = eval('addresses.{}'.format(name))
            mask = eval('self.location_data.{}'.format(name))

            print('   {}'.format(name))
            for values in self.table_select(name):
                field[mask == str(values[1])] = values[0]

        fields = ['postal_code', 'city', 'state', 'latitude', 'longitude']
        addresses = np.recarray(self.location_data.shape,
                                dtype=list(zip(fields, ['U6'] * len(fields))))

        for field in fields:
            idx_sub(field, self.table_select(field))

        for a in addresses:
            self.cur.execute(self.table_insert('address', fields), a)

        # for location in self.location_data:
            # city_id = self.table_find_id('city', 'value', location.city)
            # state_id = self.table_find_id('state', 'value', location.state)
            # postal_code_id = self.table_find_id('postal_code', 'value',
                                                # location.postal_code)
            # latitude_id = self.table_find_id('latitude', 'value',
                                             # location.latitude)
            # longitude_id = self.table_find_id('longitude', 'value',
                                              # location.longitude)
            # field_ids = (city_id, state_id, postal_code_id, latitude_id,
                         # longitude_id)

            # self.cur.execute(self.table_insert('address', fields), field_ids)

    @status()
    def psql_city(self):
        """Populate PostgreSQL city table."""
        [self.cur.execute(self.table_insert('city', 'value'), (x, ))
         for x in self.city]

    @status()
    def psql_country(self):
        """Populate PostgreSQL country table."""
        [self.cur.execute(self.table_insert('country', ['value', 'name']),
                          (x, y)) for (x, y) in self.country]

    @status()
    def psql_latitude(self):
        """Populate PostgreSQL latitude table."""
        [self.cur.execute(self.table_insert('latitude', 'value'), (x, ))
         for x in self.latitude]

    @status()
    def psql_longitude(self):
        """Populate PostgreSQL longitude table."""
        [self.cur.execute(self.table_insert('longitude', 'value'), (x, ))
         for x in self.longitude]

    @status()
    def psql_postal_code(self):
        """Populate PostgreSQL postal code table."""
        [self.cur.execute(self.table_insert('postal_code', 'value'), (x, ))
         for x in self.postal_code]

    @status()
    def psql_state(self):
        """Populate PostgreSQL state table."""
        [self.cur.execute(self.table_insert('state', 'value'), (x, ))
         for x in self.state]

    @status()
    def psql_times(self):
        """Populate PostgreSQL times table."""
        [self.cur.execute(self.table_insert('event_time', 'value'), (x, ))
         for x in self.time]

    def unzip(self):
        """Unzip csv file if compression attribute csv_zipped is True."""
        if self.csv_zipped:
            with gzip.open('{}.gz'.format(self.csv_file), 'r') as f:
                csv_data = f.read()
                csv_data = csv_data.decode().replace('\r', '')
            with open(self.csv_file, 'w') as f:
                f.write(csv_data)

    def zip_file(self):
        """Zip csv file after data has been extracted."""
        if not self.csv_zipped:
            with open(self.csv_file, 'rb') as f_in:
                with gzip.open('{}.gz'.format(self.csv_file), 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

        os.remove(self.csv_file)

