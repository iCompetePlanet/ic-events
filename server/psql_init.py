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


__version__ = '1.0.0'


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

    @staticmethod
    def cmd_double_field(table, field_1, field_2):
        """Return command for adding a unique two field row entry to PostgreSQL.

        :param table: name of table to append
        :param field_1: name of first field to define
        :param field_2: name of second field to define
        :return: command to append a two field row to a table
        :rtype: str
        """
        return '''INSERT INTO {table_name} ({field_1}, {field_2})
                  SELECT %s, %s WHERE NOT EXISTS (
                      SELECT {field_1} FROM {table_name} WHERE
                      {field_1}=%s AND {field_2}=%s)
                  '''.format(table_name=table, field_1=field_1, field_2=field_2)

    @staticmethod
    def cmd_single_field(table, field):
        """Return command for adding a unique entry to a PostgreSQL table.

        :param str table: name of table to append
        :param str field: name of table field to define
        :returns: command to append a single field to a table
        :rtype: str
        """
        return '''INSERT INTO {table_name} ({field_name})
                  SELECT %s WHERE NOT EXISTS (
                      SELECT {field_name} FROM {table_name} WHERE
                          {field_name}=%s)'''.format(table_name=table,
                                                     field_name=field)

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

        self.conn.commit()

    @status()
    def load_countries(self):
        """Load countries from self.csv file."""
        self.csv_file = 'countries.csv'
        self.unzip()
        country = np.genfromtxt(self.csv_file, dtype=[('abbr', 'U2'),
                                                      ('name', 'U50')],
                                delimiter=',', usecols=[0, 1])
        self.country = country.view(np.recarray)

    @status()
    def load_locations(self):
        """Load location data from self.csv file."""
        self.csv_file = 'us_postal_codes.csv'
        self.unzip()
        loc = np.genfromtxt(self.csv_file, dtype=[('postal_code', 'U5'),
                                                  ('city', 'U30'),
                                                  ('state', 'U2'),
                                                  ('lat', 'f8'), ('lon', 'f8')],
                            delimiter=',', skip_header=1,
                            usecols=[0, 1, 3, 5, 6])

        self.location_data = loc.view(np.recarray)
        self.city = np.unique(self.location_data.city)
        self.state = np.unique(self.location_data.state)
        self.postal_code = np.unique(self.location_data.postal_code)
        self.latitude = np.unique(self.location_data.lat)
        self.longitude = np.unique(self.location_data.lon)

    @status()
    def load_times(self):
        """Create list of times for a day spaced five minutes apart"""
        day = dt.datetime(1, 1, 1)
        delta = dt.timedelta(minutes=5)
        day_time = [day + delta * x for x in range(288)]
        self.time = [x.time() for x in day_time]

    def psql_connection(self):
        """Connect to PostgreSQL database and establish a curser."""
        self.conn = psycopg2.connect(database=self.db_name, user=self.db_user,
                                     password=self.db_pwd)
        self.cur = self.conn.cursor()

    @status()
    def psql_tables(self):
        """Create PostgreSQL tables."""
        def cmd_create_serial_table(table_name, fields):
            """Return command to create a serial table in a PostgreSQL database.

            :param str table_name: name of new table
            :param list fields: name data type pairs ['n_1 dt_1', 'n_2 dt_2']
            :returns: command to create a serial table
            :rtype: str
            """
            return '''CREATE TABLE {table_name} (
                      id SERIAL UNIQUE NOT NULL PRIMARY KEY, {fields});
                      '''.format(table_name=table_name,
                                 fields=', '.join(fields))

        def cmd_drop_table(table_name):
            """Return command for dropping a table from a PostgreSQL database.

            :param str table_name: name of table to drop
            :returns: command to remove table from database
            :rtype: str
            """
            return 'DROP TABLE if EXISTS {} CASCADE;'.format(table_name)

        tables = {'city': ['city_name TEXT'],
                  'country': ['country_abbr CHAR(2)', 'country_name TEXT'],
                  'dates': ['dates_value DATE'],
                  'events': ['event_name TEXT'],
                  'latitude': ['latitude_value REAL'],
                  'longitude': ['longitude_value REAL'],
                  'postal_code': ['postal_code_value CHAR(5)'],
                  'state': ['state_name CHAR(2)'],
                  'times': ['time_value TIME'],
                  'url': ['url_name TEXT']}

        composite = ['event INTEGER',
                     'city INTEGER',
                     'state INTEGER',
                     'postal_code INTEGER',
                     'country INTEGER',
                     'latitude INTEGER',
                     'longitude INTEGER',
                     'dates INTEGER',
                     'times INTEGER',
                     'url INTEGER']

        [self.cur.execute(cmd_drop_table(x)) for x in tables]
        [self.cur.execute(cmd_create_serial_table(x, tables[x]))
         for x in tables]

        self.cur.execute(cmd_drop_table('event'))
        self.cur.execute('''CREATE TABLE {table_name} (
                            {fields});'''.format(table_name='event',
                                                 fields=', '.join(composite)))

    @status()
    def psql_city(self):
        """Populate PostgreSQL city table."""
        cmd = self.cmd_single_field('city', 'city_name')
        [self.cur.execute(cmd, (x, x)) for x in self.city]

    @status()
    def psql_country(self):
        """Populate PostgreSQL country table."""
        cmd = self.cmd_double_field('country', 'country_abbr', 'country_name')
        [self.cur.execute(cmd, (x.abbr, x.name, x.abbr, x.name))
         for x in self.country]

    @status()
    def psql_latitude(self):
        """Populate PostgreSQL latitude table."""
        cmd = self.cmd_single_field('latitude', 'latitude_value')
        [self.cur.execute(cmd, (x, x)) for x in self.latitude]

    @status()
    def psql_longitude(self):
        """Populate PostgreSQL longitude table."""
        cmd = self.cmd_single_field('longitude', 'longitude_value')
        [self.cur.execute(cmd, (x, x)) for x in self.longitude]

    @status()
    def psql_postal_code(self):
        """Populate PostgreSQL postal code table."""
        cmd = self.cmd_single_field('postal_code', 'postal_code_value')
        [self.cur.execute(cmd, (x, x)) for x in self.postal_code]

    @status()
    def psql_state(self):
        """Populate PostgreSQL state table."""
        cmd = self.cmd_single_field('state', 'state_name')
        [self.cur.execute(cmd, (x, x)) for x in self.state]

    @status()
    def psql_times(self):
        """Populate PostgreSQL times table."""
        cmd = self.cmd_single_field('times', 'time_value')
        [self.cur.execute(cmd, (x, x)) for x in self.time]

    def unzip(self):
        """Unzip csv file if compression attribute csv_zipped is True."""
        if self.csv_zipped:
            with gzip.open('{}.gz'.format(self.csv_file), 'r') as f:
                csv_data = f.read()
                csv_data = csv_data.decode().replace('\r', '')
            with open(self.csv_file, 'w') as f:
                f.write(csv_data)

    def zip(self):
        """Zip csv file after data has been extracted."""
        if not self.csv_zipped:
            with open(self.csv_file, 'rb') as f_in:
                with gzip.open('{}.gz'.format(self.csv_file), 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

        os.remove(self.csv_file)
