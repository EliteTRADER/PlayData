'''
Created on Jul 26, 2015

@author: shaunz
'''
from QueryData.InfluxQuery import InfluxDB
from pandas.core.config import option_context

influx = InfluxDB(db_name='Econ')
one = influx.query('cpi_us_household_fuel', db_name='Econ')
two = influx.query('ng_yoy')

with option_context('display.max_rows', 9999, 'display.max_columns', 3):
        print one
        print two