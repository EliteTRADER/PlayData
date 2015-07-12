'''
Created on Jul 9, 2015

@author: shaunz
'''

from InfluxQuery import InfluxDB

influx = InfluxDB(db_name='FRED')
influx.interpret('cpi_us*2+1.5*(wage_us_hours+2)')