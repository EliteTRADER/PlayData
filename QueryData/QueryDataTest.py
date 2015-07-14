'''
Created on Jul 9, 2015

@author: shaunz
'''

from InfluxQuery import InfluxDB

influx = InfluxDB(db_name='Quandl')
results = influx.interpret('cpi_us*2+5')
print results[0][:5]
