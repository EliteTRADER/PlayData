'''
Created on Jul 9, 2015

@author: shaunz
'''

from InfluxQuery import InfluxDB

influx = InfluxDB(db_name='Quandl')
expression = '(cpi_us/lag(cpi_us,12)-1)*100'
results = influx.interpret(expression)
shifted_results = results.tshift(6,freq='M').tshift(1,freq='D')
print results[-10:]
print shifted_results[-10:]