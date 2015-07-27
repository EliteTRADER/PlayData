'''
Created on Jul 9, 2015

@author: shaunz
'''

from InfluxQuery import InfluxDB
from pandas.core.common import isnull

influx = InfluxDB(db_name='FRED')
expression = '(avg(wti_spot,M)/lag(avg(wti_spot,M),12)-1)*100'
#x = influx._close_parentheses('lag(avg(wti_spot,M),12)-1)*100')
#print 'lag(avg(wti_spot,M),12)-1)*100'[:x+1]
results = influx.interpret(expression)
print results[-24:]