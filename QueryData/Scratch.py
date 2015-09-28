'''
Created on Jul 26, 2015

@author: shaunz
'''
from QueryData.InfluxQuery import InfluxDB
from pandas.core.config import option_context


influx = InfluxDB()
one = influx.query('cn_gdp_cum')
two = influx.interpret('anticum(cn_gdp_cum,Q)*100')

with option_context('display.max_rows', 9999, 'display.max_columns', 3):
        print one
        print two
'''
new_series = {}
for each in one.iterrows():
    if each[0].month != 1:
        prev_date = each[0] + DateOffset(months=-1)
        prev_date = prev_date.to_period('M').to_timestamp('M')
        monthly = each[1]['value']-one.loc[prev_date]
        new_series[each[0]] = monthly['value']
    else:
        new_series[each[0]] = each[1]['value']
     
Series(new_series)
''' 
