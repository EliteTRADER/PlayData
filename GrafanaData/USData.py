'''
Created on Jul 18, 2015

@author: shaunz
'''
from QueryData.InfluxQuery import InfluxDB
from influxdb.influxdb08 import DataFrameClient
from pandas import DataFrame
import timeit

influx_Fred = InfluxDB(db_name='FRED')

interest_list = [
                 [ 'wage_us_yoy',   '(wage_us_weekly_nonsupervisory/lag(wage_us_weekly_nonsupervisory,12)-1)*100' ],
                 [ 'cpi_us_yoy',    '(cpi_us/lag(cpi_us,12)-1)*100' ],
                 [ 'cpi_us_food_away_home', '(cpi_us_food_away_home/lag(cpi_us_food_away_home,12)-1)*100' ],
                 [ 'cpi_us_food_at_home', '(cpi_us_food_at_home/lag(cpi_us_food_at_home,12)-1)*100' ],
                 ]

processed_list = [
                  ['wage_lag_15m', 'mlag(wage_us_yoy,15)' ],
                  ]

df = DataFrameClient('localhost',8086,'root','root')
if({'name':'Econ'} not in df.get_list_database()):
    df.create_database('Econ')
df.switch_database('Econ')

#add all items in interested list
start = timeit.default_timer()
for item in interest_list:
    if item[0] in df.get_list_series():
        df.delete_series(item[0])
    results = influx_Fred.interpret(item[1])
    results = results.replace(to_replace='NaN',value='.')
    results = DataFrame({'value':results['value']})
    df.write_points({item[0]:results})
    print item
print 'total time in seconds: %.2f' % (timeit.default_timer() - start)

#add all items in processed list
start = timeit.default_timer()
influx_Econ = InfluxDB(db_name='Econ')
for item in processed_list:
    if item[0] in df.get_list_series():
        df.delete_series(item[0])
    results = influx_Econ.interpret(item[1])
    results = results.replace(to_replace='NaN',value='.')
    results = DataFrame({'value':results['value']})
    df.write_points({item[0]:results})
    print item
print 'total time in seconds: %.2f' % (timeit.default_timer() - start)


