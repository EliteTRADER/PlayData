'''
Created on Jul 27, 2015

@author: shaunz
'''
from QueryData.InfluxQuery import InfluxDB
from influxdb.influxdb08 import DataFrameClient
from pandas import DataFrame
import timeit

influx = InfluxDB()

interest_list = [
                 ['cn_net_export', '(cn_export_total-cn_import_total)/1000'],
                 ['cn_fdi_monthly', 'anticum(cn_fdi_cum,M)'],
                 ['cn_gdp_quarterly', 'anticum(cn_gdp_cum,Q)*100'],
                 ]

processed_list = [
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
    results = influx.interpret(item[1])
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

