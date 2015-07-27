'''
Created on Jul 18, 2015

@author: shaunz
'''
from QueryData.InfluxQuery import InfluxDB
from influxdb.influxdb08 import DataFrameClient
from pandas import DataFrame
import timeit

influx_Fred = InfluxDB()

interest_list = [
                 [ 'cpi_us_yoy', '(cpi_us/lag(cpi_us,12)-1)*100' ],
                 [ 'cpi_us_food_away_home_yoy', '(cpi_us_food_away_home/lag(cpi_us_food_away_home,12)-1)*100' ],
                 [ 'cpi_us_food_at_home_yoy', '(cpi_us_food_at_home/lag(cpi_us_food_at_home,12)-1)*100' ],
                 [ 'cpi_us_household_fuel_yoy', '(cpi_us_household_fuel/lag(cpi_us_household_fuel,12)-1)*100' ],
                 [ 'cpi_us_motor_fuel_yoy', '(cpi_us_motor_fuel/lag(cpi_us_motor_fuel,12)-1)*100' ],
                 [ 'cpi_us_owners_equiv_yoy', '(cpi_us_owners_equivalent/lag(cpi_us_owners_equivalent,12)-1)*100' ],
                 [ 'cpi_us_rent_yoy', '(cpi_us_rent/lag(cpi_us_rent,12)-1)*100' ],
                 [ 'cpi_us_household_fuel_yoy', '(cpi_us_household_fuel/lag(cpi_us_household_fuel,12)-1)*100' ],
                 [ 'cpi_us_medical_service_yoy', '(cpi_us_medical_service/lag(cpi_us_medical_service,12)-1)*100' ],
                 [ 'cpi_us_medical_commod_yoy', '(cpi_us_medical_commod/lag(cpi_us_medical_commod,12)-1)*100' ],
                 [ 'cpi_us_apparel_yoy', '(cpi_us_apparel/lag(cpi_us_apparel,12)-1)*100' ],
                 [ 'cpi_us_motor_yoy', '(cpi_us_motor/lag(cpi_us_motor,12)-1)*100' ],
                 [ 'cpi_us_education_yoy', '(cpi_us_education/lag(cpi_us_education,12)-1)*100' ],
                 [ 'cpi_us_communication_yoy', '(cpi_us_communication/lag(cpi_us_communication,12)-1)*100' ],
                 
                 [ 'wage_us_yoy', '(wage_us_weekly_nonsupervisory/lag(wage_us_weekly_nonsupervisory,12)-1)*100' ],
                 [ 'wti_yoy', '(avg(wti_spot,M)/lag(avg(wti_spot,M),12)-1)*100' ],
                 [ 'zillow_median_sale_price_yoy', '(zillow_median_sale_price/lag(zillow_median_sale_price,12)-1)*100' ],
                 [ 'ng_yoy', '(ng_hh_spot/lag(ng_hh_spot,12)-1)*100' ],
                 [ 'gdp_us_yoy', '(gdp_us/lag(gdp_us,4)-1)*100' ],
                 ]

processed_list = [
                  [ 'wage_lag_15m', 'mlag(wage_us_yoy,15)' ],
                  [ 'zillow_median_sale_price_15m', 'mlag(zillow_median_sale_price_yoy,15)' ],
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


