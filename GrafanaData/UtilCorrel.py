'''
Created on Jul 18, 2015

@author: shaunz
'''
from QueryData.InfluxQuery import InfluxDB
from pandas.stats.moments import rolling_corr
from pandas.tools.merge import concat
from pandas.core.common import isnull
from pandas.core.config import option_context

db = InfluxDB(db_name='Econ')
series_1 = db.interpret('cpi_us_food_away_home')
series_2 = db.interpret('wage_us_yoy')
series_1.rename(columns={'value':'value_1'}, inplace=True)
series_2.rename(columns={'value':'value_2'}, inplace=True)
periods = 60
analysis_type = 'history'

print series_1.tshift(2,freq='M')
if(analysis_type=='snapshot'):
    for i in range(-36, 37): # go back i periods
        shifted_1 = series_1.shift(i)
        combined = concat([shifted_1['value_1'],series_2['value_2']],axis=1,join='outer')
        combined = combined.loc[isnull(combined['value_1']) != True]
        combined = combined.loc[isnull(combined['value_2']) != True]
        results = rolling_corr(combined['value_1'],combined['value_2'],window=periods)
        print '%d,%.4f' % (i, results[-1:].values)
elif(analysis_type=='history'):
    shifted_1 = series_1.shift(-15)
    combined = concat([shifted_1['value_1'],series_2['value_2']],axis=1,join='outer')
    combined = combined.loc[isnull(combined['value_1']) != True]
    combined = combined.loc[isnull(combined['value_2']) != True]
    results = rolling_corr(combined['value_1'],combined['value_2'],window=periods)
    with option_context('display.max_rows', 9999, 'display.max_columns', 3):
        print results
else:
    print 'type %s not recognized' % analysis_type

