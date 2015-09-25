'''
Created on Sep 22, 2015

@author: shaunz
'''
from QueryData.InfluxQuery import InfluxDB
import plotly.plotly as py
from plotly.graph_objs import Data,Scatter


class PlotlyAPI(object):
    
    def __init__(self):
        self.influx = InfluxDB()
    
    def _create_scatter(self,expression,mode):
        try:
            axis_separate = expression.index(':')
            axis_label = expression[:axis_separate]
            expression = expression[axis_separate+1:]
        except:
            axis_label = 'y1'
            
        try:
            name_separate = expression.index(';')
            legend = expression[name_separate+1:]
        except:
            name_separate = len(expression)
            legend = expression
        result = self.influx.interpret(expression[:name_separate])
        x = result.index.to_pydatetime()
        y = result['value'].get_values()
        return Scatter(x=x,y=y,mode=mode,name=legend)
    
    def _create_ts(self,exp_list,title,mode='lines'):
        scatters = []
        for exp in exp_list:
            scatters.append(self._create_scatter(exp,mode))
        data = Data(scatters)
        py.plot(data, filename=title)